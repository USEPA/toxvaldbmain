#-------------------------------------------------------------------------------------
#' Load PPRTV (NCEA) from toxval source to toxval
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source.db The version of toxval_source from which the tables are loaded.
#' @param log If TRUE, send output to a log file
#' @export
#--------------------------------------------------------------------------------------
toxval.load.pprtv.ncea <- function(toxval.db, source.db, log=F){
  source <- "PPRTV (NCEA)"
  source_table = "source_pprtv_ncea"
  verbose = log
  #####################################################################
  cat("start output log, log files for each source can be accessed from output_log folder\n")
  #####################################################################
  if(log) {
    con1 = file.path(toxval.config()$datapath,paste0(source,"_",Sys.Date(),".log"))
    con1 = log_open(con1)
    con = file(paste0(toxval.config()$datapath,source,"_",Sys.Date(),".log"))
    sink(con, append=TRUE)
    sink(con, append=TRUE, type="message")
  }
  #####################################################################
  cat("clean source_info by source\n")
  #####################################################################
  import.source.info.by.source(toxval.db, source)
  #####################################################################
  cat("clean by source\n")
  #####################################################################
  clean.toxval.by.source(toxval.db,source)

  #####################################################################
  cat("load data to res\n")
  #####################################################################
  query = paste0("select * from ",source_table)
  res = runQuery(query,source.db,T,F)
  res = res[ , !(names(res) %in% c("source_id","clowder_id","parent_hash","create_time","modify_time","created_by"))]
  res = res[ , !(names(res) %in% c("qc_flags","qc_notes","version","parent_chemical_id"))]
  res$source = source
  res$details_text = paste(source,"Details")
  print(dim(res))

  #####################################################################
  cat("Add the code from the original version from Aswani\n")
  #####################################################################
  res[res$name == "Inorganic Phosphates",4] = "98059-61-1"
  names(res)[is.element(names(res),"phenotype")] <- "critical_effect"
  res$subsource <- "EPA ORD"
  name.list <- names(res)
  name.list[is.element(name.list,"species")] <- "species_original"
  names(res) <- name.list
  if(is.element("species_original",names(res))) res[,"species_original"] <- tolower(res[,"species_original"])
  res1 <- res[,!is.element(names(res),c("pod_numeric","pod_type","pod_units"))]
  res2 <- res[,!is.element(names(res),c("toxval_numeric","toxval_type","toxval_units"))]
  names(res2)[is.element(names(res2),"pod_numeric")] <- "toxval_numeric"
  names(res2)[is.element(names(res2),"pod_type")] <- "toxval_type"
  names(res2)[is.element(names(res2),"pod_units")] <- "toxval_units"
  res2 <- res2[,names(res1)]
  res <- rbind(res1,res2)
  ##########################################################
  cat("remove the redundancy from the source_hash\n")
  ##########################################################
  x=seq(from=1,to=nrow(res))
  y = paste0(res$source_hash,"_",x)
  res$source_hash = y

  res$source_url <- "https://www.epa.gov/pprtv/basic-information-about-provisional-peer-reviewed-toxicity-values-pprtvs"
  res$toxval_numeric <- as.numeric(res$toxval_numeric)
  res[grep("\\-", res$study_duration_value),"study_duration_value"] <- gsub("(\\d+\\s*)(\\-\\s*)(\\d+)","\\3",res[grep("\\-", res$study_duration_value),"study_duration_value"])
  res$study_duration_value <- as.numeric(res$study_duration_value)
  # trims leading and trailing whitespaces from the dataframe
  res <- data.frame(lapply(res, function(x) if(class(x)=="character") trimws(x) else(x)), stringsAsFactors=F, check.names=F)
  res$toxval_numeric_original <- res$toxval_numeric
  res[is.na(res[,"document_name"]),"document_name"] <- "-"
  res <- unique(res)

  res[is.element(res$toxval_type,c("Chronic Reference Concentration (c-RfC)",
                                   "Chronic Reference Dose (c-RfD)",
                                   "Sub-chronic Reference Dose (s-RfD)",
                                   "Sub-chronic Reference Concentration (s-RfC)")),
      "species_original"] = "-"
  res[is.element(res$toxval_type,c("Chronic Reference Concentration (c-RfC)",
                                   "Chronic Reference Dose (c-RfD)",
                                   "Sub-chronic Reference Dose (s-RfD)",
                                   "Sub-chronic Reference Concentration (s-RfC)")),
      "human_ra"] = "Y"
  res[is.element(res$toxval_type,c("Chronic Reference Concentration (c-RfC)",
                                   "Chronic Reference Dose (c-RfD)",
                                   "Sub-chronic Reference Dose (s-RfD)",
                                   "Sub-chronic Reference Concentration (s-RfC)")),
      "target_species"] = "Human"
   res$human_eco = "human health"

  res[is.element(res$study_type,c("Human","human")),"species_original"] = "Human"

  cremove = c("rfv_id","","","")
  res = res[ , !(names(res) %in% cremove)]
  #####################################################################
  cat("find columns in res that do not map to toxval or record_source\n")
  #####################################################################
  cols1 = runQuery("desc record_source",toxval.db)[,1]
  cols2 = runQuery("desc toxval",toxval.db)[,1]
  cols = unique(c(cols1,cols2))
  cols = c(cols,c("uf_a","uf_d","uf_h","uf_l","uf_s","uf_c"))
  colnames(res)[which(names(res) == "species")] = "species_original"
  res = res[ , !(names(res) %in% c("record_url","short_ref"))]
  nlist = names(res)
  nlist = nlist[!is.element(nlist,c("casrn","name","hero_id"))]
  nlist = nlist[!is.element(nlist,cols)]
  if(length(nlist)>0) {
    cat("columns to be dealt with\n")
    print(nlist)
    browser()
  }
  print(dim(res))

  # examples ...
  # names(res)[names(res) == "source_url"] = "url"
  # colnames(res)[which(names(res) == "phenotype")] = "critical_effect"

  #####################################################################
  cat("Generic steps \n")
  #####################################################################
  res = unique(res)
  res = fill.toxval.defaults(toxval.db,res)
  res = generate.originals(toxval.db,res)
  if(is.element("species_original",names(res))) res[,"species_original"] = tolower(res[,"species_original"])
  res$toxval_numeric = as.numeric(res$toxval_numeric)
  print(dim(res))
  res=fix.non_ascii.v2(res,source)
  res = data.frame(lapply(res, function(x) if(class(x)=="character") trimws(x) else(x)), stringsAsFactors=F, check.names=F)
  res = unique(res)
  res = res[,!is.element(names(res),c("casrn","name"))]
  print(dim(res))

  #####################################################################
  cat("add toxval_id to res\n")
  #####################################################################
  count = runQuery("select count(*) from toxval",toxval.db)[1,1]
  if(count==0) tid0 = 1
  else tid0 = runQuery("select max(toxval_id) from toxval",toxval.db)[1,1] + 1
  tids = seq(from=tid0,to=tid0+nrow(res)-1)
  res$toxval_id = tids
  print(dim(res))

  #####################################################################
  cat("pull out record source to refs\n")
  #####################################################################
  colsuf = c("uf_a","uf_d","uf_h","uf_l","uf_s","uf_c")
  cols = c("toxval_id",colsuf)
  resuf = res[,cols]
  res = res[ , !(names(res) %in% c(colsuf))]

  cols = runQuery("desc record_source",toxval.db)[,1]
  nlist = names(res)
  keep = nlist[is.element(nlist,cols)]
  refs = res[,keep]
  cols = runQuery("desc toxval",toxval.db)[,1]
  nlist = names(res)
  remove = nlist[!is.element(nlist,cols)]
  res = res[ , !(names(res) %in% c(remove))]
  print(dim(res))

  #####################################################################
  cat("add extra columns to refs\n")
  #####################################################################
  refs$record_source_type = "-"
  refs$record_source_note = "-"
  refs$record_source_level = "-"
  print(dim(res))

  #####################################################################
  cat("load res and refs to the database\n")
  #####################################################################
  res = unique(res)
  refs = unique(refs)
  res$datestamp = Sys.Date()
  res$source_table = source_table
  res$source_url = "https://www.epa.gov/pprtv/basic-information-about-provisional-peer-reviewed-toxicity-values-pprtvs"
  res$subsource_url = "-"
  res$details_text = paste(source,"Details")
  #for(i in 1:nrow(res)) res[i,"toxval_uuid"] = UUIDgenerate()
  #for(i in 1:nrow(refs)) refs[i,"record_source_uuid"] = UUIDgenerate()
  runInsertTable(res, "toxval", toxval.db, verbose)
  runInsertTable(refs, "record_source", toxval.db, verbose)
  print(dim(res))

  #####################################################################
  cat("load uf to the database\n")
  #####################################################################
  toxids <- runQuery("select toxval_id, chemical_id from toxval where source = 'PPRTV (NCEA)' and toxval_type like '%Reference%'",toxval.db,T,F)
  parids <- runQuery("select toxval_id as parent_id, chemical_id from toxval where source = 'PPRTV (NCEA)' and toxval_type not like '%Reference%'",toxval.db,T,F)
  toxval_uf <- merge(toxids, parids)
  toxval_uf <- merge(toxval_uf, unique(resuf))
  names(toxval_uf)[4:9] <- c("UCF.interspecies","UCF.database.incomplete","UCF.intraspecies","UCF.LOAEL.vs.NOAEL","UCF.subchronic.to.chronic","UCF.composite")

  toxval_uf <- gather(toxval_uf, "uf_type","uf",4:9)
  toxval_uf <- toxval_uf[,c("toxval_id","parent_id","uf_type","uf")]
  toxval_uf <- toxval_uf[!is.na(toxval_uf$uf),]
  runInsertTable(toxval_uf,"toxval_uf",toxval.db)
  toxval_relationship <- merge(toxids, parids)

  toxval_relationship <- rbind(
    data.frame(toxval_id_1 = toxval_relationship$toxval_id,
               toxval_id_2 = toxval_relationship$parent_id,
               relationship = "RFD derived from"
    ),
    data.frame(toxval_id_1 = toxval_relationship$parent_id,
               toxval_id_2 = toxval_relationship$toxval_id,
               relationship = "used to derive RFD"
    )
  )
  runInsertTable(toxval_relationship,"toxval_relationship",toxval.db)

  #####################################################################
  cat("do the post processing\n")
  #####################################################################
  toxval.load.postprocess(toxval.db,source.db,source,do.convert.units=F)

  if(log) {
    #####################################################################
    cat("stop output log \n")
    #####################################################################
    closeAllConnections()
    log_close()
    output_message = read.delim(paste0(toxval.config()$datapath,source,"_",Sys.Date(),".log"), stringsAsFactors = F, header = F)
    names(output_message) = "message"
    output_log = read.delim(paste0(toxval.config()$datapath,"log/",source,"_",Sys.Date(),".log"), stringsAsFactors = F, header = F)
    names(output_log) = "log"
    new_log = log_message(output_log, output_message[,1])
    writeLines(new_log, paste0(toxval.config()$datapath,"output_log/",source,"_",Sys.Date(),".txt"))
  }
  #####################################################################
  cat("finish\n")
  #####################################################################
}
