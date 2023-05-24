#--------------------------------------------------------------------------------------
#' Load the ATSDR MRLs 2020 data from toxval_source to toxval
#'
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source.db The source database to use.
#' @param log If TRUE, send output to a log file
#' @export
#--------------------------------------------------------------------------------------
toxval.load.atsdr_mrl_2020 <- function(toxval.db,source.db, log=F){
  verbose = log
  printCurrentFunction(toxval.db)
  source <- "ATSDR MRLs 2020"
  source_table = "source_atsdr"
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
  res = res[ , !(names(res) %in% c("source_id","clowder_id","parent_hash","create_time","modify_time","created_by","parent_chemical_id"))]
  res$source = source
  res$details_text = paste(source,"Details")

  #####################################################################
  cat("find columns in res that do not map to toxval or record_source\n")
  #####################################################################
  cat("breakout mrl, duration, factors\n")
  res$toxval_type = "MRL"
  res$study_type <- res$duration
  res[grep("15 - 364 days\\.|15 - 364 daysermediate",res$study_type),"study_type"] <- "intermediate"
  res[grep("1 - 14 days",res$study_type),"study_type"] <- "acute"
  res[grep("1 year or.*",res$study_type),"study_type"] <- "chronic"

  colnames(res)[which(names(res) == "route")] = "exposure_route"
  res[grep("Inh.*",res$exposure_route),"exposure_route"] <- "Inhalation"
  res[grep("Rad.*",res$exposure_route),"exposure_route"] <- "External Radiation MRLs"
  res[grep("Inh.*",res$exposure_route),"exposure_route"] <- "Inhalation"

  res$toxval_numeric <-  res$mrl
  res$toxval_numeric <-  gsub("(\\d+\\.*\\d*)(\\s*.*)","\\1",res$toxval_numeric)
  res$toxval_numeric <- as.numeric(res$toxval_numeric)
  res$toxval_units <- gsub("(\\d+\\.*\\d*\\s*)(.*)","\\2",res$mrl)

  res$study_duration_value <- res$duration
  res$study_duration_value <- gsub("(\\d+\\s*\\-*\\s*\\d*)(\\s*.*)","\\1",res$study_duration_value)
  res$study_duration_value <- gsub("^\\s+|\\s+$","",res$study_duration_value)

  res$study_duration_units <- res$duration
  res[grep("chronic",res$study_type),"study_duration_units"] <- "year"
  res[grep("acute",res$study_type),"study_duration_units"] <- "day"
  res[grep("intermediate",res$study_type),"study_duration_units"] <- "day"

  res[grep("15 - 364",res$study_duration_value),"study_duration_value"] <- "364"
  res[grep("1 - 14",res$study_duration_value),"study_duration_value"] <- "14"
  res$study_duration_value <-  as.numeric(res$study_duration_value)

  res$year <- res$date
  res$year <-  gsub("(\\d+\\-\\w+\\-)(\\d{4})", "\\2", res$year)
  colnames(res)[which(names(res) == "endpoint")] = "critical_effect"
  colnames(res)[which(names(res) == "total_factors")] = "uf"
  res = res[ , !(names(res) %in% c("source_name_sid","data_collection","source_name_cid","mrl","total_factors","status","duration","mrl","date"))]

  res1 <- res
  res2 <- res
  # update res2 toxval_numeric field by multiplying values from existing toxval_numeric with uncertainity factors
  for(i in 1:nrow(res2)) res2[i,"toxval_numeric"] <- res2[i,"toxval_numeric"]*res2[i,"uf"]
  #assign new column toxval_type
  res1$toxval_type <- "ATSDR MRL"
  res2$toxval_type <- "NOAEL"
  #combine res1 & res2 to create res and select required fields
  res <- rbind(res1,res2)

  res[grep("Neurol.",res$critical_effect),"critical_effect"] <- "neurologic"
  res[grep("Hemato.",res$critical_effect),"critical_effect"] <- "hematological"
  res[grep("Resp.",res$critical_effect),"critical_effect"] <- "respiratory"
  res[grep("Gastro.",res$critical_effect),"critical_effect"] <- "gastrointestinal"
  res[grep("Repro.",res$critical_effect),"critical_effect"] <- "reproductive"
  res[grep("Develop.",res$critical_effect),"critical_effect"] <- "developmental"
  res[grep("Metab.",res$critical_effect),"critical_effect"] <- "metabolic"
  res[grep("Body Wt.",res$critical_effect),"critical_effect"] <- "body weight"
  res[grep("Immuno.",res$critical_effect),"critical_effect"] <- "immunological"
  res[grep("Musculo.",res$critical_effect),"critical_effect"] <- "musculoskeletal"
  res[grep("Reprod.",res$critical_effect),"critical_effect"] <- "reproductive"
  res[grep("Endocr.",res$critical_effect),"critical_effect"] <- "endocrine"
  res[grep("Lymphor.",res$critical_effect),"critical_effect"] <- "lymphatic"

  res$toxval_units <- gsub("µ","u", res$toxval_units)
  res$toxval_units <- gsub("\\/day","-day", res$toxval_units)
  res$exposure_route <- tolower(res$exposure_route)
  res[grep("external.*",res$exposure_route),"exposure_route"] <- "radiation"

  res <- unique(res)
  res$toxval_numeric_qualifier <- "="
  res$risk_assessment_class <- res$study_type
  res$exposure_method <- "-"
  res$human_eco <- "human health"
  res$media <- "-"
  res$sex <- "-"
  res[,"species_original"] <- "-"
  res$human_ra = "Y"
  res$target_species = "Human"
  res$subsource <- "CDC"
  res$details_text <- "ATSDR Details"
  res$toxval_numeric_original <- res$toxval_numeric
  res[is.na(res[,"document_name"]),"document_name"] <- "-"
  res = res[ , !(names(res) %in% c("qc_flags","qc_notes","version"))]
  res <- unique(res)

  cols1 = runQuery("desc record_source",toxval.db)[,1]
  cols2 = runQuery("desc toxval",toxval.db)[,1]
  cols3 = runQuery("desc toxval_uf",toxval.db)[,1]
  cols = unique(c(cols1,cols2,cols3))
  colnames(res)[which(names(res) == "species")] = "species_original"
  nlist = names(res)
  nlist = nlist[!is.element(nlist,c("casrn","name"))]
  nlist = nlist[!is.element(nlist,cols)]
  if(length(nlist)>0) {
    cat("columns to be dealt with\n")
    print(nlist)
    browser()
  }
  # examples ...
  # names(res)[names(res) == "source_url"] = "url"
  # colnames(res)[which(names(res) == "phenotype")] = "critical_effect"
  ##########################################################
  cat("remove the redundancy from the source_hash\n")
  ##########################################################
  x=seq(from=1,to=nrow(res))
  y = paste0(res$source_hash,"_",x)
  res$source_hash = y

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
  res = res[,!is.element(names(res),c("casrn","name","parent_chemical_id"))]

  #####################################################################
  cat("add toxval_id to res\n")
  #####################################################################
  count = runQuery("select count(*) from toxval",toxval.db)[1,1]
  if(count==0) tid0 = 1
  else tid0 = runQuery("select max(toxval_id) from toxval",toxval.db)[1,1] + 1
  tids = seq(from=tid0,to=tid0+nrow(res)-1)
  res$toxval_id = tids

  #####################################################################
  cat("pull out toxval_uf to uf\n")
  #####################################################################
  uf = res[,c("toxval_id","uf")]
  uf$uf_type = "total"

  #####################################################################
  cat("pull out record source to refs\n")
  #####################################################################
  cols = runQuery("desc record_source",toxval.db)[,1]
  nlist = names(res)
  keep = nlist[is.element(nlist,cols)]
  refs = res[,keep]
  cols = runQuery("desc toxval",toxval.db)[,1]
  nlist = names(res)
  remove = nlist[!is.element(nlist,cols)]
  res = res[ , !(names(res) %in% c(remove))]

  #####################################################################
  cat("add extra columns to refs\n")
  #####################################################################
  refs$record_source_type = "website"
  refs$record_source_note = "to be cleaned up"
  refs$record_source_level = "primary (risk assessment values)"

  #####################################################################
  cat("load res and refs to the database\n")
  #####################################################################
  res = unique(res)
  refs = unique(refs)
  res$datestamp = Sys.Date()
  res$source_table = source_table
  res$source_url = "https://www.atsdr.cdc.gov/mrls/index.html"
  res$subsource_url = "-"
  res$details_text = paste(source,"Details")
  #for(i in 1:nrow(res)) res[i,"toxval_uuid"] = UUIDgenerate()
  #for(i in 1:nrow(refs)) refs[i,"record_source_uuid"] = UUIDgenerate()
  runInsertTable(res, "toxval", toxval.db, verbose)
  runInsertTable(refs, "record_source", toxval.db, verbose)
  print(dim(res))

  #####################################################################
  cat("do the post processing\n")
  #####################################################################
  toxval.load.postprocess(toxval.db,source.db,source)

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
