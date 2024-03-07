#-------------------------------------------------------------------------------------
#' Load teh uterotophic and Hershberger data
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param verbose Whether the loaded rows should be printed to the console.
#' @param log If TRUE, send output to a log file
#' @export
#--------------------------------------------------------------------------------------
toxval.load.ut_hb <- function(toxval.db,source.db,log=F) {
  printCurrentFunction(toxval.db)
  source <- "Uterotrophic Hershberger DB"
  source_table = "direct load"
  verbose=F
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
  file = paste0(toxval.config()$datapath,"ut_hb/UT for ToxValDB.xlsx")
  print(file)
  ut = read.xlsx(file)
  ut = ut[is.element(ut$response,"Active"),]
  nlist = c("casrn",
            "name",
            "study_type",
            "toxval_type",
            "toxval_numeric",
            "toxval_numeric_qualifier",
            "toxval_units",
            "critical_effect",
            "species",
            "exposure_route",
            "study_duration_value",
            "study_duration_units",
            "pmid",
            "guideline")
  ut = ut[,nlist]
  ut$url = "https://ntp.niehs.nih.gov/go/40658"
  ut$long_ref = "Kleinstreuer et al., A Curated Database of Rodent Uterotrophic Bioactivity, EHP Vol 124 2106, https://doi.org/10.1289/ehp.1510183"


  file = paste0(toxval.config()$datapath,"ut_hb/HB for ToxValDB.xlsx")
  hb = read.xlsx(file)
  nlist = c("casrn","name","long_ref","species","strain","study_duration_value","study_duration_units","study_type","exposure_route","exposure_method","toxval_units",
            "critical_effect","Androgenic.call","Antiandrogenic.call","HB.total.activity","other.in.vivo.effects",
            "In.vivo.NOEL.for.antiandrogenic",
            "in.vivo.LOEL.antiandrogenic",
            "HB.NOEL",
            "HB.LOEL"
  )
  hb = hb[,nlist]
  hb$url = "https://www.sciencedirect.com/science/article/pii/S089062381830145X"
  nlist1 = c("casrn","name","long_ref","species","strain","study_duration_value","study_duration_units","study_type","exposure_route","exposure_method","toxval_units",
            "critical_effect","Androgenic.call","Antiandrogenic.call","HB.total.activity","other.in.vivo.effects","url",
            "In.vivo.NOEL.for.antiandrogenic")
  hb1 = hb[,nlist1]
  names(hb1)[ncol(hb1)] = "toxval_numeric"
  hb1$toxval_type = "NOEL"
  hb1$study_type = "acute"

  nlist2 = c("casrn","name","long_ref","species","strain","study_duration_value","study_duration_units","study_type","exposure_route","exposure_method","toxval_units",
             "critical_effect","Androgenic.call","Antiandrogenic.call","HB.total.activity","other.in.vivo.effects","url",
             "in.vivo.LOEL.antiandrogenic")

  hb2 = hb[,nlist2]
  names(hb2)[ncol(hb2)] = "toxval_numeric"
  hb2$toxval_type = "LOEL"
  hb2$study_type = "acute"

  nlist3 = c("casrn","name","long_ref","species","strain","study_duration_value","study_duration_units","study_type","exposure_route","exposure_method","toxval_units",
             "critical_effect","Androgenic.call","Antiandrogenic.call","HB.total.activity","other.in.vivo.effects","url",
             "HB.NOEL")
  hb3 = hb[,nlist3]
  names(hb3)[ncol(hb3)] = "toxval_numeric"
  hb3$toxval_type = "NOEL"
  hb3$study_type = "Hershberger"

  nlist4 = c("casrn","name","long_ref","species","strain","study_duration_value","study_duration_units","study_type","exposure_route","exposure_method","toxval_units",
             "critical_effect","Androgenic.call","Antiandrogenic.call","HB.total.activity","other.in.vivo.effects","url",
             "HB.LOEL")
  hb4 = hb[,nlist4]
  names(hb4)[ncol(hb4)] = "toxval_numeric"
  hb4$toxval_type = "LOEL"
  hb4$study_type = "Hershberger"

  hb1 = hb1[!is.na(hb1$toxval_numeric),]
  hb2 = hb2[!is.na(hb2$toxval_numeric),]
  hb3 = hb3[!is.na(hb3$toxval_numeric),]
  hb4 = hb4[!is.na(hb4$toxval_numeric),]
  hb3 = hb3[is.element(hb3$HB.total.activity,"active"),]
  hb2 = hb2[is.element(hb2$HB.total.activity,"active"),]
  hb = rbind(hb1,hb2,hb3,hb4)
  hb[is.element(hb$Androgenic.call,"Positive"),"Androgenic.call"] = "Androgenic"
  hb[is.element(hb$Antiandrogenic.call,"Positive"),"Antiandrogenic.call"] = "Antiandrogenic"
  for(i in 1:nrow(hb)) {
    temp = hb[i,c("HB.total.activity","Androgenic.call","Antiandrogenic.call","critical_effect","other.in.vivo.effects")]
    temp = temp[!is.na(temp)]
    temp = paste(temp,collapse="|")
    hb[i,"critical_effect"] = temp
  }
  cremove = c("HB.total.activity","Androgenic.call","Antiandrogenic.call","other.in.vivo.effects")
  hb = hb[ , !(names(hb) %in% cremove)]
  hb = hb[!is.na(hb$toxval_units),]
  hb = hb[hb$toxval_numeric>0,]

  nlist = c("casrn","name","toxval_type","toxval_numeric_qualifier","toxval_numeric","toxval_units",
            "study_type","exposure_method","exposure_route","study_duration_value","study_duration_units",
            "species","strain","critical_effect","long_ref","pmid","url","guideline")

  ut$exposure_method = "-"
  ut$strain = "-"
  hb$toxval_numeric_qualifier = "="
  hb$pmid = NA
  hb$guideline = "-"
  ut = ut[,nlist]
  hb = hb[,nlist]
  res = rbind(hb,ut)
  res = res[!is.na(res$casrn),]

  cat("set the source_hash\n")
  res$source_hash = NA
  for (i in 1:nrow(res)){
    row <- res[i,]
    res[i,"source_hash"] = digest(paste0(row,collapse=""), serialize = FALSE)
    if(i%%1000==0) cat(i," out of ",nrow(res),"\n")
  }
  res$source = source
  res = source_chemical.extra(toxval.db,source.db,res,source,chem.check.halt=FALSE,casrn.col="casrn", name.col="name",verbose=F)

  #####################################################################
  cat("Add the code from the original version from Aswani\n")
  #####################################################################
  #browser()
  # cremove = c("study_source","chemical_index")
  # res = res[ , !(names(res) %in% cremove)]

  #####################################################################
  cat("find columns in res that do not map to toxval or record_source\n")
  #####################################################################
  cols1 = runQuery("desc record_source",toxval.db)[,1]
  cols2 = runQuery("desc toxval",toxval.db)[,1]
  cols = unique(c(cols1,cols2))
  colnames(res)[which(names(res) == "species")] = "species_original"
  res = res[ , !(names(res) %in% c("record_url","short_ref"))]
  nlist = names(res)
  nlist = nlist[!is.element(nlist,c("casrn","name","chemical_index"))]
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
  res = res[!is.na(res$toxval_numeric),]
  res = res[res$toxval_numeric>0,]
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
  res$source_url = "-"
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

