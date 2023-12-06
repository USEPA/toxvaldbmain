#--------------------------------------------------------------------------------------
#' Load HAWC from toxval_source to toxval
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source.db The version of toxval_source from which the tables are loaded.
#' @param log If TRUE, send output to a log file
#' @export
#--------------------------------------------------------------------------------------
toxval.load.hawc <- function(toxval.db, source.db, log=FALSE, remove_null_dtxsid=TRUE){
  printCurrentFunction(toxval.db)
  source <- "HAWC Project"
  source_table = "source_hawc"
  verbose = log
  runQuery("update source_chemical set source='HAWC Project' where source='HAWC'",source.db)
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
  # Whether to remove records with NULL DTXSID values
  if(!remove_null_dtxsid){
    query = paste0("select * from ",source_table)
  } else {
    query = paste0("select * from ",source_table, " ",
                   # Filter out records without curated chemical information
                   "WHERE chemical_id IN (SELECT chemical_id FROM source_chemical WHERE dtxsid is NOT NULL)")
  }
  res = runQuery(query,source.db,TRUE,FALSE)
  res = res[,!names(res) %in% toxval.config()$non_hash_cols[!toxval.config()$non_hash_cols %in% c("chemical_id")]]
  res$source = source
  res$details_text = paste(source,"Details")
  print(paste0("Dimensions of source data: ", toString(dim(res))))

  #####################################################################
  # Old code and no longer necessary
  # cat("Add the code from the original version from Aswani\n")
  #####################################################################
  # unique(res$study_type)
  # res$study_type <- tolower(res$study_type)
  # para_vals <- grep("\\(",res$study_type)
  # res[para_vals, "study_type"] <- gsub("(.*)(\\s+\\(.*)","\\1",res[para_vals, "study_type"])
  # ##### fix exposure_route
  # unique(res$exposure_route)
  # res$exposure_route <- tolower(res$exposure_route)
  # oral_vals <- grep("oral", res$exposure_route)
  # res[oral_vals, "exposure_route"] <- gsub("(oral)(\\s+.*)","\\1",res[oral_vals, "exposure_route"])
  # injection_vals <- grep("injection", res$exposure_route)
  # res[injection_vals, "exposure_route"] <- gsub("(.*)(\\s+injection)","\\1",res[injection_vals, "exposure_route"])
  # ####### fix exposure_method
  # unique(res$exposure_method)
  # res$exposure_method <- tolower(res$exposure_method)
  # oral_vals <- grep("oral", res$exposure_method)
  # res[oral_vals, "exposure_method"] <- gsub("(oral\\s+)(.*)","\\2",res[oral_vals, "exposure_method"])
  # injection_vals <- grep("injection", res$exposure_method)
  # res[injection_vals, "exposure_method"] <- gsub("(.*\\s+)(injection)","\\2",res[injection_vals, "exposure_method"])
  # res$exposure_method <- tolower(res$exposure_method)

  ######### fix study duration value and units
  # #hour vals
  # hour_vals <- grep("hour", res$study_duration_value, ignore.case = T)
  # res[hour_vals,"study_duration_units"] <- "hour"
  # res[hour_vals,"study_duration_value"] <- gsub("^([0-9]+)(\\s+)(hours)","\\1",res[hour_vals,"study_duration_value"])
  # # day vals
  # day_vals <- grep("day", res$study_duration_value, ignore.case = T)
  # res[day_vals,"study_duration_units"] <- "day"
  # res[day_vals,"study_duration_value"] <- gsub("^([0-9]+)(\\s+)(days)(.*)","\\1",res[day_vals,"study_duration_value"])
  # day_vals <- grep("day", res$study_duration_value, ignore.case = T)
  # res[day_vals,"study_duration_value"] <- gsub("^([0-9]+\\-)([0-9]+)(\\s+)(days)(.*)","\\2",res[day_vals,"study_duration_value"])
  # #week vals
  # week_vals <- grep("week", res$study_duration_value, ignore.case = T)
  # res[week_vals,"study_duration_units"] <- "week"
  # res[week_vals,"study_duration_value"] <- gsub("^([0-9]+)(\\s+)(weeks)(.*)","\\1",res[week_vals,"study_duration_value"])
  # week_vals <- grep("week", res$study_duration_value, ignore.case = T)
  # res[week_vals,"study_duration_value"] <- gsub("^(.*[^0-9]+)([0-9]+)(\\s+)(weeks)(.*)","\\2",res[week_vals,"study_duration_value"])
  # #month vals (without PND)
  # month_vals <- grep("months$", res$study_duration_value, ignore.case = T)
  # res[month_vals,"study_duration_units"] <- "month"
  # res[month_vals,"study_duration_value"] <- gsub("^([0-9]+)(\\s+)(months)","\\1",res[month_vals,"study_duration_value"])
  # #one time vals
  # one_time_vals <- grep("one time", res$study_duration_value, ignore.case = T)
  # res[one_time_vals,"study_duration_units"] <- "one time"
  # res[one_time_vals,"study_duration_value"] <- "1"
  # # GD range vals
  # GD_vals <- grep("GD\\s+.*\\-[^a-zA-Z]+$", res$study_duration_value, ignore.case = T)
  # res[GD_vals,"study_duration_units"] <- "GD"
  # res[GD_vals,"study_duration_value"] <- gsub("^(GD)(\\s+.*\\-\\s*)(.*)","\\3",res[GD_vals,"study_duration_value"])
  #
  # # GD until vals
  # GD_until_vals <- grep("GD.*until.*[^0]$", res$study_duration_value, ignore.case = T)
  # res[GD_until_vals,"study_duration_units"] <- "GD"
  # res[GD_until_vals,"study_duration_value"] <- gsub("^(GD.*GD\\s+)(.*)","\\2",res[GD_until_vals,"study_duration_value"])
  #
  # GD_until_zero_vals <- grep("GD.*until.*[0]$", res$study_duration_value, ignore.case = T)
  # res[GD_until_zero_vals,"study_duration_units"] <- res[GD_until_zero_vals,"study_duration_value"]
  # res[GD_until_zero_vals,"study_duration_value"] <- ""
  #
  # #PND range vals
  # PND_vals <- grep(".*PND\\s*.*[^0a-zA-Z]$", res$study_duration_value, ignore.case = T)
  # res[PND_vals,"study_duration_units"] <- "PND"
  # res[PND_vals,"study_duration_value"] <- gsub("^(.*PND\\s*)(\\d+)","\\2",res[PND_vals,"study_duration_value"])
  # res[which(res$study_duration_value == "2-15" & res$study_duration_units == "PND"),"study_duration_value"] <- gsub("(\\d+\\-)(\\d+)","\\2",res[which(res$study_duration_value == "2-15" & res$study_duration_units == "PND"),"study_duration_value"])
  # PND_vals <- grep(".*PND\\s*[^0]\\d+$", res$study_duration_value, ignore.case = T)
  # res[PND_vals,"study_duration_units"] <- "PND"
  # res[PND_vals,"study_duration_value"] <- gsub("^(.*PND\\s*)(\\d+)(.*?)","\\2",res[PND_vals,"study_duration_value"])
  # res[which(res$study_duration_value == "21, not PND 0" & res$study_duration_units == "PND"),"study_duration_value"] <- gsub("(\\d+)(\\,.*)","\\1",res[which(res$study_duration_value == "21, not PND 0" & res$study_duration_units == "PND"),"study_duration_value"])
  # # GD or PND zero vals
  # zero_vals <- grep("PND0|GD0", res$study_duration_value, ignore.case = T)
  # res[zero_vals,"study_duration_units"] <- res[zero_vals,"study_duration_value"]
  # res[zero_vals,"study_duration_value"] <- ""
  # # 1 OR 2 years vals
  # or_vals <- grep("or", res$study_duration_value, ignore.case = T)
  # res[or_vals,"study_duration_units"] <- gsub("(.*or\\s+)(\\d+)(\\s+)(\\w+)","\\4",res[or_vals,"study_duration_value"])
  # res[or_vals,"study_duration_value"] <- gsub("(.*or\\s+)(\\d+)(\\s+)(\\w+)","\\2",res[or_vals,"study_duration_value"])
  # # PND 3-10 vals
  # PND_vals <- grep("PND", res$study_duration_value, ignore.case = T)
  # res[PND_vals,"study_duration_units"] <-"PND"
  # res[PND_vals,"study_duration_value"] <- gsub("(PND.*\\-)(\\d+)","\\2",res[PND_vals,"study_duration_value"])
  # res[which(res$study_duration_value == "-"),"study_duration_value"] <- ""

  #res$study_duration_value <- as.numeric(res$study_duration_value)

  cremove = c("assessment","target","noel_original","loel_original","fel_original",
            "endpoint_url_original","study_id","authors_short","full_text_url","study_url_original",
            "experiment_name","experiment_type","chemical_source","guideline_compliance","dosing_regime_id",
            "route_of_exposure","exposure_duration_value","exposure_duration_text","doses","endpoint_url",
            "study_url")
  res = res[ , !(names(res) %in% cremove)]

  #####################################################################
  cat("find columns in res that do not map to toxval or record_source\n")
  #####################################################################
  cols1 = runQuery("desc record_source",toxval.db)[,1]
  cols2 = runQuery("desc toxval",toxval.db)[,1]
  cols = unique(c(cols1,cols2))
  colnames(res)[which(names(res) == "species")] = "species_original"
  res = res[ , !(names(res) %in% c("record_url","short_ref"))]
  nlist = names(res)
  nlist = nlist[!is.element(nlist,c("casrn","name"))]
  nlist = nlist[!is.element(nlist,cols)]
  if(length(nlist)>0) {
    cat("columns to be dealt with\n")
    print(nlist)
    browser()
  }
  print(dim(res))


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
  if(count==0) {
    tid0 = 1
  } else {
    tid0 = runQuery("select max(toxval_id) from toxval",toxval.db)[1,1] + 1
  }
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
  refs$record_source_type = "website"
  refs$record_source_note = "to be cleaned up"
  refs$record_source_level = "primary (risk assessment values)"
  print(paste0("Dimensions of references after adding ref columns: ", toString(dim(refs))))

  #####################################################################
  cat("load res and refs to the database\n")
  #####################################################################
  res = unique(res)
  refs = unique(refs)
  res$datestamp = Sys.Date()
  res$source_table = source_table
  res$source_url = "https://hawcproject.org/assessment/public/"
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
  toxval.load.postprocess(toxval.db,source.db,source,do.convert.units=FALSE, remove_null_dtxsid=remove_null_dtxsid)

  if(log) {
    #####################################################################
    cat("stop output log \n")
    #####################################################################
    closeAllConnections()
    logr::log_close()
    output_message = read.delim(paste0(toxval.config()$datapath,source,"_",Sys.Date(),".log"), stringsAsFactors = FALSE, header = FALSE)
    names(output_message) = "message"
    output_log = read.delim(paste0(toxval.config()$datapath,"log/",source,"_",Sys.Date(),".log"), stringsAsFactors = FALSE, header = FALSE)
    names(output_log) = "log"
    new_log = log_message(output_log, output_message[,1])
    writeLines(new_log, paste0(toxval.config()$datapath,"output_log/",source,"_",Sys.Date(),".txt"))
  }
  #####################################################################
  cat("finish\n")
  #####################################################################
  return(0)
}
