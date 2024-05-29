#-------------------------------------------------------------------------------------
#' Load Health Canada data from toxval_source to toxval
#'
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source.db The version of toxval_source from which the tables are loaded.
#' @param log If TRUE, send output to a log file
#' @export
#--------------------------------------------------------------------------------------
toxval.load.healthcanada <- function(toxval.db,source.db,log=F) {
  printCurrentFunction(toxval.db)
  source <- "Health Canada"
  source_table = "source_health_canada"
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
  names(res)[names(res)=="trv_source"] <- "long_ref"

  #####################################################################
  cat("checks, finds and replaces non ascii characters in res with XXX\n")
  #####################################################################
  res1 <- res
  res1 <- as.data.frame(apply(res1, 2, function(y) as.character(gsub("µ", "u", y))),stringsAsFactors = F)
  res1 <- as.data.frame(apply(res1, 2, function(y) as.character(gsub("[³]", "3", y))),stringsAsFactors = F)
  res1$study_duration_value <- res1$duration
  res1[grep(".*60 years$",res1$study_duration_value),"study_duration_value"] <- "<= 60 years"
  res1[grep(".*12 months occupational exposure$",res1$study_duration_value),"study_duration_value"] <- ">= 12 months occupational exposure"
  replacement_value <- c("days 1-24 (rabbits) and 1-19 (rats) of gestational period","Duration and Dosing Regime: single bolus dose (0, 12.5, 50, 200, or 800 ng 2,3,7,8-TCDD)/kg-bw) on day 15 of gestation (Oshako et al., 2001); subcutaneous loading dose 25, 60, or 300 ng TCDD/kgbw) followed by weekly maintenance doses (5, 12, or 60 ng TCDD/kgbw) beginning 2 weeks prior to mating, and continuing through mating, gestation and lactation (Faqi et al., 1998)",
                         "F0: prior to and during mating (males and females) and throughout gestation lactation; F1: from weaning through reproduction until weaning of F2 pups","gestational days 0-17","nd","chronic","1, 2, 6, 8 weeks (various groups)","4 and 8 months","105 to 107 weeks","1 gestational period, 48 d post-natal exposure","3 dosing regimes: for 3 months before pregnancy, for 2 months before and 21 d during pregnancy, or for 21 d during pregnancy only",
                         "Duration and Dosing Regime: single bolus dose (0, 12.5, 50, 200, or 800 ng 2,3,7,8-TCDD)/kg-bw) on day 15 of gestation (Oshako et al., 2001); subcutaneous loading dose 25, 60, or 300 ng TCDD/kgbw) followed by weekly maintenance doses (5, 12, or 60 ng TCDD")
  new_replacement_value <- c(19,15,2,17,0,0,8,8,107,48,3,15)
  new_replacement_unit <- c("days","days","generations","days","-","-", "weeks","months","weeks","days","months","days")
  replacement_table <- data.frame(replacement_value, new_replacement_value, new_replacement_unit, stringsAsFactors = F)

  for (i in 1:length(replacement_table$replacement_value)){
    for (k in 1:nrow(res1)){
      res1$study_duration_value[k][res1$duration[k] %in% replacement_table$replacement_value[[i]]] <- replacement_table$new_replacement_value[i]
    }
  }

  res1$study_duration_value <-  gsub("^\\D+(\\d.*)", "\\1", res1$study_duration_value)
  res1$study_duration_units <- word(res1$study_duration_value,2)
  res1$study_duration_value <- word(res1$study_duration_value,1)
  num_alpha_val <- grep("[0-9]+[a-zA-Z]+", res1$study_duration_value, value = T)
  num_alpha_unit <- gsub("\\d","",num_alpha_val)
  num_alpha_vals <- gsub("[a-zA-Z]","",num_alpha_val)
  res1$study_duration_value[res1$duration %in% num_alpha_val] <- num_alpha_vals
  res1$study_duration_units[res1$duration %in% num_alpha_val] <- num_alpha_unit

  for (i in 1:length(replacement_table$replacement_value)){
    for (k in 1:nrow(res1)){
      res1$study_duration_units[k][res1$duration[k] %in% replacement_table$replacement_value[[i]]] <- replacement_table$new_replacement_unit[i]
    }
  }

  res1$study_duration_value <- gsub(".*-", "", res1$study_duration_value)
  res1$study_duration_units <- gsub("\\bd\\b","days", res1$study_duration_units)
  res1$study_duration_units <- gsub("\\bh\\b","hours", res1$study_duration_units)
  res1$study_duration_units <- gsub("\\bwk\\b","weeks", res1$study_duration_units)
  res1$study_duration_units <- gsub("\\byear\\b","years", res1$study_duration_units)
  res1$study_duration_units <- gsub("\\,|\\;|\\s+|\\)", "",res1$study_duration_units)
  res1$study_duration_value <-  as.numeric(res1$study_duration_value)
  res1$toxval_numeric <- as.numeric(res1$toxval_numeric)
  res1$study_duration_qualifier <- res1$duration
  res1$study_duration_qualifier <- gsub("[a-zA-Z0-9.,\\+() ;:/-]", "",res1$study_duration_qualifier)
  names(res1) <- tolower(names(res1))

  res1["exposure_method_original"] = '-'
  exp_val <- grep(".*\\:",res1$exposure_route)
  for (i in 1:length(exp_val)){
    res1[exp_val[i],"exposure_method_original"] <- res1[exp_val[i],"exposure_route"]
    res1[exp_val[i],"exposure_route_original"] <- res1[exp_val[i],"exposure_route"]
    res1[exp_val[i],"exposure_method"] <- gsub("(.*\\:)(.*)","\\2",res1[exp_val[i],"exposure_route"])
    res1[exp_val[i],"exposure_method"] <- gsub("^\\s+","",res1[exp_val[i],"exposure_method"])
    res1[exp_val[i],"exposure_route"] <- gsub("(.*\\:)(.*)","\\1",res1[exp_val[i],"exposure_route"])
    res1[exp_val[i],"exposure_route"] <- gsub("\\:","",res1[exp_val[i],"exposure_route"])
    res1[exp_val[i],"exposure_route"] <- gsub("\\s+$","",res1[exp_val[i],"exposure_route"])
  }

  for(i in 1:nrow(res1)) {
    if(res1[i,"exposure_route"]=="nd" && res1[i,"toxval_type"]=="inhalation SF") res1[i,"exposure_route"] = "inhalation"
    else if(res1[i,"exposure_route"]=="nd" && res1[i,"toxval_type"]=="inhalation UR") res1[i,"exposure_route"] = "inhalation"
    else if(res1[i,"exposure_route"]=="nd" && res1[i,"toxval_type"]=="oral TDI") res1[i,"exposure_route"] = "oral"
    if(res1[i,"exposure_route"]=="oal") res1[i,"exposure_route"] = "oral"
    if(res1[i,"exposure_route"]=="Cr(IV) in drinking water") res1[i,"exposure_route"] = "oral"
    if(res1[i,"exposure_route"]=="sub-cutaneous injection") res1[i,"exposure_route"] = "injection"
    if(res1[i,"exposure_route"]=="various, primarily inhalation (cadmium oxide dusts and/or fumes)") res1[i,"exposure_route"] = "oral"
    if(is.na(res1[i,"exposure_route_original"])) res1[i,"exposure_route_original"]=res1[i,"exposure_route"]
    if(res1[i,"exposure_route_original"]=='-') res1[i,"exposure_route_original"]=res1[i,"exposure_route"]
  }
  open_paranthesis_effect <- which(str_count(res1$critical_effect,"\\)") == 0 & str_count(res1$critical_effect,"\\(") == 1)
  open_paranthesis_effect2 <- grep("[^\\)]$",res1[which(str_count(res1$critical_effect,"\\)") == 0 & str_count(res1$critical_effect,"\\(") == 1),"critical_effect"])
  critical_effect_to_clean <- open_paranthesis_effect[open_paranthesis_effect2]
  res1[critical_effect_to_clean,"critical_effect"] <- gsub("\\(","",res1[critical_effect_to_clean,"critical_effect"])
  open_paranthesis_effect <-which(str_count(res1$critical_effect,"\\(") == 6)
  res1[open_paranthesis_effect,"critical_effect"] <- gsub("(.*\\(.*\\)\\;\\s+)(\\()(.*)","\\1\\3",res1[open_paranthesis_effect,"critical_effect"])
  res <- res1
  cremove = c("row_id","study_id","study_reference","dosing_regime","duration",
            "uncertainty_factors","threshold_endpoint","trv_derivation","cancer_class","study_duration_qualifier")
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
  res$source_url = "source_url"
  res$subsource_url = "subsource_url"
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
