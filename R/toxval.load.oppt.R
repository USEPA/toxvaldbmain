#--------------------------------------------------------------------------------------
#' Load new_oppt_table from toxval_source to toxval
#'
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source.db The source database to use.
#' @param log If TRUE, send output to a log file
#'
#' @export
#--------------------------------------------------------------------------------------
toxval.load.oppt <- function(toxval.db, source.db, log=F){
  printCurrentFunction(toxval.db)
  source <- "EPA OPPT"
  source_table = "source_oppt"
  verbose = log
  #####################################################################
  cat("start output log, log files for each source can be accessed from output_log folder\n")
  #####################################################################
  if(log) {
    con1 = file.path(toxval.config()$datapath,paste0(source,"_",Sys.Date(),".log"))
    con1 = logr::log_open(con1)
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
  res <- res[!(is.na(res$toxval_numeric)| res$toxval_numeric == ""),]

  res <- res[is.element(res[,"study_type"],c("Acute Dermal Toxicity",
                                             "Acute Inhalation Toxicity",
                                             "Acute Oral Toxicity",
                                             "Developmental Toxicity",
                                             "Repeated-Dose Toxicity",
                                             "Reproductive Toxicity",
                                             "Reproductive/Developmental Toxicity",
                                             "acute","chronic","reproduction","repeat dose","subacute","subchronic",
                                             "reproduction developmental")),]

  res[is.element(res,"#N/A")] <- "-"
  res$document_name = res$srcf
  res = res[ , !(names(res) %in% c("srcf"))]
  unique_types <-unique(res$toxval_type)
  unique_types <- unique(gsub("^\\s+|\\s+$", "", unique_types))
  toxval_type_values <- grep("^[A-Z]{0,6}[0-9]{0,6}$",unique_types ,value = T)
  non_toxval_type_values <- grep(paste(toxval_type_values, collapse = "|"), res$toxval_type, value = T, invert = T)
  res$toxval_type[res$toxval_type %in% non_toxval_type_values] <- "-"

  #####################################################################
  cat("remove entries with character values which are 'not established',
      'no adequate data', 'not reported', 'not determined' from toxval_numeric\n")
  ####################################################################
  res[grep("^[a-zA-Z]+\\s+[a-zA-Z]+\\.*$", res$toxval_numeric, ignore.case = T),"toxval_numeric"] <- ""
  res[grep("\\bno\\b|\\bnot\\b", res$toxval_numeric, ignore.case = T),"toxval_numeric"] <- ""
  res[grep("^not established", res$toxval_numeric, ignore.case = T),"toxval_numeric"] <- ""

  #####################################################################
  cat("convert percentages into numeric values in toxval_numeric and apply % as unit in corresponding toxval_units\n")
  ####################################################################
  percent_rows <- grep(".*%", res$toxval_numeric, value = T)
  res$toxval_units[which(res$toxval_numeric %in% percent_rows)] <- "%"
  percent_num_values <- as.numeric(sub("%","", percent_rows))
  res$toxval_numeric[res$toxval_numeric %in% percent_rows] <- percent_num_values

  #####################################################################
  cat("removing trailing special characters from toxval_numeric values\n")
  ####################################################################
  res$toxval_numeric <- gsub("\\s+$", "",res$toxval_numeric)

  res[grep(".*[^a-zA-Z0-9]+$", res$toxval_numeric),"toxval_numeric"] <- gsub("(\\d+\\.*\\d*)(\\s*[^a-zA-Z0-9]+$)","\\1",res[grep(".*[^a-zA-Z0-9]+$", res$toxval_numeric),"toxval_numeric"])
  #####################################################################
  cat("fix range values in toxval_numeric\n")
  ####################################################################
  res[grep("\\d+\\.*\\d*\\s*\\-\\s*\\d*\\.*\\d*$", res$toxval_numeric),"toxval_numeric"] <- gsub("(^\\d+\\.*\\d*)(\\s*\\-\\s*\\d*\\.*\\d*$)", "\\1", res[grep("\\d+\\.*\\d*\\s*\\-\\s*\\d*\\.*\\d*$", res$toxval_numeric),"toxval_numeric"])
  res[grep("\\d+\\.*\\d*\\s*\\/\\s*\\d*\\.*\\d*$", res$toxval_numeric),"toxval_numeric"] <- gsub("(^\\d+\\.*\\d*)(\\s*\\/\\s*\\d*\\.*\\d*$)", "\\1", res[grep("\\d+\\.*\\d*\\s*\\/\\s*\\d*\\.*\\d*$", res$toxval_numeric),"toxval_numeric"])

  res[grep("\\d+\\.*\\d*\\s*XXX.*\\s*\\d*\\.*\\d*$", res$toxval_numeric),"toxval_numeric"] <- gsub("(^\\d+\\.*\\d*)(\\s*XXX.*\\s*\\d*\\.*\\d*$)", "\\1", res[grep("\\d+\\.*\\d*\\s*XXX.*\\s*\\d*\\.*\\d*$", res$toxval_numeric),"toxval_numeric"])
  # 25013-15- 4 and 108-10- 1 in toxval_numeric, numeric data aquired from toxval_type field

  res[which(res$toxval_numeric == "25013-15- 4" & res$toxval_type_original == "In an NTP assay, mouse lymphoma cells"),"toxval_numeric"] <- ""

  res[grep("25013-15- 4", res$toxval_numeric),"toxval_type"] <- "lowest cytotoxic concentration"
  res[grep("25013-15- 4", res$toxval_numeric),"toxval_units"] <- "ug/plate"
  res[grep("25013-15- 4", res$toxval_numeric),"toxval_numeric"] <- "333"


  res[grep("108-10- 1", res$toxval_numeric),"toxval_units"] <- "ug/plate"
  res[grep("108-10- 1", res$toxval_numeric),"toxval_numeric"] <- "100"
  #####################################################################
  cat("micron unit values\n")
  ####################################################################
  res[grep("\\d+\\.*\\d*\\s*XXX.*\\s*$", res$toxval_numeric),"toxval_units"] <- "u"
  res[grep("\\d+\\.*\\d*\\s*XXX.*\\s*$", res$toxval_numeric),"toxval_numeric"] <- gsub("(\\d+\\.*\\d*)(\\s*XXX.*\\s*$)","\\1",res[grep("\\d+\\.*\\d*\\s*XXX.*\\s*$", res$toxval_numeric),"toxval_numeric"])

  #####################################################################
  cat("Remove all comma within values\n")
  ####################################################################
  res$toxval_numeric <- gsub("^\\s+|\\s+$", "",res$toxval_numeric)
  res[grep("^\\d+\\,\\d+$", res$toxval_numeric),"toxval_numeric"] <- gsub("(.*)(\\,)(.*)", "\\1\\3", res[grep("^\\d+\\,\\d+$", res$toxval_numeric),"toxval_numeric"])


  #####################################################################
  cat("hyphen to empty in toxval_numeric\n")
  ####################################################################
  res$toxval_numeric <- gsub("^\\-$", "", res$toxval_numeric)

  # #####################################################################
  # cat("find casrn and name values represented in toxval_numeric and replace them with empty\n")
  # ####################################################################
  # res[grep(".*\\-.*\\-.*",res$toxval_numeric),"toxval_numeric"] <- ""
  # #####################################################################
  # cat("replace the toxval_numeric cell values containing multiple values seperated by backslash,
  #     standard hyphens and non standard hyphens with the lower concentration value \n")
  # ####################################################################
  # res$toxval_numeric <- gsub("^\\s+|\\s+$", "", res$toxval_numeric)
  # res[grep("^0\\.0070 \\/ 0\\.0065$",res$toxval_numeric),"toxval_numeric"] <- 0.0065
  # res[grep("^\\d+$|^\\d+\\.*\\d+$",res$toxval_numeric, invert = T),"toxval_numeric"] <- gsub("(^\\d+\\.*\\d*)(.*)(\\d+\\.*\\d*$)","\\1",res[grep("^\\d+$|^\\d+\\.*\\d+$",res$toxval_numeric, invert = T),"toxval_numeric"])
  # res[grep(".*u$",res$toxval_numeric),"toxval_units"] <- gsub("(.*\\s+)(u$)","\\2",res[grep(".*u$",res$toxval_numeric),"toxval_numeric"])
  # res[grep(".*u$",res$toxval_numeric),"toxval_numeric"] <- gsub("(.*)(\\s+u$)","\\1",res[grep(".*u$",res$toxval_numeric),"toxval_numeric"])
  #

  #####################################################################
  cat("Saturated concentration of 2,5-dihydrothiophene 1,1-dioxide in air at 25oC in toxval numeric field\n")
  ####################################################################

  res[grep("[a-zA-Z]+",res$toxval_numeric),"toxval_numeric"] <- ""

  #####################################################################
  cat("convert data types for toxval_numeric and study duration value as numeric\n")
  ####################################################################
  #browser()
  x1 = res$toxval_numeric
  x2 = gsub("~", "", x1)
  x3 = gsub(",", "", x2)
  x4 = gsub(" ", "", x3)
  x5 = as.numeric(x4)
  res$toxval_numeric = x5
  res$study_duration_value[res$study_duration_value == "-"] <- ""
  res$study_duration_value <- as.numeric(res$study_duration_value)
  res["oppt_id"] <- c(1:length(res[,1]))
  res <- res[c("oppt_id",names(res[-32]))]

  #####################################################################
  cat("get the exposure method, toxval_type\n")
  ####################################################################
  new_res <- res[,-1]
  new_res$exposure_method <- "-"
  new_res$risk_assessment_class <- "-"
  for(i in 1:nrow(new_res)) {
    for(col in names(new_res)) {
      new_res[i,col] <- stringr::str_trim(new_res[i,col],"both")
    }
    er0 <- new_res[i,"exposure_route"]
    st0 <- new_res[i,"study_type"]
    if(is.element(er0,c("vapor","gavage","drinking water","10% solution","liquid diet"))) new_res[i,"exposure_method"] <- er0
    if(st0=="Acute Dermal Toxicity") {new_res[i,"study_type"]<-"acute";new_res[i,"exposure_route"]<-"dermal";new_res[i,"risk_assessment_class"]<-"acute"}
    if(st0=="Acute Inhalation Toxicity") {new_res[i,"study_type"]<-"acute";new_res[i,"exposure_route"]<-"inhalation";new_res[i,"risk_assessment_class"]<-"acute"}
    if(st0=="Acute Oral Toxicity") {new_res[i,"study_type"]<-"acute";new_res[i,"exposure_route"]<-"oral";new_res[i,"risk_assessment_class"]<-"acute"}
    if(st0=="Developmental Toxicity") {new_res[i,"study_type"]<-"developmental";new_res[i,"risk_assessment_class"]<-"developmental"}
    if(st0=="Repeated-Dose Toxicity") {new_res[i,"study_type"]<-"repeat dose";new_res[i,"risk_assessment_class"]<-"repeat dose"}
    if(st0=="Reproductive Toxicity") {new_res[i,"study_type"]<-"reproductive";new_res[i,"risk_assessment_class"]<-"reproductive"}
    if(st0=="Reproductive/Developmental Toxicity") {new_res[i,"study_type"]<-"reproductive developmental";new_res[i,"risk_assessment_class"]<-"reproductive developmental"}
    if(is.na(new_res[i,"exposure_route"])) {new_res[i,"exposure_route"] <- "-"; new_res[i,"exposure_method"] <- "-"}
    else if(new_res[i,"exposure_route"]=="Oral - other") {new_res[i,"exposure_route"] <- "oral"; new_res[i,"exposure_method"] <- "other"}
    else if(new_res[i,"exposure_route"]=="Oral diet") {new_res[i,"exposure_route"] <- "oral"; new_res[i,"exposure_method"] <- "diet"}
    else if(new_res[i,"exposure_route"]=="Oral drinking water") {new_res[i,"exposure_route"] <- "oral"; new_res[i,"exposure_method"] <- "drinking water"}
    else if(new_res[i,"exposure_route"]=="Oral gavage") {new_res[i,"exposure_route"] <- "oral"; new_res[i,"exposure_method"] <- "gavage"}
    else if(new_res[i,"exposure_route"]=="Other") {new_res[i,"exposure_route"] <- "other"; new_res[i,"exposure_method"] <- "other"}

    tvt <- new_res[i,"toxval_type"]
    if(grepl("LD50", tvt)) tvt <- "LD50"
    else if(grepl("LC50", tvt)) tvt <- "LC50"
    else if(grepl("LOAEC", tvt)) tvt <- "LOAEC"
    else if(grepl("LOAEL", tvt)) tvt <- "LOAEL"
    else if(grepl("NOAEC", tvt)) tvt <- "NOAEC"
    else if(grepl("NOAEL", tvt)) tvt <- "NOAEL"
    new_res[i,"toxval_type"] <- tvt

    tvu <- new_res[i,"toxval_units"]
    if(grepl("mg/L", tvu, fixed=TRUE)) tvu <- "mg/L"
    else if(grepl("mg/kg-day", tvu, fixed=TRUE)) tvu <- "mg/kg-day"
    else if(grepl("ppm", tvu)) tvu <- "ppm"
    new_res[i,"toxval_units"] <- tvu

    if(grepl("sex", new_res[i,"sex"])) new_res[i,"sex"] <- "male and female"
  }
  res = new_res
  res = res[is.element(res$toxval_type,c("NOAEC","LOAEL","NOAEL","LD50","LC50","LOAEC")),]
  res = res[ , !(names(res) %in% c("toxval_numeric_qualifier2","toxval_numeric2","oppt_id.1"))]
  slist = unique(res$species)
  res$strain = res$species
  for(species in slist) {
    if(grepl("rabbit", species)) res[is.element(res$species,species),"species"] = "rabbit"
    else if(grepl("mouse|mice", species)) res[is.element(res$species,species),"species"] = "mouse"
    else if(grepl("Guinea pig", species)) res[is.element(res$species,species),"species"] = "guinea pig"
    else if(grepl("rat", species)) res[is.element(res$species,species),"species"] = "rat"
    else if(grepl("dog", species)) res[is.element(res$species,species),"species"] = "dog"
    else res[is.element(res$species,species),"species"] = "unknown"
  }

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
  refs$record_source_type = "website"
  refs$record_source_note = "to be cleaned up"
  refs$record_source_level = "primary (risk assessment values)"
  print(dim(res))

  #####################################################################
  cat("load res and refs to the database\n")
  #####################################################################
  res = unique(res)
  refs = unique(refs)
  res$datestamp = Sys.Date()
  res$source_table = source_table
  res$source_url = "https://nepis.epa.gov/"
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
    logr::log_close()
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
