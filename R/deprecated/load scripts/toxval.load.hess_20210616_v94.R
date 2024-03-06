#--------------------------------------------------------------------------------------
#' Load the HESS data from toxval_source to toxval
#'
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source.db The source database to use.
#' @param log If TRUE, send output to a log file
#' @export
#--------------------------------------------------------------------------------------
toxval.load.hess <- function(toxval.db,source.db, log=F){
  printCurrentFunction(toxval.db)
  source <- "HESS"
  source_table = "source_hess"
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
  #extract toxval_subtype info from study_type
  res = res[res$toxval_numeric!='-',]
  res = res[res$toxval_numeric!='',]
  res1 <- res
  res1[grep("^TG.*\\:",res1$toxval_subtype), "toxval_subtype"] <- gsub("(.*)(\\:.*)","\\1",res1[grep("^TG.*\\:",res1$toxval_subtype), "toxval_subtype"])
  res1[grep("\\:|\\;",res1$toxval_subtype), "toxval_subtype"] <- gsub("(.*)(\\:|\\;\\s+)(.*)(\\)$)","\\3",res1[grep("\\:|\\;",res1$toxval_subtype), "toxval_subtype"])
  res1[grep("TG.*\\)+",res1$toxval_subtype), "toxval_subtype"] <- gsub("(.*\\s*)(TG.*)(\\))","\\2",res1[grep("TG.*\\)+",res1$toxval_subtype), "toxval_subtype"])
  res1[grep("TG.*\\)+\\s*\\(+",res1$toxval_subtype), "toxval_subtype"] <- gsub("(TG.*)(\\).*\\(.*)","\\1",res1[grep("TG.*\\)+\\s*\\(+",res1$toxval_subtype), "toxval_subtype"])
  res1[grep("^Repeated.*TG.*",res1$toxval_subtype), "toxval_subtype"] <- gsub("(.*)(TG.*$)","\\2",res1[grep("^Repeated.*TG.*",res1$toxval_subtype), "toxval_subtype"])
  res1[grep("OECD.*combined",res1$toxval_subtype, ignore.case = T), "toxval_subtype"] <- gsub("(.*OECD\\s*\\d+)(\\s*.*)","\\1",res1[grep("OECD.*combined",res1$toxval_subtype, ignore.case = T), "toxval_subtype"])
  res1[grep("TG|OECD",res1$toxval_subtype, invert = T), "toxval_subtype"] <- "-"

   # extracting duration value and units from study type
  res1[grep("\\(",res1$study_duration_value),"study_duration_value"] <- gsub("(.*)(\\(.*)","\\2",res1[grep("\\(",res1$study_duration_value),"study_duration_value"])
  res1[grep("\\(",res1$study_duration_units),"study_duration_units"] <- gsub("(.*)(\\(.*)","\\2",res1[grep("\\(",res1$study_duration_units),"study_duration_units"])
  res1[grep("\\d+\\-*(hour|day|week|year)",res1$study_duration_value, ignore.case = T),"study_duration_value"] <- gsub("(.*)(\\b\\d+\\-*(hour|day|week|year)\\b)(.*)","\\2",res1[grep("\\d+\\-*(hour|day|week|year)",res1$study_duration_value, ignore.case = T),"study_duration_value"])
  res1[grep("\\d+\\-*(hour|day|week|year)",res1$study_duration_units, ignore.case = T),"study_duration_units"] <- gsub("(.*)(\\b\\d+\\-*(hour|day|week|year)\\b)(.*)","\\2",res1[grep("\\d+\\-*(hour|day|week|year)",res1$study_duration_units, ignore.case = T),"study_duration_units"])
  res1[grep("\\(|day|week",res1$study_duration_value, invert = T),"study_duration_value"] <- NA
  res1[grep("\\(|day|week",res1$study_duration_units, invert = T),"study_duration_units"] <- "-"
  res1[grep("\\d+M", res1$study_duration_value, ignore.case = T),"study_duration_value"] <- gsub("(.*[^0-9]+)([0-9]+)(M.*)","\\2",res1[grep("\\d+M", res1$study_duration_value, ignore.case = T),"study_duration_value"])
  res1[grep("\\d+M", res1$study_duration_units, ignore.case = T),"study_duration_units"] <- "month"
  res1[grep("\\d+w|\\d+\\s*week|\\d+\\-*week|[^\\/]week", res1$study_duration_units, ignore.case = T),"study_duration_units"] <- "week"
  res1[grep("\\d+w|\\d+\\s*week|\\d+\\-*week|[^\\/]week", res1$study_duration_value, ignore.case = T),"study_duration_value"]<- gsub("(\\,.*$|\\;.*$|\\:.*$)","",res1[grep("\\d+w|\\d+\\s*week|\\d+\\-*week|[^\\/]week", res1$study_duration_value, ignore.case = T),"study_duration_value"])
  res1[grep("\\d+w|\\d+\\s*week|\\d+\\-*week|[^\\/]week", res1$study_duration_value, ignore.case = T),"study_duration_value"]<-gsub("[^0-9]+(\\d+)[^0-9]+","\\1",res1[grep("\\d+w|\\d+\\s*week|\\d+\\-*week|[^\\/]week", res1$study_duration_value, ignore.case = T),"study_duration_value"])
  res1[grep("\\d+\\-*week", res1$study_duration_value, ignore.case = T),"study_duration_value"] <-gsub("^(\\d+)(.*)","\\1",res1[grep("\\d+\\-*week", res1$study_duration_value, ignore.case = T),"study_duration_value"])
  res1[grep("\\d+d|\\d+\\s*day|\\d+\\-*day|days\\/|day\\/", res1$study_duration_units, ignore.case = T),"study_duration_units"] <- "day"
  res1[grep("\\d+d|\\d+\\s*day|\\d+\\-*day|days\\/|day\\/", res1$study_duration_value, ignore.case = T),"study_duration_value"] <- gsub("(.+?)(d.*)","\\1",res1[grep("\\d+d|\\d+\\s*day|\\d+\\-*day|days\\/|day\\/", res1$study_duration_value, ignore.case = T),"study_duration_value"])
  res1[grep("^\\(TG.*\\)$",res1$study_duration_value),"study_duration_value"] <- NA
  res1[grep("^\\(TG.*\\)$",res1$study_duration_units),"study_duration_units"] <- "-"
  res1[grep("TG",res1$study_duration_value),"study_duration_units"] <- "week"
  res1[grep("TG",res1$study_duration_value),"study_duration_value"] <- gsub("(^\\(+)(\\d+)(\\s+.*)","\\2", res1[grep("TG",res1$study_duration_value),"study_duration_value"])
  res1[grep("\\( male \\) days)",res1$study_duration_value),"study_duration_value"] <-gsub("(.*\\-)(\\d+)(.*)","\\2",res1[grep("\\( male \\) days)",res1$study_duration_value),"study_type"])
  res1[grep("\\( male \\) days)",res1$study_duration_units),"study_duration_units"] <- "day"
  res1[grep("\\-\\s*\\d+",res1$study_duration_value),"study_duration_value"] <- gsub("(.*\\-)(.*)","\\2",res1[grep("\\-\\s*\\d+",res1$study_duration_value),"study_duration_value"])
  res1[grep("\\-\\s*\\d+",res1$study_duration_units),"study_duration_units"] <- "-"
  res1[grep("d\\/w",res1$study_duration_value),"study_duration_value"] <- gsub("(^\\(\\s+)(\\d+)(\\s+.*)","\\2",res1[grep("W",res1$study_duration_value),"study_duration_value"])
  res1[grep("\\(407\\)",res1$study_duration_value),"study_duration_value"] <- gsub("(.*\\()(\\d+)(\\s*day.*)","\\2",res1[grep("\\(407\\)",res1$study_duration_value),"study_type"])
  res1[grep("\\(407\\)",res1$study_duration_units),"study_duration_units"] <- gsub("(.*\\()(\\d+)(\\s*)(day)(.*)","\\4",res1[grep("\\(407\\)",res1$study_duration_units),"study_type"])
  res1$study_duration_value <- gsub("[^0-9]+","",res1$study_duration_value)
  res1[grep("weeeks",res1$study_duration_units),"study_duration_units"] <- "week"
  res1[grep("R14",res1$study_duration_units),"study_duration_value"] <- gsub("(.*\\()(\\d+)(\\s+)(day)(.*)","\\2",res1[grep("R14",res1$study_duration_units),"study_type"])
  res1[grep("R14",res1$study_duration_units),"study_duration_units"] <- gsub("(.*\\()(\\d+)(\\s+)(day)(.*)","\\4",res1[grep("R14",res1$study_duration_units),"study_type"])
  res1[grep("W",res1$study_duration_units),"study_duration_units"] <- "week"
  res1$study_duration_value <- as.numeric(res1$study_duration_value)

  # species and strain clean up
  res1[grep("Rat|Ra t",res1$species, ignore.case = T),"species"] <- "rat"
  res1[grep("Rat|Ra t",res1$strain, ignore.case = T),"strain"] <- gsub("Rat|Ra t","",res1[grep("Rat|Ra t",res1$strain, ignore.case = T),"strain"])
  res1$strain <- gsub("(^r*\\s+\\(+)(.*)(\\)+.*)","\\2",res1$strain)
  res1$strain <- gsub("^\\s+|\\s+$","",res1$strain)
  res1$strain <- gsub("(^\\()(.*)(\\)$)","\\2",res1$strain)
  res1$strain <- gsub("(.*)(\\).*\\(.*)","\\1",res1$strain)
  res1$strain <- gsub("(.*)(\\,*\\s+rats|male.*)","\\1",res1$strain)
  res1$strain <- gsub("(.*)(\\,\\s+$)","\\1",res1$strain)
  res1[grep("\\([A-Za-z]+$",res1$strain),"strain"] <- paste(res1[grep("\\([A-Za-z]+$",res1$strain),"strain"],")", sep = "")
  res1$strain <- gsub("^$","-",res1$strain)
  res1[grep("\\(", res1$exposure_route),"exposure_route"] <- gsub("(.*)(\\s*\\(.*\\))","\\1",res1[grep("\\(", res1$exposure_route),"exposure_route"])
  res1$exposure_route <- gsub("\\s+$","",res1$exposure_route)
  res1$exposure_route <- gsub("\\*$","",res1$exposure_route)
  res1[grep("\\(", res1$exposure_method),"exposure_method"] <- gsub("(\\s+\\(5days/week\\))","",res1[grep("\\(", res1$exposure_method),"exposure_method"])
  res1[grep("\\(", res1$exposure_method),"exposure_method"] <- gsub("(.*\\s*\\()(.*)(\\))","\\2",res1[grep("\\(", res1$exposure_method),"exposure_method"])
  res1$source_url <- "https://www.nite.go.jp/en/chem/qsar/hess_update-e.html"
  res1[grep("^\\d+\\-day",res1$study_type),"study_type"] <- gsub("(^\\d+\\-day\\s+)(.*)","\\2",res1[grep("^\\d+\\-day",res1$study_type),"study_type"])
  res1[grep("^Repeated\\s*\\-*dose",res1$study_type, ignore.case = T),"study_type"] <- "subchronic"
  res1[grep("^Combined",res1$study_type, ignore.case = T),"study_type"] <- gsub("([^\\(])(\\s*\\(+.*)","\\1",res1[grep("^Combined",res1$study_type, ignore.case = T),"study_type"])
  res1[grep("\\:",res1$study_type),"study_type"] <- gsub("(.*\\:)(.*)","\\2",res1[grep("\\:",res1$study_type),"study_type"])
  res1[grep("\\(",res1$study_type),"study_type"] <- gsub("(\\(.*\\))","",res1[grep("\\(",res1$study_type),"study_type"])
  res1[grep("OECD",res1$study_type),"study_type"] <- gsub("(.*OECD\\s+\\d+\\s+)(.*)","\\2",res1[grep("OECD",res1$study_type),"study_type"])
  res1$study_type <- gsub("^\\s+|\\s+$","",res1$study_type)
  res1$study_type <- gsub("^$","-",res1$study_type)
  res1[grep("^\\d+",res1$toxval_numeric),"toxval_numeric"] <- gsub("(^\\d+\\.*\\d*)(\\s*.*)","\\1",res1[grep("^\\d+",res1$toxval_numeric),"toxval_numeric"])
  res1[grep("^[^[:alnum:]]+",res1$toxval_numeric),"toxval_numeric"] <- gsub("(^[^[:alnum:]]\\s*)(\\d+\\.*\\d*)(\\s*.*)","\\2",res1[grep("^[^[:alnum:]]+",res1$toxval_numeric),"toxval_numeric"])
  res1[grep("^[^[:alnum:]]+",res1$toxval_numeric_qualifier),"toxval_numeric_qualifier"] <- gsub("(^[^[:alnum:]])(\\s*\\d+\\.*\\d*)(\\s*.*)","\\1",res1[grep("^[^[:alnum:]]+",res1$toxval_numeric_qualifier),"toxval_numeric_qualifier"])
  res1[grep("^(\\-)",res1$toxval_numeric),"toxval_numeric"] <- NA
  res1[grep("^(\\-)",res1$toxval_numeric_qualifier),"toxval_numeric_qualifier"] <- "-"
  min_val <- function(x) if(all(is.na(x))) NA else min(x,na.rm = T)
  getmin_val <- function(col) str_extract_all(col, pattern = "[0-9\\.]+") %>%
    lapply(.,function(x)min_val(as.numeric(x)) ) %>%
    unlist()

  res1[grep("^[a-zA-Z]+\\s*[a-zA-Z]*\\s*\\:",res1$toxval_numeric),"toxval_numeric"] <- getmin_val(res1[grep("^[a-zA-Z]+\\s*[a-zA-Z]*\\s*\\:",res1$toxval_numeric),"toxval_numeric"])
  res1[grep("^(ca\\.)",res1$toxval_numeric, ignore.case = T),"toxval_numeric"] <- gsub("(ca\\.\\s+)(\\d+)(\\s+.*)","\\2",res1[grep("^(ca\\.)",res1$toxval_numeric_qualifier, ignore.case = T),"toxval_numeric"])
  res1[grep("^(ca\\.)",res1$toxval_numeric_qualifier, ignore.case = T),"toxval_numeric_qualifier"] <- gsub("(^ca\\.)(\\s+.*)","\\1",res1[grep("^(ca\\.)",res1$toxval_numeric_qualifier, ignore.case = T),"toxval_numeric_qualifier"])
  # no description and not reported qalifier as hyphen
  res1[grep("^no", res1$toxval_numeric_qualifier, ignore.case = T), "toxval_numeric_qualifier"] <- "-"
  # no description and not reported units values as hyphen
  res1[grep("^no", res1$toxval_units, ignore.case = T), "toxval_units"] <- "-"
  # no description and not reported units values as hyphen
  res1[grep("^no", res1$toxval_numeric, ignore.case = T), "toxval_numeric"] <- NA
  res1[grep("^male female\\s+\\:\\s+[^[:alnum:]]", res1$toxval_numeric_qualifier, ignore.case = T), "toxval_numeric_qualifier"] <- gsub("(^male female\\s+\\:\\s+)([^[:alnum:]])(.*)","\\2",res1[grep("^male female\\s+\\:\\s+[^[:alnum:]]", res1$toxval_numeric_qualifier, ignore.case = T), "toxval_numeric_qualifier"])
  res1[grep("\\(", res1$toxval_numeric_qualifier, ignore.case = T), "toxval_numeric_qualifier"] <- "="
  res1[grep("\\;", res1$toxval_numeric), "toxval_numeric"] <- getmin_val(res1[grep("\\;", res1$toxval_numeric), "toxval_numeric"])
  res1[grep("mg/kg/day", res1$toxval_numeric), "toxval_numeric"] <- NA
  res1[which(res1$toxval_numeric == ""), "toxval_numeric"] <- NA
  res1$toxval_numeric <- as.numeric(res1$toxval_numeric)
  res1[grep("<", res1$toxval_numeric_qualifier, ignore.case = T), "toxval_numeric_qualifier"] <- "<"
  res1[grep("\\?", res1$toxval_numeric_qualifier), "toxval_numeric_qualifier"] <- ">="
  res1[grep("\\d+", res1$toxval_numeric_qualifier), "toxval_numeric_qualifier"] <- "="
  res1[which(res1$toxval_numeric_qualifier == ""), "toxval_numeric_qualifier"] <- "-"

  # value with only unit(mg/kg/day), has no numeric val or qualifier
  res1[which(res1$toxval_numeric_qualifier == "mg/kg/day"), "toxval_numeric_qualifier"] <- "-"

  #res1[grep("^\\d+", res1$toxval_numeric), "toxval_numeric"] <- gsub("(^\\d+\\.*\\d*)(\\s*)(.*)","\\1",res1[grep("^\\d+", res1$toxval_numeric), "toxval_numeric"])
  res1[grep("^\\d+", res1$toxval_units), "toxval_units"] <- gsub("(^\\d+\\.*\\d*)(\\s*)(.*)","\\3",res1[grep("^\\d+", res1$toxval_units), "toxval_units"])

  # <2.5 ( male : 230,  female : 290 mg/kg) units unkown as hyphen
  res1[grep("^[^[:alnum:]]\\d+\\.*\\d*\\s*\\(", res1$toxval_units), "toxval_units"] <- "-"
  res1[grep("titer", res1$toxval_units), "toxval_units"] <- gsub("(.*\\:\\s+\\d+\\s+)(.*)(\\(.*\\))(.*$)","\\2\\4", res1[grep("titer", res1$toxval_units), "toxval_units"])
  res1[grep("\\(", res1$toxval_units), "toxval_units"] <- gsub("(.*?)(\\(.*)","\\1",res1[grep("\\(", res1$toxval_units), "toxval_units"])
  res1$toxval_units <- gsub("\\s+$", "", res1$toxval_units)
  res1[grep("mg/kg/day$", res1$toxval_units), "toxval_units"] <- "mg/kg/day"
  res1[grep("ppm", res1$toxval_units), "toxval_units"] <- "ppm"
  res1[grep("mg/kg$", res1$toxval_units), "toxval_units"] <- "mg/kg"
  res1[grep("\\%$", res1$toxval_units), "toxval_units"] <- "%"
  res1[grep("\\d+$", res1$toxval_units), "toxval_units"] <- "-"
  res1$toxval_units <- gsub("\\-$", "", res1$toxval_units)
  res1[grep("\\d+", res1$toxval_units), "toxval_units"] <- gsub("(.*\\d+\\s+)(.*)","\\2",res1[grep("\\d+", res1$toxval_units), "toxval_units"])
  res1[which(res1$toxval_units == ""), "toxval_units"] <- "-"
  res <- res1
  cremove = c("curator_note","purity","institution",
              "klimisch_reliability","associated_publication","document_note",
              "document_identifier","subject_type","route",
              "vehicle","dose_level","deaths",
              "reported_noel","reported_noael","reported_loel",
              "reported_loael","clinical_observation","functional_observational_battery",
              "body_weight_changes","food_consumption","water_consumption",
              "urinalysis","hematology","blood_chemistry",
              "thyroid_hormone","absolute_organ_weight","relative_organ_weight",
              "organ_weight","necropsy","macroscopic_finding",
              "histopathology","reproductive_tissue_evaluation","estrous_cycle_characterization",
              "bone_marrow_cellularity_counts","liver_biochemistry","reproductive_endpoint",
              "other_findings"    )
  res = res[ , !(names(res) %in% cremove)]
  res = res[!is.na(res$toxval_numeric),]

  # #####################################################################
  # cat("fix repeat dose study type\n")
  # #####################################################################
  # x = res[res$study_type=="repeat dose",]
  # y = res[res$study_type!="repeat dose",]
  #
  #
  # browser()
  #
  # x[is.element(x$study_duration_units,"Years"),"study_type"] = "chronic"
  # x[is.element(x$study_duration_units,"Hours"),"study_type"] = "acute"
  # x[is.element(x$study_duration_units,"Minutes"),"study_type"] = "acute"
  # x = x[is.element(x$study_type,"repeat-dose"),]
  # x1 = x[is.element(x$study_duration_units,"Weeks"),]
  # x2 = x[is.element(x$study_duration_units,"Days"),]
  # x3 = x[is.element(x$study_duration_units,"Months"),]
  # x4 = x[is.element(x$study_duration_units,""),]
  # x5 = x[is.element(x$study_duration_units,"Other"),]
  #
  # x1a = x1[!is.na(x1$study_duration_value),]
  # x1b = x1[is.na(x1$study_duration_value),]
  # x1a$study_type = "chronic"
  # x1a[x1a$study_duration_value<14,"study_type"] = "subchronic"
  # x1a[x1a$study_duration_value<4,"study_type"] = "subacute"
  #
  # x2a = x2[!is.na(x2$study_duration_value),]
  # x2b = x2[is.na(x2$study_duration_value),]
  # x2a$study_type = "chronic"
  # x2a[x2a$study_duration_value<100,"study_type"] = "subchronic"
  # x2a[x2a$study_duration_value<28,"study_type"] = "subacute"
  #
  # x3a = x3[!is.na(x3$study_duration_value),]
  # x3b = x3[is.na(x3$study_duration_value),]
  # x3a$study_type = "chronic"
  # x3a[x3a$study_duration_value<14,"study_type"] = "subchronic"
  # x3a[x3a$study_duration_value<4,"study_type"] = "subacute"
  #
  # res = rbind(x1a,x1b,x2a,x2b,x3a,x3b,x4,x5,y)

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
  res$source_url = "https://www.nite.go.jp/en/chem/qsar/hess-e.html"
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
