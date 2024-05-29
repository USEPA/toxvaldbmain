#-------------------------------------------------------------------------------------
#' Load the DOD ERED data from toxval_source to toxval
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source.db The source database to use.
#' @param log If TRUE, send output to a log file
#' @export
#--------------------------------------------------------------------------------------
toxval.load.dod.ered <- function(toxval.db,source.db,log=F) {
  printCurrentFunction(toxval.db)

  #####################################################################
  cat("start output log, log files for each source can be accessed from output_log folder\n")
  #####################################################################
  source <- "DOD ERED"
  source_table = "source_dod_ered"
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
  names(res)[names(res)== "common_name"] <- "species_original"
  names(res)[names(res)== "data_source"] <- "long_ref"
  names(res)[names(res)== "data_year"] <- "year"
  res$toxval_numeric <- res$exposure_conc
  res$toxval_numeric_qualifier <- res$exposure_conc
  res$toxval_units <- res$exposure_units
  res$toxval_type <- res$risk
  res$study_duration_value <- res$study_duration
  res$study_duration_units <- res$study_duration
  names(res)[names(res)== "life_stage"] <- "lifestage"
  res <- generate.originals(toxval.db,res)

  print(names(res))
  names.list <- c("source_hash","study_type","species_original","name","casrn","media","exposure_route",
                  "exposure_conc","exposure_units" ,
                  "study_duration","risk","test_tissue_type","critical_effect","toxval_numeric",
                  "toxval_numeric_qualifier","toxval_units","toxval_type","long_ref","year","lifestage",
                  "study_duration_value","study_duration_units","study_type_original","media_original",
                  "exposure_route_original","critical_effect_original","year_original",
                  "toxval_numeric_original","toxval_numeric_qualifier_original","toxval_units_original",
                  "toxval_type_original","study_duration_value_original","study_duration_units_original",
                  "document_name","chemical_id","source")

  new_dod <- res[,(names(res)%in% names.list)]
  #####################################################################
  cat("combine organ information with critical effect\n")
  #####################################################################
  new_dod$critical_effect <- paste(new_dod$test_tissue_type,new_dod$critical_effect, sep = "|")
  #print(unique(new_dod$critical_effect))
  new_dod[grep("^N\\/R", new_dod$critical_effect), "critical_effect"] <- gsub("(^N\\/R\\|)(.*)","\\2",new_dod[grep("^N\\/R", new_dod$critical_effect), "critical_effect"])
  new_dod[grep("^N\\/A", new_dod$critical_effect), "critical_effect"] <- gsub("(^N\\/A\\|)(.*)","\\2",new_dod[grep("^N\\/A", new_dod$critical_effect), "critical_effect"])
  new_dod[grep("N\\/R$", new_dod$critical_effect), "critical_effect"] <- gsub("(\\|N\\/R$)(.*)","\\2",new_dod[grep("N\\/R$", new_dod$critical_effect), "critical_effect"])
  new_dod[grep("N\\/A$", new_dod$critical_effect), "critical_effect"] <- gsub("(\\|N\\/A$)(.*)","\\2",new_dod[grep("N\\/A$", new_dod$critical_effect), "critical_effect"])
  new_dod[grep("^N\\/R\\|N\\/A$|^N\\/A$", new_dod$critical_effect), "critical_effect"] <- "-"
  #print(unique(new_dod$critical_effect))
  new_dod <- new_dod[ , !(names(new_dod) %in% "test_tissue_type")]

  #####################################################################
  cat("create fields subsource and source_url in new_dod table\n")
  #####################################################################
  new_dod[,"subsource"] <- "USACE_ERDC_ERED_database_12_07_2018"
  new_dod[,"source_url"] <- "https://ered.el.erdc.dren.mil/"
  #####################################################################
  cat("Get unique risk values\n")
  #####################################################################
  risk_names <- gsub('[0-9]+','',as.character(new_dod$risk))
  dod_risk <- unique(gsub('\\s+',"",unique((risk_names))))
  #####################################################################
  cat("create a copy of study_duration and assign duration units for long term studies (NOEC,LOEC) as d (days)\n")
  #####################################################################
  new_dod$new_study_duration <- new_dod$study_duration
  dod_noec_loec <- new_dod[grep("NOEC|LOEC", new_dod$risk), c("risk","new_study_duration")]
  dod_noec_loec <- dod_noec_loec[!is.na(dod_noec_loec$new_study_duration),]
  dod_noec_loec$new_study_duration <- gsub(".*[a-zA-Z]+(|\\.)$","",dod_noec_loec$new_study_duration)
  dod_noec_loec <- dod_noec_loec[!(is.na(dod_noec_loec$new_study_duration)| dod_noec_loec$new_study_duration==""),]
  noec_loec_days <- paste(dod_noec_loec$new_study_duration, "d", sep = " ")
  new_dod[,"new_study_duration"][rownames(new_dod) %in% rownames(dod_noec_loec)] <- noec_loec_days
  #####################################################################
  cat("Assign duration units for short term studies (ED,LC) as d (days) if duration value > 4 and as h (hours) if <= 4\n")
  #####################################################################
  dod_ED_LC <- new_dod[grep("ED|LC", new_dod$risk), c("risk","new_study_duration")]
  dod_ED_LC$new_study_duration <- gsub(".*[a-zA-Z]+(|\\.)$","",dod_ED_LC$new_study_duration)
  dod_ED_LC <- dod_ED_LC[!(is.na(dod_ED_LC$new_study_duration)| dod_ED_LC$new_study_duration==""),]
  dod_ED_LC$new_study_duration <-gsub("\\(.*\\)","",dod_ED_LC$new_study_duration)
  dod_ED_LC[2] <- lapply(dod_ED_LC[2], as.numeric)
  dod_ED_LC <- dod_ED_LC[!(is.na(dod_ED_LC$new_study_duration)),]
  ed_lc_hours <- dod_ED_LC[which(dod_ED_LC[,2] > 4),]
  ed_lc_h <-  paste(ed_lc_hours$new_study_duration, "h", sep = " ")
  ed_lc_days <- dod_ED_LC[which(dod_ED_LC[,2] <= 4),]
  ed_lc_d <-  paste(ed_lc_days$new_study_duration, "d", sep = " ")
  dod_ED_LC[,2][rownames(dod_ED_LC) %in% rownames(ed_lc_hours)] <- ed_lc_h
  dod_ED_LC[,2][rownames(dod_ED_LC) %in% rownames(ed_lc_days)] <- ed_lc_d
  new_dod[,"new_study_duration"][rownames(new_dod) %in% rownames(dod_ED_LC)] <- dod_ED_LC$new_study_duration
  #####################################################################
  cat("Assign duration units for the rest of the short term studies as h (hours)\n")
  #####################################################################
  short_term_risk <- gsub("NOEC|LOEC|ED|LC", "" , dod_risk)
  short_term_risk <- short_term_risk[!(is.na(short_term_risk)| short_term_risk =="")]
  short_term_risk <- paste0("\\b", short_term_risk, "\\b")
  dod_short_term <- new_dod[grep(paste(short_term_risk, collapse = "|"), new_dod$risk), c("risk","new_study_duration")]
  dod_short_term$new_study_duration <- gsub(".*[a-zA-Z]+(|\\.)$","",dod_short_term$new_study_duration)
  dod_short_term <- dod_short_term[!(is.na(dod_short_term$new_study_duration)| dod_short_term$new_study_duration==""),]
  all_short_term <- paste(dod_short_term$new_study_duration, "h", sep = " ")
  new_dod[,"new_study_duration"][rownames(new_dod) %in% rownames(dod_short_term)] <- all_short_term
  #####################################################################
  cat("create fields study_duration_values and study_duration_units\n")
  #####################################################################
  new_dod[,"study_duration_value"] <- "-"
  new_dod[,"study_duration_units"] <- "-"
  sd <- gsub("\\s+","",as.character(new_dod$new_study_duration))
  num_sd <- gsub('[a-zA-Z]+|\\/','',sd)
  char_sd <-gsub('[0-9]+|\\.|[0-9]+\\s*[a-zA-Z]+\\s*[0-9]+|\\;|\\b(st)|\\(|\\)','',sd)
  new_dod$study_duration_value <- num_sd
  new_dod$study_duration_units <- char_sd

  #####################################################################
  cat("fix the cells with multiple values which had characters represented within them eg.4 of 6mo \n")
  #####################################################################
  sdv = unique(new_dod$study_duration_value)
  for(i in 1:length(sdv)) {
    val0 = sdv[i]
    for(ccar in c("(","-",";")) {
      if(grepl(ccar, val0, fixed = TRUE)) {
        sccar = ccar
        if(ccar=="(") sccar="\\("
        val1 = str_split(val0,sccar)[[1]][1]
         new_dod[is.element(new_dod$study_duration_value,val0),"study_duration_value"] = val1
      }
    }
    if(val0=="<1") new_dod[is.element(new_dod$study_duration_value,val0),"study_duration_value"] = "1"
    if(val0=="96114)") new_dod[is.element(new_dod$study_duration_value,val0),"study_duration_value"] = "196114"
  }

  #####################################################################
  cat(" Convert Exposure concentration data type to numeric, keeping the lower conc values for occurences of multilples within each cell\n")
  #####################################################################
  non_num_expo_conc <- grep('.*\\-|<', new_dod$exposure_conc, value =T)
  lower_conc_value <- gsub('\\-.*|<', '', non_num_expo_conc)
  new_dod[,"exposure_conc_qualifier"] <- "-"
  qual_val <- grep('<', new_dod$exposure_conc, value =T)
  qual_symbols <- gsub('\\d|\\.', '', qual_val)
  new_dod$exposure_conc_qualifier[new_dod$exposure_conc %in% qual_val] <- qual_symbols
  new_dod$exposure_conc[new_dod$exposure_conc %in% non_num_expo_conc] <- lower_conc_value
  new_dod$exposure_conc[is.na(new_dod$exposure_conc)] <- ""
  new_dod[,"exposure_conc"] <- sapply(new_dod[,"exposure_conc"], as.numeric)
  new_dod$toxval_numeric <- new_dod$exposure_conc
  new_dod$toxval_numeric_qualifier <- new_dod$exposure_conc_qualifier
  new_dod$toxval_units <- new_dod$exposure_units
  new_dod$toxval_type <- new_dod$risk
  new_dod$toxval_type <- gsub("\\s+","", new_dod$toxval_type)
  new_dod$toxval_type <-  gsub(".*\\/.*", "-",new_dod$toxval_type)
  #####################################################################
  cat(" fix inconsistencies in study duration values and units to assign proper data types\n")
  #####################################################################
  new_dod$study_duration_value <- gsub(".*of|.*to|\\+.*","",new_dod$study_duration_value)
  new_dod$study_duration_value <- gsub("\\-\\(","",new_dod$study_duration_value)
  new_dod$study_duration_value <-gsub("\\(.*\\)","", new_dod$study_duration_value)
  new_dod$study_duration_value <-gsub("\\-{2}","",new_dod$study_duration_value)
  new_dod$study_duration_value <-gsub(".*\\-|.*;|\\.{2}|\\)","",new_dod$study_duration_value)
  new_dod$study_duration_value <-gsub("<1","1",new_dod$study_duration_value)
  new_dod$study_duration_value <-gsub("96114","96",new_dod$study_duration_value)
  new_dod$study_duration_value <- as.numeric(new_dod$study_duration_value)
  new_dod$study_duration_units <- gsub("\\bh\\b","hours", new_dod$study_duration_units)
  new_dod$study_duration_units <- gsub("\\bd\\b","days", new_dod$study_duration_units)
  new_dod$study_duration_units <- gsub("\\bmo\\b","months", new_dod$study_duration_units)
  new_dod$study_duration_units <- gsub("\\bwk\\b","weeks", new_dod$study_duration_units)
  names(new_dod)[names(new_dod)== "species"] <- "species_scientific_name"

  new_dod[new_dod$study_duration_units=="days+hours","study_duration_units"] = "days"
  new_dod[new_dod$study_duration_units=="egg-swimupMaxdh","study_duration_units"] = "-"
  new_dod[new_dod$study_duration_units=="egg-swimupmaxdh","study_duration_units"] = "-"
  new_dod[new_dod$study_duration_units=="-hours","study_duration_units"] = "hours"
  new_dod[new_dod$study_duration_units=="dph","study_duration_units"] = "days post-hatch"
  new_dod[new_dod$study_duration_units=="m","study_duration_units"] = "minutes"
  new_dod[new_dod$study_duration_units=="crab","study_duration_units"] = "-"
  new_dod[new_dod$study_duration_units=="<days","study_duration_units"] = "days"
  new_dod[new_dod$study_duration_units=="snapshot","study_duration_units"] = "-"
  new_dod[new_dod$study_duration_units=="-days","study_duration_units"] = "days"

  res <- new_dod
  names.list <- c("source_hash","study_type","species_original","lifestage","name","casrn","media","exposure_route" ,
                  "critical_effect","toxval_numeric","toxval_numeric_qualifier","toxval_units","toxval_type","long_ref","year",
                  "study_duration_value","study_duration_units","study_type_original","media_original","exposure_route_original","critical_effect_original","year_original",
                  "toxval_numeric_original","toxval_numeric_qualifier_original","toxval_units_original","toxval_type_original","study_duration_value_original","study_duration_units_original",
                  "subsource","source_url","document_name","chemical_id","source")

  res <- res[,(names(res)%in% names.list)]

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
  res = res[!is.na(res$toxval_numeric),]
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
  res$source_url = "https://ered.el.erdc.dren.mil/"
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
