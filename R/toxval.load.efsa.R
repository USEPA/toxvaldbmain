#-------------------------------------------------------------------------------------
#' Load EFSA data from toxval_source to toxval
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source.db The source database to use.
#' @param log If TRUE, send output to a log file
#' @export
#--------------------------------------------------------------------------------------
toxval.load.efsa <- function(toxval.db,source.db, log=F) {
  printCurrentFunction(toxval.db)

  #####################################################################
  cat("start output log, log files for each source can be accessed from output_log folder\n")
  #####################################################################
  source <- "EFSA"
  source_table = "source_efsa"
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
  res = unique(res)
  print(dim(res))

  #####################################################################
  cat("Add the code from the original version from Aswani\n")
  #####################################################################


  nlist = c("source","subsource",
            "chemical_id",
            "toxval_type","toxval_numeric_qualifier","toxval_numeric","toxval_units",
            "exposure_method","exposure_route",
            "study_type",
            "study_duration_value","study_duration_units",
            "species_original","strain","sex",
            "critical_effect",
            "qc_status","source_hash",
            "year",
            "title","doi","guideline","publication_date","document_name"
  )

  res = res[,nlist]
  res$long_ref = paste(res$title,res$publication_date)
  res$url = paste0("https://doi.org/",res$doi)

  print(nrow(res))
  res = res[is.element(res$study_type,c("acute","chronic/long-term","subchronic","reproductive","short-term","human")),]
  print(nrow(res))

  # [1] "adoption_date"            "adoptiondate"             "basis_code"               "basis_id"                 "casrn"
  # [6] "chemical"                               "com_ecsubinvententryref"  "com_id"                   "com_structureshown"
  # [11] "com_type"                 "comgroup_id"              "comp_value"               "comparam_code"            "comparam_id"
  # [16] "control"                           "details_text"             "deviation"                "doctype_code"
  # [21] "doctype_id"               "document_id"                                                "doseunit_code"
  # [26] "doseunit_id"                                   "durationunit_code"        "durationunit_id"
  # [31] "effect_desc"                         "endpoint_id"              "endpointstudy_id"         "exp_duration"
  # [36]           "factstudy_id"             "genotox_id"               "glp_compl"
  # [41] "group_remarks"            "group_unit"                               "guideline_code"           "guideline_id"
  # [46] "guideline_qualifier"      "guidelinefulltxt"         "hazard_id"                                "inchi"
  # [51] "inchi_notationsource"     "is_carcinogenic"          "is_genotoxic"             "is_mutagenic"             "iupacname"
  # [56] "limittest"                "molecularformula"         "name"                     "number_individuals"       "op_id"
  # [61]          "publicationdate"                        "qualifier_code_x"         "qualifier_code_y"
  # [66] "qualifier_id_x"           "qualifier_id_y"           "qualifier_y"              "record_source_type"       "record_url"
  # [71] "regulation"               "regulation_code"          "regulation_id"            "regulationfulltext"       "remarks"
  # [76] "remarks_study"            "route"                    "route_code"               "route_id"
  # [81] "smilesnotation"           "smilesnotationsource"                       "source_download"
  # [86] "source_url"               "species_code"             "species_id"
  # [91] "strain_code"              "strain_id"                    "study_id"
  # [96]                "sub_casnumber"            "sub_com_id"               "sub_description"          "sub_ecsubinvententryref"
  # [101] "sub_id"                   "sub_name"                 "sub_op_class"             "sub_type"                 "subparam_code"
  # [106] "subparam_id"              "subparamname"                             "substancecomponent_id"    "targettissue"
  # [111] "targettissue_code"        "targettissue_id"          "testsubstance"            "testtype_code"            "testtype_id"
  # [116]                    "tox_id"                   "toxicity"                 "toxicity_code"            "toxicity_id"
  # [121] "toxref_id"
  # [126] "trx_id_x_3"               "trx_id_x_77"              "trx_id_y_105"             "trx_id_y_67"              "unit_milli"
  # [131] "value_milli"

  #res = res[ , !(names(res) %in% c("record_url","human_eco","url","source_study_id","efsa_id","output_id"  ))]

  #####################################################################
  cat("find columns in res that do not map to toxval or record_source\n")
  #####################################################################
  cols1 = runQuery("desc record_source",toxval.db)[,1]
  cols2 = runQuery("desc toxval",toxval.db)[,1]
  cols = unique(c(cols1,cols2))
  colnames(res)[which(names(res) == "species")] = "species_original"
  res = res[ , !(names(res) %in% c("record_url","short_ref"))]
  nlist = names(res)
  nlist = nlist[!is.element(nlist,c("casrn","name","doi","publication_date"))]
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
  #refs$record_source_type = "website"
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
  #res$source_url = "https://www.efsa.europa.eu/en/data-report/chemical-hazards-database-openfoodtox"
  #res$subsource_url = "-"
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
