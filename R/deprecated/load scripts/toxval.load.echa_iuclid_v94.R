#--------------------------------------------------------------------------------------
#
#' Loading the ECHA IUCLID data to toxval from toxval_source
#' This method is different from most because there are multiple tables (one per study
#' type) for this source
#' @param toxval.db The database version to use
#' @param source.db The source database
#' @param log If TRUE, send output to a log file
#--------------------------------------------------------------------------------------
toxval.load.echa_iuclid <- function(toxval.db,source.db,log=F,reset=F) {
  source = "ECHA IUCLID"

  # if(reset) {
  #   cat("remove all rows for ECHA IUCLID\n")
  #   runQuery(paste0("delete from toxval_notes where toxval_id in (select toxval_id from toxval where source='",source,"')"),toxval.db)
  #   runQuery(paste0("delete from toxval_qc_notes where toxval_id in (select toxval_id from toxval where source='",source,"')"),toxval.db)
  #   runQuery(paste0("delete from record_source where toxval_id in (select toxval_id from toxval where source='",source,"')"),toxval.db)
  #   runQuery(paste0("delete from toxval_uf where toxval_id in (select toxval_id from toxval where source='",source,"')"),toxval.db)
  #   runQuery(paste0("delete from toxval_uf where parent_id in (select toxval_id from toxval where source='",source,"')"),toxval.db)
  #   runQuery(paste0("delete from toxval_relationship where toxval_id_1 in (select toxval_id from toxval where source='",source,"')"),toxval.db)
  #   runQuery(paste0("delete from toxval_relationship where toxval_id_2 in (select toxval_id from toxval where source='",source,"')"),toxval.db)
  #   runQuery(paste0("delete from toxval where source='",source,"'"),toxval.db)
  # }

  ohtname.list = c(
                   "Repeated Dose Toxicity Oral",
                   "Repeated Dose Toxicity Dermal",
                   "Repeated Dose Toxicity Inhalation",
#                   "Repeated Dose Toxicity Other",
                   "Acute Toxicity Dermal",
                   "Acute Toxicity Inhalation",
                   "Acute Toxicity Oral",
#                   "Acute Toxicity Other Routes",
                   "Carcinogenicity",
                   "Developmental Toxicity Teratogenicity",
                   "Immunotoxicity",
                   "Neurotoxicity")

  verbose=F
  if(log) {
   #####################################################################
   cat("start output log, log files for each source can be accessed from output_log folder\n")
   #####################################################################
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

  for(ohtname in ohtname.list) {
    #####################################################################
    cat("++++++++++++++++++++++++++++++++++++++++++++++\n")
    cat("OHT:",ohtname,"\n")
    cat("++++++++++++++++++++++++++++++++++++++++++++++\n\n")
    #####################################################################
    oht = str_replace_all(ohtname," ","")
    oht = tolower(oht)
    source_table = paste0("source_iuclid_",oht)
    subsource = ohtname
    chem_source = paste0("IUCLID_",oht)
    count = runQuery(paste0("select count(*) from toxval where source='",source,"' and subsource='",subsource,"'"),toxval.db)[1,1]
    if(count>0) {
      runQuery(paste0("delete from toxval_notes where toxval_id in (select toxval_id from toxval where source='",source,"' and subsource='",subsource,"')"),toxval.db)
      runQuery(paste0("delete from toxval_qc_notes where toxval_id in (select toxval_id from toxval where source='",source,"' and subsource='",subsource,"')"),toxval.db)
      runQuery(paste0("delete from record_source where toxval_id in (select toxval_id from toxval where source='",source,"' and subsource='",subsource,"')"),toxval.db)
      runQuery(paste0("delete from toxval_uf where toxval_id in (select toxval_id from toxval where source='",source,"' and subsource='",subsource,"')"),toxval.db)
      runQuery(paste0("delete from toxval_uf where parent_id in (select toxval_id from toxval where source='",source,"' and subsource='",subsource,"')"),toxval.db)
      runQuery(paste0("delete from toxval_relationship where toxval_id_1 in (select toxval_id from toxval where source='",source,"' and subsource='",subsource,"')"),toxval.db)
      runQuery(paste0("delete from toxval_relationship where toxval_id_2 in (select toxval_id from toxval where source='",source,"' and subsource='",subsource,"')"),toxval.db)
      runQuery(paste0("delete from toxval where source='",source,"' and subsource='",subsource,"'"),toxval.db)
    }

    #####################################################################
    cat("load data to res\n")
    #####################################################################
    query = paste0("select * from ",source_table)
    #query = paste0("select * from ",source_table," limit 100")
    res = runQuery(query,source.db,T,F)
    res = res[ , !(names(res) %in% c("source_id","clowder_id","parent_hash","create_time","modify_time","created_by"))]
    res$source = source
    res$subsource = subsource
    res$details_text = paste(source,"Details")
    print(dim(res))
    #file = paste0(toxval.config()$datapath,"echa_iuclid/",source_table," ",source.db," ",Sys.Date(),".xlsx")
    #write.xlsx(res,file)

    #####################################################################
    cat("Add code to deal with specific issues for this source\n")
    #####################################################################
    nlist = c("source","subsource","chemical_id","casrn","name","source_hash","source_url",
              "toxval_type","toxval_units","toxval_qualifier","toxval_numeric",
              "study_type","study_duration_value","study_duration_units",
              "exposure_route","exposure_method","media",
              "species","strain","sex",
              "effect_level_basis",
              "reference_type","reference_title","reference_year","guideline","document_name",
              "admindata_studyresulttype_code" )

    x = nlist[!is.element(nlist,names(res))]
    if(is.element(subsource,c("Carcinogenicity","Developmental Toxicity Teratogenicity","Immunotoxicity","Neurotoxicity"))) {
      nlist = nlist[!is.element(nlist,x)]
      x = nlist[!is.element(nlist,names(res))]
    }
    if(length(x)>0) browser()
    res = res[,nlist]
    print(dim(res))
    res = res[is.element(res$admindata_studyresulttype_code,
                         c("experimental study")),]
    cat("filter for experimental studies only\n")
    print(dim(res))

    res$toxval_numeric_qualifier = res$toxval_qualifier
    res[is.na(res$toxval_numeric_qualifier),"toxval_numeric_qualifier"] = "="
    res$critical_effect = res$effect_level_basis

    res$year = res$reference_year
    res$long_ref = paste(res$reference_type,res$reference_title,res$document_name,res$reference_year)
    res$url = res$source_url

    #####################################################################
    cat("find columns in res that do not map to toxval or record_source\n")
    #####################################################################
    cols1 = runQuery("desc record_source",toxval.db)[,1]
    cols2 = runQuery("desc toxval",toxval.db)[,1]
    cols = unique(c(cols1,cols2))
    colnames(res)[which(names(res) == "species")] = "species_original"
    res = res[ , !(names(res) %in% c("record_url","short_ref",
                                     "echa_url","toxval_qualifier","effect_level_basis","literature_referenceyear",
                                     "literaturetitle","admindata_studyresulttype_code","reference_type","reference_title","reference_year"
                                     ))]
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

    res = res[!is.na(res$toxval_numeric),]
    maxval = max(res$toxval_numeric,na.rm=T)
    cutoff = 1E8
    bad = res[res$toxval_numeric>cutoff,]
    file = paste0(toxval.config()$datapath,"echa_iuclid/bad values ",ohtname," ",Sys.Date(),".xlsx")
    write.xlsx(bad,file)
    res = res[res$toxval_numeric<cutoff,]
    cat("Max toxval_numeric: ",maxval," :",nrow(bad),"\n")
    #if(maxval>cutoff) browser()

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
    res$source_url = "https://echa.europa.eu/information-on-chemicals/registered-substances"
    res$subsource_url = "-"
    res$details_text = paste(source,"Details")
    runInsertTable(res, "toxval", toxval.db, verbose)
    runInsertTable(refs, "record_source", toxval.db, verbose)
    print(dim(res))
    #toxval.load.source_chemical.echa_iuclid(toxval.db,source.db,source,verbose=T,chem_source)

    #####################################################################
    cat("do the post processing\n")
    #####################################################################
    cat(ohtname,"\n")
    toxval.load.postprocess(toxval.db,source.db,source,do.convert.units=F,chem_source,subsource)
  }
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
