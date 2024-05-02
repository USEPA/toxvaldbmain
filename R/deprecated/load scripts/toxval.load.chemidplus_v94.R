#-------------------------------------------------------------------------------------
#' Load ChemID Plus Acute data data to toxval
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param verbose Whether the loaded rows should be printed to the console.
#' @param log If TRUE, send output to a log file
#' @param do.init if TRUE, read the data in from the file and set up the matrix
#' @export
#--------------------------------------------------------------------------------------
toxval.load.chemidplus <- function(toxval.db,source.db,log=F,do.init=F) {
  printCurrentFunction(toxval.db)
  source <- "ChemIDPlus"
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
  if(!exists("CHEMIDPLUS")) do.init=T
  if(do.init) {
    file = paste0(toxval.config()$datapath,"ChemIDPlus/ChemIDplus Toxicity Experimental Records.xlsx")
    mat = read.xlsx(file)
    nlist = c("casrn",
              "chemical_name",
              "source_name",
              "property_name",
              "property_value_numeric_qualifier",
              "property_value_point_estimate_final",
              "property_value_units_final",
              "reference",
              "url")
    mat = mat[,nlist]
    nlist = c("casrn",
              "name",
              "source",
              "property_name",
              "toxval_numeric_qualifier",
              "toxval_numeric",
              "toxval_units",
              "long_ref",
              "url")
    names(mat) = nlist
    mat$study_type = "acute"
    mat$species = NA
    mat$exposure_route = NA
    mat$toxval_type = NA

    plist = unique(mat$property_name)
    for(pn in plist) {
      temp = str_split(pn,"_")[[1]]
      toxval_type = temp[length(temp)]
      exposure_route = temp[length(temp)-1]
      if(exposure_route=="skin") exposure_route = "dermal"
      species = temp[1]
      if(species=="guinea") species = "guinea pig"
      mat[is.element(mat$property_name,pn),"species"] = species
      mat[is.element(mat$property_name,pn),"exposure_route"] = exposure_route
      mat[is.element(mat$property_name,pn),"toxval_type"] = toxval_type

    }
    mat[is.na(mat$toxval_numeric_qualifier),"toxval_numeric_qualifier"] = "="
    res = mat
    res = subset(res,select = -c(property_name))
    CHEMIDPLUS <<- res
  }
  res = CHEMIDPLUS

  # Perform deduping
  res = toxval.load.dedup(res)

  cat("set the source_hash\n")
  #res = fix.non_ascii.v2(res,source)
  res$source_hash = NA
  for (i in 1:nrow(res)){
    row <- res[i,]
    res[i,"source_hash"] = digest(paste0(row,collapse=""), serialize = FALSE)
    if(i%%1000==0) cat(i," out of ",nrow(res),"\n")
  }

  res = source_chemical.chemidplus(toxval.db,source.db,res,source,chem.check.halt=FALSE,
                                 casrn.col="casrn",name.col="name",verbose=F)
  # nlist = names(res)
  # nlist = nlist[!is.element(nlist,"dtxsid")]
  # res = res[,nlist]
  # res <- res[!is.na(res[,"toxval_units"]),]
  # res[,"toxval_type"] <- toupper(res[,"toxval_type"])
  # x <- res[,"toxval_numeric_qualifier"]
  # x[is.element(x,"'='")] <- "="
  # res[,"toxval_numeric_qualifier"] <- x
  #
  # x <- res[,"toxval_units"]
  # x[is.element(x,"mg/kg/day")] <- "mg/kg-day"
  # x[is.element(x,"mg/kg/wk")] <- "mg/kg-wk"
  # x[is.element(x,"mg/m^3")] <- "mg/m3"
  # x[is.element(x,"mg/L/day")] <- "mg/L"
  # res[,"toxval_units"] <- x
  # res[,"exposure_route"] <- tolower(res[,"exposure_route"])
  # res[,"exposure_method"] <- tolower(res[,"exposure_method"])

  # x <- res[,"study_type"]
  # x[is.element(x,"DEV")] <- "developmental"
  # x[is.element(x,"MGR")] <- "reproductive"
  # x[is.element(x,"CHR")] <- "chronic"
  # x[is.element(x,"DNT")] <- "developmental neurotoxicity"
  # x[is.element(x,"SUB")] <- "subchronic"
  # x[is.element(x,"NEU")] <- "neurotoxicity"
  # x[is.element(x,"REP")] <- "reproductive"
  # x[is.element(x,"OTH")] <- "other"
  # x[is.element(x,"SAC")] <- "subacute"
  # x[is.element(x,"ACU")] <- "acute"
  # res[,"study_type"] <- x
  #
  # x <- res[,"study_duration_units"]
  # x[is.element(x,"GD")] <- "days"
  # x[is.element(x,"PND")] <- "days"
  # x[is.element(x," day (PND)")] <- "days"
  # x[is.element(x,"days (premating)")] <- "days"
  # x[is.element(x,"weeks (premating)")] <- "weeks"
  # res[,"study_duration_units"] <- x
  # res[,"study_duration_value"] <- as.numeric(res[,"study_duration_value"])

  # name.list <- names(res)
  # name.list[is.element(name.list,"dose_end_unit")] <- "study_duration_units"
  # name.list[is.element(name.list,"dose_end")] <- "study_duration_value"
  # names(res) <- name.list

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

