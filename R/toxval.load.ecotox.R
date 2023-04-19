#-------------------------------------------------------------------------------------
#' Load ECOTOX from the datahub to toxval
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source.db The version of toxval source - used to manage chemicals
#' @param verbose Whether the loaded rows should be printed to the console.
#' @param log If TRUE, send output to a log file
#' @param do.load If TRUE, load the data from the input file and put into a global variable
#' @export
#--------------------------------------------------------------------------------------
toxval.load.ecotox <- function(toxval.db,source.db,log=F,do.load=F,sys.date="2023-01-26") {
  printCurrentFunction(toxval.db)
  source <- "ECOTOX"
  source_table = "direct_load"

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
  if(!exists("ECOTOX")) do.load=T
  if(do.load) {
    cat("load ECOTOX data\n")
    file = paste0(toxval.config()$datapath,"ecotox/ecotox_files/ECOTOX Prod ",sys.date,".RData")
    #file = paste0(toxval.config()$datapath,"ecotox/ecotox_files/ECOTOX Dev small ",sys.date,".RData")
    print(file)
    load(file=file)
    print(dim(ECOTOX))
    ECOTOX <- unique(ECOTOX)
    print(dim(ECOTOX))
    ECOTOX <<- ECOTOX
    dict = unique(ECOTOX[,c("species_scientific_name","species_common_name","species_group","habitat")])
    file = paste0(toxval.config()$datapath,"ecotox/ecotox_files/ECOTOX_dictionary_",sys.date,".xlsx")
    write.xlsx(dict,file)
  }
  res = ECOTOX
  nlist = c( "dsstox_substance_id","dsstox_casrn","dsstox_pref_nm",
  "species_number","species_common_name","species_scientific_name","habitat","organism_lifestage","species_group",
  "effect",
  "endpoint","conc1_type_std","conc1_units_std",
  "conc1_author","conc1_mean_std",
  "conc1_author","conc1_mean_op","conc1_mean","conc1_min_op","conc1_min","conc1_max_op","conc1_max",
  "conc1_units_std","conc1_mean_std",
  "observed_duration_units_std","observed_duration_std",
  "exposure_group","exposure_type","media_type",
  "source","author","title","publication_year","reference_number",
  "effect_measurement")

  res = res[,nlist]
  nlist = c( "dtxsid","casrn","name",
             "species_id","common_name","latin_name","habitat","lifestage","ecotox_group",
             "study_type",
             "toxval_type","toxval_subtype","toxval_units",
             "toxval_numeric_qualifier","toxval_numeric",
             "conc1_author","conc1_mean_op","conc1_mean","conc1_min_op","conc1_min","conc1_max_op","conc1_max",
             "conc1_units_std","conc1_mean_std",
             "study_duration_units","study_duration_value",
             "exposure_route","exposure_method","media",
             "long_ref","author","title","year","pmid",
             "effect_measurement")

  names(res) = nlist
  res$source = source
  res[res=="NA"] = NA
  res[res=="NR"] = NA
  res[res==""] = NA
  res$toxval_numeric_qualifier = "="

  cat("fix the toxval_numeric ranges\n")
  x = res$conc1_author
  x = str_replace_all(x,"NR","")
  x = str_replace_all(x,"/","")
  xq = x
  xq[] = "="
  hits = grep("-",x)
  if(length(hits)>0) {
    for(i in 1:length(hits)) x[hits[i]] = str_split(x[hits[i]],"-")[[1]][1]
    qlist = c("<=",">=","<",">","~","ca ")
    for(ql in qlist) {
      cat("  ",ql,"\n")
      ql2 = ql
      if(ql=="ca ") ql2 = "~"
      qhits = grep(ql,x)
      for(i in 1:length(qhits)) {
        x[qhits[i]] = str_replace(x[qhits[i]],ql,"")
        xq[qhits[i]] = ql2
      }
    }
  }
  res$toxval_numeric_qualifier = xq
  res1 = res[!is.na(res$toxval_numeric),]
  cat(nrow(res),"\n")
  res = res[!is.na(res$toxval_type),]
  cat(nrow(res),"\n")

  res[is.element(res$study_duration_units,"d"),"study_duration_units"] = "days"
  x = res$toxval_type
  x = str_replace_all(x,"/","")
  x = str_replace_all(x,"\\*","")
  res$toxval_type = x

  x = res$study_duration_value
  x = str_replace_all(x,"/","")
  x = str_replace_all(x,"~","")
  x = str_replace_all(x,"\\*","")
  x = str_replace_all(x,">=","")
  x = str_replace_all(x,"<=","")
  x = str_replace_all(x,">","")
  x = str_replace_all(x,"<","")
  x = str_replace_all(x," ","")
  res$study_duration_value = as.numeric(x)

  x = res$exposure_route
  x = str_replace_all(x,"ENV","environmental")
  x = str_replace_all(x,"MULTIPLE","multiple")
  x = str_replace_all(x,"IN VITRO","in vitro")
  x = str_replace_all(x,"ORAL","oral")
  x = str_replace_all(x,"INJECT","injection")
  x = str_replace_all(x,"TOP","dermal")
  x = str_replace_all(x,"AQUA","aquatic")
  x = str_replace_all(x,"UNK","unknown")
  res$exposure_route = x

  res$long_ref = paste(res$long_ref,res$author,res$title,res$year)
  res$critical_effect = paste(res$study_type, res$effect_measurement)
  res = subset(res,select = -c(effect_measurement,
                               conc1_author,conc1_mean_op,conc1_mean,conc1_min_op,conc1_min,conc1_max_op,conc1_max,
                               conc1_units_std,conc1_mean_std))

  cat("set the source_hash\n")
  res$key = NA
  for (i in 1:nrow(res)){
    row <- res[i,]
    res[i,"key"] = digest(paste0(row,collapse=""), serialize = FALSE)
    if(i%%10000==0) cat(i," out of ",nrow(res),"\n")
  }

  res = subset(res,select = -c(key))
  res = unique(res)
  print(dim(res))

  cat("set the source_hash\n")
  res$source_hash = NA
  for (i in 1:nrow(res)){
    row <- res[i,]
    res[i,"source_hash"] = digest(paste0(row,collapse=""), serialize = FALSE)
    if(i%%1000==0) cat(i," out of ",nrow(res),"\n")
  }

  res = source_chemical.ecotox(toxval.db,source.db,res,source,chem.check.halt=FALSE,
                               casrn.col="casrn",name.col="name",verbose=F)
  tt.list <- res$toxval_type
  tt.list <- gsub("(^[a-zA-Z]+)([0-9]*)(.*)","\\1",tt.list)

  res1 <- res[tt.list!="LT",]
  res2 <- res[tt.list=="LT",]

  dose.units <- res2$toxval_units
  time.value <- res2$study_duration_value
  dose.value <- res2$toxval_numeric
  dose.value <- gsub("NR",NA, dose.value)
  dose.qualifier <- as.data.frame(str_extract_all(dose.value, "[^0-9.E+-]+", simplify = TRUE))[,1]
  dose.value <- gsub("[^0-9.E+-]+", "", dose.value)
  time.units <- res2$study_duration_units
  type <- res2$toxval_type

  for(i in 1:length(dose.value)) {
    if(!is.na(as.numeric(dose.value[i]))) {
      dose.value[i] <- signif(as.numeric(dose.value[i]),digits=4)
    }
  }
  new.type <- paste0(type,"@",dose.qualifier," ",dose.value," ",dose.units)
  res3 <- res2
  res3$toxval_type <- new.type
  res3$toxval_numeric <- time.value
  res3$toxval_units <- time.units
  res <- rbind(res1,res3)
  x <- res[,"casrn"]
  for(i in 1:length(x)) x[i] <- fix.casrn(x[i])
  res[,"casrn"] <- x

  res <- res[!is.na(res[,"toxval_numeric"]),]
  res$quality <- paste("Control type:",res$quality)
  res <- res[!is.na(res$toxval_numeric),]
  res <- res[res$toxval_numeric>=0,]
  res$toxval_numeric_qualifier[which(is.na(res$toxval_numeric_qualifier)|res$toxval_numeric_qualifier == "")] <- "-"
  res$exposure_method <- res$media

  #####################################################################
  cat("add other columns to res\n")
  #####################################################################
  res$source <- source
  #Get rid of wacko characters
  do.fix.units <- T
  if(do.fix.units) {
    res$toxval_units <- str_replace_all(res$toxval_units,"AI ","")
    res$toxval_units <- str_replace_all(res$toxval_units,"ae ","")
    res$toxval_units <- str_replace_all(res$toxval_units,"ai ","")
  }
  res$toxval_type <- str_replace_all(res$toxval_type,fixed("*"),"")
  res$toxval_type <- str_replace_all(res$toxval_type,fixed("/"),"")
  res$study_type <- str_replace_all(res$study_type,fixed("~"),"")
  res$study_duration_value <- str_replace_all(res$study_duration_value,fixed("~"),"")
  res$study_duration_value <- str_replace_all(res$study_duration_value,fixed(">"),"")
  res$study_duration_value <- str_replace_all(res$study_duration_value,fixed("<"),"")
  res$study_duration_value <- str_replace_all(res$study_duration_value,fixed("="),"")
  res$study_duration_value <- str_replace_all(res$study_duration_value,fixed("NR"),"-999")
  res$study_duration_value <- as.numeric(res$study_duration_value)
  res$study_duration_value[is.na(res$study_duration_value)] <- -999
  res$study_duration_units <- str_replace_all(res$study_duration_units,"Day(s)","Day")
  res$study_duration_class <- "chronic"
  res <- unique(res)
  res$subsource <- "EPA ORD"
  res$source_url <- "https://cfpub.epa.gov/ecotox/"
  res$details_text <- "ECOTOX Details"
  res$datestamp <- Sys.time()
  res$datestamp <- as.character(res$datestamp)
  res[,"species_original"] <- tolower(res[,"common_name"])
  res <- fill.toxval.defaults(toxval.db,res)
  res <- generate.originals(toxval.db,res)
  print(dim(res))
  res <- unique(res)
  print(dim(res))

  #####################################################################
  cat("Add the code from the original version from Aswani\n")
  #####################################################################
  cremove = c("chemical_index","common_name","latin_name","ecotox_group")
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
  res = fill.toxval.defaults(toxval.db,res)
  res = res[!is.na(res$toxval_numeric),]
  res = res[res$toxval_numeric>0,]
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
  res$subsource <- "EPA ORD"
  res$source_url <- "https://cfpub.epa.gov/ecotox/"
  res$details_text = paste(source,"Details")
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
