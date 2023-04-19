#--------------------------------------------------------------------------------------
#
#' Load the alaska_dec (old ACToR - flex)data  from toxval sourcedb to toxval
#' @param toxval.db The database version to use
#' @param source.db The source database
#' @param log If TRUE, output log inoformation to a file
#--------------------------------------------------------------------------------------
toxval.load.alaska_dec <- function(toxval.db, source.db,log=F){
  verbose = log
  printCurrentFunction(toxval.db)
  source <- "Alaska DEC"
  source_table = "source_alaska_dec"
  if(log) {
    #####################################################################
    cat("start output log, log files for each source can be accessed from output_log folder\n")
    #####################################################################
    con1 <- file.path(toxval.config()$datapath,paste0(source,"_",Sys.Date(),".log"))
    con1 <- log_open(con1)
    con <- file(paste0(toxval.config()$datapath,source,"_",Sys.Date(),".log"))
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
  query <- paste0("select * from ",source_table)
  res <- runQuery(query,source.db,T,F)
  res <- res[ , !(names(res) %in% c("source_id","clowder_id","parent_hash","create_time","modify_time","created_by"))]
  print(dim(res))
  #####################################################################
  cat("add other columns to res\n")
  #####################################################################
  res$source <- source
  colnames(res)[which(names(res) == "phenotype")] <- "critical_effect"
  res$details_text<-"-"
  res$human_eco <- "human health"
  res$species_original = "human"
  res <- unique(res)
  res <- fill.toxval.defaults(toxval.db,res)
  res <- generate.originals(toxval.db,res)
  if(is.element("species_original",names(res))) res[,"species_original"] <- tolower(res[,"species_original"])
  res$species_original = "Human (RA)"
  res$toxval_numeric <- as.numeric(res$toxval_numeric)
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
  names(res)[names(res) == "source_url"] = "url"

  #####################################################################
  cat("add toxval_id to res\n")
  #####################################################################
  count = runQuery("select count(*) from toxval",toxval.db)[1,1]
  if(count==0) tid0 = 1
  else tid0 = runQuery("select max(toxval_id) from toxval",toxval.db)[1,1] + 1
  tids = seq(from=tid0,to=tid0+nrow(res)-1)
  res$toxval_id = tids

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

  #####################################################################
  cat("add extra columns to refs\n")
  #####################################################################
  refs$record_source_type <- "website"
  refs$record_source_note <- "to be cleaned up"
  refs$record_source_level <- "primary (risk assessment values)"

  #####################################################################
  cat("load res and refs to the database\n")
  #####################################################################
  res = unique(res)
  refs = unique(refs)
  res$datestamp = Sys.Date()
  res$source_table = source_table
  res$source_url = "https://dec.alaska.gov/spar/csp/"
  res$subsource_url = "-"
  res$details_text = paste(source,"Details")
  for(i in 1:nrow(res)) res[i,"toxval_uuid"] = UUIDgenerate()
  for(i in 1:nrow(refs)) refs[i,"record_source_uuid"] = UUIDgenerate()
  runInsertTable(res, "toxval", toxval.db, verbose)
  runInsertTable(refs, "record_source", toxval.db, verbose)

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

  #
  # #####################################################################
  # cat("Get rid of strange characters\n")
  # #####################################################################
  # res=fix.non_ascii.v2(res,source)
  #
  # #####################################################################
  # cat("trims leading and trailing whitespaces from the dataframe\n")
  # #####################################################################
  # res <- data.frame(lapply(res, function(x) if(class(x)=="character") trimws(x) else(x)), stringsAsFactors=F, check.names=F)
  # res <- unique(res)
  #
  # #####################################################################
  # cat("map chemicals\n")
  # #####################################################################
  # res <- res[,!is.element(names(res),c("casrn","name"))]
  #
  # #####################################################################
  # cat("add toxval_id to res\n")
  # #####################################################################
  # count <- runQuery("select count(*) from toxval",toxval.db)[1,1]
  # if(count==0) tid0 <- 1
  # else tid0 <- runQuery("select max(toxval_id) from toxval",toxval.db)[1,1] + 1
  # tids <- seq(from=tid0,to=tid0+nrow(res)-1)
  # res$toxval_id <- tids
  #
  # #####################################################################
  # cat("pull out record source to refs\n")
  # #####################################################################
  # refs <- res[,c("toxval_id","source","source_url","document_name")]
  # names(refs)[names(refs) == "source_url"] <- "url"
  #
  #
  # #####################################################################
  # cat("delete unused columns from res\n")
  # #####################################################################
  # res <- res[ , !(names(res) %in% c("source_url","document_name"))]
  #
  # #####################################################################
  # cat("load res and refs to the database\n")
  # #####################################################################
  # res <- unique(res)
  # refs <- unique(refs)
  # res$datestamp <- Sys.Date()
  # for(i in 1:nrow(res)) res[i,"toxval_uuid"] <- UUIDgenerate()
  # for(i in 1:nrow(refs)) refs[i,"record_source_uuid"] <- UUIDgenerate()
  # runInsertTable(res, "toxval", toxval.db)
  # runInsertTable(refs, "record_source", toxval.db)
  #
  # #####################################################################
  # cat("do the post processing\n")
  # #####################################################################
  # toxval.load.prostprocess(toxval.db,source.db,source)
  #
  # if(log) {
  #   #####################################################################
  #   cat("stop output log \n")
  #   #####################################################################
  #   closeAllConnections()
  #   log_close()
  #   output_message <- read.delim(paste0(toxval.config()$datapath,source,"_",Sys.Date(),".log"), stringsAsFactors = F, header = F)
  #   names(output_message) <- "message"
  #   output_log <- read.delim(paste0(toxval.config()$datapath,"log/",source,"_",Sys.Date(),".log"), stringsAsFactors = F, header = F)
  #   names(output_log) <- "log"
  #   new_log <- log_message(output_log, output_message[,1])
  #   writeLines(new_log, paste0(toxval.config()$datapath,"output_log/",source,"_",Sys.Date(),".txt"))
  # }
  # #####################################################################
  # cat("finish\n")
  # #####################################################################
}
