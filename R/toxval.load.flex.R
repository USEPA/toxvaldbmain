#-------------------------------------------------------------------------------------
#' Load the FLEX data (old ACToR data) from files to toxval. This will load all
#' Excel file in the folder ACToR replacements/
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param verbose Whether the loaded rows should be printed to the console.
#' @param only.new if TRUE only files where the sources is not alrady in the database
#' will be loaded
#' @export
#--------------------------------------------------------------------------------------
toxval.load.flex <- function(toxval.db,verbose=F,only.new=F) {
  printCurrentFunction(toxval.db)
  datapath <- toxval.config()$datapath
  target <- paste0(datapath,"ACToR replacements/")
  file.list <- grep(".xlsx",dir(target),value=T)
  source.list <- runQuery("select distinct source from toxval",toxval.db)[,1]
  for(i in file.list){
    #Read it in
    openxlsx::read.xlsx(paste0(target,i)) -> res
    res <- unique(res)
    if(verbose){cat(i,"\n")}
    carefulcheck <- nrow(res)

    #Source Check
    cols <- which(colnames(res)=="source")
    sourcecheck <- res[1,cols]
    if(sourcecheck %in% source.list & only.new == T){sourcecheck = "FAILED"}
    if(sourcecheck=="FAILED"){cat(i,"was already loaded and was not loaded again\n")}
    else{

      #Get rid of strange characters
      for(j in 1: ncol(res)) res[,j] <- iconv(res[,j],"latin1","ASCII",sub="")

      source <- res$source[1]
      cat("==================================================\n",source,"\n==================================================\n")
      clean.toxval.by.source(toxval.db,source)

      #Get chemical_id
      cols <- which(colnames(res)=="name")
      res[nchar(res[,cols])>255,cols] <- substr(res[nchar(res[,cols])>255,cols],1,255)
      res <- res[!is.na(res[,"casrn"]),]
      res <- unique(res)
      cat("step1",nrow(res),"\n")
      cols <- c(which(colnames(res)=="casrn"),which(colnames(res)=="name"))
      cas.list <- res[,cols]
      cid.list <- get.cid.list.toxval(toxval.db, cas.list,source)
      res$chemical_id <- cid.list$chemical_id
      #res <- merge(cid.list,res,by="name")
      cat("step2",nrow(res),"\n")
      res <- res[,!is.element(names(res),c("casrn","name"))]

      count <- runQuery("select count(*) from toxval",toxval.db)[1,1]
      if(count==0) tid0 <- 1
      else tid0 <- runQuery("select max(toxval_id) from toxval",toxval.db)[1,1] + 1
      tids <- seq(from=tid0,to=tid0+nrow(res)-1)
      res$toxval_id <- tids
      cat("step3",nrow(res),"\n")

      #Get the record sources
      refs <- NULL
      if("long_ref" %in% names(res)){
        sourcecols <- runQuery("desc record_source",toxval.db)[,1]
        cols <- which(colnames(res)%in%sourcecols)
        refs <- res[,cols]
        refs$record_source_type <- "website"
        refs$record_source_note <- "to be cleaned up"
        refs$record_source_level <- "secondary"
        if(is.element("source_url",names(res))) refs$url <- res$source_url
      }
      else if("source_url" %in% names(res)){
        sourcecols <- runQuery("desc record_source",toxval.db)[,1]
        cols <- which(colnames(res)%in%sourcecols)
        refs <- res[,cols]
        refs$record_source_type <- "website"
        refs$record_source_note <- "to be cleaned up"
        refs$record_source_level <- "primary (risk assessment values)"
        refs$url <- res$source_url
        if(source=='WHO IPCS') refs$document_name <- "pesticides_hazard_2009.pdf"
        if(source=='Alaska DEC') refs$document_name <- "cleanuplevels.pdf"
      }
      cat("step4",nrow(res),"\n")

      sourcecols <- runQuery("desc toxval",toxval.db)[,1]
      res <- res[,is.element(names(res),sourcecols)]
      cat("step5",nrow(res),"\n")

      res <- unique(res)
      res <- fill.toxval.defaults(toxval.db,res)
      res <- generate.originals(toxval.db,res)
      name.list <- names(res)
      name.list[is.element(name.list,"phenotype")] <- "critical_effect"
      names(res) <- name.list

      res$datestamp <- Sys.time()
      if(is.element("species_original",names(res))) res[,"species_original"] <- tolower(res[,"species_original"])
      cat("step6",nrow(res),"\n")
      res <- unique(res)
      cat("step7",nrow(res),"\n")
      #for(i in 1:nrow(res)) res[i,"toxval_uuid"] <- UUIDgenerate()

      runInsertTable(res, "toxval", toxval.db)
      if(!is.null(refs)) {
        #for(i in 1:nrow(refs)) refs[i,"record_source_uuid"] <- UUIDgenerate()
        runInsertTable(refs, "record_source", toxval.db)
      }
      cat(i, "was successfully loaded!\n")
    }
  }
}
