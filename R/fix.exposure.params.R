#-------------------------------------------------------------------------------------
#' Fix the exposure fields: exposure_method, exposure_route, exposure_form
#' based on a 3 column dictionary ~/dictionary/exposure_route_method_form.xlsx
#' @param toxval.db The version of toxval in which the data is altered.
#' @param source The source to be fixed. If source=NULL, fix all sources
#' @param subsource The subsource to be fixed (NULL default)
#' @param fill.toxval_fix If TRUE (default) read the dictionaries into the toxval_fix table
#' @param report.only Whether to report or write/export data. Default is FALSE (write/export data)
#' @return The database will be altered (if report.only=TRUE, return missing entries)
#' @export
#-------------------------------------------------------------------------------------
fix.exposure.params <- function(toxval.db, source=NULL, subsource=NULL, report.only=FALSE) {
  printCurrentFunction(toxval.db)
  # Read dictionary
  file = paste0(toxval.config()$datapath,"dictionary/exposure_route_method_form.xlsx")
  dict = openxlsx::read.xlsx(file)
  dict$index1 = paste(dict[,1],dict[,2],dict[,3])
  dict$index2 = paste(dict[,4],dict[,5],dict[,6])

  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  missing = NULL
  for(source in slist) {
    cat("\n-----------------------------------------------------\n")
    cat(source,subsource,"\n")
    cat("-----------------------------------------------------\n")
    res = runQuery(paste0("select source,toxval_id,exposure_route_original,exposure_method_original,exposure_form_original from toxval where source='",
                          source,"' AND qc_status NOT LIKE '%fail%'",query_addition),
                   toxval.db)
    res$index1 = paste(res[,3],res[,4],res[,5])
    ilist = unique(res$index1)
    # Check each record for appropriate exposure parameters
    for(index in ilist) {
      temp1 = res[res$index1==index,]
      if(is.element(index,dict$index1)) {
        # cat(source,index,"\n")
        x = dict[is.element(dict$index1,index),]
        eroute = x[1,"exposure_route"]
        emethod = x[1,"exposure_method"]
        eform = x[1,"exposure_form"]
        tlist = temp1$toxval_id
        tval = paste(tlist,collapse=",")
        # Update exposure parameters if specified
        if (!report.only) {
          query = paste0("update toxval set exposure_route='",eroute,"', exposure_method='",emethod,"', exposure_form='",eform,"' where toxval_id in (",tval,")")
          runQuery(query,toxval.db)
        }
        # for(i in 1:nrow(temp1)) {
        #   tid = temp1[i,"toxval_id"]
        #   query = paste0("update toxval set exposure_route='",eroute,"', exposure_method='",emethod,"', exposure_form='",eform,"' where toxval_id=",tid)
        #   #print(query)
        #   runQuery(query,toxval.db)
        # }
      }
      else {
        # If report only, track missing entries but do not update toxval
        missing = rbind(missing,temp1)
        if (!report.only) {
          cat("Found missing exposure_route, method, form combination\nSee the file dictionary/missing/missing_exposure_route_method_form.xlsx\n and add to the dictionary\n")
          #browser()
        }
      }
    }
  }
  # Return or record missing entries
  if(!is.null(missing) & !report.only) {
    if(is.null(source)) file = paste0(toxval.config()$datapath,"dictionary/missing/missing_exposure_route_method_form all source.xlsx")
    else {
      file = paste0(toxval.config()$datapath,"dictionary/missing/missing_exposure_route_method_form ",source,".xlsx")
      if(!is.null(subsource)) file = paste0(toxval.config()$datapath,"dictionary/missing/missing_exposure_route_method_form ",source," ",subsource,".xlsx")
    }
    openxlsx::write.xlsx(missing,file)
  }
  if (report.only) {
    return(missing)
  }
}

