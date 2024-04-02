#-------------------------------------------------------------------------------------
#' Fix the study duration fields: study_duration_value, study_duration_units, study_duration_class
#' based on a 3 column dictionary ~/dictionary/exposure_route_method_form.xlsx
#' @param toxval.db The version of toxval in which the data is altered.
#' @param source The source to be fixed. If source=NULL, fix all sources
#' @param fill.toxval_fix If TRUE (default) read the dictionaries into the toxval_fix table
#' @param dict.date The dated version of the dictionary to use
#' @param report.only Whether to report or write/export data. Default is FALSE (write/export data)
#' @return The database will be altered
#' @export
#-------------------------------------------------------------------------------------
fix.study_duration.params <- function(toxval.db, source=NULL,subsource=NULL, dict.date="2023-08-23", report.only=FALSE) {
  printCurrentFunction(toxval.db)
  file = paste0(toxval.config()$datapath,"dictionary/study_duration_params"," ",dict.date,".xlsx")
  dict = read.xlsx(file)
  dict$index1 = paste(dict[,1],dict[,2],dict[,3])
  dict$index2 = paste(dict[,4],dict[,5],dict[,6])

  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source
  missing = NULL
  for(source in slist) {
    cat("\n-----------------------------------------------------\n")
    cat(source,"\n")
    cat("-----------------------------------------------------\n")
    res = runQuery(paste0("select source,toxval_id,
                          study_duration_value_original, study_duration_units_original, study_duration_class_original
                          from toxval where source='",source,"'"),toxval.db)
    res$index1 = paste(res[,3],res[,4],res[,5])
    ilist = unique(res$index1)
    for(index in ilist) {
      temp1 = res[res$index1==index,]
      if(is.element(index,dict$index1)) {
        #cat(source,index,"\n")
        x = dict[is.element(dict$index1,index),]
        x1 = x[1,"study_duration_value"]
        x2 = x[1,"study_duration_units"]
        x3 = x[1,"study_duration_class"]
        tlist = temp1$toxval_id
        tval = paste(tlist,collapse=",")
        if(!report.only) {
          query = paste0("update toxval set study_duration_value='",x1,"', study_duration_units='",x2,"', study_duration_class='",x3,"' where toxval_id in (",tval,")")
          runQuery(query,toxval.db)
        }
      }
      else {
        missing = rbind(missing,temp1)
      }
    }
  }
  if(!is.null(missing)) {
    if(!report.only) {
      cat("found missing study_duration_value, study_duration_units, study_duration_class combination\nSee the file dictionary/missing/missing_study_duration_params",dict.date,".xlsx\nand add to the dictionary\n")
      file = paste0(toxval.config()$datapath,"dictionary/missing/missing_study_duration_params ",source,".xlsx")
      if(!is.null(subsource)) file = paste0(toxval.config()$datapath,"dictionary/missing/missing_study_duration_params ",source," ",subsource,".xlsx")
      write.xlsx(missing,file)
    }
  }
  if(report.only) return(missing)
}

