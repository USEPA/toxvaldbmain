#-------------------------------------------------------------------------------------
#' Delete the contents of the toxval database
#' @param toxval.db The version of toxval from which the data is deleted.
#' @export
#--------------------------------------------------------------------------------------
toxval.load.initial <- function(toxval.db) {
  printCurrentFunction(toxval.db)

  exclude.list <- c("source_chemical","toxval","info","source_chemical")
  table.list <- runQuery("show tables",toxval.db)[,1]
  table.list <- table.list[!is.element(table.list,exclude.list)]
  for(table in table.list) {
    cat("delete from",table,"\n")
    query <- paste0("delete from ",table)
    runInsert(query,toxval.db,do.halt=T)
  }
  cat("delete from info\n")
  runInsert("delete from info",toxval.db,do.halt=T)
  cat("delete from toxval\n")
  runInsert("delete from toxval",toxval.db,do.halt=T)
  cat("delete from source_chemical\n")
  runInsert("delete from source_chemical",toxval.db,do.halt=T)

  runInsert("alter table source_chemical auto_increment=1",toxval.db,T)
  runInsert("alter table toxval auto_increment=1",toxval.db,T)
  runInsert("alter table study_details auto_increment=1",toxval.db,T)
  runInsert("alter table species auto_increment=1",toxval.db,T)

  name <- "ToxValDB V9"
  url <- "NA"
  description <- "ToxVal summarizes numerical toxicity values from multiple databases"
  query <- paste("insert into info (name,url,description,date_created)
                 values ('",name,"','",url,"','",description,"',CURRENT_DATE())",sep="")
  runInsert(query,toxval.db,T,F)
}
