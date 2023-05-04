#-------------------------------------------------------------------------------------
#' Initialize the database. THis sill load the species, info and dictionary tables
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param reset If TRUE, delete all content from the database
#' @param date_string The date of the dictionary versions
#' @export
#--------------------------------------------------------------------------------------
toxval.init.db <- function(toxval.db,reset=F,date_string="2022-05-25") {
  printCurrentFunction(toxval.db)

  if(reset) {
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
  }
  #################################################################
  cat("load the info table\n")
  #################################################################
  query = paste0("insert into info (name,description,date_created) values ('res_toxvald_v94','ToxVal DB v9.4','",Sys.Date(),"')")
  runQuery(query,toxval.db)

  #################################################################
  cat("load the dictionaries\n")
  #################################################################
  import.dictionary(toxval.db)

  #################################################################
  cat("load the species table\n")
  #################################################################
  toxval.load.species(toxval.db)
}
