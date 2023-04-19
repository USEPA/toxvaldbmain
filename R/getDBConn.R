library(RMySQL)
library(DBI)
#--------------------------------------------------------------------------------------
#' Get the names the database server, user, and pass or returns error message
#' @return print the database connection information
#--------------------------------------------------------------------------------------
getDBConn <- function() {
  printCurrentFunction()
  if(!exists("DB.SERVER")) cat("DB.SERVER not defined\n")
  else cat("DB.SERVER: ",DB.SERVER,"\n")
  if(!exists("DB.USER")) cat("DB.USER not defined\n")
  else cat("DB.USER: ",DB.USER,"\n")
  if(!exists("DB.PASSWORD")) cat("DB.PASSWORD not defined\n")
  else cat("DB.PASSWORD: ",DB.PASSWORD,"\n")
}
