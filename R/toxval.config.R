options(java.parameters = "-Xmx1000m")
library(DBI)
library(RCurl)
library(RMySQL)
#library(RPostgreSQL)
library(XML)
library(data.table)
library(dplyr)
library(gsubfn)
library(jsonlite)
library(openxlsx)
library(stringi)
library(stringr)
library(tidyr)
library(digest)
library(uuid)
library(tibble)
library(janitor)
library(textclean)
library(logr)

#--------------------------------------------------------------------------------------
#'
#' Define a set of global variables. These include the source path (datapath)
#' and the source databases (e.g. dev_toxval_{version} and dev_toxval_source_{version})
#' and the urls for the ACToR web services.
#'
#' @return Returns a set of parameters to be used throughout the package
#' @export
#--------------------------------------------------------------------------------------
toxval.config <- function() {
  toxval.db <- "res_toxval_v92"
  dsstox.db <- "ro_prod_dsstox"
  source.db <- "res_toxval_source_v5"

  datapath = "/ccte/ACToR1/ToxValDB9/Repo/"
  actorws.prod <- "https://actorws.epa.gov/actorws/toxval/v01/toxval_source"
  actorws.dev <- "http://ag.epa.gov:8528/actorws/toxval/v01/toxval_source"
  retval <- list(dsstox.db=dsstox.db,
              toxval.db=toxval.db,
              source.db=source.db,
              datapath=datapath,
              actorws.prod=actorws.prod,
              actorws.dev=actorws.dev)
  retval
}
