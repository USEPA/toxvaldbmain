options(java.parameters = "-Xmx1000m")
# library(DBI)
# library(RCurl)
# library(RMySQL)
# library(RPostgreSQL)
# library(XML)
# library(data.table)
# library(dplyr)
# library(gsubfn)
# library(jsonlite)
# library(openxlsx)
# library(stringi)
# library(stringr)
# library(tidyr)
# library(digest)
# library(uuid)
# library(tibble)
# library(janitor)
# library(textclean)
# library(magrittr)
# library(logr)

#--------------------------------------------------------------------------------------
#' @title toxval.config
#' @description Define a set of global variables. These include the source path (datapath)
#' and the source databases (e.g. dev_toxval_{version} and dev_toxval_source_{version})
#' and the urls for the ACToR web services.
#' @details DETAILS
#' @return Returns a set of parameters to be used throughout the package
#' @export
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @rdname toxval.config
#--------------------------------------------------------------------------------------
toxval.config <- function() {
  toxval.db <- "res_toxval_v95"
  dsstox.db <- "ro_prod_dsstox"
  source.db <- "res_toxval_source_v5"

  datapath = "Repo/"#"/ccte/ACToR1/ToxValDB9/Repo/"
  actorws.prod <- "https://actorws.epa.gov/actorws/toxval/v01/toxval_source"
  actorws.dev <- "http://ag.epa.gov:8528/actorws/toxval/v01/toxval_source"

  hashing_cols = c(
    # toxval table fields
    'name', 'casrn', 'toxval_type', 'toxval_subtype', 'toxval_numeric', 'toxval_units',
    'toxval_numeric_qualifier', 'study_type', 'study_duration_class',
    'study_duration_qualifier', 'study_duration_value', 'study_duration_units',
    'species', 'strain', 'sex', 'critical_effect', 'population',
    'exposure_route', 'exposure_method', 'exposure_form', 'media',
    'lifestage', 'generation', 'year',
    # record_source table fields (only long_ref for now)
    'long_ref' #, 'title', 'author', 'journal', 'volume', 'issue', 'page'
  )

  non_hash_cols = c("chemical_id", "parent_chemical_id", "source_id","clowder_id","document_name","source_hash","qc_status",
                    "parent_hash","create_time","modify_time","created_by", "qc_flags", "qc_notes", "version",
                    "raw_input_file", "source_version_date")
  retval <- list(dsstox.db=dsstox.db,
                 toxval.db=toxval.db,
                 source.db=source.db,
                 datapath=datapath,
                 actorws.prod=actorws.prod,
                 actorws.dev=actorws.dev,
                 non_hash_cols=non_hash_cols,
                 hashing_cols=hashing_cols)
  retval
}
