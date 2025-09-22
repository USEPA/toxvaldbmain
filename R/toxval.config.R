#--------------------------------------------------------------------------------------
#' @title toxval.config
#' @description Define a set of global variables. These include the source path (datapath)
#' and the source databases (e.g. dev_toxval_{version} and dev_toxval_source_{version}).
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

  retval <- list(
    toxval.db=Sys.getenv("toxval.db"),
    source.db=Sys.getenv("source.db"),
    datapath=Sys.getenv("datapath")
  )

  # Check input .Renviron variables
  for(param in names(retval)){
    if (is.null(retval[[param]]) || is.na(retval[[param]]) || retval[[param]] == '') stop(paste0("'", param, "' not set in .Renviron file..."))
  }

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

  non_hash_cols = c("source_hash", "chemical_id", "parent_chemical_id", "source_id","clowder_id","document_name","source_hash","qc_status",
                    "parent_hash","create_time","modify_time","created_by", "qc_flags", "qc_notes", "qc_category", "version",
                    "raw_input_file", "source_version_date")

  dedup_hierarchy = c(
    "HEAST" = "IRIS",
    "HAWC PFAS 150" = "PFAS 150 SEM v2",
    "OW Drinking Water Standards" = "EPA OW NPDWR",
    "TEST" = "ChemIDplus",
    "NIOSH IDLH" = "NIOSH",
    "Cal OEHHA REL derivations" = "Cal OEHHA"
  )

  # Return configuration variables
  return(
    append(retval,
           list(
             hashing_cols=hashing_cols,
             non_hash_cols=non_hash_cols,
             dedup_hierarchy=dedup_hierarchy
           )
    )
  )
}
