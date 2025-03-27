#' @title fix.exposure_route.not_specified.by.source
#' @description Function to use a dictionary file to update cases where exposure_route reported as "not specified".
#' @param toxval.db The database version to use
#' @param source The source name
#' @param subsource The specific subsource to process, if desired (Default: NULL)
#' @return None. SQL UPDATE statements are pushed to the database.
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[dplyr]{pull}}, \code{\link[dplyr]{mutate-joins}}, \code{\link[dplyr]{select}}, \code{\link[dplyr]{rename}}, \code{\link[dplyr]{mutate}}, \code{\link[dplyr]{filter}}, \code{\link[dplyr]{distinct}}
#'  \code{\link[readxl]{read_excel}}
#'  \code{\link[writexl]{write_xlsx}}
#' @rdname fix.exposure_route.not_specified.by.source
#' @export
#' @importFrom dplyr pull left_join select rename mutate filter distinct
#' @importFrom readxl read_xlsx
#' @importFrom writexl write_xlsx
fix.exposure_route.not_specified.by.source <- function(toxval.db, source, subsource){

  printCurrentFunction(paste(toxval.db,":", source, subsource))

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and a.subsource = '", subsource, "'")
  }

  # Get list of sources
  slist = runQuery("select distinct source from toxval", toxval.db) %>%
    dplyr::pull(source)
  if(!is.null(source)) slist = source
  source_string = slist %>%
    paste0(collapse="', '")

  # Pull data
  mat = runQuery(
    paste0(
      "select a.toxval_id, a.source, a.subsource, b.toxval_type_supercategory, a.toxval_type, a.exposure_route ",
      "from toxval a ",
      "left join toxval_type_dictionary b on a.toxval_type = b.toxval_type ",
      "where a.exposure_route = 'not specified' ",
      "and a.source in ('", source_string, "')", query_addition
      ),
  toxval.db)

  # If no cases found, return
  if(!nrow(mat)) return()

  dict = readxl::read_xlsx(paste0(toxval.config()$datapath, "dictionary/exposure_route_not_specified_dict.xlsx"))

  mat_fix = mat %>%
    dplyr::left_join(dict,
                     by = c("source", "toxval_type_supercategory", "toxval_type", "exposure_route")) %>%
    dplyr::select(-exposure_route) %>%
    dplyr::rename(exposure_route = exposure_route_fix)

  missing = mat_fix %>%
    dplyr::filter(is.na(exposure_route)) %>%
    dplyr::mutate(exposure_route_fix = NA,
                  exposure_route = "not specified") %>%
    dplyr::select(-toxval_id, -subsource) %>%
    dplyr::distinct()

  # Export missing log
  if(nrow(missing)){
    # Generate filename
    missing_file = paste0(toxval.config()$datapath, "dictionary/missing/missing_exposure_route_not_specified_",
                          ifelse(is.null(source), "All_Sources_", source),
                          " ", subsource,".xlsx") %>%
      gsub("_ \\.xlsx", ".xlsx", .) %>%
      gsub(" \\.xlsx", ".xlsx", .)
    # Export for source
    writexl::write_xlsx(missing, missing_file)
  }

  # Select columns needed for update
  mat_fix = mat_fix %>%
    dplyr::select(toxval_id, exposure_route)

  ##############################################################################
  ### Batch Update
  ##############################################################################
  batch_size <- 50000
  startPosition <- 1
  endPosition <- nrow(mat_fix)
  if(endPosition < batch_size) batch_size = endPosition
  incrementPosition <- batch_size

  while(startPosition <= endPosition){
    if(incrementPosition > endPosition) incrementPosition = endPosition
    message("...Inserting new data in batch: ", batch_size, " startPosition: ", startPosition," : incrementPosition: ", incrementPosition,
            " (",round((incrementPosition/endPosition)*100, 3), "%)", " at: ", Sys.time())

    update_query <- paste0("UPDATE toxval a ",
                           "INNER JOIN z_updated_df b ",
                           "ON (a.toxval_id = b.toxval_id) ",
                           "SET a.exposure_route = b.exposure_route ",
                           "WHERE a.toxval_id in (",toString(mat_fix$toxval_id[startPosition:incrementPosition]),")",
                           query_addition)

    runUpdate(table="toxval",
              updateQuery = update_query,
              updated_df = mat_fix[startPosition:incrementPosition,],
              db=toxval.db)

    startPosition <- startPosition + batch_size
    incrementPosition <- startPosition + batch_size - 1
  }
}
