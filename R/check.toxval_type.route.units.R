#-------------------------------------------------------------------------------------
#' Output distinct combinations of toxval_type, exposure_route, and toxval_units to check
#' @param toxval.db The version of toxvaldb to use.
#' @param source Source to check
#' @param subsource Subsource to check (NULL default)
#' @param load.dict Name of dictionary containing expected combinations, or NULL if dictionary should not be read
#' @export
#-------------------------------------------------------------------------------------
check.toxval_type.route.units <- function(toxval.db,
                                          source=NULL,
                                          subsource=NULL,
                                          load.dict="Repo/dictionary/toxval_type.route.units.dictionary.xlsx") {
  printCurrentFunction(paste(toxval.db,":", source))

  # Initialize values for slist and output_file to match all sources
  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  output_file = paste0("Repo/QC Reports/toxval_type.route.units_to_check_ALL SOURCES_", Sys.Date(), ".xlsx")

  # Alter slist and output_file if source is specified
  if(!is.null(source)) {
    slist = source
    output_file = paste0("Repo/QC Reports/toxval_type.route.units_to_check_", source, "_", Sys.Date(), ".xlsx")
  }

  source_string = slist %>%
    paste0(., collapse="', '")

  # Handle addition of subsource for queries and output_file
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
    output_file = paste0("Repo/QC Reports/toxval_type.route.units_to_check_",
                         source, "_", subsource, "_", Sys.Date(), ".xlsx")
  }

  # Track all type/route/units combinations identified
  all_combinations = tibble::tibble(
    source = character(),
    study_type = character(),
    exposure_route = character(),
    toxval_type = character(),
    toxval_units = character(),
  )

  # Get all type/route/units combinations for current source
  curr_combo_query = paste0("SELECT DISTINCT source, study_type, exposure_route, toxval_type, toxval_units ",
                            "FROM toxval WHERE source in ('",source_string,"')", query_addition)
  all_combinations = runQuery(curr_combo_query, toxval.db) %>%
    # Append current source data to cumulative DF
    dplyr::bind_rows(all_combinations) %>%
    dplyr::distinct()

  # Read in dictionary containing expected combinations, if specified
  if(is.null(load.dict)) load.dict = "not a file"
  if(file.exists(load.dict)) {
    expected_combinations = readxl::read_xlsx(load.dict) %>%
      dplyr::mutate(expected = 1) %>%
      dplyr::distinct()

    # Filter expected combinations from output
    all_combinations = all_combinations %>%
      dplyr::left_join(expected_combinations,
                       by=c("source", "study_type", "exposure_route", "toxval_type", "toxval_units")) %>%
      dplyr::filter(is.na(expected)) %>%
      dplyr::select(-expected) %>%
      dplyr::distinct()
  }

  # Write combinations to output file
  if(nrow(all_combinations) > 0) {
    writexl::write_xlsx(all_combinations, output_file)
    cat(paste0(nrow(all_combinations),
               " distinct type/route/units combinations written to ", output_file, "\n"))
  } else {
    cat("No unexpected type/route/units combinations identified\n")
  }
}
