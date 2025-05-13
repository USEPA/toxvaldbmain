#-------------------------------------------------------------------------------------
#' Load the Genetox data from Grace
#' @param toxval.db The database to use.
#' @param source.db The source database to use.
#' @param sys.date The input file version
#' @param verbose If TRUE output debug information
#' @export
#--------------------------------------------------------------------------------------
toxval.load.genetox.all <- function(toxval.db, source.db, sys.date="2024-04-01", verbose=FALSE) {
  printCurrentFunction(toxval.db)

  # Handle genetox_details
  runQuery("DROP TABLE IF EXISTS genetox_details", toxval.db)
  file <- paste0(toxval.config()$datapath,"genetox/combined_genetox_standard_",sys.date,".xlsx")
  res0 = readxl::read_xlsx(file, guess_max = 21474836)

  # Drop entries missing CASRN (logic brought over from old script)
  res = res0 %>%
    # Drop entries missing CASRN (logic brought over from old script)
    tidyr::drop_na(casrn) %>%
    dplyr::mutate(
      # Clean names
      name = name %>%
        gsub("unnamed.+", "-", .) %>%
        fix.replace.unicode() %>%
        stringr::str_squish(),

      # Clean species
      species = species %>%
        fix.replace.unicode() %>%
        stringr::str_squish(),

      # Clean strain
      strain = strain %>%
        fix.replace.unicode() %>%
        stringr::str_squish(),

      # Clean cytotoxicity
      cytotoxicity = cytotoxicity %>%
        fix.replace.unicode() %>%
        stringr::str_squish(),

      # Fix type in year column
      year = year %>%
        gsub("2106", "2016", .),

      # Clean genetox_results
      genetox_results = genetox_results %>%
        fix.replace.unicode() %>%
        stringr::str_squish(),

      # Clean assay code
      assay_code = assay_code %>%
        gsub("&amp;", "&", .) %>%
        stringr::str_squish(),

      # Set sex to lowercase
      sex = tolower(sex),

      # Clean glp
      glp = glp %>%
        fix.replace.unicode() %>%
        stringr::str_squish(),
      genetox_details_id = 1:dplyr::n(),
      clowder_doc_id = "https://clowder.edap-cluster.com/files/680b9adfe4b096bca880a4ad"
    ) %>%
    dplyr::select(-dtxsid) %>%
    # Clean name/casrn and set chemical_id in source_chemical table
    source_chemical.extra(toxval.db, source.db, ., "Genetox Details")

  # Select columns of interest (old logic moved over)
  res2 = res %>%
    dplyr::select(-c(casrn, name, chemical_index))

  # Ensure only appropriate columns are present (old logic - untouched)
  n1 = names(res2)
  n2 = runQuery("desc genetox_summary_20210910",toxval.db)[,1]
  missing_n = n2[!n2 %in% n1]
  # n3 = n1[is.element(n1,n2)]
  # res2 = res2[,n3]
  # Fill blank fields
  res2[, missing_n] <- "-"

  # Send genetox_details data to ToxVal
  runInsertTable(res2, "genetox_details", toxval.db,verbose)

  #--------------------------------------------------------------------------------------

  # Handle genetox_summary
  runQuery("DROP TABLE IF EXISTS genetox_summary", toxval.db)
  file <- paste0(toxval.config()$datapath,"genetox/genetox_summary_",sys.date,".xlsx")
  res0 = readxl::read_xlsx(file, col_types = "text")

  res = res0 %>%
    # Drop entries with NA casrn (updated old logic)
    tidyr::drop_na(casrn) %>%
    dplyr::select(-dtxsid) %>%
    dplyr::mutate(
      # Clean chemical name
      name = name %>%
        fix.replace.unicode() %>%
        gsub("unnamed.+", "-", .) %>%
        gsub("List Acronyms", "", .) %>%
        stringr::str_squish(),

      # Set appropriate columns to numeric type
      reports_pos = as.numeric(reports_pos),
      reports_neg = as.numeric(reports_neg),
      reports_other = as.numeric(reports_other),
      genetox_summary_id = 1:dplyr::n(),
      clowder_doc_id = "https://clowder.edap-cluster.com/files/680b9b14e4b096bca880a4c7"
    ) %>%

    # Clean name/casrn and set chemical_id in source_chemical table
    source_chemical.extra(toxval.db, source.db, ., "Genetox Summary")

  # Select columns of interest (old logic moved over)
  res2 = res %>%
    dplyr::select(-c(casrn,name,chemical_index))

  # Ensure only appropriate columns are present (old logic - untouched)
  n1 = names(res2)
  n2 = runQuery("desc genetox_summary_20210910",toxval.db)[,1]
  missing_n = n2[!n2 %in% n1]
  # Fill blank fields
  res2[, missing_n] <- "-"

  # Send genetox_summary data to ToxVal
  runInsertTable(res2, "genetox_summary", toxval.db)
}
