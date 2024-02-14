#-------------------------------------------------------------------------------------
#' Load the Genetox data from Grace
#' @param toxval.db The database to use.
#' @param source.db The source database to use.
#' @param sys.date The input file version
#' @param verbose If TRUE output debug information
#' @export
#--------------------------------------------------------------------------------------
toxval.load.genetox.all <- function(toxval.db, source.db, sys.date="2021-09-10", verbose=FALSE) {
  printCurrentFunction(toxval.db)

  # Handle genetox_details
  runQuery("delete from genetox_details", toxval.db)
  file <- paste0(toxval.config()$datapath,"genetox/dataprep/combined_genetox_standard_",sys.date,".xlsx")
  res0 = readxl::read_xlsx(file)

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
        tolower() %>%
        stringr::str_squish(),

      # Clean strain
      strain = strain %>%
        fix.replace.unicode() %>%
        tolower() %>%
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
        stringr::str_squish()
    ) %>%
    # Clean name/casrn and set chemical_id in source_chemical table
    source_chemical.extra(toxval.db, source.db, ., "Genetox Details")

  # Select columns of interest (old logic moved over)
  res2 = res %>%
    dplyr::select(-c(casrn, name, chemical_index))

  # Ensure only appropriate columns are present (old logic - untouched)
  n1 = names(res2)
  n2 = runQuery("desc genetox_details",toxval.db)[,1]
  n3 = n1[is.element(n1,n2)]
  res2 = res2[,n3]

  # Send genetox_details data to ToxVal
  runInsertTable(res2, "genetox_details", toxval.db,verbose)

  #--------------------------------------------------------------------------------------

  # Handle genetox_summary
  runQuery("delete from genetox_summary", toxval.db)
  file <- paste0(toxval.config()$datapath,"genetox/dataprep/genetox_summary_",sys.date,".xlsx")
  res0 = readxl::read_xlsx(file, col_types = "text")

  res = res0 %>%
    # Drop entries with NA casrn (updated old logic)
    tidyr::drop_na(casrn) %>%

    dplyr::mutate(
      # Clean chemical name
      name = name %>%
        fix.replace.unicode() %>%
        stringr::str_squish(),

      # Set appropriate columns to numeric type
      reports_pos = as.numeric(reports_pos),
      reports_neg = as.numeric(reports_neg),
      reports_other = as.numeric(reports_other)
    ) %>%

    # Clean name/casrn and set chemical_id in source_chemical table
    source_chemical.extra(toxval.db, source.db, ., "Genetox Summary")

  # Select columns of interest (old logic moved over)
  res2 = res %>%
    dplyr::select(-c(casrn,name,chemical_index))

  # Send genetox_summary data to ToxVal
  runInsertTable(res2, "genetox_summary", toxval.db)
}
