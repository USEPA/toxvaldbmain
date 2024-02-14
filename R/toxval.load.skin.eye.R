#--------------------------------------------------------------------------------------
#' Load the Skin eye data
#'
#' @param toxval.db Database version
#' @param verbose if TRUE, print diagnostic messages along the way
#' @export
#--------------------------------------------------------------------------------------
toxval.load.skin.eye <- function(toxval.db, source.db, verbose=FALSE) {
  printCurrentFunction(toxval.db)
  runQuery("delete from skin_eye",toxval.db)

  cat("  load GHS data\n")
  # Load skin and eye data
  file <- paste0(toxval.config()$datapath,"skin_eye/GHS skin and eye data v3.xlsx")
  mat <- readxl::read_xlsx(file, sheet="Data")

  # Clean skin and eye data
  mat = mat %>% dplyr::mutate(
    record_url = "-",
    score = dplyr::case_when(
        score == "N/A" ~ "NC",
        TRUE ~ score
      )
    )

  # Load GHS sources data
  file <- paste0(toxval.config()$datapath,"skin_eye/GHS_sources.xlsx")
  dict <- readxl::read_xlsx(file)

  # Add record_url information to GHS data
  url_lookup = dict %>%
    dplyr::select(tidyr::all_of(c("Abbreviation", "Source URL"))) %>%
    dplyr::rename(source = Abbreviation, record_url = `Source URL`)
  # Map by source name
  mat = mat %>%
    dplyr::select(-record_url) %>%
    dplyr::left_join(url_lookup,
                     by = "source")

  # Rename columns
  names(mat) <- c("casrn","name","endpoint","source","score","route","classification","hazard_code",
                  "hazard_statement","rationale","note","note2","valueMassOperator","valueMass",
                  "valueMassUnits","authority","record_url")

  mat = mat %>%
    # Add result_text column
    tidyr::unite(
      "result_text",
      hazard_statement, hazard_code, rationale,
      sep = " ",
      na.rm = TRUE
    ) %>%
    # Select relevant columns
    dplyr::select(tidyselect::all_of(c("casrn","name","endpoint","source","score","classification",
                                       "result_text","authority","record_url"))) %>%
    # Perform final cleaning operations
    dplyr::mutate(
      # Fix casrn values
      casrn = sapply(casrn, FUN=fix.casrn) %>%
        dplyr::na_if("NOCAS"),

      # Remove HTML and other artifacts from name
      name = name %>%
        gsub("&alpha;", "a", .) %>%
        gsub("&le;", "<=", .) %>%
        gsub("<br>", " ", .) %>%
        gsub("\\\\'n|\\\\n|\\\\r", " ", .) %>%
        gsub("\\\\'", "'", .) %>%
        gsub('\\\\"', '"', .) %>%
        fix.replace.unicode() %>%
        stringr::str_squish(),

      # Clean up result_text formatting
      result_text = result_text %>%
        gsub("- - ", "", .) %>%
        stringr::str_squish()
    )

  # Clean name/casrn and set chemical_id in source_chemical table
  mat2 = source_chemical.extra(toxval.db, source.db, mat, "Skin Eye")
  mat2 = subset(mat2,select=-c(casrn,name,chemical_index))
  # Load first set of skin_eye data to ToxVal
  runInsertTable(mat2, "skin_eye", toxval.db, verbose)

  cat("  load eChemPortal data\n")
  file <- paste0(toxval.config()$datapath,"skin_eye/echemportal_skin_eye_parsed.xlsx")
  mat <- readxl::read_xlsx(file)

  mat = mat %>%
    # Rename result column to result_text
    dplyr::rename(result_text = result) %>%
    # Add source information
    dplyr::mutate(
      # Add hard-coded information
      source = "ECHA eChemPortal",
      authority = "Screening",

      # Get appropriate study_type
      study_type = dplyr::case_when(
        study_type == "Eye irritation" ~ "eye irritation",
        study_type == "Skin irritation / corrosion" ~ "skin irritation",
        study_type == "Skin sensitisation" ~ "skin sensitization",
        TRUE ~ study_type
      ),

      # Clean name column
      name = dplyr::case_when(
        grepl("unnamed", name) ~ as.character(NA),
        TRUE ~ name
      ) %>%
        fix.replace.unicode() %>%
        stringr::str_squish(),

      # Clean species column
      species = dplyr::case_when(
        grepl("not applicable", species, ignore.case=TRUE) ~ stringr::str_extract(species, "[hH]uman|[cH]hicken|[wW]hite [lL]eghorn"),
        TRUE ~ species
      ) %>%
        gsub("other:", "", .) %>%
        gsub("N\\/A:?|NA:?|n\\/a:?|N\\/a:?", "", .) %>%
        fix.replace.unicode() %>%
        tolower() %>%
        stringr::str_squish(),

      # Clean strain column
      strain = strain %>%
        gsub("other:", "", .) %>%
        gsub("'", "", .) %>%
        gsub('"', "", .) %>%
        gsub("albiino", "albino", .) %>%
        fix.replace.unicode() %>%
        tolower() %>%
        gsub("^,", "", .) %>%
        stringr::str_squish(),

      # Clean result_text field
      result_text = result_text %>%
        gsub("other:", "", .) %>%
        # Remove string of \\\
        gsub("\\\\\\\\\\\\", "", .) %>%
        fix.replace.unicode() %>%
        stringr::str_squish(),

      # Remove unicode symbols from guideline
      guideline = fix.replace.unicode(guideline)
    )

  # Write output to Excel
  file <- paste0(toxval.config()$datapath,"skin_eye/skin_eye raw.xlsx")
  writexl::write_xlsx(mat,file)

  # Clean name/casrn and set chemical_id in source_chemical table
  mat2 = source_chemical.extra(toxval.db, source.db, mat, "Skin Eye") %>%
    dplyr::select(-c(casrn,name, chemical_index))

  # Push skin_eye data to ToxVal
  runInsertTable(mat2, "skin_eye", toxval.db, verbose=verbose)
}
