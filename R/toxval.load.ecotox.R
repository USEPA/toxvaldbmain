#--------------------------------------------------------------------------------------
#' Load ECOTOX from the datahub to toxval
#' @param toxval.db The database version to use
#' @param source.db The source database
#' @param log If TRUE, send output to a log file
#' @param remove_null_dtxsid If TRUE, delete source records without curated DTXSID value
#' @param sys.date The version of the data to be used
#--------------------------------------------------------------------------------------
toxval.load.ecotox <- function(toxval.db, source.db, log=FALSE, remove_null_dtxsid=TRUE, sys.date="2024-09-19"){
  source = "ECOTOX"
  source_table = "direct load"
  verbose = log
  # Whether to load ECOTOX data from stored RData
  do.load = FALSE
  #####################################################################
  cat("start output log, log files for each source can be accessed from output_log folder\n")
  #####################################################################
  if(log) {
    con1 = file.path(toxval.config()$datapath,paste0(source,"_",Sys.Date(),".log"))
    con1 = logr::log_open(con1)
    con = file(paste0(toxval.config()$datapath,source,"_",Sys.Date(),".log"))
    sink(con, append=TRUE)
    sink(con, append=TRUE, type="message")
  }
  #####################################################################
  cat("clean source_info by source\n")
  #####################################################################
  import.source.info.by.source(toxval.db, source)

  #####################################################################
  cat("clean by source\n")
  #####################################################################
  clean.toxval.by.source(toxval.db,source)

  #####################################################################
  cat("load data to res\n")
  #####################################################################
  # Load ECOTOX data
  if(!exists("ECOTOX")) do.load=TRUE
  if(do.load) {
    cat("load ECOTOX data\n")
    file = paste0(toxval.config()$datapath,"ecotox/ecotox_files/ECOTOX ",sys.date,".RData")
    load(file=file)
    ECOTOX <- ECOTOX %T>%
      # lowercase names
      { names(.) <- tolower(names(.)) } %>%
      # Filter out NA DTXSID values
      dplyr::filter(!is.na(dsstox_substance_id)) %>%
      dplyr::distinct()

    res0 <<- ECOTOX

    # Write dictionary for current ECOTOX version
    dict = dplyr::distinct(ECOTOX[,c("species_scientific_name","species_common_name","species_group","habitat")])
    file = paste0(toxval.config()$datapath,"ecotox/ecotox_files/ECOTOX_dictionary_",sys.date,".xlsx")
    writexl::write_xlsx(dict, file)
  } else {
    res0 <- ECOTOX
  }

  # Create column renaming map
  rename_list <- c(
    dtxsid = "dsstox_substance_id",
    casrn = "cas_number",
    name = "chemical_name",
    species_id = "species_number",
    common_name = "species_common_name",
    latin_name = "species_scientific_name",
    habitat = "habitat",
    ecotox_group = "species_group",
    study_type = "effect",
    toxval_type = "endpoint",
    conc1_author = "conc1_author",
    conc1_units_author = "conc1_units_author",
    conc1_mean_op = "conc1_mean_op",
    conc1_mean = "conc1_mean",
    conc1_min_op = "conc1_min_op",
    conc1_min = "conc1_min",
    conc1_max_op = "conc1_max_op",
    conc1_max = "conc1_max",
    conc1_units_std = "conc1_units_std",
    conc1_mean_std = "conc1_mean_std",
    exposure_route = "exposure_group",
    exposure_method = "exposure_type",
    media = "media_type",
    reference = "source",
    author = "author",
    title = "title",
    year = "publication_year",
    # pmid = "reference_number",
    # External ECOTOX reference number
    external_source_id = "reference_number",
    observed_duration_std="observed_duration_std",
    observed_duration_units_std="observed_duration_units_std",
    observed_duration_mean_op="observ_duration_mean_op",
    observed_duration_mean="observ_duration_mean",
    observed_duration_min_op="observ_duration_min_op",
    observed_duration_min="observ_duration_min",
    observed_duration_max_op="observ_duration_max_op",
    observed_duration_max="observ_duration_max",
    observed_duration_unit="observ_duration_unit",
    observed_duration_unit_desc="observ_duration_unit_desc",
    effect_measurement = "effect_measurement",
    exposure_duration_mean_op = "exposure_duration_mean_op",
    exposure_duration_mean = "exposure_duration_mean",
    exposure_duration_min_op = "exposure_duration_min_op",
    exposure_duration_min = "exposure_duration_min",
    exposure_duration_max_op = "exposure_duration_max_op",
    exposure_duration_max = "exposure_duration_max",
    exposure_duration_unit = "exposure_duration_unit",
    exposure_duration_unit_desc = "exposure_duration_unit_desc",
    exposure_duration_std = "exposure_duration_std",
    exposure_duration_units_std = "exposure_duration_units_std",
    quality = "control_type",
    organism_lifestage = "organism_lifestage",
    organism_lifestage_age_tom = "organism_lifestage_age_tom",
    # Do we want the qualifier information or not?
    sex = "organism_sex",
    organism_sex_tom = "organism_sex_tom",
    result_sample_unit_desc = "result_sample_unit_desc"
  )

  # Filter to specific toxval_type entries
  toxval_type_list = c("BMD",
                       "LEL", "LOAEC", "LOEC", "LOAEL", "LOEL",
                       "NEL", "NOAEC", "NOEC", "NOAEL", "NOEL")

  res <- res0 %>%
    # Apply name mapping, and select only renamed columns (columns of interest)
    dplyr::rename(dplyr::all_of(rename_list)) %>%
    # Remove records with test_location "Field *", only retain "Lab" or "Not Reported"
    dplyr::filter(!grepl("Field", test_location)) %>%
    dplyr::select(dplyr::all_of(names(rename_list))) %>%

    dplyr::mutate(
      external_source_id_desc = "ECOTOX Reference Number",
      # Set species to lower and select latin_name when common_name is not available
      species_original = dplyr::case_when(
        !is.na(common_name) ~ common_name,
        TRUE ~ latin_name
      ) %>%
        # Replace escaped apostrophe
        gsub("\\\\'", "'", .) %>%
        tolower(),

      exposure_method = stringr::str_squish(exposure_method),

      # Clean toxval_type
      toxval_type = toxval_type %>%
        gsub("\\/|\\*","", .) %>%
        gsub("--", "-", .),

      # Translate exposure_route values
      exposure_route = dplyr::case_when(
        grepl("multiple", exposure_route, ignore.case=TRUE) ~ "multiple",
        grepl("oral", exposure_route, ignore.case=TRUE) ~ "oral",
        grepl("aquatic", exposure_route, ignore.case=TRUE) ~ "aquatic",
        grepl("unknown", exposure_route, ignore.case=TRUE) ~ "unknown",
        exposure_route == "Topical" ~ "dermal",
        TRUE ~ exposure_route
      ) %>% tolower(),

      sex = dplyr::case_when(
        # If they conflict, use measured in organism_sex_tom
        sex %in% c("NR", "NC") ~ organism_sex_tom,
        grepl("Measured in:", organism_sex_tom, fixed=TRUE) ~ stringr::str_extract(organism_sex_tom, "(?<=\\bMeasured in:\\s)(\\w+)"),
        TRUE ~ sex
      ) %>%
        gsub("Both", "male/female", .) %>%
        tolower()
    ) %>%
    # Filter to specific toxval_type entries
    dplyr::filter(grepl(paste0("^", toxval_type_list,
                               collapse = "|"),
                        toxval_type)) %>%

    # Combine existing values to create long_ref and critical_effect
    tidyr::unite(
      "long_ref",
      reference, author, title, year,
      sep = " ",
      remove = FALSE,
      na.rm = TRUE
    ) %>%
    tidyr::unite(
      "critical_effect",
      study_type, effect_measurement,
      sep = ": ",
      remove = FALSE,
      na.rm = TRUE
    ) %>%
    # Append select result_sample_unit_desc values to critical_effect
    dplyr::mutate(
      critical_effect = dplyr::case_when(
        result_sample_unit_desc %in% c("Juvenile",
                                       "Embryo",
                                       "Mature (no specified age)",
                                       "Adult",
                                       "Litters",
                                       "Fetus",
                                       "Pregnant female",
                                       "Parent, 1st generation",
                                       "Male fetus",
                                       "Female fetus") ~ paste0(result_sample_unit_desc, ": ", critical_effect),
        TRUE ~ critical_effect
      ) %>%
        gsub("\\/$", "", .),
      # Extract generation from result_sample_unit_desc
      # https://regex101.com/r/JNxhAZ/1
      generation = stringr::str_extract(result_sample_unit_desc,
                                        "(\\S+)\\s+(?:generation?[A-Za-z])"),
      lifestage = dplyr::case_when(
        # Extract life stage after "Measured in:" when generation is NA
        # https://regex101.com/r/rJ8XT0/1
        is.na(generation) & grepl("Measured in:", organism_lifestage_age_tom, fixed=TRUE) ~
          stringr::str_extract(organism_lifestage_age_tom, "(?<=\\bMeasured in:\\s)(\\w+)"),
        TRUE ~ "-"
      )
    ) %>%

    # Replace NA values ("NA", "NR", "")
    dplyr::mutate(dplyr::across(tidyselect::where(is.character), ~dplyr::na_if(., "NA")),
                  dplyr::across(tidyselect::where(is.character), ~dplyr::na_if(., "NR")),
                  dplyr::across(tidyselect::where(is.character), ~dplyr::na_if(., ""))) %>%

    # Remove effect_measurement field
    dplyr::select(-effect_measurement) %>%

    # Programmatically set toxval_numeric, units, and qualifier from conc1_* fields
    ecotox.select.toxval.numeric(in_data = .) %>%

    dplyr::mutate(
      #Get rid of wacko characters (seem to mean auto or AI generated)
      toxval_units = toxval_units %>%
        # Starts with match to remove
        gsub("^AI |^ae |^ai ", "", .) %>%
        stringr::str_squish(),

      # Fill in toxval_numeric_qualifier
      toxval_numeric_qualifier = dplyr::case_when(
        is.na(toxval_numeric_qualifier) | toxval_numeric_qualifier == "" ~ "-",
        TRUE ~ as.character(toxval_numeric_qualifier)
      )
    ) %>%

    # Programmatically set study_duration, units, and qualifier from observ* fields
    ecotox.select.study.duration(in_data = ., dur_col = "exposure_duration") %>%

    dplyr::mutate(
      # Clean study_duration_units
      study_duration_units = study_duration_units %>%
        gsub("Day\\(s\\)", "days", .) %>%
        gsub("Hour\\(s\\)", "hours", .) %>%
        gsub("Week\\(s\\)", "weeks", .) %>%
        gsub("Month\\(s\\)", "months", .) %>%
        gsub("Minute\\(s\\)", "minutes", .) %>%
        gsub("Second\\(s\\)", "seconds", .) %>%
        gsub("Year\\(s\\)", "Years", .) %>%
        gsub("post ", "post-", .) %>%
        gsub("pre ", "pre-", .) %>%
        gsub(";", "; ", .) %>%
        gsub("dayss", "days", .) %>%
        stringr::str_squish() %>%
        tolower(),

      # Use -999 as proxy for NA in study_duration_value
      study_duration_value = tidyr::replace_na(study_duration_value, -999),

      # Fill study_duration_qualifer NA vals with "-"
      study_duration_qualifier = tidyr::replace_na(study_duration_qualifier, "-"),

      # Set source information
      source = !!source
    )

  # Fix toxval_type, numeric, and units for type "LT*"
  res1 <- res[!grepl("^LT", res$toxval_type),]
  res2 <- res[grepl("^LT", res$toxval_type),]

  res2 <- res2 %>%
    dplyr::mutate(toxval_type = paste0(toxval_type,
                                       "@ ",
                                       toxval_numeric_qualifier[!toxval_numeric_qualifier %in% c("-")],
                                       signif(toxval_numeric, digits=4), " ",
                                       toxval_units),
                  toxval_numeric = study_duration_value,
                  toxval_units = study_duration_units)

  # Rejoin res
  res <- res1 %>%
    dplyr::bind_rows(res2) %>%
    # Perform final cleaning/field addition operations
    dplyr::mutate(
      quality = paste("Control type:",quality),
      # study_duration_class = "chronic",

      # Fix CASRN values
      casrn = sapply(casrn, FUN=fix.casrn)
    ) %>%

    # Filter toxval_numeric greater than 0
    dplyr::filter(toxval_numeric >= 0) %>%

    # Drop rows with NA toxval cols
    tidyr::drop_na(toxval_numeric, toxval_type, toxval_units) %>%

    # Remove duplicate entries
    dplyr::distinct() %>%

    # Remove excess whitespace, replace NA with "-"
    dplyr::mutate(dplyr::across(tidyselect::where(is.character), ~stringr::str_squish(.) %>%
                                  tidyr::replace_na("-")))

  # Remove intermediates
  rm(res1, res2, ECOTOX)

  # Filter out identified "fail" studies by long_ref
  fail_studies = readxl::read_xlsx(paste0(toxval.config()$datapath,"ecotox/ecotox_files/ecotox_studies_fail.xlsx")) %>%
    dplyr::select(long_ref, `fail or revise`) %>%
    tidyr::separate_rows(long_ref, sep = "\\|") %>%
    dplyr::mutate(long_ref = stringr::str_squish(long_ref)) %>%
    dplyr::filter(`fail or revise` %in% c("fail"),
                  !long_ref %in% c("-")) %>%
    dplyr::pull(long_ref) %>%
    unique()

  res = res %>%
    dplyr::filter(!long_ref %in% fail_studies)

  #####################################################################
  cat("add other columns to res\n")
  #####################################################################
  res <- fill.toxval.defaults(toxval.db, res)

  # Curate chemicals to toxval_source source_chemical table
  # res = source_chemical.ecotox(toxval.db,source.db,res,source,chem.check.halt=FALSE,
  #                              casrn.col="casrn",name.col="name",verbose=FALSE)

  # Build chem map
  chem_map = source_chemical.ecotox(toxval.db,
                                    source.db,
                                    res %>%
                                      dplyr::select(casrn, name, dtxsid) %>%
                                      dplyr::distinct(),
                                    source,
                                    chem.check.halt=FALSE,
                                    casrn.col="casrn", name.col="name", verbose=FALSE)

  # Add chem_map info to res
  res <- res %>%
    dplyr::left_join(chem_map %>%
                       dplyr::select(-chemical_index),
                     by = c("dtxsid", "name", "casrn"))

  # Remove intermediate
  rm(chem_map)

  # Ensure each row has a chemical_id
  if(anyNA(res$chemical_id)){
    cat("Error joining chemical_id back to ECOTOX res...\n")
    browser()
  }

  # Perform deduping (reporting time elapse - ~14-24 minutes)
  system.time({
    hashing_cols = c(toxval.config()$hashing_cols[!(toxval.config()$hashing_cols %in% c("critical_effect", "study_type"))],
                     "species_id", "common_name", "latin_name", "ecotox_group", "external_source_id")
    res = toxval.load.dedup(res,
                            hashing_cols = c(hashing_cols, paste0(hashing_cols, "_original"))) %>%
      # Update critical_effect delimiter to "|"
      dplyr::mutate(critical_effect = critical_effect %>%
                      gsub(" |::| ", "|", x=., fixed = TRUE),
                    study_type = study_type %>%
                      gsub(" |::| ", "|", x=., fixed = TRUE))
  })

  cat("set the source_hash\n")
  # Vectorized approach to source_hash generation
  non_hash_cols <- toxval.config()$non_hash_cols
  res = res %>%
    tidyr::unite(hash_col, tidyselect::all_of(sort(names(.)[!names(.) %in% non_hash_cols])), sep="-", remove = FALSE) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(source_hash = digest::digest(hash_col, serialize = FALSE)) %>%
    dplyr::ungroup() %>%
    dplyr::select(-hash_col)

  ##############################################################################
  ### Phase 1: Apply QC fixes from internal QC (matching by source_hash for now)
  ##############################################################################
  # qc_fixes = readxl::read_xlsx(paste0(toxval.config()$datapath,"ecotox/ecotox_files/qc_files/ECOTOX_QC_fixes_20240930.xlsx")) %>%
  #   # Remove fields not QC'd
  #   dplyr::select(-c("toxval_numeric_qualifier", "name", "common_name",
  #                    "critical_effect", "generation", "lifestage", "sex",
  #                    "exposure_method", "study_type")) %>%
  #   dplyr::select(dplyr::any_of(c(names(res), qc_notes="notes"))) %>%
  #   tidyr::separate_rows(source_hash, sep = ",") %>%
  #   dplyr::mutate(source_hash = stringr::str_squish(source_hash)) %>%
  #   tidyr::pivot_longer(cols = -c("source_hash"),
  #                       names_to = "field_fix",
  #                       values_to = "value_fix",
  #                       values_transform = list(value_fix = as.character)
  #   )

  qc_fixes = lapply(list.files(paste0(toxval.config()$datapath,"ecotox/ecotox_files/qc_files/Phase 1"),
                               full.names = TRUE,
                               pattern = "xlsx") %>%
                      .[!grepl("~", .)],
                    function(f_name){
                      readxl::read_xlsx(f_name, col_types = "text") %>%
                        # Remove fields known to not have been changed during QC
                        dplyr::select(-dplyr::any_of(c("species", "species_original", "common_name"))) %>%
                        dplyr::mutate(qc_file_name = basename(f_name),
                                      # dplyr::across(dplyr::any_of(
                                      #   c("study_duration_value",
                                      #     "toxval_numeric",
                                      #     "toxval_numeric_original")),
                                      #   ~as.character(.))
                                      # Replace NA with - before join to see which fields are
                                      # NA due to not being present in a file
                                      dplyr::across(dplyr::everything(), ~tidyr::replace_na(., "-"))
                        )
                    }) %>%
    dplyr::bind_rows() %>%
    # reset numeric and units to be same as _original
    dplyr::mutate(toxval_numeric = toxval_numeric_original,
                  toxval_units = toxval_units_original) %>%
    # Combine different QC columns across files
    tidyr::unite(col = "qc_notes",
                 notes, cw_qc, cw_edit,
                 sep = "|",
                 na.rm = TRUE) %>%
    dplyr::filter(!qc_notes == "need text"# ,
                  # !grepl("fail", qc_notes)
    ) %>%
    dplyr::select(dplyr::any_of(c(names(res), "qc_notes", "qc_file_name"))) %>%
    tidyr::separate_longer_delim(source_hash, delim = ",") %>%
    tidyr::separate_longer_delim(source_hash, delim = "|::|") %>%
    dplyr::mutate(source_hash = stringr::str_squish(source_hash)) %>%
    tidyr::pivot_longer(cols = -c("source_hash", "qc_notes", "qc_file_name"),
                        names_to = "field_fix",
                        values_to = "value_fix",
                        values_transform = list(value_fix = as.character)
    ) %>%
    dplyr::mutate(
      value_fix = value_fix %>%
        tidyr::replace_na("N/A"),
      qc_notes = qc_notes %>%
        gsub("^chaned", "changed", .) %>%
        gsub("^duplicate of", "fail - duplicate of ", .) %>%
        gsub("^delete", "fail", .) %>%
        gsub("fail - ", "fail: ", .) %>%
        gsub("^fail,", "fail:", .) %>%
        stringr::str_squish(),
      qc_status = dplyr::case_when(
        grepl("pass|edit|changed", qc_notes) ~ "pass",
        TRUE ~ "fail"
      ),
      # If qc_status = fail, do not update the record fields
      value_fix = dplyr::case_when(
        qc_status == "fail" ~ "N/A",
        TRUE ~ value_fix
      ),
      qc_category = dplyr::case_when(
        grepl("edit|change", qc_notes) ~ "Source overall passed QC, and this record was expert reviewed and revised from ECOTOX source",
        TRUE ~ "Source overall passed QC, and this record was expert reviewed"
      )
    )

  # Check for duplicate/conflicting field changes across files
  qc_fixes_dups = toxval.load.dedup(qc_fixes,
                                    hashing_cols = c("source_hash", "field_fix"))

  # Check if any collapsed values in fixes
  if(any(grepl("|::|", qc_fixes_dups$value_fix, fixed = TRUE))){
    stop("Conflicting qc_fix changes applied to record field across QC files...")
  }

  res_fix = res %>%
    dplyr::filter(source_hash %in% unique(qc_fixes$source_hash)) %>%
    tidyr::pivot_longer(-source_hash,
                        names_to = "field_orig",
                        values_to = "value_orig",
                        values_transform = list(value_orig = as.character)
    ) %>%
    # Join QC fixes - changed field values
    dplyr::left_join(qc_fixes %>%
                       dplyr::select(source_hash, field_fix, value_fix) %>%
                       dplyr::distinct(),
                     by = c("source_hash", "field_orig"="field_fix")) %>%
    dplyr::rowwise() %>%
    # Select final field value based on if QC'd field was changed
    dplyr::mutate(qc_changed = identical(value_fix, value_orig),
                  value_final = dplyr::case_when(
                    qc_changed == FALSE & !value_fix %in% c(NA, "N/A") ~ value_fix,
                    TRUE ~ value_orig
                  )) %>%
    dplyr::ungroup() %>%
    # Join QC fixes - status and category
    dplyr::left_join(qc_fixes %>%
                       dplyr::select(source_hash, qc_status, qc_category) %>%
                       dplyr::distinct(),
                     by = "source_hash") %>%
    dplyr::select(source_hash, field_orig, value_final, qc_status, qc_category) %>%
    tidyr::pivot_wider(id_cols = c("source_hash", "qc_status", "qc_category"),
                       names_from = "field_orig",
                       values_from = "value_final") %>%
    # Update data type as needed to rejoin
    dplyr::mutate(
      dplyr::across(dplyr::any_of(c("species_id", "year", "toxval_numeric",
                                    "external_source_id", "study_duration_value")), ~as.numeric(.)))

  if(any(duplicated(res_fix$source_hash))){
    stop("Duplicate res_fix source_hash values found...")
  }

  if(anyNA(res_fix$toxval_numeric)){
    stop("NA toxval_numeric found")
  }

  # Clean up QC'd critical_effect, remove "-" in piped effects
  res_fix = res_fix %>%
    dplyr::mutate(dplyr::across(dplyr::any_of(c("critical_effect",
                                                "long_ref")),
                                ~ gsub(" | -", "", ., fixed = TRUE) %>%
                                  stringr::str_squish()))

  # # Review QC'd fields
  # tmp = res_fix %>%
  #   dplyr::select(source_hash, dplyr::any_of(hashing_cols), species_original, qc_status, qc_category) %>%
  #   dplyr::select(-ecotox_group, -species_id, -external_source_id, -common_name, -latin_name) %>%
  #   dplyr::filter(!qc_status == "fail") %>%
  #   dplyr::distinct()
  #
  # for(field in names(tmp)){
  #   message(field)
  #   print(unique(tmp[[field]]))
  # }

  res = res %>%
    # Filter out old that changed
    dplyr::filter(!source_hash %in% res_fix$source_hash,
                  # Filter out records in same study reference that were not manually checked
                  !external_source_id %in% res_fix$external_source_id) %>%
    # Join updated records
    dplyr::bind_rows(res_fix) %>%
    # Drop old source_hash field to prep for rehashing of QC'd records
    dplyr::select(-source_hash)

  ### Perform deduping and hashing again after QC fixes applied
  # Perform deduping (reporting time elapse - ~14-24 minutes)
  system.time({
    hashing_cols = c(toxval.config()$hashing_cols[!(toxval.config()$hashing_cols %in% c("critical_effect", "study_type"))],
                     "species_id", "common_name", "latin_name", "ecotox_group", "external_source_id")
    res = toxval.load.dedup(res,
                            hashing_cols = c(hashing_cols, paste0(hashing_cols, "_original"))) %>%
      # Update critical_effect delimiter to "|"
      dplyr::mutate(critical_effect = critical_effect %>%
                      gsub(" |::| ", "|", x=., fixed = TRUE),
                    study_type = study_type %>%
                      gsub(" |::| ", "|", x=., fixed = TRUE))
  })

  # Check for fields that were collapsed
  collapsed_res_fields = lapply(names(res), function(f){
    if(sum(stringr::str_detect(res[[f]], '\\|::\\|'
    ), na.rm = TRUE) > 0){
      return(f)
    }
  }) %>%
    purrr::compact() %>%
    unlist()

  # No field collapsing in hashing columns or identifier columns
  if(any(collapsed_res_fields %in% c(hashing_cols, "chemical_id", "dtxsid"))){
    stop("Recording collapsing occurred in hashing or unique identifier columns...")
  }

  cat("set the source_hash\n")
  # Vectorized approach to source_hash generation
  non_hash_cols <- toxval.config()$non_hash_cols
  res = res %>%
    tidyr::unite(hash_col, tidyselect::all_of(sort(names(.)[!names(.) %in% non_hash_cols])), sep="-", remove = FALSE) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(source_hash = digest::digest(hash_col, serialize = FALSE)) %>%
    dplyr::ungroup() %>%
    dplyr::select(-hash_col)

  ##############################################################################
  ### Phase 2: Apply QC fixes from ICF QC (matching by source_hash for now)
  ##############################################################################
  qc_fixes = lapply(list.files(paste0(toxval.config()$datapath,"ecotox/ecotox_files/qc_files/Phase 2"),
                               full.names = TRUE,
                               pattern = "xlsx") %>%
                      .[!grepl("~", .)],
                    function(f_name){
                      readxl::read_xlsx(f_name, col_types = "text") %>%
                        # Remove fields known to not have been changed during QC
                        # dplyr::select(-dplyr::any_of(c("species", "species_original", "common_name"))) %>%
                        dplyr::mutate(qc_file_name = basename(f_name),
                                      # dplyr::across(dplyr::any_of(
                                      #   c("study_duration_value",
                                      #     "toxval_numeric",
                                      #     "toxval_numeric_original")),
                                      #   ~as.character(.))
                                      # Replace NA with - before join to see which fields are
                                      # NA due to not being present in a file
                                      dplyr::across(dplyr::everything(), ~tidyr::replace_na(., "-"))
                        ) %>%
                        dplyr::rename(critical_effect = toxicological_effect)
                    }) %>%
    dplyr::bind_rows() %>%
    dplyr::filter(!source_hash %in% c("-")) %>%
    # reset numeric and units to be same as _original
    dplyr::mutate(toxval_numeric = toxval_numeric_original,
                  toxval_units = toxval_units_original) %>%
    # Combine different QC columns across files
    dplyr::rename(qc_notes = ICF_notes_tag) %>%
    dplyr::mutate(qc_notes = tolower(qc_notes)) %>%
    # tidyr::unite(col = "qc_notes",
    #              notes, cw_qc, cw_edit,
    #              sep = "|",
    #              na.rm = TRUE) %>%
    dplyr::filter(!qc_notes == "need text"# ,
                  # !grepl("fail", qc_notes)
    )%>%
    dplyr::select(dplyr::any_of(c(names(res), "qc_notes", "qc_file_name"))) %>%
    tidyr::separate_longer_delim(source_hash, delim = ",") %>%
    tidyr::separate_longer_delim(source_hash, delim = "|::|") %>%
    dplyr::mutate(source_hash = stringr::str_squish(source_hash)) %>%
    tidyr::pivot_longer(cols = -c("source_hash", "qc_notes", "qc_file_name"),
                        names_to = "field_fix",
                        values_to = "value_fix",
                        values_transform = list(value_fix = as.character)
    ) %>%
    dplyr::mutate(
      value_fix = value_fix %>%
        tidyr::replace_na("N/A"),
      qc_notes = qc_notes %>%
        gsub("^chaned", "changed", .) %>%
        gsub("^duplicate of", "fail - duplicate of ", .) %>%
        gsub("^delete", "fail", .) %>%
        gsub("fail - ", "fail: ", .) %>%
        gsub("^fail,", "fail:", .) %>%
        stringr::str_squish(),
      qc_status = dplyr::case_when(
        grepl("pass|edit|changed|add", qc_notes) ~ "pass",
        TRUE ~ "fail"
      ),
      # If qc_status = fail, do not update the record fields
      value_fix = dplyr::case_when(
        qc_status == "fail" ~ "N/A",
        TRUE ~ value_fix
      ),
      qc_category = dplyr::case_when(
        grepl("edit|change", qc_notes) ~ "Source overall passed QC, and this record was expert reviewed and revised from ECOTOX source",
        TRUE ~ "Source overall passed QC, and this record was expert reviewed"
      )
    )

  # Get list of source_hash values with added records
  qc_add = qc_fixes %>%
    dplyr::filter(qc_notes == "add") %>%
    dplyr::pull(source_hash) %>%
    unique()

  # Create duplicate records to append to res in a later step to "add" these new records
  res_qc_add = res %>%
    dplyr::filter(source_hash %in% qc_add) %>%
    dplyr::mutate(source_hash = paste0(source_hash, "_add"))

  # Flag "add" records
  qc_fixes = qc_fixes %>%
    dplyr::mutate(source_hash = dplyr::case_when(
      qc_notes == "add" ~ paste0(source_hash, "_add"),
      TRUE ~ source_hash
    ))

  # Check for duplicate/conflicting field changes across files
  qc_fixes_dups = toxval.load.dedup(qc_fixes,
                                    hashing_cols = c("source_hash", "field_fix"))

  # Check if any collapsed values in fixes
  if(any(grepl("|::|", qc_fixes_dups$value_fix, fixed = TRUE))){
    View(qc_fixes_dups %>% dplyr::filter(grepl("|::|", qc_fixes_dups$value_fix, fixed = TRUE)),
         "qc_fixes_dups")
    stop("Conflicting qc_fix changes applied to record field across QC files...")
  }

  res_fix = res %>%
    dplyr::filter(source_hash %in% unique(qc_fixes$source_hash)) %>%
    dplyr::bind_rows(res_qc_add) %>%
    dplyr::select(-qc_status, -qc_category) %>%
    tidyr::pivot_longer(-source_hash,
                        names_to = "field_orig",
                        values_to = "value_orig",
                        values_transform = list(value_orig = as.character)
    ) %>%
    # Join QC fixes - changed field values
    dplyr::left_join(qc_fixes %>%
                       dplyr::select(source_hash, field_fix, value_fix) %>%
                       dplyr::distinct(),
                     by = c("source_hash", "field_orig"="field_fix")) %>%
    dplyr::rowwise() %>%
    # Select final field value based on if QC'd field was changed
    dplyr::mutate(qc_changed = identical(value_fix, value_orig),
                  value_final = dplyr::case_when(
                    qc_changed == FALSE & !value_fix %in% c(NA, "N/A") ~ value_fix,
                    TRUE ~ value_orig
                  )) %>%
    dplyr::ungroup() %>%
    # Join QC fixes - status and category
    dplyr::left_join(qc_fixes %>%
                       dplyr::select(source_hash, qc_status, qc_category) %>%
                       dplyr::distinct(),
                     by = "source_hash") %>%
    dplyr::select(source_hash, field_orig, value_final, qc_status, qc_category) %>%
    tidyr::pivot_wider(id_cols = c("source_hash", "qc_status", "qc_category"),
                       names_from = "field_orig",
                       values_from = "value_final") %>%
    # Update data type as needed to rejoin
    dplyr::mutate(
      dplyr::across(dplyr::any_of(c("species_id", "year", "toxval_numeric",
                                    "external_source_id", "study_duration_value")), ~as.numeric(.)))

  if(any(duplicated(res_fix$source_hash))){
    stop("Duplicate res_fix source_hash values found...")
  }

  if(anyNA(res_fix$toxval_numeric)){
    stop("NA toxval_numeric found")
  }

  # Clean up QC'd critical_effect, remove "-" in piped effects
  res_fix = res_fix %>%
    dplyr::mutate(dplyr::across(dplyr::any_of(c("critical_effect",
                                                "long_ref")),
                                ~ gsub(" | -", "", ., fixed = TRUE) %>%
                                  stringr::str_squish()))

  # # Review QC'd fields
  # tmp = res_fix %>%
  #   dplyr::select(source_hash, dplyr::any_of(hashing_cols), species_original, qc_status, qc_category) %>%
  #   dplyr::select(-ecotox_group, -species_id, -external_source_id, -common_name, -latin_name) %>%
  #   dplyr::filter(!qc_status == "fail") %>%
  #   dplyr::distinct()
  #
  # for(field in names(tmp)){
  #   message(field)
  #   print(unique(tmp[[field]]))
  # }

  res = res %>%
    # Filter out old that changed
    dplyr::filter(!source_hash %in% res_fix$source_hash,
                  # Filter out records in same study reference that were not manually checked
                  !external_source_id %in% res_fix$external_source_id) %>%
    # Join updated records
    dplyr::bind_rows(res_fix) %>%
    # Drop old source_hash field to prep for rehashing of QC'd records
    dplyr::select(-source_hash)

  ### Perform deduping and hashing again after QC fixes applied
  # Perform deduping (reporting time elapse - ~14-24 minutes)
  system.time({
    hashing_cols = c(toxval.config()$hashing_cols[!(toxval.config()$hashing_cols %in% c("critical_effect", "study_type"))],
                     "species_id", "common_name", "latin_name", "ecotox_group", "external_source_id")
    res = toxval.load.dedup(res,
                            hashing_cols = c(hashing_cols, paste0(hashing_cols, "_original"))) %>%
      # Update critical_effect delimiter to "|"
      dplyr::mutate(critical_effect = critical_effect %>%
                      gsub(" |::| ", "|", x=., fixed = TRUE),
                    study_type = study_type %>%
                      gsub(" |::| ", "|", x=., fixed = TRUE))
  })

  # Check for fields that were collapsed
  collapsed_res_fields = lapply(names(res), function(f){
    if(sum(stringr::str_detect(res[[f]], '\\|::\\|'
    ), na.rm = TRUE) > 0){
      return(f)
    }
  }) %>%
    purrr::compact() %>%
    unlist()

  # No field collapsing in hashing columns or identifier columns
  if(any(collapsed_res_fields %in% c(hashing_cols, "chemical_id", "dtxsid"))){
    stop("Recording collapsing occurred in hashing or unique identifier columns...")
  }

  cat("set the source_hash\n")
  # Vectorized approach to source_hash generation
  non_hash_cols <- toxval.config()$non_hash_cols
  res = res %>%
    tidyr::unite(hash_col, tidyselect::all_of(sort(names(.)[!names(.) %in% non_hash_cols])), sep="-", remove = FALSE) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(source_hash = digest::digest(hash_col, serialize = FALSE)) %>%
    dplyr::ungroup() %>%
    dplyr::select(-hash_col)

  ##############################################################################
  ### Start of standard toxval.load logic
  ##############################################################################
  # Set default qc_status
  res = res %>%
    dplyr::mutate(qc_status = dplyr::case_when(
      qc_status %in% c(NA, "-") ~ "not determined",
      TRUE ~ qc_status
    ))
  #####################################################################
  cat("Add code to deal with specific issues for this source\n")
  #####################################################################

  # Data cleaned in above sections

  #####################################################################
  cat("find columns in res that do not map to toxval or record_source\n")
  #####################################################################
  cols1 = runQuery("desc record_source",toxval.db)[,1]
  cols2 = runQuery("desc toxval",toxval.db)[,1]
  cols = unique(c(cols1,cols2))
  colnames(res)[which(names(res) == "species")] = "species_original"
  res = res[ , !(names(res) %in% c("record_url","short_ref"))]
  nlist = names(res)
  nlist = nlist[!nlist %in% c("casrn","name",
                              # Do not remove fields that would become "_original" fields
                              unique(gsub("_original", "", cols)))]
  nlist = nlist[!nlist %in% cols]

  # Remove columns that are not used in toxval
  res = res %>% dplyr::select(!dplyr::any_of(nlist))

  # Check if any non-toxval column still remaining in nlist
  nlist = names(res)
  nlist = nlist[!nlist %in% c("casrn","name",
                              # Do not remove fields that would become "_original" fields
                              unique(gsub("_original", "", cols)))]
  nlist = nlist[!nlist %in% cols]

  if(length(nlist)>0) {
    cat("columns to be dealt with\n")
    print(nlist)
    browser()
  }
  print(dim(res))

  #####################################################################
  cat("Generic steps \n")
  #####################################################################
  res = dplyr::distinct(res)
  res = fill.toxval.defaults(toxval.db,res)
  res = generate.originals(toxval.db,res)
  res$toxval_numeric = as.numeric(res$toxval_numeric)
  print(paste0("Dimensions of source data after originals added: ", toString(dim(res))))
  res=fix.non_ascii.v2(res,source)
  # Remove excess whitespace
  res = res %>%
    dplyr::mutate(dplyr::across(tidyselect::where(is.character), stringr::str_squish))
  res = dplyr::distinct(res)
  res = res[, !names(res) %in% c("casrn","name")]
  print(paste0("Dimensions of source data after ascii fix and removing chemical info: ", toString(dim(res))))

  #####################################################################
  cat("add toxval_id to res\n")
  #####################################################################
  count = runQuery("select count(*) from toxval",toxval.db)[1,1]
  if(count==0) {
    tid0 = 1
  } else {
    tid0 = runQuery("select max(toxval_id) from toxval",toxval.db)[1,1] + 1
  }
  tids = seq(from=tid0,to=tid0+nrow(res)-1)
  res$toxval_id = tids
  print(dim(res))

  #####################################################################
  cat("pull out record source to refs\n")
  #####################################################################
  cols = runQuery("desc record_source",toxval.db)[,1]
  nlist = names(res)
  keep = nlist[is.element(nlist,cols)]
  refs = res[,keep]
  cols = runQuery("desc toxval",toxval.db)[,1]
  nlist = names(res)
  remove = nlist[!is.element(nlist,cols)]
  res = res[ , !(names(res) %in% c(remove))]
  print(dim(res))

  #####################################################################
  cat("add extra columns to refs\n")
  #####################################################################
  refs$record_source_type = "-"
  refs$record_source_note = "-"
  refs$record_source_level = "-"
  print(paste0("Dimensions of references after adding ref columns: ", toString(dim(refs))))

  #####################################################################
  cat("load res and refs to the database\n")
  #####################################################################
  res = dplyr::distinct(res)
  refs = dplyr::distinct(refs)
  res$datestamp = Sys.Date()
  res$source_table = source_table
  res$subsource <- "EPA ORD"
  res$source_url <- "https://cfpub.epa.gov/ecotox/"
  res$subsource_url = "-"
  res$details_text = paste(source,"Details")

  # Batch upload to ensure it works
  chunk <- 20000
  n <- nrow(res)
  r  <- rep(1:ceiling(n/chunk),each=chunk)[1:n]
  d_res <- split(res,r)
  d_refs <- split(refs, r)
  rm(res, refs)

  for(i in seq_len(length(d_res))){
    cat("(a) Uploading to toxval ", i, " of ", length(d_res), "(chunk size: ", chunk, ") (", format(Sys.time(),usetz = TRUE),")\n")
    runInsertTable(d_res[[i]], "toxval", toxval.db, verbose)
    cat("(b) Uploading to record_source ", i, " of ", length(d_res), "(chunk size: ", chunk, ") (", format(Sys.time(),usetz = TRUE),")\n")
    runInsertTable(d_refs[[i]], "record_source", toxval.db, verbose)
  }
  cat("Finished inserting into toxval and record_source...(", format(Sys.time(),usetz = TRUE),")\n")
  # Get rid of unneeded intermediates
  rm(d_res, d_refs)

  #####################################################################
  cat("do the post processing\n")
  #####################################################################
  toxval.load.postprocess(toxval.db,source.db,source,do.convert.units=FALSE, remove_null_dtxsid=remove_null_dtxsid)

  if(log) {
    #####################################################################
    cat("stop output log \n")
    #####################################################################
    closeAllConnections()
    logr::log_close()
    output_message = read.delim(paste0(toxval.config()$datapath,source,"_",Sys.Date(),".log"), stringsAsFactors = FALSE, header = FALSE)
    names(output_message) = "message"
    output_log = read.delim(paste0(toxval.config()$datapath,"log/",source,"_",Sys.Date(),".log"), stringsAsFactors = FALSE, header = FALSE)
    names(output_log) = "log"
    new_log = log_message(output_log, output_message[,1])
    writeLines(new_log, paste0(toxval.config()$datapath,"output_log/",source,"_",Sys.Date(),".txt"))
  }
  #####################################################################
  cat("finish\n")
  #####################################################################
  return(0)

  #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
}
