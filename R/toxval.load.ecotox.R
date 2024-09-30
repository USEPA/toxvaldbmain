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
  ### Start of standard toxval.load logic
  ##############################################################################

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
