#' toxval_release_readiness_check.R
#' Function to assist with querying for a source's release readiness
#' @param toxval.db The database version to use
#' @param source.db The source database
#' @param source_name Name of source to check (e.g., IRIS)
toxval_release_readiness_check <- function(toxval.db, source.db, source_name){
  # Check input param
  if(is.null(source_name) || is.na(source_name)) stop("Input 'source_name' must not be NULL or NA")

  message("Generating report for...", source_name)
  # Check whether source is direct load
  is_direct_load = runQuery(paste0("SELECT DISTINCT source_table FROM toxval WHERE source='",
                                   source_name, "'"),
                            db=toxval.db)$source_table %in% c("direct load", "direct_load")

  # Get source index
  src_index = runQuery(paste0("SELECT chemprefix, source_table FROM chemical_source_index ",
                              "WHERE source = '", source_name,"'"),
                       db = source.db)

  # TODO Test more NULL cases
  if(!is_direct_load) {
    # Get source table name from source name
    src_tbl = src_index$source_table
    # Check if input source_name exists
    if(!length(src_tbl)){
      message("Input source_name '", source_name,"' not found in chemical_source_index table")
      return()
    }

    # Check if input databases exist on server
    tbl_check <- runQuery("SHOW TABLES", source.db) %>%
      .[. == src_tbl]

    if(!length(tbl_check)){
      message(source_name, " provided table ", src_tbl, " does not exist...skipping...")
      return()
    }
    # PUll source data
    src_data = runQuery(paste0("SELECT source_hash, chemical_id, create_time FROM ", src_tbl),
                        db = source.db)

  } else {
    # Pull source data for "direct load" source type
    src_data = runQuery(paste0("SELECT source_hash, chemical_id FROM toxval WHERE source='", source_name, "'"),
                                db= toxval.db)
  }

  # source_hash list
  src_hash = src_data$source_hash
  # Chemical ID list
  chem_id_list = unique(src_data$chemical_id)
  # Get import date
  import_date_val = ifelse(is.null(unique(src_data$create_time)), NA, unique(src_data$create_time))

  # Baseline information
  out = data.frame(
    # Set direct_load type
    is_direct_load = !!is_direct_load,
    # Report import date
    import_date = import_date_val,

    # Report number of records
    import_record_count = length(src_hash),
    # Get load date
    load_date = runQuery(paste0("SELECT distinct datestamp FROM toxval ",
                                "WHERE source = '", source_name, "'"),
                         db = toxval.db) %>%
      dplyr::pull(datestamp),
    load_record_count = runQuery(paste0("SELECT count(*) as n FROM toxval ",
                                        "WHERE source = '", source_name, "'"),
                                 db = toxval.db) %>%
      dplyr::pull(n)
  )

  # Get chemical curation counts
  chem_curation = runQuery(paste0("SELECT chemical_id, dtxsid FROM source_chemical ",
                                  "WHERE chemical_id in ('",
                                  paste0(chem_id_list, collapse="', '"),
                                  "')"),
                           db=source.db)

  # Summarize chemical curation
  chem_curation_summ = chem_curation %>%
    dplyr::mutate(has_dtxsid = !is.na(dtxsid)) %>%
    dplyr::group_by(has_dtxsid) %>%
    dplyr::summarise(n = n()) %>%
    tidyr::pivot_wider(names_from = "has_dtxsid", values_from = "n")

  # Fill blank hashing cols
  chem_curation_summ[, c("TRUE", "FALSE")[!c("TRUE", "FALSE") %in% names(chem_curation_summ)]] <- 0
  # Append to report
  out = out %>%
    dplyr::bind_cols(
      chem_curation_summ = chem_curation_summ %>%
        dplyr::mutate(chem_curation_perc = round((`TRUE`/(`TRUE`+`FALSE`)) * 100, 3),
                      missing_chem_curation_frac = paste0(`FALSE`, "/", (`TRUE`+`FALSE`))) %>%
        dplyr::select(-`TRUE`, -`FALSE`)
    )

  # Check uncurated chemicals
  chems_to_curate = chem_curation %>%
    dplyr::filter(is.na(dtxsid))

  # Check if they've ever had a mapping
  missing_chems_curated = list.files("Repo/chemical_mapping",
                                     recursive = TRUE,
                                     pattern = src_index$chemprefix,
                                     full.names = TRUE) %>%
    .[grepl("DSSTox Files", .)] %>%
    lapply(., function(f){ readxl::read_xlsx(f) %>%
        dplyr::mutate(curation_filename = f)}) %>%
    dplyr::bind_rows()

  if(nrow(missing_chems_curated)){
    missing_chems_curated = missing_chems_curated %>%
      dplyr::rename(external_id = Extenal_ID, dtxsid = DSSTox_Substance_Id) %>%
      dplyr::filter(external_id %in% chems_to_curate$chemical_id,
                    !is.na(dtxsid)) %>%
      dplyr::select(chemical_id = external_id, dtxsid, name=Substance_Name,
                    casrn=Substance_CASRN, curation_filename) %>%
      dplyr::distinct() %>%
      dplyr::arrange(chemical_id)
  } else {
    # Return blank
    missing_chems_curated = missing_chems_curated %>%
      dplyr::mutate(chemical_id = NA) %>%
      .[0,]
  }

  # Report how many chemical records missing mappings that have mappings
  if(!is_direct_load) {
    n_records_no_chem_map_val = runQuery(paste0("SELECT count(*) as n FROM ", src_tbl,
                                                " WHERE chemical_id IN ('",
                                                paste0(unique(chems_to_curate$chemical_id),
                                                       collapse = "', '"),
                                                "')"),
                                         db = source.db) %>%
      dplyr::pull(n)
  } else {
    n_records_no_chem_map_val = runQuery(paste0("SELECT count(*) as n FROM toxval",
                                                " WHERE chemical_id IN ('",
                                                paste0(unique(chems_to_curate$chemical_id),
                                                       collapse = "', '"),
                                                "') AND source='", source_name, "'"),
                                         db = toxval.db) %>%
      dplyr::pull(n)
  }

  out = out %>%
    dplyr::mutate(n_records_no_chem_map = !!n_records_no_chem_map_val,
                  has_dtxsid_mapped = length(unique(missing_chems_curated$chemical_id)))

  # Check extraction document linkage
  extraction_doc = runQuery(paste0("SELECT * FROM documents_records WHERE ",
                                   "source_hash IN ('",
                                   paste0(src_hash, collapse="', '"),
                                   "')"),
                            db = source.db)

  out = out %>%
    dplyr::bind_cols(
      # Summarize extraction document curation
      extraction_doc_summ = data.frame(
        missing_extraction_doc = length(src_hash[!src_hash %in% extraction_doc$source_hash]),
        has_extraction_doc = length(unique(extraction_doc$source_hash))
      ) %>%
        dplyr::mutate(extract_doc_perc = round((has_extraction_doc/(has_extraction_doc + missing_extraction_doc)) * 100, 3),
                      missing_extraction_doc_fraction = paste0(missing_extraction_doc, "/", (has_extraction_doc+missing_extraction_doc))) %>%
        dplyr::select(-has_extraction_doc, -missing_extraction_doc)
    )

  # Check record_source table entries
  record_source_clowder = runQuery(paste0("SELECT toxval_id, clowder_doc_id, record_source_level ",
                                          "FROM record_source ",
                                          "WHERE toxval_id in (SELECT toxval_id FROM toxval ",
                                          "WHERE source_hash in ('",
                                          paste0(unique(extraction_doc$source_hash), collapse="', '"),
                                          "')",
                                          ") AND clowder_doc_id IS NOT NULL AND clowder_doc_id != '-'"),
                                   db = toxval.db)
  record_src_doc_fields = c("record_source_extraction_doc_n", "record_source_origin_doc_n")
  if(nrow(record_source_clowder)){
    record_source_clowder_summ = record_source_clowder %>%
      dplyr::group_by(record_source_level) %>%
      dplyr::summarise(n=n()) %>%
      tidyr::pivot_wider(names_from = "record_source_level", values_from = "n") %T>% {
        names(.) <- record_src_doc_fields
      }
  } else {
    # Handle null case
    record_source_clowder_summ = data.frame(
      record_source_extraction_doc_n = 0,
      record_source_origin_doc_n = 0
    )
  }
  # Fill blank hashing cols
  record_source_clowder_summ[, record_src_doc_fields[!record_src_doc_fields %in% names(record_source_clowder_summ)]] <- 0
  # Add record_source curation report
  out = out %>%
    dplyr::bind_cols(record_source_clowder_summ)

  # Check if export directory exists, create if not present
  if(!dir.exists("Repo/release_readiness_check")) dir.create("Repo/release_readiness_check")

  list(summary = out %>%
         tidyr::pivot_longer(cols=dplyr::everything(),
                             names_to = "check_name",
                             values_to = "check_outcome",
                             values_transform = as.character),
       missing_chems_mapped = missing_chems_curated) %T>%
    # Save export
    writexl::write_xlsx(paste0("Repo/release_readiness_check/",
                               source_name, "_", Sys.Date(),".xlsx")) %>%
    # Return dataframe list
    return()
}
