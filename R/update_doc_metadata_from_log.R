#' @title update_doc_metadata_from_log
#' @description Parse metadata logs to fill in NULL values from record_source table records.
#' @param toxval.db The database version to use
#' @return None. SQL update statements are executed.
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[dplyr]{bind_rows}}, \code{\link[dplyr]{mutate}}, \code{\link[dplyr]{distinct}}, \code{\link[dplyr]{across}}, \code{\link[dplyr]{filter}}, \code{\link[dplyr]{group_by}}, \code{\link[dplyr]{reexports}}, \code{\link[dplyr]{case_when}}, \code{\link[dplyr]{count}}, \code{\link[dplyr]{select}}, \code{\link[dplyr]{mutate-joins}}, \code{\link[dplyr]{group_split}}
#'  \code{\link[readxl]{read_excel}}
#'  \code{\link[stringr]{str_trim}}, \code{\link[stringr]{str_split}}
#'  \code{\link[tidyr]{pivot_longer}}, \code{\link[tidyr]{unite}}, \code{\link[tidyr]{pivot_wider}}, \code{\link[tidyr]{drop_na}}
#' @rdname cvt_update_doc_metadata_from_log
#' @export
#' @importFrom dplyr bind_rows mutate distinct across filter group_by everything ungroup case_when count select left_join group_split all_of
#' @importFrom readxl read_xlsx
#' @importFrom stringr str_squish str_split
#' @importFrom tidyr pivot_longer unite pivot_wider drop_na
update_doc_metadata_from_log <- function(toxval.db){
  # Get list of logs
  meta_logs = list.files(paste0(toxval.config()$datapath,
                                "Document Identifier Curation/find_pmid output"),
                         pattern = "xlsx",
                         full.names = TRUE) %>%
    .[!grepl("All sources_first_pass_pmids.xlsx$", .)]

  # Load all log files
  meta = lapply(meta_logs, readxl::read_xlsx, col_types = "text") %>%
    dplyr::bind_rows() %>%
    dplyr::filter(!long_ref %in% c(NA, "-")) %>%
    dplyr::mutate(dplyr::across(where(is.character), ~ dplyr::na_if(., "-"))) %>%
    dplyr::select(-toxval_id, # -source,
                  -`Unnamed: 0`, - pages,
                  -HEROID, -parsed, -pmid_search) %>%
    dplyr::mutate(
      # Remove ending punctuation in all fields except long_ref
      dplyr::across(-long_ref, ~ gsub("[.]$|:$|,$", "", .) %>%
                      stringr::str_squish()
      )
    ) %>%
    dplyr::distinct()

  # Check for duplicates in logs
  meta %>%
    # Comment out select -source line to help manually review files
    dplyr::select(-source) %>%
    dplyr::distinct() %>%
    dplyr::count(long_ref) %>%
    dplyr::filter(n > 1)

  meta = meta %>%
    dplyr::select(-source) %>%
    # Pivot to compare to the database field values next
    tidyr::pivot_longer(
      cols = -c("long_ref"),
      names_to = "field_name"
    ) %>%
    # Remove NA field values
    dplyr::filter(!is.na(value)) %>%
    tidyr::unite(
      col = "field_key",
      long_ref, field_name,
      sep = "_",
      remove = FALSE
    )

  # Pull database records
  toxval_docs = runQuery(paste0("SELECT distinct long_ref, ",
                                "title, author, journal, volume, year, url, doi, pmid ",
                                "FROM record_source ",
                                "WHERE long_ref != '-' AND long_ref is NOT NULL"),
                         toxval.db) %>%
    tidyr::pivot_longer(
      cols = -c("long_ref"),
      names_to = "field_name",
      values_transform = as.character
    ) %>%
    # Filter to those with NULL values
    dplyr::filter(value %in% c(NA, "-")) %>%
    tidyr::unite(
      col = "field_key",
      long_ref, field_name,
      sep = "_",
      remove = FALSE
    ) %>%
    dplyr::mutate(dplyr::across(where(is.character), ~ dplyr::na_if(., "-")))

  meta_out = meta %>%
    # Filter to metadata with a new value to fill in
    dplyr::filter(field_key %in% toxval_docs$field_key) %>%
    # Collapse conflicting duplicate log entries for field values
    dplyr::group_by(field_key) %>%
    dplyr::mutate(dplyr::across(dplyr::everything(), ~paste0(unique(.), collapse = " <::> "))) %>%
    dplyr::ungroup() %>%
    dplyr::distinct()

  # Check for duplicates
  meta_out %>%
    dplyr::count(field_key) %>%
    dplyr::filter(n > 1)

  # If there is new metadata to add, continue
  if(nrow(meta_out)){
    # Query record_source_id values with long_ref
    rs_id = runQuery("SELECT record_source_id, long_ref FROM record_source",
                     toxval.db) %>%
      dplyr::filter(long_ref %in% meta_out$long_ref)

    # Generate field groups to only push updates to fields with a new value
    meta_update_groups = meta_out %>%
      dplyr::select(long_ref, field_name) %>%
      dplyr::group_by(long_ref) %>%
      dplyr::mutate(field_name_group = toString(sort(unique(field_name)))) %>%
      dplyr::ungroup() %>%
      dplyr::distinct() %>%
      dplyr::select(-field_name) %>%
      dplyr::distinct() %>%
      dplyr::arrange(field_name_group)

    # Join to get record_source_id and split into groups
    meta_out = meta_out %>%
      tidyr::pivot_wider(
        id_cols = c(long_ref),
        names_from = "field_name"
      ) %>%
      dplyr::left_join(rs_id,
                       by = "long_ref") %>%
      # Join to get metadata field groups
      dplyr::left_join(meta_update_groups,
                       by = "long_ref") %>%
      # Split by group
      dplyr::group_split(field_name_group)

    # Loop through the metadata update field groups (only update fields that have new values to push)
    for(df in meta_out){
      # Get vector of metadata fields
      metadata_fields = unique(df$field_name_group) %>%
        stringr::str_split(", ") %>%
        .[[1]] %>%
        stringr::str_squish()

      update_df = df %>%
        # Select metadata fields
        dplyr::select(dplyr::all_of(c("record_source_id", metadata_fields))) %>%
        # Drop NA values
        tidyr::drop_na(dplyr::all_of(metadata_fields))

      # Push update (uncomment db_update_tbl call when ready)
      if(nrow(update_df)){
        update_query <- paste0("UPDATE record_source a ",
                               "INNER JOIN z_updated_df b ",
                               "ON (a.record_source_id = b.record_source_id) ",
                               "SET ",
                               paste0(
                                 paste0(
                                   "a.", names(update_df)[!names(update_df) %in% c("record_source_id")],
                                   " = b.", names(update_df)[!names(update_df) %in% c("record_source_id")]
                                 ),
                                 collapse = ", "
                               ),
                               " WHERE a.record_source_id in (", toString(update_df$record_source_id),")"
        )

        # Push update statements
        runUpdate(table="record_source",
                  updateQuery = update_query,
                  updated_df = update_df,
                  db=toxval.db)
      }
    }
  } else {
    message("No new metadata field values to fill-in...")
  }
}
