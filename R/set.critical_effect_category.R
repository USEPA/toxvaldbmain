#-------------------------------------------------------------------------------------
#' Sets the final category for each term/study_type pair in the critical_effect_terms table
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @return None. SQL update statement is performed
#' @export
#-------------------------------------------------------------------------------------
set.critical_effect_category <- function(toxval.db){
  message("Pulling critical_effect_category dictionary from critical_effect_categorizations table...")
  query <- paste0("SELECT term, study_type, category, COUNT(*) as category_count ",
                  "FROM critical_effect_categorizations ",
                  "WHERE category IS NOT NULL ",
                  "GROUP BY term, study_type, category ",
                  "HAVING category_count > 1")

  pairs_set <- runQuery(query, toxval.db)

  query <- paste0("SELECT term, study_type, category ",
                  "FROM critical_effect_categorizations ",
                  "WHERE lanid = 'oma_rule'")

  oma_pairs <- runQuery(query, toxval.db)

  combined_df <- pairs_set %>%
    dplyr::select(-category_count) %>%
    dplyr::bind_rows(oma_pairs) %>%
    dplyr::rename(critical_effect_category = category)

  out = runQuery("SELECT * FROM critical_effect_terms",
                 toxval.db) %>%
    dplyr::select(-critical_effect_category) %>%
    dplyr::left_join(combined_df,
                     by=c("term", "study_type")) %>%
    dplyr::filter(!is.na(critical_effect_category))

  ##############################################################################
  ### Batch Update
  ##############################################################################
  batch_size <- 50000
  startPosition <- 1
  endPosition <- nrow(out)
  incrementPosition <- batch_size

  while(startPosition <= endPosition){
    message("...Inserting new data in batch: ", batch_size, " startPosition: ", startPosition," : incrementPosition: ", incrementPosition,
            " (",round((incrementPosition/endPosition)*100, 3), "%)", " at: ", Sys.time())

    update_query <- paste0("UPDATE critical_effect_terms cet ",
                           "INNER JOIN z_updated_df zud ",
                           "ON (cet.id = zud.id) ",
                           "SET cet.critical_effect_category = zud.critical_effect_category ",
                           "WHERE cet.source_hash is NOT NULL")

    runUpdate(table="critical_effect_terms",
              updateQuery = update_query,
              updated_df = out[startPosition:incrementPosition,],
              db=toxval.db)

    startPosition <- startPosition + batch_size
    incrementPosition <- startPosition + batch_size - 1
  }
}
