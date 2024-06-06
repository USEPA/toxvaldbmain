#-------------------------------------------------------------------------------------
#' Sets the final category for each term/study_type pair in the terms table
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @return None. SQL update statement is performed
#' @export
#-------------------------------------------------------------------------------------
set.critical_effect_category <- function(toxval.db){
  query <- paste0("SELECT term, study_type, category, COUNT(*) as category_count
  FROM critical_effect_categorizations WHERE category IS NOT NULL
  GROUP BY term, study_type, category
  HAVING category_count > 1")

  pairs_set <- runQuery(query, toxval.db)

  query <- paste0("SELECT term, study_type, category FROM critical_effect_categorizations
                  WHERE lanid = 'oma_rule'")
  oma_pairs <- runQuery(query, toxval.db)

  combined_df <- rbind(pairs_set[, c("term", "study_type", "category")], oma_pairs)

  colnames(combined_df)[colnames(combined_df) == "category"] <- "critical_effect_category"

  update_query <- paste0("UPDATE critical_effect_terms as cet INNER JOIN z_updated_df zud
                         ON cet.term = zud.term AND cet.study_type = zud.study_type
                         SET cet.critical_effect_category = zud.critical_effect_category")

  runUpdate(table="critical_effect_terms", updateQuery=update_query, updated_df=combined_df, db=toxval.db)

}
