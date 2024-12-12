#' @title toxvaldb_release_comparison_stats.R
#' @description Pull comparison stats between toxvaldb versions.
#' @param repoDir Path to Repo/ folder
#' @export
#' @return None. RData file is saved.
toxvaldb_release_comparison_stats <- function(repoDir){
  # https://stackoverflow.com/questions/656951/search-for-whole-word-match-in-mysql
  human_species_rel_orig = c("rat", "mouse", "dog", "rabbit", "human") %>%
    # Use for MySQL <v8.0.4 (doesn't allow for \b word boundary regex)
    # https://stackoverflow.com/questions/7567700/oracle-regexp-like-and-word-boundaries
    paste0("regexp_like(common_name, '(^|\\\\W)", .,"($|\\\\W)')", collapse = " OR ")
  # Use for MySQL v8.0.4+
  # paste0("b.common_name REGEXP '\\b", ., "\\b'", collapse = " OR ")

  toxvaldb_list = list(
    v9.6 = c("res_toxval_v96", Sys.getenv("db_server")),
    v9.5 = c("res_toxval_v95", Sys.getenv("db_server")),
    v9.4 = c("res_toxval_v94", Sys.getenv("db_server")),
    v9.3 = c("res_toxval_v93", Sys.getenv("db_server")),
    v9.2 = c("res_toxval_v92", Sys.getenv("db_server")),
    v9.1.1 = c("dev_toxval_v9_1_1", Sys.getenv("db_server")),
    v9.1 = c("dev_toxval_v9_1", Sys.getenv("db_server")),
    v9 = c("dev_toxval_v9", Sys.getenv("db_server")),
    v8 = c("dev_toxval_v8", Sys.getenv("db_server"))
  )

  # Loop through versions and com
  toxvaldb_release_comparison = lapply(names(toxvaldb_list), function(db){
    message("Pulling ", db, " (", which(db == names(toxvaldb_list))," of ", length(toxvaldb_list),")")
    # Set database name
    toxval.db = toxvaldb_list[[db]][1]
    # Set database host server
    Sys.setenv(db_server = toxvaldb_list[[db]][2])

    # Early versions used "species_common" instead of "common_name"
    if(db %in% c("v8", "v9.1", "v9.1.1")){
      human_species_rel = human_species_rel_orig %>%
        gsub("common_name", "species_common", .)
    } else {
      human_species_rel = human_species_rel_orig
    }

    data.frame(
      # Overall record count
      n_records = runQuery("SELECT count(*) as n_records FROM toxval", toxval.db),
      # Source count
      n_source = ifelse(db %in% c("v9.5"),
                        # v9.5 had source overwritten with supersource label, which combined
                        # source values and made the source count seem lower. Use source_table for improved reporting
                        runQuery("SELECT count(distinct source, source_table) as n_source FROM toxval", toxval.db),
                        runQuery("SELECT count(distinct source) as n_source FROM toxval", toxval.db)) %>%
        unlist(),
      # DTXSID count
      n_dtxsid = runQuery("SELECT count(distinct dtxsid) as n_dtxsid FROM toxval", toxval.db),
      # Count of records with human-relevant species
      # common_name %in% c("Rat", "Mouse", "Dog", "Rabbit", "Human")
      n_human_species_rel = runQuery(paste0("SELECT count(*) as n_human_species_rel FROM toxval a ",
                                            "LEFT JOIN ",
                                            # v9 had a separate species_ecotox table
                                            ifelse(db == "v9", "species_ecotox ", "species "),
                                            "b ON a.species_id = b.species_id ",
                                            "WHERE ", human_species_rel),
                                     toxval.db
      )
    ) %>%
      tidyr::pivot_longer(dplyr::everything(),
                          names_to = "stat",
                          values_to = "count") %>%
      dplyr::mutate(version = db)
  }) %>%
    dplyr::bind_rows() %>%
    # Reformat verison
    dplyr::mutate(version = version %>%
                    gsub("dev_toxval_", "", .) %>%
                    gsub("res_toxval_", "", .) %>%
                    gsub("_", ".", .) %>%
                    gsub("v9", "v9.", .) %>%
                    gsub("\\.\\.", ".", .) %>%
                    gsub("\\.$", "", .) %>%
                    factor()
    )

  # Save RData
  message("Saving stats to: ", paste0(repoDir, "release_files/version_comparison/",
                 names(toxvaldb_list)[1], ".RData"))
  save(toxvaldb_release_comparison,
       file = paste0(repoDir, "release_files/version_comparison/",
                     names(toxvaldb_list)[1], ".RData")
  )
}
