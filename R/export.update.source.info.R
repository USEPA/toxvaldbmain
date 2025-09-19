#' @title export.update.source.info
#' @description Pull updated field information for source info dictionary
#' @param toxval.db Database version
#' @param source.db The source to be updated
#' @param dict_date Date stamp of the dictionary file to update
#' @export
#' @return Overwrite existing source info dictionary with updated fields
#'
export.update.source.info <- function(toxval.db, source.db, dict_date = "2025-05-29"){

  # Load source info dictionary
  file = paste0(toxval.config()$datapath,"dictionary/source_info ", dict_date, ".xlsx")
  source_info = readxl::read_xlsx(file) %>%
    dplyr::select(-records, -chemicals, -source_year)

  # Get list of live sources
  src_live = runQuery("SELECT distinct source, supersource, source_table FROM toxval",
                      toxval.db)

  # Pull summary data
  src_summ = lapply(seq_len(nrow(src_live)), function(i){

    src_tbl = src_live$source_table[i]

    if(src_tbl == "direct load"){
      src_tbl = src_live$source[i]
      # direct load sources do not have source_version_date, so create NA field
      query = paste0("SELECT source_hash, chemical_id, null AS source_version_date ",
                     "FROM toxval WHERE source = '", src_live$source[i], "'")
      db = toxval.db
    } else {
      query = paste0("SELECT source_hash, chemical_id, source_version_date ",
                     "FROM ", src_tbl)
      db = source.db
    }

    # Query and format date
    tmp = runQuery(query, db) %>%
      dplyr::mutate(source_version_date = format(as.Date(source_version_date, format="%Y-%m-%d"),"%Y")) %>%
      dplyr::filter(!source_version_date %in% c("0000-00-00"))

    t_version = switch(src_tbl,
                       "ToxRefDB" = "2024-04-11",
                       "ChemIDplus" = "2022-10-19",
                       "Uterotrophic Hershberger DB" = "2018-10-01",
                       "ECOTOX" = "2024-09-19",
                       NA
    )

    if(!is.na(t_version)){
      t_version = t_version %>%
        as.Date(., format="%Y-%m-%d") %>%
        format("%Y")
    } else {
      t_version = toString(sort(unique(tmp$source_version_date)))
    }

    # Summarize
    data.frame(
      # Source table name
      source_table = src_tbl,
      # Number of records
      records = length(unique(tmp$source_hash)),
      # Number of chemicals
      chemicals = length(unique(tmp$chemical_id)),
      # Source year
      source_year = t_version
    ) %>%
      return()
  }) %>%
    dplyr::bind_rows()

  # Join, overwrite, return
  source_info %>%
    dplyr::left_join(src_summ,
                     by="source_table") %T>%
    {
      writexl::write_xlsx(., file)
    } %>%
    return()
}
