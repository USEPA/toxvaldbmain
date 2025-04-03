get_supplemental_tables <- function(toxval.db, source.db){

  chem_src_index = runQuery("SELECT distinct chemprefix, curation_type FROM chemical_source_index",
                            source.db)

  source_catalog = runQuery(paste0("SELECT distinct supersource, source, source_table, chemical_id FROM toxval"),
                            toxval.db) %>%
    tidyr::separate(chemical_id, into = c("chemprefix", "chemical_hash")) %>%
    dplyr::select(-chemical_hash) %>%
    dplyr::distinct() %>%
    dplyr::left_join(chem_src_index,
                     by = "chemprefix")



  supp_tables = list()

  supp_tables[["README"]] =
    data.frame(
      `Suplemental Table` = c(
        "Table S1.",
        "Table S2.",
        "Table S3.",
        "Table S4a.",
        "Table S4b.",
        "Table S4c.",
        "Table S5.",
        "Table S6.",
        "Table S7."
      ),
      Description = c(
        "Factors used to convert food or water exposure in ppm to mg/kg-day.",
        "Source name acronyms.",
        "Effect type (toxval_type) acronyms.",
        "In vivo Effect type (toxval_type) by source 'in vivo'.",
        "Toxicity Values Effect type (toxval_type) by source.",
        "Exposure Guidelines Effect type (toxval_type) by source.",
        "QC summary table. Sources and QC Level 1 Sampling Counts in ToxValDB v9.6.1.",
        'Sources and counts with the QC pass status included in ToxValDB v9.6.1. Colors indicate increasing values from low to high as red to green.',
        'Sources and counts with the QC fail status included in ToxValDB v9.6.1. Colors indicate increasing values from low to high as green to red. Note the color reversal for "fail" statuses.'

      )
    )

  supp_tables[["Table S1"]] = readxl::read_xlsx(paste0(toxval.config()$datapath,
                                                       "dictionary/ppm to mgkgday by animal.xlsx"))

  supp_tables[["Table S2"]] = runQuery("SELECT * FROM source_info",
                                       toxval.db) %>%
    dplyr::select(source = supersource, full_name = supersource_full_name) %>%
    dplyr::distinct()

  supp_tables[["Table S3"]] = runQuery(paste0("select distinct toxval_type_category as toxval_type, fullname as full_name ",
                                              "from toxval_type_dictionary ",
                                              "where toxval_type in (select distinct toxval_type ",
                                              "from toxval)"),
                                       toxval.db) %>%
    dplyr::filter(!toxval_type %in% c("-"))


  source_toxval_type = runQuery(
    paste0("SELECT distinct a.supersource as source, a.toxval_type, b.toxval_type_supercategory ",
           "FROM toxval a ",
           "LEFT JOIN toxval_type_dictionary b ",
           "ON a.toxval_type = b.toxval_type"),
    toxval.db
  ) %>%
    dplyr::arrange(toxval_type_supercategory, toxval_type) %>%
    dplyr::mutate(group = dplyr::case_when(
      toxval_type_supercategory %in% c("Mortality Response Summary Value",
                                       "Dose Response Summary Value") ~ "in vivo",
      toxval_type_supercategory == "Toxicity Value" ~ "toxicity value(s)",
      toxval_type_supercategory %in% c("Acute Exposure Guidelines",
                                       "Media Exposure Guidelines") ~ "exposure guideline(s)",
      TRUE ~ NA
    )) %>%
    dplyr::filter(!is.na(group)) %>%
    dplyr::group_split(group) %>%
    lapply(., function(df){
      if(unique(df$group) == "in vivo"){
        df = df %>%
          dplyr::select(source) %>%
          dplyr::mutate(`in vivo` = "Y")
      } else {
        group = unique(df$group)
        df = df %>%
          dplyr::select(source, toxval_type) %>%
          dplyr::distinct() %>%
          # Rename to group label
          dplyr::rename("{group}" := toxval_type)
      }
      return(df)
    })

  # https://stackoverflow.com/questions/8091303/simultaneously-merge-multiple-data-frames-in-a-list
  source_toxval_type = c(list(supp_tables[["Table S2"]]),
                         source_toxval_type
  ) %>%
    purrr::reduce(left_join, by = "source") %>%
    dplyr::mutate(`in vivo` = dplyr::case_when(
      is.na(`in vivo`) ~ "N",
      TRUE ~ "Y"
    )) %>%
    dplyr::distinct() %>%
    dplyr::mutate(dplyr::across(where(is.character), ~ tidyr::replace_na(., "N/A")))

  supp_tables[["Table S4a"]] = source_toxval_type %>%
    dplyr::select(source, `in vivo`) %>%
    dplyr::distinct()

  supp_tables[["Table S4b"]] = source_toxval_type %>%
    dplyr::select(source, `toxicity value(s)`) %>%
    dplyr::distinct()

  supp_tables[["Table S4c"]] = source_toxval_type %>%
    dplyr::select(source, `exposure guideline(s)`) %>%
    dplyr::distinct()

  source_rec_n = runQuery(paste0("select supersource, source_table, count(*) as 'total records' ",
                                 "from toxval group by supersource, source_table"),
                          toxval.db)

  # Load in manually curated sampling summary dict
  qc_sampling_summary = readxl::read_xlsx(paste0(toxval.config()$datapath,
                                                 "release_files/toxval_qc_sampling_summary_dict_v96_1.xlsx"))

  supp_tables[["Table S5"]] = source_rec_n %>%
    dplyr::left_join(source_catalog,
                     by = c("supersource", "source_table")) %>%
    dplyr::select(source = supersource, `total records`,
                  `source table` = source_table, `curation type` = curation_type) %>%
    dplyr::left_join(qc_sampling_summary,
                     by = c("source", "source table"))

  qc_status_summary = toxval.summary.stats(toxval.db, summ_level = "supersource")

  supp_tables[["Table S6"]] = qc_status_summary %>%
    dplyr::select(source = supersource, chemicals, `total records`, pass, `not determined`, `pass %` = `pass percent`)

  qc_fail = qc_status_summary %>%
    dplyr::select(-c(chemicals, pass, `not determined`, `pass percent`)) %>%
    dplyr::rename(source = supersource)

  # Rename fail columns and set as caption dynamically
  qc_fail = qc_fail %>%
    tidyr::pivot_longer(cols = -c(source, `total records`))

  # Get caption list
  fail_captions = qc_fail %>%
    dplyr::select(name) %>%
    unique() %>%
    dplyr::mutate(name_sym = rep_len(letters, nrow(.)))

  # Map caption list back
  qc_fail = qc_fail %>%
    dplyr::left_join(fail_captions, by = "name") %>%
    dplyr::select(-name) %>%
    tidyr::pivot_wider(id_cols = c(source, `total records`),
                       names_from = "name_sym")

  # Add caption notes
  qc_fail_caption = data.frame(
    "total records" = c(
      "i. QC fail status tags listed as programmatically applied during load to ToxValDB toxval table, except for the “fail” status which is from a manually reviewed record.",
      "ii. Note, an individual record can have multiple fail tags (e.g., 'fail: species out of scope; Eco data out of scope for ToxValDB' would be tag 'e' and tag 'h')",
      "iii. Failure column tags:"
    )
  ) %>%
    dplyr::bind_rows(
      fail_captions %>%
        tidyr::unite(col = "total.records", name_sym, name, sep = " - ")
    ) %>%
    dplyr::rename(`total records` = `total.records`)

  supp_tables[["Table S7"]] = qc_fail %>%
    dplyr::mutate(`total records` = as.character(`total records`)) %>%
    dplyr::bind_rows(qc_fail_caption)

  writexl::write_xlsx(supp_tables,
                      paste0(toxval.config()$datapath, "/release_files/", toxval.db," Supplemental Tables_",Sys.Date(),".xlsx"))

}
