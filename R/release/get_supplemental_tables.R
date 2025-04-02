get_supplemental_tables <- function(toxval.db, source.db){

  supp_tables = list()

  supp_tables[["Table S1"]] = readxl::read_xlsx(paste0(toxval.config()$datapath,
                                                       "dictionary/ppm to mgkgday by animal.xlsx"))

  supp_tables[["Table S2"]] = runQuery("SELECT * FROM source_info",
                                       toxval.db) %>%
    dplyr::select(supersource, full_name = supersource_full_name) %>%
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
  supp_tables[["Table S4"]] = c(list(supp_tables[["Table S2"]] %>%
                                       dplyr::select(source = supersource)),
                                source_toxval_type
  ) %>%
    purrr::reduce(left_join, by = "source") %>%
    dplyr::mutate(`in vivo` = dplyr::case_when(
      is.na(`in vivo`) ~ "N",
      TRUE ~ "Y"
    )) %>%
    dplyr::distinct() %>%
    dplyr::mutate(dplyr::across(where(is.character), ~ tidyr::replace_na(., "N/A")))

}
