#--------------------------------------------------------------------------------------
#
#' prepare the cancer call data. The data comes form a series of files
#' ../NIOSH/NIOSH_CARC_2018.xlsx
#' ../IRIS/iris_cancer_call_2018-10-03.xlsx
#' ../PPRTV_ORNL/PPRTV_ORNL cancer calls 2018-10-25.xlsx
#' ../cancer_summary/cancer/NTP/NTP cancer clean.xlsx
#' ../cancer_summary/cancer/IARC/IARC cancer 2018-10-29.xlsx
#' ../cancer_summary/cancer/HealthCanada/HealthCanada_TRVs_2010_AppendixA v2.xlsx
#' ../cancer_summary/cancer/EPA_OPP_CARC/EPA_CARC.xlsx
#' ../cancer_summary/cancer/CalEPA/calepa_p65_cancer_only.xlsx
#'
#' extract all of the chemicals with cancer slope factor or unit risk with appropriate units
#'
#' @param toxval.db The version of the database to use
#-----------------------------------------------------------------------------------
toxval.load.cancer <- function(toxval.db,source.db) {
  # verbose = log
  printCurrentFunction(toxval.db)

  # runQuery("delete from cancer_summary", toxval.db)

  # chemical_id
  # dtxsid
  # source
  # exposure_route
  # cancer_call
  # url

  # EPA FERA
  file = paste0(toxval.config()$datapath, "cancer_summary/cancer/EPA FERA/cancer_summary_epa_fera_20210929.xlsx")
  epa_fera = readxl::read_xlsx(file) %>%
    dplyr::select(name,
                  casrn,
                  exposure_route,
                  cancer_call,
                  url = source_url) %>%
    dplyr::mutate(source = "EPA FERA")

  # EPA IRIS
  file = paste0(toxval.config()$datapath, "cancer_summary/cancer/EPA IRIS/WOE_Details_20230509.xlsx")
  epa_iris = readxl::read_xlsx(file) %>%
    dplyr::select(name=`CHEMICAL NAME`,
                  casrn=CASRN,
                  exposure_route=ROUTE,
                  cancer_call=`WOE DESCRIPTION`,
                  url = URL) %>%
    dplyr::mutate(source = "EPA IRIS",
                  exposure_route = dplyr::case_when(
                    is.na(exposure_route) ~ "-",
                    TRUE ~ tolower(exposure_route)
                  ))

  # EPA OPP
  file = paste0(toxval.config()$datapath, "cancer_summary/cancer/EPA OPP/cancer_summary_epa_opp_output_20250318.xlsx")
  epa_opp = readxl::read_xlsx(file) %>%
    dplyr::select(name,
                  casrn,
                  source,
                  cancer_call,
                  url)

  # EPA PPRTV
  file = paste0(toxval.config()$datapath, "cancer_summary/cancer/EPA PPRTV/pprtv_cphea_Weight of Evidence for Cancer.xlsx")
  epa_pprtv = readxl::read_xlsx(file) %>%
    dplyr::select(name=chemical,
                  casrn,
                  url,
                  Note_in_body,
                  Note) %>%
    dplyr::mutate(source = "EPA FERA") %>%
    tidyr::unite(col = "cancer_call",
                 Note_in_body, Note,
                 sep = "; ",
                 na.rm = TRUE)

  # Health Canada
  file = paste0(toxval.config()$datapath, "cancer_summary/cancer/Health Canada/health_canada_v3_appendix_cancer_calls.xlsx")
  health_canada = readxl::read_xlsx(file) %>%
    dplyr::select(name,
                  source=cancer_call_source,
                  exposure_route,
                  cancer_call) %>%
    dplyr::mutate(
      url = "https://publications.gc.ca/collections/collection_2021/sc-hc/H129-108-2021-eng.pdf"
    )

  # IARC
  file = paste0(toxval.config()$datapath, "cancer_summary/cancer/IARC/cancer_summary_iarc_2025-02-20.xlsx")
  iarc = readxl::read_xlsx(file) %>%
    dplyr::select(name,
                  casrn,
                  cancer_call,
                  source,
                  url)

################################################################################
  mat <- dplyr::bind_rows(
    epa_fera,
    epa_iris,
    epa_opp,
    epa_pprtv,
    health_canada,
    iarc
  ) %>%
    dplyr::mutate(dplyr::across(where(is.character), ~ stringr::str_squish(.) %>%
                                  tidyr::replace_na("-")))


  mat2 = source_chemical.extra(toxval.db, source.db, mat, "Cancer Summary") %>%
    dplyr::select(
      chemical_id,
      dtxsid,
      source,
      exposure_route,
      cancer_call,
      url
    ) %>%
    # Filter out those without a DTXSID
    dplyr::filter(!dtxsid %in% c("-", NA, "NODTXSID"))

  # Export a copy
  file <- paste0(toxval.config()$datapath,"/cancer_summary/cancer_summary_",Sys.Date(),".xlsx")
  sty <- openxlsx::createStyle(halign="center",valign="center",textRotation=90,textDecoration = "bold")
  openxlsx::write.xlsx(mat2,file,firstRow=TRUE,headerStyle=sty)

  # Push cancer_summary table
  runInsertTable(mat2, "cancer_summary", toxval.db)
}
