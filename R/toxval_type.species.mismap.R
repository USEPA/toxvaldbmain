library(digest)
#-----------------------------------------------------------------------------------
#' Find species and toxval_type mismaps, e.g. species other than human fro RfD, RfC, cancer sloper, MSL, etc.
#'
#' @param toxval.db Database version
#' @param source The source to be updated
#' @return Write a file with the results
#'
#-----------------------------------------------------------------------------------
toxval_type.species.mismap <- function(toxval.db) {
  printCurrentFunction(toxval.db)
  dir = paste0(toxval.config()$datapath, "data/input/") # paste0(toxval.config()$datapath,"manuscript_data")
  slist = runQuery("select distinct source from toxval",toxval.db)[,1]

  res = lapply(slist, function(src){
    cat(src,"\n")
    query = paste0("SELECT
                    a.dtxsid,a.casrn,a.name,
                    b.source,
                    b.toxval_type,
                    b.toxval_subtype,
                    e.toxval_type_supercategory,
                    b.species_original,
                    d.species_id,d.common_name,d.latin_name,d.ecotox_group,
                    b.source_hash,
                    a.cleaned_casrn,a.cleaned_name
                    FROM
                    toxval b
                    INNER JOIN source_chemical a on a.chemical_id=b.chemical_id
                    LEFT JOIN species d on b.species_id=d.species_id
                    INNER JOIN toxval_type_dictionary e on b.toxval_type=e.toxval_type
                    WHERE
                    b.source='",src,"'
                    and human_eco='human health'
                    and toxval_type_supercategory in ('Toxicity Value')")
    query %>%
      runQuery(toxval.db, TRUE, FALSE) %>%
      dplyr::distinct() %>%
      # Replace NA or - name/casrn with cleaned name/casrn
      dplyr::mutate(
        casrn = dplyr::case_when(
          casrn %in% c(NA, "-") ~ cleaned_casrn,
          TRUE ~ casrn
        ),
        name = dplyr::case_when(
          name %in% c(NA, "-") ~ cleaned_name,
          TRUE ~ name
        )
      ) %>%
      dplyr::select(-dplyr::any_of(c("cleaned_name", "cleaned_casrn"))) %>%
      dplyr::distinct()

  }) %>%
    dplyr::bind_rows()

  file = paste0(dir, "/toxval_type.species.mismap_", toxval.db, "_", Sys.Date(), ".xlsx")
  sty = openxlsx::createStyle(halign="center",valign="center",textRotation=90,textDecoration = "bold")
  openxlsx::write.xlsx(res, file, firstRow=TRUE, headerStyle=sty)
}
