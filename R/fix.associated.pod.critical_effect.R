#--------------------------------------------------------------------------------------
#' Appends associated POD information to critical_effect for derived toxval_types
#'
#' @param res The data to be altered
#' @param map_fields The fields used to map entries to associated PODs
#--------------------------------------------------------------------------------------
fix.associated.pod.critical_effect <- function(res, map_fields){
  # Get study-species/sex/generation mapping to handle associated PODs
  study_species_map = res %>%
    # Map using input map_fields
    dplyr::select(tidyselect::all_of(c(map_fields, "species", "sex", "generation"))) %>%
    dplyr::distinct() %>%
    # Remove entries where species is missing or human
    dplyr::filter(!grepl("human", species, ignore.case=TRUE),
                  !species %in% c("-", as.character(NA))) %>%
    dplyr::group_by_at(map_fields) %>%
    # Collapse species/sex/generation mappings
    dplyr::mutate(
      species = species %>%
        paste0(., "s") %>%
        gsub("ss$", "s", .) %>%
        gsub("mouses", "mice", .),

      dplyr::across(dplyr::where(is.character),
                    ~dplyr::na_if(., "-") %>%
                      dplyr::na_if("-s")),

      species = paste0(sort(unique(species[!is.na(species)])), collapse="/"),
      sex = paste0(sort(unique(sex[!is.na(sex)])), collapse="/"),
      generation = paste0(sort(unique(generation[!is.na(generation)])), collapse="/"),
    ) %>%
    dplyr::distinct() %>%
    dplyr::ungroup() %>%
    dplyr::rename(
      associated_species = species,
      associated_sex = sex,
      associated_generation = generation
    )

  res = res %>%
    # Map associated POD species/sex/generation
    dplyr::left_join(study_species_map, by=map_fields) %>%
    dplyr::mutate(
      # Append experimental species information to critical_effect for derived toxval_type
      critical_effect = dplyr::case_when(
        !grepl("\\bRfC\\b|\\bRfD\\b|\\bunit risk\\b|\\bslope factor\\b|\\bMRL\\b", toxval_type, ignore.case=TRUE) |
          critical_effect %in% c("-", as.character(NA)) ~ critical_effect,
        TRUE ~ stringr::str_c(critical_effect %>%
                                gsub("\\bin\\b.+", "", .),
                              " in ", associated_generation, " ", associated_sex, " ", associated_species) %>%
          gsub(" \\- |\\-s\\b", " ", .) %>%
          gsub("in \\- ", "in ", .) %>%
          stringr::str_squish()
      )
    ) %>%
    # Drop helper fields
    dplyr::select(-c("associated_generation", "associated_sex", "associated_species")) %>%
    return()
}
