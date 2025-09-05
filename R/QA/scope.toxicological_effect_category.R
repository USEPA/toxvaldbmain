#' @title scope.toxicological_effect_category
#' @description Query toxval to scope toxicological_effect_category curation effort.
#' @param toxval.db The version of toxval to query.
#' @return None. XLSX file is written with results.
#' @seealso
#'  \code{\link[dplyr]{filter}}, \code{\link[dplyr]{mutate}}, \code{\link[dplyr]{select}}, \code{\link[dplyr]{distinct}}, \code{\link[dplyr]{case_when}}, \code{\link[dplyr]{reexports}}, \code{\link[dplyr]{c("rowwise", "rowwise")}}, \code{\link[dplyr]{group_by}}, \code{\link[dplyr]{bind_rows}}, \code{\link[dplyr]{rename}}, \code{\link[dplyr]{mutate-joins}}, \code{\link[dplyr]{across}}, \code{\link[dplyr]{summarise}}, \code{\link[dplyr]{count}}
#'  \code{\link[tidyr]{separate_rows}}, \code{\link[tidyr]{unite}}
#'  \code{\link[stringr]{str_trim}}, \code{\link[stringr]{str_length}}
#'  \code{\link[writexl]{write_xlsx}}
#' @rdname scope.toxicological_effect_category
#' @export
#' @importFrom dplyr filter mutate select distinct case_when everything rowwise ungroup bind_rows rename left_join group_by across summarise count
#' @importFrom tidyr separate_rows unite
#' @importFrom stringr str_squish str_length
#' @importFrom writexl write_xlsx
scope.toxicological_effect_category <- function(toxval.db){

  # Check if "critical_effect" is a field name
  tbl_f_list = runQuery(paste0(
    "SELECT DISTINCT TABLE_NAME, COLUMN_NAME FROM information_schema.columns ",
    "WHERE table_schema = '", toxval.db, "' AND ",
    "TABLE_NAME = 'toxval'"
  ),
  toxval.db)

  # Boolean switch whether to use "critical_effect" or "toxicolgical_effect"
  tox_crit_term = "toxicological_effect"
  if("critical_effect_original" %in% tbl_f_list$COLUMN_NAME){
    tox_crit_term = "critical_effect_original"
  }

  # Pull all toxval data
  in_data = runQuery(paste0("SELECT source_hash, ", tox_crit_term," as toxicological_effect, study_type, exposure_route, source FROM toxval"),
                     toxval.db)

  # Constant total rows in toxval
  n_toxval = nrow(in_data)

  # Pull previous toxicological_effect_category mappings
  crit_cat_map_query = paste0("SELECT distinct source_hash, BINARY term as term, study_type, toxicological_effect_category, ",
                              "CONCAT(source_hash, '_', term) as crit_key ",
                              "FROM toxicological_effect_terms ",
                              "WHERE source_hash in (",
                              "SELECT source_hash FROM toxval ",
                               ") ORDER BY id"
                              )

  # Pull toxicological_effect_category map
  crit_cat_map <- runQuery(crit_cat_map_query, toxval.db)

  # Filter to records that have a toxicological_effect_category
  has_crit_cat <- in_data %>%
    dplyr::filter(source_hash %in% crit_cat_map$source_hash) %>%
    dplyr::mutate(scope_tox_eff_cat = 1)

  # Map toxicological_effect_category
  # List of types to overwrite as repeated dose or reprodev
  repeat_study_types = c('immunotoxicity','intermediate','repeat dose other','subchronic',
                         'neurotoxicity subchronic','neurotoxicity chronic',
                         'neurotoxicity 28-day', 'neurotoxicity','intermediate','1','104','14','2','24',
                         'immunotoxicity subchronic','immunotoxicity chronic',
                         'immunotoxicity 28-day','immunotoxicity','growth','chronic','28-day', 'short-term')

  reprodev_study_types = c('reproduction developmental',
                           'extended one-generation reproductive toxicity - with F2 generation and developmental neurotoxicity (Cohorts 1A, 1B with extension, 2A and 2B)',
                           'extended one-generation reproductive toxicity - with F2 generation and both developmental neuro- and immunotoxicity (Cohorts 1A, 1B with extension, 2A, 2B, and 3)',
                           'extended one-generation reproductive toxicity - with F2 generation (Cohorts 1A, and 1B with extension)',
                           'developmental')

  # Filter to those without a mapping
  in_data = in_data %>%
    dplyr::filter(!source_hash %in% has_crit_cat$source_hash)

  # Unwrap toxicological_effect and export missing toxicological_effect_category mappings
  missing_crit_cat = in_data %>%
    dplyr::select(source_hash, toxicological_effect, study_type, exposure_route, source) %>%
    tidyr::separate_rows(toxicological_effect, sep = "\\|") %>%
    dplyr::mutate(toxicological_effect = stringr::str_squish(toxicological_effect)) %>%
    tidyr::unite(col = "crit_key",
                 source_hash, toxicological_effect,
                 sep = "_",
                 remove = FALSE) %>%
    dplyr::distinct() %>%
    dplyr::filter(!crit_key %in% crit_cat_map$crit_key) %>%
    dplyr::select(-crit_key) %>%
    dplyr::distinct() %>%
    dplyr::mutate(
      study_type = dplyr::case_when(
        study_type %in% !!repeat_study_types ~ "repeat dose",
        study_type %in% !!reprodev_study_types ~ "reproductive developmental",
        TRUE ~ study_type
      )
    ) %>%
    dplyr::select(source_hash, term=toxicological_effect, dplyr::everything())

  # Add suggestions
  if(nrow(missing_crit_cat)){
    # Handle case of long source_hash list XLSX character limit
    # Get hash length, add 2 characters for ", " delimiter
    hash_length = min(stringr::str_length(in_data$source_hash)) + 2
    tmp = missing_crit_cat %>%
      dplyr::mutate(n_count = stringr::str_length(source_hash),
                    group_length = dplyr::case_when(
                      n_count < 10000 ~ n_count,
                      TRUE ~ floor(n_count / floor(n_count / 10000))
                    ),
                    n_group = floor(group_length / hash_length)
      )

    # Split large strings into smaller groups
    split = tmp %>%
      dplyr::filter(n_count > 10000)

    if(nrow(split)){
      split = split %>%
        dplyr::rowwise() %>%
        dplyr::mutate(out_groups = strsplit(source_hash, ", ") %>%
                        unlist() %>%
                        split(., rep(seq_along(.), each  = n_group, length.out = length(.))) %>%
                        lapply(., toString) %>%
                        paste0(collapse = ";")
        ) %>%
        dplyr::ungroup() %>%
        tidyr::separate_rows(out_groups, sep = ";") %>%
        dplyr::select(-n_count, -group_length, -n_group)

      # Filter out records with strings over limit, append split groups
      missing_crit_cat = missing_crit_cat %>%
        dplyr::filter(!source_hash %in% split$source_hash) %>%
        dplyr::bind_rows(split %>%
                           dplyr::select(-source_hash) %>%
                           dplyr::rename(source_hash = out_groups))
    }

    # Dictionary of critical_category suggestions already present in database
    crit_suggs_dict = runQuery(paste0('SELECT distinct term, study_type, toxicological_effect_category, CONCAT(term, study_type) as crit_key ',
                                      'FROM toxicological_effect_terms ORDER BY id'),
                               toxval.db)

    crit_suggs <- crit_suggs_dict %>%
      dplyr::filter(crit_key %in% unique(paste0(missing_crit_cat$term, missing_crit_cat$study_type))) %>%
      dplyr::select(-crit_key)

    # Map suggestions and classify the assignment
    missing_crit_cat = missing_crit_cat %>%
      dplyr::left_join(crit_suggs,
                       by = c("term", "study_type")) %>%
      dplyr::mutate(scope_tox_eff_cat = dplyr::case_when(
        toxicological_effect_category %in% c("", "none", "None") ~ -1,
        !is.na(toxicological_effect_category) ~ 2,
        TRUE ~ 0
      ))
  }

  # Collapse source_hash to export unique cases to map
  crit_cat_curate = missing_crit_cat %>%
    dplyr::group_by(dplyr::across(c(-source_hash))) %>%
    dplyr::summarise(source_hash = toString(source_hash)) %>%
    dplyr::ungroup() %>%
    dplyr::distinct()

  # Recombine to help summarize
  out = has_crit_cat %>%
    dplyr::bind_rows(missing_crit_cat) %>%
    dplyr::select(source_hash, exposure_route, scope_tox_eff_cat, source) %>%
    dplyr::distinct()

  # Summarize by route
  out_route = out %>%
    dplyr::group_by(exposure_route) %>%
    dplyr::count(scope_tox_eff_cat) %>%
    dplyr::mutate(prop = round(n / n_toxval * 100, 3))

  # Summarize by source
  out_source = out %>%
    dplyr::group_by(source) %>%
    dplyr::count(scope_tox_eff_cat) %>%
    dplyr::mutate(prop = round(n / n_toxval * 100, 3))

  # Summarize by source and route
  out_source_route = out %>%
    dplyr::group_by(source, exposure_route) %>%
    dplyr::count(scope_tox_eff_cat) %>%
    dplyr::mutate(prop = round(n / n_toxval * 100, 3))

  # Get hash length, add 2 characters for ", " delimiter to get a buffer
  hash_buffer = min(stringr::str_length(crit_cat_curate$source_hash)) + 2
  excel_str_limit = 32767
  hash_group_limit = floor(excel_str_limit / hash_buffer)

  crit_cat_split = crit_cat_curate %>%
    dplyr::mutate(hash_length = stringr::str_length(source_hash),
                  hash_truncation_needed = hash_length > excel_str_limit,
                  hash_count = ceiling(hash_length / hash_buffer),

                  group_divisor = dplyr::case_when(
                    hash_truncation_needed == FALSE ~ 0,
                    TRUE ~ ceiling(hash_count / hash_group_limit)
                  ))


  # Split large strings into smaller groups
  split = crit_cat_split %>%
    dplyr::filter(group_divisor > 0)

  if(nrow(split)){
    split = split %>%
      dplyr::rowwise() %>%
      dplyr::mutate(out_groups = strsplit(source_hash, ", ") %>%
                      unlist() %>%
                      split(., rep(seq_along(.), each  = hash_group_limit * 0.75, length.out = length(.))) %>%
                      lapply(., toString) %>%
                      paste0(collapse = ";")
      ) %>%
      dplyr::ungroup() %>%
      tidyr::separate_rows(out_groups, sep = ";") %>%
      dplyr::select(-hash_length, -hash_truncation_needed, -hash_count, -group_divisor) %>%
      dplyr::distinct()

    # Filter out records with strings over limit, append split groups
    crit_cat_curate = crit_cat_curate %>%
      dplyr::filter(!source_hash %in% split$source_hash) %>%
      dplyr::bind_rows(split %>%
                         dplyr::select(-source_hash) %>%
                         dplyr::rename(source_hash = out_groups))

    out_check = crit_cat_curate %>%
      dplyr::mutate(final_hash_length = stringr::str_length(source_hash)) %>%
      dplyr::filter(final_hash_length >= excel_str_limit)
  }

  # Export sheets
  writexl::write_xlsx(
    list(crit_cat_curate = crit_cat_curate,
         scope_full = out,
         route = out_route,
         source = out_source,
         source_route = out_source_route),
    paste0(toxval.config()$datapath, "dictionary/missing/scope_toxicological_effect_category_", Sys.Date(),".xlsx")
  )
}
