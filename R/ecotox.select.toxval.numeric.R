#'@title ECOTOX Select toxval_numeric
#'@description Function to select the appropriate toxval_numeric value from conc1_* fields
#'@param in_data Input ECOTOX dataframe
#'@return Processed dataframe with new toxval_numeric, units, and qualitifer fields
ecotox.select.toxval.numeric <- function(in_data){
  # Initial list to hold processed dataframes
  out_ls <- list()
  # Track original rows to ensure all remain at end
  orig_n <- nrow(in_data)
  in_data <- in_data %>%
    dplyr::mutate(tmp_id = 1:dplyr::n())
  ### First, select conc1_mean_std (standardized conc1 mean value) without issues
  out_ls$conc1_mean_std <- in_data %>%
    dplyr::rename(toxval_units = conc1_units_std) %>%
    dplyr::mutate(conc1_mean_std = gsub("\\*", "", conc1_mean_std)) %>%
    dplyr::mutate(toxval_numeric = suppressWarnings(as.numeric(conc1_mean_std)),
                  toxval_numeric_qualifier = as.character(conc1_mean_op)) %>%
    dplyr::filter(!is.na(toxval_numeric))
  # Filter out records
  in_data <- in_data %>%
    dplyr::filter(!tmp_id %in% out_ls$conc1_mean_std$tmp_id)

  ### Select conc1_mean without issues
  out_ls$conc1_mean <- in_data %>%
    dplyr::rename(toxval_units = conc1_units_author) %>%
    dplyr::mutate(conc1_mean = gsub("\\/NR|NR|\\/|\\*|>|<|>=|<=|=|~|ca|\\+|~-|,", "", conc1_mean)) %>%
    dplyr::mutate(toxval_numeric = suppressWarnings(as.numeric(conc1_mean)),
                  toxval_numeric_qualifier = as.character(conc1_mean_op)) %>%
    dplyr::filter(!is.na(toxval_numeric))
  # Filter out records
  in_data <- in_data %>%
    dplyr::filter(!tmp_id %in% out_ls$conc1_mean$tmp_id)

  ### Select conc1_min
  out_ls$conc1_min <- in_data %>%
    dplyr::rename(toxval_units = conc1_units_author) %>%
    dplyr::mutate(toxval_numeric = suppressWarnings(as.numeric(conc1_min)),
                  toxval_numeric_qualifier = as.character(conc1_min_op)) %>%
    dplyr::filter(!is.na(toxval_numeric))
  # Filter out records
  in_data <- in_data %>%
    dplyr::filter(!tmp_id %in% out_ls$conc1_min$tmp_id)

  ### Select conc1_min fixes
  out_ls$conc1_min_fix <- in_data %>%
    dplyr::rename(toxval_units = conc1_units_author) %>%
    dplyr::mutate(conc1_min = gsub("\\*|\\/", "", conc1_min)) %>%
    dplyr::mutate(toxval_numeric = suppressWarnings(as.numeric(conc1_min)),
                  toxval_numeric_qualifier = as.character(conc1_min_op)) %>%
    dplyr::filter(!is.na(toxval_numeric))
  # Filter out records
  in_data <- in_data %>%
    dplyr::filter(!tmp_id %in% out_ls$conc1_min_fix$tmp_id)

  ### Lump remaining as NA
  out_ls$unhandled_na <- in_data %>%
    dplyr::mutate(toxval_numeric = NA,
                  toxval_numeric_qualifier = as.character(NA),
                  toxval_units = NA)

  # Check remaining fixes
  # View(out_ls$unhandled_na %>% select(tidyr::starts_with("conc1_")) %>% distinct(), "unhandled_toxval_numeric")
  # in_data %>%
  #   dplyr::select(tidyr::starts_with("conc1")) %>%
  #   distinct() %>%
  #   View()

  out_ls <- dplyr::bind_rows(out_ls)

  if(nrow(out_ls) != orig_n){
    stop("Error: Input ECOTOX rows does not match output rows for ecotox.select.toxval.numeric()")
    browser()
  } else {
    out_ls %>%
      # Select out all conc1_* fields processed
      dplyr::select(-tidyr::starts_with("conc1"), -tmp_id) %>%
      return()
  }
}
