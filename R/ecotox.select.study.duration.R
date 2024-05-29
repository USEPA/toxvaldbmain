#'@title ECOTOX Select study_duration
#'@description Function to select the appropriate study_duration value from conc1_* fields
#'@param in_data Input ECOTOX dataframe
#'@return Processed dataframe with new study_duration_values, units, and qualitifer fields
ecotox.select.study.duration <- function(in_data){
  # Initial list to hold processed dataframes
  out_ls <- list()
  # Track original rows to ensure all remain at end
  orig_n <- nrow(in_data)
  in_data <- in_data %>%
    dplyr::mutate(tmp_id = 1:dplyr::n())
  ### First, select observed_duration_std (standardized study duration mean value) without issues
  out_ls$observed_duration_std <- in_data %>%
    dplyr::rename(study_duration_units = observed_duration_units_std) %>%
    dplyr::mutate(observed_duration_std = gsub("~|<|<=|>|>=", "", observed_duration_std)) %>%
    dplyr::mutate(study_duration_value = suppressWarnings(as.numeric(observed_duration_std)),
                  study_duration_qualifier = observ_duration_mean_op) %>%
    dplyr::filter(!is.na(study_duration_value))
  # Filter out records
  in_data <- in_data %>%
    dplyr::filter(!tmp_id %in% out_ls$observed_duration_std$tmp_id)

  ### Select observ_duration_mean without issues
  out_ls$observ_duration_mean <- in_data %>%
    dplyr::rename(study_duration_units = observ_duration_unit_desc) %>%
    dplyr::mutate(observ_duration_mean = gsub("~|<|<=|>|>=", "", observ_duration_mean)) %>%
    dplyr::mutate(study_duration_value = suppressWarnings(as.numeric(observ_duration_mean)),
                  study_duration_qualifier = observ_duration_mean_op) %>%
    dplyr::filter(!is.na(study_duration_value))
  # Filter out records
  in_data <- in_data %>%
    dplyr::filter(!tmp_id %in% out_ls$observ_duration_mean$tmp_id)

  ### Select observ_duration_max
  out_ls$observ_duration_max <- in_data %>%
    dplyr::rename(study_duration_units = observ_duration_unit_desc) %>%
    dplyr::mutate(observ_duration_max = gsub("\\/", "", observ_duration_max)) %>%
    dplyr::mutate(study_duration_value = suppressWarnings(as.numeric(observ_duration_max)),
                  study_duration_qualifier = observ_duration_max_op) %>%
    dplyr::filter(!is.na(study_duration_value))
  # Filter out records
  in_data <- in_data %>%
    dplyr::filter(!tmp_id %in% out_ls$observ_duration_max$tmp_id)

  ### Lump remaining as NA
  out_ls$unhandled_na <- in_data %>%
    dplyr::mutate(study_duration_value = NA,
                  study_duration_qualifier = NA,
                  study_duration_units = NA)

  # Check remaining fixes
  # View(out_ls$unhandled_na %>% select(tidyr::starts_with("observ")) %>% distinct(), "unhandled_study_duration")
  # in_data %>%
  #   dplyr::select(tidyr::starts_with("observ")) %>%
  #   distinct() %>%
  #   View()

  out_ls <- dplyr::bind_rows(out_ls)

  if(nrow(out_ls) != orig_n){
    stop("Error: Input ECOTOX rows does not match output rows for ecotox.select.study.duration()")
    browser()
  } else {

    # Fix study_duration_units
    out_ls <- out_ls %>%
      dplyr::mutate(study_duration_units = dplyr::case_when(
        study_duration_units == 'abs'~'Until abscission',
        study_duration_units == 'ac'~'Age class',
        study_duration_units == 'ant'~'Until anthesis',
        study_duration_units == 'b0.25'~'0.25 bloom stage',
        study_duration_units == 'BLM'~'Bloom stage',
        study_duration_units == 'brd'~'Brood or litter',
        study_duration_units == 'brs'~'Breeding season',
        study_duration_units == 'bt'~'Boot stage',
        study_duration_units == 'cd'~'Colony diameter',
        study_duration_units == 'cfs'~'Commercial flowering stage',
        study_duration_units == 'd'~'Day(s)',
        study_duration_units == 'dapu'~'Days after pupation',
        study_duration_units == 'dbh'~'Weeks pre-hatch',
        study_duration_units == 'dd'~'Degree days',
        study_duration_units == 'dge'~'Days gestation',
        study_duration_units == 'dla'~'Days lactation',
        study_duration_units == 'dpe'~'Days post-emergence',
        study_duration_units == 'dpel'~'Days post egg laying',
        study_duration_units == 'dpf'~'Days post fertilization',
        study_duration_units == 'dpfl'~'Days post flowering',
        study_duration_units == 'dpgm'~'Days post germination',
        study_duration_units == 'dph'~'Days post-hatch',
        study_duration_units == 'dphv'~'Days post harvest',
        study_duration_units == 'dpmm'~'Days post metamorphosis',
        study_duration_units == 'dpn'~'Days post-natal',
        study_duration_units == 'dpp'~'Days post planting/sowing',
        study_duration_units == 'ea'~'To earing or heading',
        study_duration_units == 'eb'~'Early bloom stage',
        study_duration_units == 'ej'~'Egg to juvenile',
        study_duration_units == 'el'~'Egg(s) laid',
        study_duration_units == 'em'~'Emergence',
        study_duration_units == 'ep'~'Egg to pupation',
        study_duration_units == 'epa'~'Egg to pre-adult',
        study_duration_units == 'eslk'~'Early silk stage',
        study_duration_units == 'eta'~'Egg to adult',
        study_duration_units == 'f5'~'50% flowering',
        study_duration_units == 'fb'~'Full bloom stage',
        study_duration_units == 'fi'~'Floral initiation',
        study_duration_units == 'fl'~'Flower stage',
        study_duration_units == 'flg'~'Fledging',
        study_duration_units == 'fr'~'Fruit stage',
        study_duration_units == 'fs'~'Flowering stage',
        study_duration_units == 'ge'~'Generation',
        study_duration_units == 'ges'~'Gestation',
        study_duration_units == 'gm'~'Germination',
        study_duration_units == 'gs'~'Growing season',
        study_duration_units == 'hns '~'Haun stage',
        study_duration_units == 'hpel'~'Hours post egg laying',
        study_duration_units == 'hpp'~'Hours post planting/sowing',
        study_duration_units == 'ht'~'Until hatch',
        study_duration_units == 'hv'~'Harvest',
        study_duration_units == 'ins'~'Instar',
        study_duration_units == 'inst'~'Instantaneous',
        study_duration_units == 'it'~'Intermolt to molt',
        study_duration_units == 'js'~'jointing stage',
        study_duration_units == 'lf'~'Lifetime;no associated numeric value',
        study_duration_units == 'lfd'~'Leaf drop',
        study_duration_units == 'lhv15-20'~'Leaf harvest, 15-20 cm',
        study_duration_units == 'lhv20-25'~'Leaf harvest, 20-25 cm',
        study_duration_units == 'ls'~'Leaf stage',
        study_duration_units == 'ls6'~'Six leaf stage',
        study_duration_units == 'LSI'~'Larval stage index',
        study_duration_units == 'lva'~'Larva to adult',
        study_duration_units == 'lvp'~'Larva to pupa',
        study_duration_units == 'ma'~'Maturity',
        study_duration_units == 'mmph'~'Until metamorphosis',
        study_duration_units == 'moph'~'Months post-hatch',
        study_duration_units == 'MULT'~'Multiple durations (for terrestrial rollup)',
        study_duration_units == 'pa'~'Pupa to adult',
        study_duration_units == 'pan'~'Panicle formation stage',
        study_duration_units == 'pgm'~'Post germination',
        study_duration_units == 'pm'~'Post molt',
        study_duration_units == 'pr'~'Priming',
        study_duration_units == 'pro'~'Propagation stage',
        study_duration_units == 'rhv3'~'Root harvest, 3 grams',
        study_duration_units == 'sds'~'seedling stage',
        study_duration_units == 'slk'~'Silk stage',
        study_duration_units == 'so'~'Shooting stage',
        study_duration_units == 'sst'~'Substage',
        study_duration_units == 'stg'~'Stage',
        study_duration_units == 'tls'~'Tiller stage',
        study_duration_units == 'ubi'~'Until birth',
        study_duration_units == 'utl'~'Until larva',
        study_duration_units == 'utp'~'Until pupa',
        study_duration_units == 'vg'~'Vegatative stage',
        study_duration_units == 'wpp'~'Weeks post planting/sowing',
        study_duration_units == 'wpv3'~'Weeks post V3 stage',
        study_duration_units == 'wpv5'~'Weeks post V5 stage',
        study_duration_units == 'ZGS'~'Zadoks growth stage',
        TRUE ~ study_duration_units
      ))

    out_ls %>%
      # Select out all conc1_* fields processed
      dplyr::select(-tidyr::starts_with("observ"), -tmp_id) %>%
      return()
  }
}
