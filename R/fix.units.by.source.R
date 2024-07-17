#-------------------------------------------------------------------------------------
#' Do all of the fixes to units
#'
#' \enumerate{
#'   \item All of these steps operate on the toxval_units column.
#'   \item Replace variant unit names with standard ones, running fix.single.param.new.by.source.R
#'   This fixes issues like variant names for mg/kg-day and uses the dictionary
#'   file dictionary/toxval_units_5.xlsx
#'   \item Fix special characters in toxval_units
#'   \item Fix issues with units containing extra characters for some ECOTOX records
#'   \item Convert units that are multiples of standard ones (e.g. ppb to ppm). This
#'   uses the dictionary file dictionary/toxval_units conversions 2018-09-12.xlsx
#'   \item Run conversions from molar to mg units, using MW. This uses the dictionary file
#'    dictionary/MW conversions.xlsx
#'   \item Convert ppm to mg/m3 for inhalation studies. This uses the conversion Concentration
#'   (mg/m3) = 0.0409 x concentration (ppm) x molecular weight. See
#'   https://cfpub.epa.gov/ncer_abstracts/index.cfm/fuseaction/display.files/fileID/14285.
#'   This function requires htat the DSSTox external chemical_id be set
#'   \item Convert ppm to mg/kg-day in toxval according to a species-specific
#'   conversion factor for oral exposures. This uses the dictionary file
#'   dictionary/ppm to mgkgday by animal.xlsx
#'   See: www10.plala.or.jp/biostatistics/1-3.doc
#'   This probbaly assumes feed rather than water
#'   \item Make sure that eco studies are in mg/L and human health in mg/m3
#' }
#'
#' @param toxval.db The version of toxvaldb to use.
#' @param source Source to be fixed
#' @param subsource Subsource to be fixed (NULL default)
#' @param do.convert.units If TRUE, so unit conversions, as opposed to just cleaning
#' @param report.only Whether to report or write/export data. Default is FALSE (write/export data)
#' @param report.extra If reporting, then choose whether to record extra conversion information (e.g. toxval_type, mw, species_id, etc.)
#' @export
#-------------------------------------------------------------------------------------
fix.units.by.source <- function(toxval.db, source=NULL, subsource=NULL, do.convert.units=FALSE, report.only=FALSE, report.extra=FALSE) {
  printCurrentFunction(paste(toxval.db,":", source))

  # Track affected values
  all_changed_data = data.frame()

  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source

  # Write initial query that can be appended to for report.only
  initial_query = paste0("SELECT a.source, a.toxval_numeric_original, a.toxval_numeric, ",
                         "a.toxval_units_original, a.toxval_units, a.toxval_type, ",
                         "a.mw, a.species_id, a.exposure_route, a.human_eco, a.toxval_id, ",
                         "a.source_hash, a.study_type, a.exposure_method, b.common_name ",
                         "FROM toxval a LEFT JOIN species b ON b.species_id = a.species_id ")

  # Handle addition of subsource for queries
  query_addition = " and qc_status NOT LIKE '%fail%'"
  if(!is.null(subsource)) {
    query_addition = paste0(query_addition, " and subsource='", subsource, "'")
  }

  # Get list of species_id values per common_name (used for species ppm to mg/kg-day conversion)
  species_ids = runQuery("SELECT DISTINCT species_id, common_name AS animal FROM species", toxval.db) %>%
    # Standardize species case
    dplyr::mutate(animal = toupper(animal)) %>%
    toxval.load.dedup(., hashing_cols=c("animal"), delim=", ")

  for(source in slist) {
    message("Working on source ", source, " ", match(c(source), slist), " of ", length(slist))

    # Get all data for current source in order to track changed data
    current_source_query = paste0(initial_query, "WHERE a.source='", source, "' AND a.qc_status NOT LIKE '%fail%'")
    if(!is.null(subsource)) current_source_query = paste0(current_source_query, " AND a.subsource='", subsource, "'")
    source_data = runQuery(current_source_query, toxval.db) %>%
      dplyr::mutate(
        # Set empty change_made field
        change_made = as.character(NA),

        # Reset toxval_numeric and toxval_units
        toxval_numeric = toxval_numeric_original,
        toxval_units = toxval_units_original
      )

    if(source=="ECOTOX") do.convert.units=FALSE

    cat("===============================================\n")
    cat(source,"\n")
    if(!is.null(subsource)) cat(subsource,"\n")
    cat("===============================================\n")
    if(!report.only) {
      cat("update 1\n")
      runQuery(paste0("update toxval set toxval_units_original='-' where toxval_units_original='' and source = '",source,"'",query_addition),toxval.db)
      cat("update 2\n")
      runQuery(paste0("update toxval set toxval_units=toxval_units_original where source = '",source,"'",query_addition),toxval.db)
      cat("update 3\n")
      runQuery(paste0("update toxval set toxval_numeric=toxval_numeric_original where source = '",source,"'",query_addition),toxval.db)
    }

    if(source=="ECOTOX") {
      # Handle special ECOTOX conversion case
      if(!report.only) {
        runQuery("update toxval set toxval_numeric_original=1E10 where toxval_numeric_original>1E10 and source='ECOTOX'",toxval.db)
      } else {
        # Track conversion data
        change = "ECOTOX > 1E10 set to 1E10"
        current_changes = source_data %>%
          dplyr::filter(toxval_numeric_original > 1E10) %>%
          dplyr::mutate(
            toxval_numeric = 1E10,
            change_made = dplyr::case_when(
              is.na(change_made) ~ !!change,
              TRUE ~ paste0(change_made, "; ", !!change)
            )
          )
        changed_ids = current_changes %>%
          dplyr::pull(toxval_id) %>%
          unique()
        source_data = source_data %>%
          dplyr::filter(!(toxval_id %in% changed_ids)) %>%
          dplyr::bind_rows(current_changes)
      }
    }

    # Remove special characters
    cat(">>> Fix special characters in units\n")
    unit.list = sort(runQuery(paste0("select distinct toxval_units_original from toxval where source = '",source,"'",query_addition," group by toxval_units_original"),
                              toxval.db)[,1])
    for(i in seq_len(length(unit.list))) {
      input = unit.list[i]
      output = input %>%
        stringr::str_replace_all("\uFFFD","u") %>%
        stringr::str_squish()

      if(!identical(input, output)){
        if(!report.only) {
          query <- paste0("update toxval set toxval_units_original='", output,
                          "' where toxval_units_original='",
                          input, "' and source = '",
                          source,"'",query_addition)
          runQuery(query, toxval.db)
        } else {
          # Record changes in toxval_units
          change = paste0("Special character removal: ", input, " to ", output)
          current_changes = source_data %>%
            dplyr::filter(toxval_units_original == !!input) %>%
            dplyr::mutate(
              toxval_units = !!output,
              change_made = dplyr::case_when(
                is.na(change_made) ~ !!change,
                TRUE ~ paste0(change_made, "; ", !!change)
              )
            )
          changed_ids = current_changes %>%
            dplyr::pull(toxval_id) %>%
            unique()
          source_data = source_data %>%
            dplyr::filter(!(toxval_id %in% changed_ids)) %>%
            dplyr::bind_rows(current_changes)
        }
      }
    }

    # Replace variant unit names with standard ones,
    cat(">>> Transform variant unit names to standard ones\n")
    if(!report.only) {
      fix.single.param.by.source(toxval.db, param="toxval_units", source, subsource, ignore = FALSE)
    } else {
      current_changes = fix.single.param.by.source(toxval.db, param="toxval_units", source,
                                                        subsource, ignore = FALSE, units.data=source_data)
      changed_ids = current_changes %>%
        dplyr::pull(toxval_id) %>%
        unique()
      source_data = source_data %>%
        dplyr::filter(!(toxval_id %in% changed_ids)) %>%
        dplyr::bind_rows(current_changes)
    }

    # Convert units to standard denominator (e.g. ppb to ppm by dividing by 1000)
    cat(">>> Convert units that are simple multiples of standard units\n")
    convos = openxlsx::read.xlsx(paste0(toxval.config()$datapath,"dictionary/toxval_units conversions 2022-08-22.xlsx"))
    #browser()
    if(!report.only) {
      query = paste0("select distinct toxval_units from toxval where source='",source,"'",query_addition)
      tulist = runQuery(query, toxval.db) %>%
        dplyr::mutate(included=1)
    } else {
      tulist = source_data %>%
        dplyr::select(toxval_units) %>%
        dplyr::distinct() %>%
        dplyr::mutate(included=1)
    }
    convos = convos %>%
      dplyr::left_join(tulist, by=c("toxval_units")) %>%
      dplyr::filter(included == 1) %>%
      dplyr::select(-included)
    if(nrow(convos)) {
      for (i in seq_len(nrow(convos))){
        cat("  ",convos[i,1],convos[i,2],convos[i,3],"\n")
        # Update toxval with conversion
        if(!report.only) {
          query = paste0("update toxval set toxval_units = '",convos[i,2],"', toxval_numeric = toxval_numeric*",convos[i,3]," where toxval_units = '",convos[i,1],"' and source = '",source,"'",query_addition)
          runQuery(query, toxval.db)
        } else {
          # Track simple multiple conversion info
          old_units = convos[i,1]
          new_units = convos[i,2]
          numeric_conversion = convos[i,3]
          change = paste0("Simple conversion: new units - ", convos[i,2], " - multiplier - ", convos[i,3])
          current_changes = source_data %>%
            dplyr::filter(toxval_units == old_units) %>%
            dplyr::mutate(
              toxval_units = new_units,
              toxval_numeric = toxval_numeric * !!numeric_conversion,
              change_made = dplyr::case_when(
                is.na(change_made) ~ !!change,
                TRUE ~ paste0(change_made, "; ", !!change)
              )
            )
          changed_ids = current_changes %>%
            dplyr::pull(toxval_id) %>%
            unique()
          source_data = source_data %>%
            dplyr::filter(!(toxval_id %in% changed_ids)) %>%
            dplyr::bind_rows(current_changes)
        }
      }
    }

    # Run conversions from molar to mg units, using MW
    cat(">>> Run conversions from molar to mg units, using MW\n")
    convos <- openxlsx::read.xlsx(paste0(toxval.config()$datapath,"dictionary/MW conversions.xlsx"))
    if(!report.only) {
      query = paste0("select distinct toxval_units from toxval where source='",source,"'",query_addition)
      tulist = runQuery(query, toxval.db) %>%
        dplyr::mutate(included=1)
    } else {
      tulist = source_data %>%
        dplyr::select(toxval_units) %>%
        dplyr::distinct() %>%
        dplyr::mutate(included=1)
    }
    convos = convos %>%
      dplyr::left_join(tulist, by=c("toxval_units")) %>%
      dplyr::filter(included == 1) %>%
      dplyr::select(-included)
    for (i in seq_len(nrow(convos))){
      units = convos[i,1]
      units.new = convos[i,2]
      cat("  ",convos[i,1],convos[i,2],"\n")

      # Update toxval table with conversions
      if(!report.only) {
        query = paste0("update toxval set toxval_units = '",convos[i,2],"', toxval_numeric = toxval_numeric*mw
                         where mw>0 and toxval_units = '",convos[i,1],"' and source = '",source,"'",query_addition)
        runQuery(query, toxval.db)
      } else {
        # Track molar changes in unit_conversions_table
        new_units = convos[i,2]
        old_units = convos[i,1]
        change = paste0("molar to mg: new units - ", new_units)
        current_changes = source_data %>%
          dplyr::filter(toxval_units == old_units,
                        mw > 0) %>%
          dplyr::mutate(
            toxval_units = new_units,
            toxval_numeric = toxval_numeric * mw,
            change_made = dplyr::case_when(
              is.na(change_made) ~ !!change,
              TRUE ~ paste0(change_made, "; ", !!change)
            )
          )
        changed_ids = current_changes %>%
          dplyr::pull(toxval_id) %>%
          unique()
        source_data = source_data %>%
          dplyr::filter(!(toxval_id %in% changed_ids)) %>%
          dplyr::bind_rows(current_changes)
      }
    }

    # Replace mg/kg with mg/kg-day where toxval type is NOAEL or NOEL
    toxval_type_list = c("BMD", "LEL", "LOAEC", "LOAEL", "LOEC", "LOEL", "HNEL", "NEL", "NOAEC", "NOAEL", "NOEC", "NOEL")
    # toxval_type_list = c('BMDL','BMDL05','BMDL10','HNEL','LOAEC','LOAEL','LOEC','LOEL','NEL','NOAEC','NOAEL','NOEC','NOEL')
    cat(">>> Replace mg/kg with mg/kg-day where toxval type is: ", toString(toxval_type_list), "\n")

    if(!report.only) {
      query <- paste0("update toxval set toxval_units='mg/kg-day' where (",
                      # Combine into a regex for any variations of the base toxval_type
                      paste0(paste0("toxval_type like '", toxval_type_list, "%'"), collapse = " or "),
                      ") and toxval_units='mg/kg' and source = '",source,"'",query_addition)
      runQuery(query, toxval.db)
    } else {
      # Track conversion data
      change = paste0("mg/kg to mg/kg-day for toxval_type: ", toString(toxval_type_list))
      current_changes = source_data %>%
        dplyr::filter(toxval_units == 'mg/kg',
                      grepl(paste0(toxval_type_list, collapse="|"), toxval_type)) %>%
        dplyr::mutate(
          toxval_units = "mg/kg-day",
          change_made = dplyr::case_when(
            is.na(change_made) ~ !!change,
            TRUE ~ paste0(change_made, "; ", !!change)
          )
        )
      changed_ids = current_changes %>%
        dplyr::pull(toxval_id) %>%
        unique()
      source_data = source_data %>%
        dplyr::filter(!(toxval_id %in% changed_ids)) %>%
        dplyr::bind_rows(current_changes)
    }

    # Convert ppm to mg/m3 for inhalation studies
    # https://cfpub.epa.gov/ncer_abstracts/index.cfm/fuseaction/display.files/fileID/14285
    if(!report.only) {
      cat(">>> Convert ppm to mg/m3 for inhalation studies\n")
      query = paste0("update toxval
                      set toxval_units = 'mg/m3', toxval_numeric = toxval_numeric*mw*0.0409
                      where mw>0 and toxval_units like 'ppm%' and exposure_route = 'inhalation'
                      and human_eco='human health' and source = '",source,"'",query_addition)
      runQuery(query, toxval.db)
    } else {
      # Track ppm to mgm3 conversion data
      change = "ppm to mg/m3 for inhalation human health studies - numeric*mw*0.0409"
      current_changes = source_data %>%
        dplyr::filter(mw > 0,
                      grepl("ppm", toxval_units, ignore.case=TRUE),
                      exposure_route == "inhalation",
                      human_eco == "human health") %>%
        dplyr::mutate(
          toxval_units = "mg/m3",
          toxval_numeric = toxval_numeric*mw*0.0409,
          change_made = dplyr::case_when(
            is.na(change_made) ~ !!change,
            TRUE ~ paste0(change_made, "; ", !!change)
          )
        )
      changed_ids = current_changes %>%
        dplyr::pull(toxval_id) %>%
        unique()
      source_data = source_data %>%
        dplyr::filter(!(toxval_id %in% changed_ids)) %>%
        dplyr::bind_rows(current_changes)
    }

    # Do the conversion from ppm to mg/kg-day on a species-wise basis for oral exposures
    cat(">>> Do the conversion from ppm to mg/kg-day on a species-wise basis\n")

    # Get conversion dictionary with mapped species_id values
    conv = readxl::read_xlsx(paste0(toxval.config()$datapath,"dictionary/ppm to mgkgday by animal.xlsx")) %>%
      # Standardize species case
      dplyr::mutate(
        animal_original = animal,
        animal = dplyr::case_when(
          animal == "Cow" ~ "COW FAMILY",
          TRUE ~ animal
        ) %>% toupper()
      ) %>%
      dplyr::left_join(species_ids, by=c("animal")) %>%
      # Add query fields to dictionary
      dplyr::mutate(
        food_query = stringr::str_c(
          "UPDATE toxval ",
          "SET toxval_numeric=toxval_numeric_original*", food_conversion, ", toxval_units='mg/kg-day' ",
          "WHERE source='", source, "' ",
          "AND study_type='", study_type, "' ",
          "AND toxval_units LIKE 'ppm%' ",
          "AND species_id IN (", species_id, ") ",
          "AND exposure_method IN ('feed', 'food', 'diet')",
          !!query_addition
        ),
        water_query = stringr::str_c(
          "UPDATE toxval ",
          "SET toxval_numeric=toxval_numeric_original*", water_conversion, ", toxval_units='mg/kg-day' ",
          "WHERE source='", source, "' ",
          "AND study_type='", study_type, "' ",
          "AND toxval_units LIKE 'ppm%' ",
          "AND species_id IN (", species_id, ") ",
          "AND exposure_method IN ('drinking water', 'water')",
          !!query_addition
        )
      )

    # Loop through conversion dictionary and push conversions if specified
    for(i in seq_len(nrow(conv))) {
      if(!report.only) {
        runQuery(conv$food_query[i], toxval.db)
        runQuery(conv$water_query[i], toxval.db)
      } else {
        # Record species ppm to mg/kg-day conversion info
        curr_animal = conv$animal[i]
        curr_s_ids = conv$species_id[i]
        s_id_list = stringr::str_split_1(curr_s_ids, pattern=", ")
        curr_study_type = conv$study_type[i]
        water_conversion = conv$water_conversion[i]
        food_conversion = conv$food_conversion[i]
        change = paste0("ppm to mg/kg-day by species: ", curr_animal, ", species_id: (", curr_s_ids, ")")
        current_changes = source_data %>%
          dplyr::filter(study_type == !!curr_study_type,
                        species_id %in% s_id_list,
                        exposure_method %in% c("drinking water", "water", "feed", "food", "diet"),
                        grepl("ppm", toxval_units, ignore.case=TRUE)) %>%
          dplyr::mutate(
            toxval_units = "mg/kg-day",
            toxval_numeric = dplyr::case_when(
              exposure_method %in% c("drinking water", "water") ~ toxval_numeric_original * !!water_conversion,
              exposure_method %in% c("feed", "food", "diet") ~ toxval_numeric_original * !!food_conversion,
              TRUE ~ toxval_numeric
            ),
            change_made = dplyr::case_when(
              is.na(change_made) ~ !!change,
              TRUE ~ paste0(change_made, "; ", !!change)
            )
          )
        changed_ids = current_changes %>%
          dplyr::pull(toxval_id) %>%
          unique()
        source_data = source_data %>%
          dplyr::filter(!(toxval_id %in% changed_ids)) %>%
          dplyr::bind_rows(current_changes)
      }
    }

    # Handle conversion from mg/kg diet to mg/kg-day
    cat(">>> Handle conversion from mg/kg diet to mg/kg-day\n")
    if(!report.only) {
      query = paste0("UPDATE toxval a ",
                     "INNER JOIN species b ON a.species_id=b.species_id ",
                     "SET a.toxval_units='mg/kg-day', ",
                     "a.toxval_numeric = CASE",
                     " WHEN b.common_name='Rat' THEN a.toxval_numeric / 16",
                     " WHEN b.common_name='Mouse' THEN a.toxval_numeric / 4.5",
                     " ELSE a.toxval_numeric ",
                     "WHERE a.toxval_units='mg/kg diet' ",
                     "AND b.common_name IN ('Rat', 'Mouse')",
                     query_addition %>%
                       gsub("subsource", "a.subsource", .) %>%
                       gsub("qc_status", "a.qc_status", .))
      runQuery(query, toxval.db)
    } else {
      # Track mg/kg diet conversion data
      change = "mg/kg diet to mg/kg-day"
      current_changes = source_data %>%
        dplyr::filter(
          common_name %in% c("Rat", "Mouse"),
          toxval_units == "mg/kg diet"
        ) %>%
        dplyr::mutate(
          toxval_units = "mg/kg-day",
          toxval_numeric = dplyr::case_when(
            common_name == "Rat" ~ toxval_numeric / 16,
            common_name == "Mouse" ~ toxval_numeric / 4.5,
            TRUE ~ toxval_numeric
          ),
          change_made = dplyr::case_when(
            is.na(change_made) ~ !!change,
            TRUE ~ paste0(change_made, "; ", !!change)
          )
        )
      changed_ids = current_changes %>%
        dplyr::pull(toxval_id) %>%
        unique()
      source_data = source_data %>%
        dplyr::filter(!(toxval_id %in% changed_ids)) %>%
        dplyr::bind_rows(current_changes)
    }



    # Handle special mg/kg soil case for RSL
    if(source == "RSL") {
      cat(">>> Handle special mg/kg soil case for RSL\n")
      if(!report.only) {
        query = paste0("UPDATE toxval SET toxval_units='mg/kg soil' ",
                       "WHERE toxval_type",
                       " IN ('screening level (residential soil)', 'screening level (industrial soil)') ",
                       "AND source='RSL'", query_addition)
        runQuery(query, toxval.db)
      } else {
        # Track RSL conversion data
        change = "mg/kg to mg/kg soil for RSL"
        current_changes = source_data %>%
          dplyr::filter(
            toxval_type %in% c('screening level (residential soil)', 'screening level (industrial soil)')
          ) %>%
          dplyr::mutate(
            toxval_units = "mg/kg soil",
            change_made = dplyr::case_when(
              is.na(change_made) ~ !!change,
              TRUE ~ paste0(change_made, "; ", !!change)
            )
          )
        changed_ids = current_changes %>%
          dplyr::pull(toxval_id) %>%
          unique()
        source_data = source_data %>%
          dplyr::filter(!(toxval_id %in% changed_ids)) %>%
          dplyr::bind_rows(current_changes)
      }
    }

    if(report.only) {
      # Add data changed from current source to running total
      current_changed_data = source_data %>%
        tidyr::drop_na(change_made)
      all_changed_data = dplyr::bind_rows(all_changed_data, current_changed_data) %>%
        dplyr::distinct()
    }
  }

  if(report.only) {
    if(!report.extra) {
      all_changed_data = all_changed_data %>%
        dplyr::select(c("source", "toxval_numeric_original", "toxval_units_original",
                        "toxval_numeric", "toxval_units"))
    }

    unit_conversions_table = all_changed_data %>%
      dplyr::mutate(
        conversion_factor = dplyr::case_when(
          is.na(toxval_numeric_original) | is.na(toxval_numeric) ~ NA,
          # Handles case of "0" values as well
          toxval_numeric_original == toxval_numeric ~ 1,
          toxval_numeric_original == 0 ~ NA,
          TRUE ~ toxval_numeric / toxval_numeric_original
        )
      ) %>%
      # Sanity check - filter out entries whose values/units did not change
      dplyr::filter(!((toxval_numeric == toxval_numeric_original) & (toxval_units == toxval_units_original))) %>%
      # Remove duplicate entries
      dplyr::distinct()

    writexl::write_xlsx(unit_conversions_table, paste0(toxval.config()$datapath,
                                                       "dictionary/unit_conversion_check_",
                                                       Sys.Date(),
                                                       ".xlsx"))
    return(unit_conversions_table)
  }
}
