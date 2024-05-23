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
  dsstox.db <- toxval.config()$dsstox.db

  # Track affected toxval_id values
  changed_toxval_id = data.frame()

  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  for(source in slist) {
    if(source=="ECOTOX") do.convert.units=FALSE
    if(source=="ECOTOX") {
      # Handle special ECOTOX conversion case
      if(!report.only) {
        runQuery("update toxval set toxval_numeric_original=1E10 where toxval_numeric_original>1E10 and source='ECOTOX'",toxval.db)
      } else {
        # Track conversion data
        changed_toxval_id = runQuery("SELECT DISTINCT toxval_id FROM toxval WHERE toxval_numeric_original>1E10 AND source='ECOTOX'",
                                     toxval.db) %>%
          dplyr::mutate(change_made = "ECOTOX > 1E10 set to 1E10") %>%
          dplyr::bind_rows(changed_toxval_id, .)
      }
    }
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

    # Remove special characters
    cat(">>> Fix special characters in units\n")
    unit.list = sort(runQuery(paste0("select distinct toxval_units_original from toxval where source = '",source,"'",query_addition," group by toxval_units_original"),
                              toxval.db)[,1])
    for(i in seq_len(length(unit.list))) {
      input = unit.list[i]
      output = input
      output = output %>%
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
          changed_toxval_id = runQuery(paste0("SELECT DISTINCT toxval_id FROM toxval ",
                                              "where toxval_units_original='",input,
                                              "' and source = '",source,"'",query_addition),
                                       toxval.db) %>%
            dplyr::mutate(change_made = paste0("Special character removal: ", input, " to ", output)) %>%
            dplyr::bind_rows(changed_toxval_id, .)
        }
      }
    }

    # Replace variant unit names with standard ones,
    cat(">>> Transform variant unit names to standard ones\n")
    single_param_altered = fix.single.param.by.source(toxval.db, param="toxval_units", source, subsource, ignore = FALSE, report.units=report.only)
    if(report.only) {
      changed_toxval_id = single_param_altered %>%
        dplyr::mutate(change_made = "Transform variant unit names to standard ones") %>%
        dplyr::bind_rows(changed_toxval_id, .)
    }

    # Replace mg/kg with mg/kg-day where toxval type is NOAEL or NOEL
    toxval_type_list = c('BMDL','BMDL05','BMDL10','HNEL','LOAEC','LOAEL','LOEC','LOEL','NEL','NOAEC','NOAEL','NOEC','NOEL')
    cat(">>> Replace mg/kg with mg/kg-day where toxval type is ", toxval_type_list,"\n")
    if(!report.only) {
      query <- paste0("update toxval set toxval_units='mg/kg-day' where toxval_type in ('",
                      paste0(toxval_type_list, collapse="', '"),
                      "') and toxval_units='mg/kg' and source = '",source,"'",query_addition)
      runQuery(query, toxval.db)
    } else {
      # Track conversion data
      changed_toxval_id = runQuery(paste0("SELECT DISTINCT toxval_id FROM toxval ",
                                              "WHERE toxval_type in ('",
                                          paste0(toxval_type_list, collapse="', '"),
                                          "') and toxval_units='mg/kg' and source = '",source,"'",query_addition),
                                       toxval.db) %>%
        dplyr::mutate(change_made = paste0("mg/kg to mg/kg-day for toxval_type: ", toString(toxval_type_list))) %>%
        dplyr::bind_rows(changed_toxval_id, .)
    }

    # Convert units to standard denominator (e.g. ppb to ppm by dividing by 1000)
    cat(">>> Convert units that are simple multiples of standard units\n")
    convos = read.xlsx(paste0(toxval.config()$datapath,"dictionary/toxval_units conversions 2022-08-22.xlsx"))
    #browser()
    query = paste0("select distinct toxval_units from toxval where source='",source,"'",query_addition)
    tulist = runQuery(query,toxval.db)[,1]
    convos = convos[is.element(convos$toxval_units,tulist),]
    nrows = dim(convos)[1]
    if(nrows>0) {
      for (i in seq_len(nrows)){
        cat("  ",convos[i,1],convos[i,2],convos[i,3],"\n")
        # Update toxval with conversion
        if(!report.only) {
          query = paste0("update toxval set toxval_units = '",convos[i,2],"', toxval_numeric = toxval_numeric*",convos[i,3]," where toxval_units = '",convos[i,1],"' and source = '",source,"'",query_addition)
          runQuery(query, toxval.db)
        } else {
          # Track simple multiple conversion info
          changed_toxval_id = runQuery(paste0("SELECT DISTINCT toxval_id FROM toxval ",
                                             "WHERE toxval_units='",convos[i,1],"' and source='",source,"'",query_addition),
                                      toxval.db) %>%
            dplyr::mutate(change_made = paste0("Simple conversion: new units - ", convos[i,2], " - multiplier - ", convos[i,3])) %>%
            dplyr::bind_rows(changed_toxval_id, .)
        }
      }
    }

    # Run conversions from molar to mg units, using MW
    cat(">>> Run conversions from molar to mg units, using MW\n")
    convos <- read.xlsx(paste0(toxval.config()$datapath,"dictionary/MW conversions.xlsx"))
    query = paste0("select distinct toxval_units from toxval where source='",source,"'",query_addition)
    tulist = runQuery(query,toxval.db)[,1]
    convos = convos[is.element(convos$toxval_units,tulist),]
    nrows = dim(convos)[1]
    for (i in seq_len(nrows)){
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
        changed_toxval_id = runQuery(paste0("SELECT DISTINCT toxval_id FROM toxval ",
                                      "WHERE mw>0 and toxval_units = '",convos[i,1],"' and source = '",source,"'",query_addition),
                               toxval.db) %>%
          dplyr::mutate(change_made = paste0("molar to mg: new units - ", convos[i, 2])) %>%
          dplyr::bind_rows(changed_toxval_id, .)
      }
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
      changed_toxval_id = runQuery(paste0("SELECT DISTINCT toxval_id FROM toxval ",
                                    "where mw>0 and toxval_units like 'ppm%' and exposure_route = 'inhalation' ",
                                    "and human_eco='human health' and source = '",source,"'",query_addition),
                             toxval.db) %>%
        dplyr::mutate(change_made = paste0("ppm to mg/m3 for inhalation human health studies - numeric*mw*0.0409")) %>%
        dplyr::bind_rows(changed_toxval_id, .)
    }

    # Do the conversion from ppm to mg/kg-day on a species-wise basis for oral exposures
    cat(">>> Do the conversion from ppm to mg/kg-day on a species-wise basis\n")
    conv = read.xlsx(paste0(toxval.config()$datapath,"dictionary/ppm to mgkgday by animal.xlsx"))
    for (i in seq_len(nrow(conv))){
      species <- conv[i,1]
      factor <- conv[i,2]
      sid.list <- runQuery(paste0("select species_id from species where common_name ='",species,"'"),toxval.db)[,1]
      for(sid in sid.list) {
        cat(species, sid,":",length(sid.list),"\n")

        # Update toxval with conversion
        if(!report.only) {
          query = paste0("update toxval
                         set toxval_numeric = toxval_numeric_original*",factor,", toxval_units = 'mg/kg-day'
                         where exposure_route='oral'
                         and toxval_units='ppm'
                         and species_id = ",sid," and source = '",source,"'",query_addition)
          runQuery(query, toxval.db)
        } else {
          # Record ppm to mg/kg-day conversion info
          changed_toxval_id = runQuery(paste0("SELECT DISTINCT toxval_id FROM toxval ",
                                            "where exposure_route='oral' and toxval_units='ppm' ",
                                            "and species_id = ",sid," and source = '",source,"'",query_addition),
                                     toxval.db) %>%
            dplyr::mutate(change_made = paste0("ppm to mg/kg-day by species: ", species, " species_id: ", sid)) %>%
            dplyr::bind_rows(changed_toxval_id, .)
        }
      }
    }
  }

  if(report.only) {
    # Get string containing toxval_id values for all altered entries
    toxval_id_str = changed_toxval_id %>%
      dplyr::distinct() %>%
      dplyr::pull(toxval_id) %>%
      paste0("'", ., "'") %>%
      paste(collapse=", ")

    # Get report data for altered entries using report.extra parameter
    if(report.extra) {
      query = paste0("SELECT a.toxval_id, a.source, a.toxval_numeric_original, a.toxval_units_original, a.toxval_numeric, a.toxval_units, ",
                     "a.toxval_type, a.mw, a.species_id, b.common_name, a.exposure_route, a.human_eco ",
                     "FROM toxval a ",
                     "LEFT JOIN species b ON b.species_id = a.species_id ",
                     "WHERE a.toxval_id IN (", toxval_id_str, ")")

      # Define table here to handle case of no unit changes
      unit_conversions_table = tibble::tibble(
        source = character(),
        toxval_numeric_original = numeric(),
        toxval_units_original = character(),
        toxval_numeric = numeric(),
        toxval_units = character(),
        toxval_type = character(),
        mw = numeric(),
        species_id = numeric(),
        exposure_route = character(),
        human_eco = character()
      )
    } else {
      query = paste0("SELECT toxval_id, source, toxval_numeric_original, toxval_units_original, toxval_numeric, toxval_units ",
                     "FROM toxval WHERE toxval_id IN (", toxval_id_str, ")")

      # Define table here to handle case of no unit changes
      unit_conversions_table = tibble::tibble(
        source = character(),
        toxval_numeric_original = numeric(),
        toxval_units_original = character(),
        toxval_numeric = numeric(),
        toxval_units = character()
      )
    }

    # Finalize unit_conversions_table
    query_results = runQuery(query, toxval.db)
    unit_conversions_table = unit_conversions_table %>%
      # Bind rows to base table to avoid errors when no units were changed
      dplyr::bind_rows(query_results) %>%
      # Add conversion_factor
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
      dplyr::distinct() %>%
      dplyr::left_join(changed_toxval_id,
                       by="toxval_id")

    writexl::write_xlsx(unit_conversions_table, paste0(toxval.config()$datapath,
                                                       "dictionary/unit_conversion_check_",
                                                       Sys.Date(),
                                                       ".xlsx"))
    return(unit_conversions_table)
  }
}
