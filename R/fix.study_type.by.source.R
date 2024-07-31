#-------------------------------------------------------------------------------------
#' Fix the study_type using manual curation on a source-by-source basis
#'
#' This function replaces the original export.for_study_type and fix.study_type.manual,
#' with the intention of making it easier to fix the study types on a source-by-source basis
#' All of the work will happen in the directory ~/Repo/dictionary/study_type_by_source.
#' Each source will have its own file and will not have a date attached to make maintenance easier.
#' To start the process, run this with mode="export". This will write a source-specific file
#' to the export_temp directory. Open either the xlsx or csv (if the xslx is corrupted)
#' and place this file into the main directory (study_type_by_source) and edit it there as
#' documented in the main documentation. Next run this function with mode="import".
#' This will load your changes into the database. It is suggested that before working on a new
#' source that the old version in the study_type_by_source get pushed to the old_versions directory
#'
#' @param toxval.db The version of toxval in which the data is altered.
#' @param mode Either export or import
#' @param source The source you want to work on. If NULL, this will run all sources
#' @param subsource The subsource to be fixed
#' @param custom.query.filter Additional filters for the query. Example: custom.query.filter = paste0(" and b.human_eco='human health' and ",
#' "e.toxval_type_supercategory in ('Point of Departure','Lethality Effect Level','Toxicity Value')")
#' @param report.only Whether to report or write/export data. Default is FALSE (write/export data)
#' @return The database will be altered
#' @export
#--------------------------------------------------------------------------------------
fix.study_type.by.source = function(toxval.db, mode="export", source=NULL, subsource=NULL,
                                    custom.query.filter=NULL, report.only=FALSE){
  printCurrentFunction(toxval.db)

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(query_addition, " and subsource='", subsource, "'")
  }

  if(!is.null(custom.query.filter)){
    query_addition = paste0(query_addition, " ", custom.query.filter)
  }

  # Export directory
  dir = paste0(toxval.config()$datapath,"dictionary/study_type_by_source/")

  # Get list of sources
  slist = runQuery("select distinct source from toxval", toxval.db) %>%
    dplyr::pull(source)
  if(!is.null(source)) slist = source
  source_string = slist %>%
    paste0(collapse="', '")
  #----------------------------------------------------------------------------
  # Run the export process
  #----------------------------------------------------------------------------
  if(mode=="export") {

    cat("Checking old '", source,"'logged study_type already imported...\n")
    import_logged <- list.files(paste0(dir),
                                pattern = source %>%
                                  # Escape parentheses for regex
                                  gsub("\\(", "\\\\(", .) %>%
                                  gsub("\\)", "\\\\)", .),
                                recursive = TRUE,
                                full.names = TRUE) %>%
      # Ignore files in specific subfolders
      .[!grepl("export_temp|old files", .)] %>%
      lapply(., function(f){
        readxl::read_xlsx(f) %>%
          dplyr::mutate(dplyr::across(c("study_duration_value"), ~as.character(.)))
      }) %>%
      dplyr::bind_rows()

    if(nrow(import_logged)){
      import_logged = import_logged %>%
        dplyr::pull(source_hash) %>%
        unique() %>%
        paste0(collapse="', '")
    }

    query = paste0("SELECT a.dtxsid, a.casrn, a.name, ",
                   "b.source, b.subsource, b.risk_assessment_class, b.toxval_type, b.toxval_subtype, ",
                   "b.toxval_units, b.study_type_original, b.study_type, ",
                   "b.study_type as study_type_corrected, b.study_duration_value, ",
                   "b.study_duration_units, ",
                   "d.common_name, ",
                   "b.generation, b.lifestage, b.exposure_route, b.exposure_method, ",
                   "b.critical_effect, ",
                   "f.long_ref, f.title, ",
                   "b.source_hash, ",
                   "g.toxval_type_supercategory ",
                   "FROM toxval b ",
                   "INNER JOIN source_chemical a on a.chemical_id=b.chemical_id ",
                   "LEFT JOIN species d on b.species_id=d.species_id ",
                   "INNER JOIN record_source f on b.toxval_id=f.toxval_id ",
                   "LEFT JOIN toxval_type_dictionary g ON b.toxval_type=g.toxval_type ",
                   "WHERE b.source IN ('", source_string, "') ",
                   query_addition %>%
                     gsub("subsource", "b.subsource", .),
                   "and b.source_hash NOT IN ('", import_logged, "') ",
                   "and b.qc_status NOT LIKE '%fail%' ",
                   "and (g.toxval_type_supercategory is NULL OR g.toxval_type_supercategory in ('Dose Response Summary Value', 'Mortality Response Summary Value')) ",
                   "and f.record_source_level NOT in ('extraction', 'origin')")

    missing_data = runQuery(query, toxval.db) %>%
      dplyr::distinct() %>%
      # Tag reason why entry is missing study_type
      dplyr::mutate(
        missing_toxval_type_dict_entry = dplyr::case_when(
          # Specific supercategory to manually add study_type
          toxval_type_supercategory %in% c('Dose Response Summary Value', 'Mortality Response Summary Value') ~ 0,
          # Ones that did not map to anything in toxval_type_dictionary
          is.na(toxval_type_supercategory) ~ 1,
          # Anything else, ignore
          TRUE ~ NA
        )
      ) %>%
      dplyr::filter(!is.na(missing_toxval_type_dict_entry))

    if(!nrow(missing_data)){
      message("No study_type missing for ", source)
      return()
    }

    # Write output by source
    for(source in missing_data %>% dplyr::pull(source) %>% unique()) {
      curr_missing = missing_data %>%
        dplyr::filter(source == !!source)
      out_file = paste0("Repo/dictionary/study_type_by_source/export_temp/toxval_new_study_type ", source, " ", subsource) %>%
        stringr::str_squish() %>%
        paste0(".xlsx")
      if(nrow(curr_missing)){
        writexl::write_xlsx(curr_missing, out_file)
      } else {
        message("No study_type missing for ", source)
      }
    }
  }

  #----------------------------------------------------------------------------
  # Run the import process
  #----------------------------------------------------------------------------
  if(mode=="import") {
    # Track changed entries for report.only
    changed_data = data.frame()

    if(!report.only) {
      # Set study_type to toxval_type_supercategory for all but Response Summary supercategories
      query = paste0("UPDATE toxval a LEFT JOIN toxval_type_dictionary b ON a.toxval_type=b.toxval_type ",
                     "SET a.study_type=b.toxval_type_supercategory ",
                     "WHERE b.toxval_type_supercategory NOT IN ",
                     "('Dose Response Summary Value', 'Mortality Response Summary Value') ",
                     "AND a.source IN ('", source_string, "') ",
                     "AND b.toxval_type_supercategory != a.study_type ",
                     "AND a.qc_status not like '%fail%' ",
                     query_addition %>%
                       gsub("subsource", "a.subsource", .))
      runQuery(query, toxval.db)

      # Push study_type updates from manual dictionaries using source_hash
      dir = paste0(toxval.config()$datapath,"dictionary/study_type_by_source/")
      for(source in slist) {
        file_list <- list.files(paste0(dir),
                                pattern = source %>%
                                  stringr::str_squish() %>%
                                  # Escape parentheses for regex
                                  gsub("\\(", "\\\\(", .) %>%
                                  gsub("\\)", "\\\\)", .),
                                recursive = TRUE,
                                full.names = TRUE) %>%
          # Ignore files in specific subfolders
          .[!grepl("export_temp|old files", .)]

        if(length(file_list)){
          cat("Pulling study_type maps for import...\n")
          mat = lapply(file_list, function(f){
            readxl::read_xlsx(f) %>%
              dplyr::mutate(dplyr::across(c("study_duration_value"), ~as.character(.)))
          }) %>%
            dplyr::bind_rows() %>%
            dplyr::filter(!dtxsid %in% c(NA, "NODTXSID", "-")) %>%
            dplyr::mutate(dplyr::across(tidyselect::where(is.character), ~stringr::str_squish(.)))
        } else {
          # Create empty dataframe
          mat = data.frame(matrix(ncol=4,nrow=0,
                                  dimnames=list(NULL, c("dtxsid", "source", "study_type_corrected", "source_hash"))))
        }

        temp0 = mat %>%
          dplyr::filter(source == !!source) %>%
          dplyr::select(study_type=study_type_corrected, source_hash, subsource) %>%
          dplyr::distinct()

        if(!is.null(subsource)){
          temp0 = temp0 %>%
            dplyr::filter(subsource == !!subsource)
        }

        if(any(duplicated(temp0$source_hash))){
          cat("Unresolved duplicate source_hash mappings...")
          browser()
          stop()
        }

        batch_size <- 500
        startPosition <- 1
        endPosition <- nrow(temp0)
        incrementPosition <- batch_size

        while(startPosition <= endPosition){
          if(incrementPosition > endPosition) incrementPosition = endPosition
          message("...Inserting new data in batch: ", batch_size, " startPosition: ", startPosition," : incrementPosition: ", incrementPosition,
                  " (",round((incrementPosition/endPosition)*100, 3), "%)", " at: ", Sys.time())

          updateQuery = paste0("UPDATE toxval a INNER JOIN z_updated_df b ",
                               "ON (a.source_hash = b.source_hash) SET a.study_type = b.study_type ",
                               "WHERE a.source_hash in ('",
                               paste0(temp0$source_hash[startPosition:incrementPosition], collapse="', '"), "') ",
                               "AND a.qc_status NOT LIKE '%fail%' and a.human_eco = 'human health'")

          runUpdate(table="toxval",
                    updateQuery = updateQuery,
                    updated_df = temp0 %>% dplyr::select(source_hash, study_type),
                    db=toxval.db)

          startPosition <- startPosition + batch_size
          incrementPosition <- startPosition + batch_size - 1
        }
      }

      # Query/Export missing study_type
      query = paste0("SELECT a.dtxsid, a.casrn, a.name, ",
                     "b.source, b.subsource, b.risk_assessment_class, b.toxval_type, b.toxval_subtype, ",
                     "b.toxval_units, b.study_type_original, b.study_type, ",
                     "b.study_type as study_type_corrected, b.study_duration_value, ",
                     "b.study_duration_units, ",
                     "d.common_name, ",
                     "b.generation, b.lifestage, b.exposure_route, b.exposure_method, ",
                     "b.critical_effect, ",
                     "f.long_ref, f.title, ",
                     "b.source_hash, ",
                     "g.toxval_type_supercategory ",
                     "FROM toxval b ",
                     "INNER JOIN source_chemical a on a.chemical_id=b.chemical_id ",
                     "LEFT JOIN species d on b.species_id=d.species_id ",
                     "INNER JOIN record_source f on b.toxval_id=f.toxval_id ",
                     "LEFT JOIN toxval_type_dictionary g ON b.toxval_type=g.toxval_type ",
                     "WHERE b.source IN ('", source, "') ",
                     query_addition %>%
                       gsub("subsource", "b.subsource", .),
                     "and b.qc_status NOT LIKE '%fail%' ",
                     "and (g.toxval_type_supercategory is NULL OR g.toxval_type_supercategory in ('Dose Response Summary Value', 'Mortality Response Summary Value')) ",
                     "and f.record_source_level NOT in ('extraction', 'origin') ",
                     "and b.source_hash not in ('", paste0(temp0$source_hash, collapse = "', '"),"')")

      missing_data = runQuery(query, toxval.db) %>%
        dplyr::distinct() %>%
        # Tag reason why entry is missing study_type
        dplyr::mutate(
          missing_toxval_type_dict_entry = dplyr::case_when(
            # Specific supercategory to manually add study_type
            toxval_type_supercategory %in% c('Dose Response Summary Value', 'Mortality Response Summary Value') ~ 0,
            # Ones that did not map to anything in toxval_type_dictionary
            is.na(toxval_type_supercategory) ~ 1,
            # Anything else, ignore
            TRUE ~ NA
          )
        ) %>%
        dplyr::filter(!is.na(missing_toxval_type_dict_entry))

      if(!nrow(missing_data)){
        message("No study_type missing for ", source)
        return()
      }

      # Write output by source
      for(source in missing_data %>% dplyr::pull(source) %>% unique()) {
        curr_missing = missing_data %>%
          dplyr::filter(source == !!source)
        out_file = paste0("Repo/dictionary/study_type_by_source/export_temp/toxval_new_study_type ", source, " ", subsource) %>%
          stringr::str_squish() %>%
          paste0(".xlsx")
        if(nrow(curr_missing)){
          writexl::write_xlsx(curr_missing, out_file)
        } else {
          message("No study_type missing for ", source)
        }
      }
    } else {
      # If report.only, track study_type=toxval_type_supercategory change
      query = paste0("SELECT a.*, b.toxval_type_supercategory ",
                     "FROM toxval a LEFT JOIN toxval_type_dictionary b ON a.toxval_type=b.toxval_type ",
                     "WHERE b.toxval_type_supercategory NOT IN ",
                     "('Dose Response Summary Value', 'Mortality Response Summary Value') ",
                     "AND a.source IN ('", source_string, "') ",
                     "AND b.toxval_type_supercategory != a.study_type ",
                     "AND a.qc_status not like '%fail%' ",
                     query_addition %>%
                       gsub("subsource", "a.subsource", .))
      changed_data = runQuery(query, toxval.db) %>%
        dplyr::mutate(study_type = toxval_type_supercategory) %>%
        return()
    }
  }
}
