library(RMySQL)
library(DBI)
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
    query_addition = paste0(query_addition, " and a.subsource='", subsource, "'")
  }

  if(!is.null(custom.query.filter)){
    query_addition = paste0(query_addition, " ", custom.query.filter)
  }

  slist = runQuery("select distinct source from toxval", toxval.db) %>%
    dplyr::pull(source)
  if(!is.null(source)) slist = source
  source_string = slist %>%
    paste0(collapse="', '")
  #----------------------------------------------------------------------------
  # Run the export process
  #----------------------------------------------------------------------------
  if(mode=="export") {
    query = paste0("SELECT a.*, b.toxval_type_supercategory ",
                   "FROM toxval a LEFT JOIN toxval_type_dictionary b ON a.toxval_type=b.toxval_type ",
                   "WHERE ",
                   "(a.study_type IS NULL OR a.study_type IN ('-', '')) ",
                   "AND a.source IN ('", source_string, "') ",
                   "AND a.qc_status not like '%fail%' ",
                   query_addition)

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

    # Write output by source
    for(source in missing_data %>% dplyr::pull(source)) {
      curr_missing = missing_data %>%
        dplyr::filter(source == !!source)
      out_file = paste0("Repo/dictionary/study_type_by_source/toxval_new_study_type ", source, " ", subsource) %>%
        stringr::str_squish() %>%
        paste0(".xlsx")
      writexl::write_xlsx(curr_missing, out_file)
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
                     query_addition)
      runQuery(query, toxval.db)

      # Push study_type updates from manual dictionaries using source_hash
      dir = paste0(toxval.config()$datapath,"dictionary/study_type_by_source/")
      for(source in slist) {
        file_list <- list.files(paste0(dir),
                                pattern = paste0(source, " ", subsource) %>%
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
          mat = lapply(file_list, readxl::read_xlsx) %>%
            dplyr::bind_rows() %>%
            dplyr::filter(!dtxsid %in% c(NA, "NODTXSID", "-")) %>%
            dplyr::mutate(dplyr::across(tidyselect::where(is.character), ~stringr::str_squish(.)))
        } else {
          # Create empty dataframe
          mat = data.frame(matrix(ncol=4,nrow=0,
                                  dimnames=list(NULL, c("dtxsid", "source", "study_type_corrected", "source_hash"))))
        }

        temp0 = mat %>%
          dplyr::select(dtxsid, source_name=source, study_type_corrected, source_hash) %>%
          dplyr::filter(source_name == source) %>%
          dplyr::distinct()

        if(any(duplicated(temp0$source_hash))){
          cat("Unresolved duplicate source_hash mappings...")
          browser()
          stop()
        }

        cat(source,nrow(temp0),"\n")
        temp0$key = paste(temp0$dtxsid,temp0$source_name,temp0$study_type_corrected,temp0$source_hash)
        temp = unique(temp0[,c("source_hash","study_type_corrected")])
        names(temp) = c("source_hash","study_type")

        temp.old = runQuery(paste0("SELECT b.source_hash, b.study_type from toxval b ",
                                   # "INNER JOIN toxval_type_dictionary e on b.toxval_type=e.toxval_type ",
                                   "where b.dtxsid != 'NODTXSID' and b.source = '", source, "'",
                                   " and b.qc_status NOT LIKE '%fail%' and b.human_eco = 'human health'",
                                   query_addition %>%
                                     gsub("subsource", "b.subsource", .)), toxval.db)

        temp$code = paste(temp$source_hash,temp$study_type)
        temp.old$code = paste(temp.old$source_hash,temp.old$study_type)
        n1 = nrow(temp)
        n2 = nrow(temp.old)
        temp3 = temp[!is.element(temp$code,temp.old$code),]
        n3 = nrow(temp3)
        cat("==============================================\n")
        cat(source,subsource,n1,n2,n3," [n1 is new records, n2 is old records, n3 is number of records to be updated]\n")
        cat("==============================================\n")
        batch_size <- 500
        startPosition <- 1
        endPosition <- nrow(temp3)
        incrementPosition <- batch_size

        while(startPosition <= endPosition){
          if(incrementPosition > endPosition) incrementPosition = endPosition
          message("...Inserting new data in batch: ", batch_size, " startPosition: ", startPosition," : incrementPosition: ", incrementPosition,
                  " (",round((incrementPosition/endPosition)*100, 3), "%)", " at: ", Sys.time())

          updateQuery = paste0("UPDATE toxval a INNER JOIN z_updated_df b ",
                               "ON (a.source_hash = b.source_hash) SET a.study_type = b.study_type ",
                               "WHERE a.source_hash in ('",
                               paste0(temp3$source_hash[startPosition:incrementPosition], collapse="', '"), "') ",
                               "AND a.qc_status NOT LIKE '%fail%' and a.human_eco = 'human health'")

          runUpdate(table="toxval",
                    updateQuery = updateQuery,
                    updated_df = temp3 %>% dplyr::select(source_hash, study_type),
                    db=toxval.db)

          startPosition <- startPosition + batch_size
          incrementPosition <- startPosition + batch_size - 1
        }
      }

      # Get entries that are still missing study_type
      query = paste0("SELECT a.*, b.toxval_type_supercategory ",
                     "FROM toxval a LEFT JOIN toxval_type_dictionary b ON a.toxval_type=b.toxval_type ",
                     "WHERE ",
                     "(a.study_type IS NULL OR a.study_type IN ('-', '')) ",
                     "AND a.source IN ('", source_string, "') ",
                     "AND a.qc_status not like '%fail%' ",
                     query_addition)

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

      # Write output by source
      for(source in missing_data %>% dplyr::pull(source)) {
        curr_missing = missing_data %>%
          dplyr::filter(source == !!source)
        out_file = paste0("Repo/dictionary/study_type_by_source/toxval_new_study_type ", source, " ", subsource) %>%
          stringr::str_squish() %>%
          paste0(".xlsx")
        writexl::write_xlsx(curr_missing, out_file)
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
                     query_addition)
      changed_data = runQuery(query, toxval.db) %>%
        dplyr::mutate(study_type = toxval_type_supercategory) %>%
        return()
    }
  }
}
