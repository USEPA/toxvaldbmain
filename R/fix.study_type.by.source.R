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
    query_addition = paste0(query_addition, " and b.subsource='", subsource, "'")
  }
  if(!is.null(custom.query.filter)){
    query_addition = paste0(query_addition, " ", custom.query.filter)
  }

  dir = paste0(toxval.config()$datapath,"dictionary/study_type_by_source/")
  # dir = "data/study_type_by_source/"
  slist = runQuery("select distinct source from toxval",toxval.db)[1,]
  if(!is.null(source)) slist = source
  #----------------------------------------------------------------------------
  # Run the export process
  #----------------------------------------------------------------------------
  if(mode=="export") {
    for(source in slist) {
      query = paste0("SELECT a.dtxsid, a.casrn, a.name, ",
                    "b.source, b.risk_assessment_class, b.toxval_type, b.toxval_subtype, ",
                    "b.toxval_units, b.study_type_original, b.study_type, ",
                    "b.study_type as study_type_corrected, b.study_duration_value, ",
                    "b.study_duration_units, ",
                    "d.common_name, ",
                    "b.generation, b.lifestage, b.exposure_route, b.exposure_method, ",
                    "b.critical_effect, ",
                    "f.long_ref, f.title, ",
                    "b.source_hash ",
                    "FROM toxval b ",
                    "INNER JOIN source_chemical a on a.chemical_id=b.chemical_id ",
                    "LEFT JOIN species d on b.species_id=d.species_id ",
                    "INNER JOIN record_source f on b.toxval_id=f.toxval_id ",
                    "INNER JOIN toxval_type_dictionary e on b.toxval_type=e.toxval_type ",
                    "WHERE b.source='", source, "'",
                    query_addition)

      mat = runQuery(query,toxval.db,T,F)
      mat = unique(mat)
      mat$fixed = 0
      dir1 = paste0(dir,"export_temp/")
      file = paste0(dir1,"/toxval_new_study_type ", source, " ", subsource) %>%
        stringr::str_squish() %>%
        paste0(".xlsx")
      sty = createStyle(halign="center",valign="center",textRotation=90,textDecoration = "bold")
      write.xlsx(mat,file,firstRow=T,headerStyle=sty)
      file = paste0(dir1,"/toxval_new_study_type ",source, " ", subsource) %>%
        stringr::str_squish() %>%
        paste0(".csv")
      write.csv(mat,file=file,row.names=F)
    }
  }

  #----------------------------------------------------------------------------
  # Run the import process
  #----------------------------------------------------------------------------
  if(mode=="import") {

    # Store aggregate missing entries
    missing.all = data.frame()

    for(source in slist) {
      file = paste0(dir,"toxval_new_study_type ",source, " ", subsource) %>%
        stringr::str_squish() %>%
        paste0(".xlsx")
      print(file)
      if(file.exists(file)){
        mat = readxl::read_xlsx(file)
        mat = mat[mat$dtxsid!='NODTXSID',]
        mat = mat[!is.na(mat$dtxsid),]
        #mat = fix.trim_spaces(mat)
        mat <- mat %>%
          dplyr::mutate(dplyr::across(where(is.character), ~stringr::str_squish(.)))
      } else {
        # Create empty dataframe
        mat = data.frame(matrix(ncol=4,nrow=0,
                                dimnames=list(NULL, c("dtxsid", "source", "study_type_corrected", "source_hash"))))
      }

      temp0 = mat %>%
        dplyr::select(dtxsid, source_name=source, study_type_corrected, source_hash) %>%
        dplyr::filter(source_name == source) %>%
        dplyr::distinct()

      cat(source,nrow(temp0),"\n")
      temp0$key = paste(temp0$dtxsid,temp0$source_name,temp0$study_type_corrected,temp0$source_hash)
      temp = unique(temp0[,c("source_hash","study_type_corrected")])
      names(temp) = c("source_hash","study_type")

      temp.old = runQuery(paste0("SELECT b.source_hash, b.study_type from toxval b ",
                                        "INNER JOIN toxval_type_dictionary e on b.toxval_type=e.toxval_type ",
                                        "where b.dtxsid != 'NODTXSID' and b.source = '", source, "'",
                                        query_addition), toxval.db)

      shlist = unique(temp0$source_hash)
      shlist.db = unique(temp.old$source_hash)
      missing = shlist.db[!is.element(shlist.db,shlist)]
      if(length(missing)>0) {
        cat("Missing source_hash in replacement file:",source," missing ",length(missing)," out of ",length(shlist.db),"\n")

        query = paste0("SELECT a.dtxsid, a.casrn, a.name, ",
                       "b.source, b.risk_assessment_class, b.toxval_type, b.toxval_subtype, ",
                       "b.toxval_units, b.study_type_original, b.study_type, ",
                       "b.study_type as study_type_corrected, b.study_duration_value, ",
                       "b.study_duration_units, ",
                       "d.common_name, ",
                       "b.generation, b.lifestage, b.exposure_route, b.exposure_method, ",
                       "b.critical_effect, ",
                       "f.long_ref, f.title, ",
                       "b.source_hash ",
                       "FROM toxval b ",
                       "INNER JOIN source_chemical a on a.chemical_id=b.chemical_id ",
                       "LEFT JOIN species d on b.species_id=d.species_id ",
                       "INNER JOIN record_source f on b.toxval_id=f.toxval_id ",
                       "INNER JOIN toxval_type_dictionary e on b.toxval_type=e.toxval_type ",
                       "WHERE b.source='", source, "'",
                       query_addition)

        if(!is.null(subsource)) {
          query = paste0(query, " and b.subsource='",subsource,"'")
        }

        replacements = runQuery(query,toxval.db,T,F)
        # Check if any returned from query
        if(nrow(replacements)){
          replacements$fixed = 0
          # Filter to entries from missing source_hash vector
          replacements = replacements[is.element(replacements$source_hash,missing),]
          # Check if any missing
          if(nrow(replacements)){
            if(!report.only) {
              file = paste0(toxval.config()$datapath,"dictionary/study_type/missing_study_type ", source," ", subsource) %>%
                stringr::str_squish() %>%
                paste0(".csv")
              write.csv(replacements,file,row.names=F)
            }
            missing.all = rbind(missing.all, replacements)
          }
        }
      }

      if(!report.only) {
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
        endPosition <- nrow(temp3)# runQuery(paste0("SELECT max(id) from documents"), db=db) %>% .[[1]]
        incrementPosition <- batch_size

        while(startPosition <= endPosition){
          message("...Inserting new data in batch: ", batch_size, " startPosition: ", startPosition," : incrementPosition: ", incrementPosition, " at: ", Sys.time())

          updateQuery = paste0("UPDATE toxval a INNER JOIN z_updated_df b ",
                               "ON (a.source_hash = b.source_hash) SET a.study_type = b.study_type",
                               " WHERE a.source_hash in ('",
                               paste0(temp3$source_hash[startPosition:incrementPosition], collapse="', '"), "')")

          runUpdate(table="toxval",
                    updateQuery = updateQuery,
                    updated_df = temp3 %>% dplyr::select(source_hash, study_type),
                    db=toxval.db)

          startPosition <- startPosition + batch_size
          incrementPosition <- startPosition + batch_size - 1
        }
      }
    }

    if(report.only) {
      return(missing.all)
    }
  }
}
