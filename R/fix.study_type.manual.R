#-------------------------------------------------------------------------------------
#' Fix the study_type using manual curation
#' @param toxval.db The version of toxval in which the data is altered.
#' @param source The source to be fixed
#' @param subsource The subsource to be fixed
#' @param dict.date The dated version of the dictionary to use
#' @param report.only Whether to report or write/export data. Default is FALSE (write/export data)
#' @return The database will be altered
#' @export
#--------------------------------------------------------------------------------------
fix.study_type.manual = function(toxval.db, source=NULL, subsource=NULL, dict.date="2023-08-21", report.only=FALSE){
  printCurrentFunction(toxval.db)

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  file = paste0(toxval.config()$datapath,"dictionary/study_type/toxval_new_study_type ",toxval.db," ",dict.date,".xlsx")
  print(file)
  mat = readxl::read_xlsx(file)
  mat = mat[mat$dtxsid!='NODTXSID',]
  mat = mat[!is.na(mat$dtxsid),]
  mat = fix.trim_spaces(mat)

  if(!is.null(source)){
    slist = source
  } else {
    slist = sort(unique(mat$source))
  }

  # Store aggregate missing entries
  missing.all = data.frame()

  for(source in slist) {
    temp0 = mat %>%
      dplyr::select(dtxsid, source_name=source, study_type_corrected, source_hash) %>%
      dplyr::filter(source_name == source) %>%
      distinct()

    cat(source,nrow(temp0),"\n")
    temp0$key = paste(temp0$dtxsid,temp0$source_name,temp0$study_type_corrected,temp0$source_hash)
    temp = unique(temp0[,c("source_hash","study_type_corrected")])
    names(temp) = c("source_hash","study_type")

    temp.old = runQuery(paste0("select source_hash, study_type from toxval where dtxsid != 'NODTXSID' and source = '",source,
                            "'",query_addition," and toxval_type in (select toxval_type from toxval_type_dictionary ",
                            "where toxval_type_supercategory in ('Point of Departure', 'Lethality Effect Level', 'Toxicity Value')) ",
                            "and human_eco = 'human health' ",
                            # Filter out those that already have a study_type present
                            "and study_type = '-'"),toxval.db)

    shlist = unique(temp0$source_hash)
    shlist.db = unique(temp.old$source_hash)
    missing = shlist.db[!is.element(shlist.db,shlist)]
    if(length(missing)>0) {
      cat("Missing source_hash in replacement file:",source," missing ",length(missing)," out of ",length(shlist.db),"\n")

      query = paste0("SELECT ",
                    "a.dtxsid,a.name, ",
                    "b.source, ",
                    "b.risk_assessment_class, ",
                    "b.toxval_type, ",
                    "b.toxval_subtype, ",
                    "b.toxval_units, ",
                    "b.study_type_original, ",
                    "b.study_type, ",
                    "b.study_type as study_type_corrected, ",
                    "b.study_duration_value, ",
                    "b.study_duration_units, ",
                    "d.common_name, ",
                    "b.generation, ",
                    "b.exposure_route, ",
                    "b.exposure_method, ",
                    "b.critical_effect, ",
                    "f.long_ref, ",
                    "f.title, ",
                    "b.source_hash ",
                    "FROM ",
                    "toxval b ",
                    "INNER JOIN source_chemical a on a.chemical_id=b.chemical_id ",
                    "LEFT JOIN species d on b.species_id=d.species_id ",
                    "INNER JOIN record_source f on b.toxval_id=f.toxval_id ",
                    "INNER JOIN toxval_type_dictionary e on b.toxval_type=e.toxval_type ",
                    "WHERE ",
                    "b.source='",source,"' ",
                    "and b.human_eco='human health' ",
                    "and e.toxval_type_supercategory in ('Point of Departure','Lethality Effect Level')")
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
            file = paste0(toxval.config()$datapath,"dictionary/study_type/missing_study_type ",source," ",subsource," ",dict.date,".csv") %>%
            stringr::str_squish()
            write.csv(replacements,file,row.names=F)
          }
          missing.all = rbind(missing.all, replacements)
          #browser()
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
      # for(i in 1:nrow(temp3)) {
      #   hk = temp3[i,"source_hash"]
      #   st = temp3[i,"study_type"]
      #   query = paste0("update toxval set study_type='",st,"' where source_hash='",hk,"'")
      #   #print(query)
      #   runQuery(query,toxval.db)
      #   if(i%%100==0) cat("finished",i,"out of",nrow(temp3),"\n")
      # }

      # Prepare for batched updates
      # Batch update
      # https://www.mssqltips.com/sqlservertip/5829/update-statement-performance-in-sql-server/
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
                  updated_df = temp3 %>% select(source_hash, study_type),
                  db=toxval.db)

        startPosition <- startPosition + batch_size
        incrementPosition <- startPosition + batch_size - 1
      }
    }

    # check = runQuery(paste0("select dtxsid,source,study_type,source_hash from toxval where dtxsid!='NODTXSID' and source='",source,
    #                         "' and toxval_type in (select toxval_type from toxval_type_dictionary where toxval_type_supercategory in ('Point of Departure','Lethality Effect Level','Toxicity Value'))
    #                         and human_eco='human health'"),toxval.db)
    # check = unique(check)
    # check$key = paste(check[,1],check[,2],check[,3],check[,4])
    # check = check[order(check$source_hash),]
    # temp0 = temp0[order(temp0$source_hash),]
    # missing.new = check[!is.element(check$key,temp0$key),]
    # missing.old = temp0[!is.element(temp0$key,check$key),]
    # n1 = nrow(missing.old)
    # n2 = nrow(missing.new)
    # cat("  check: ",source,n1,n2,"\n")
    # #if(n1>0 || n2>0) {
    # if(n2>0) {
    #     # temp0 is read from the input (new study type spreadsheet)
    #   # check is from the current version of the database
    #   cat("\n\nStopping here means that the study_type fix process did not work for source:",source,"\n")
    #   browser()
    # }
  }
  if(report.only) {
    return(missing.all)
  }
}
