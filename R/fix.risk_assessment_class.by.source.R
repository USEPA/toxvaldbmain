#-------------------------------------------------------------------------------------
#' Set the risk assessment class of toxval according to an excel dictionary.
#' Values may beset multiple times, so the excel sheet should be ordered so that
#' the last ones to be set are last
#'
#' @param toxval.db The version of toxval in which the data is altered.
#' @param source The source to be updated
#' @param subsource The subsource to be updated (NULL default)
#' @param restart If TRUE, delete all values and start from scratch
#' @param report.only Whether to report or write/export data. Default is FALSE (write/export data)
#' @export
#--------------------------------------------------------------------------------------
fix.risk_assessment_class.by.source <- function(toxval.db, source=NULL, subsource=NULL, restart=TRUE, report.only=FALSE) {
  printCurrentFunction(paste(toxval.db,":", source, subsource))
  file = paste0(toxval.config()$datapath,"dictionary/RAC_rules_by_source v92.xlsx")
  print(file)
  # conv = openxlsx::read.xlsx(file)
  # print(dim(conv))
  # conv = conv[conv$order>0,]
  # conv = conv[order(conv$term),]
  # conv = conv[order(conv$risk_assessment_class),]
  # conv = conv[order(conv$order),]
  # conv = conv[conv$useme==1,]
  # conv = conv[!is.na(conv$source),]

  conv = readxl::read_xlsx(file) %>%
    dplyr::filter(order > 0, useme == 1, !is.na(source)) %>%
    dplyr::arrange(term, risk_assessment_class, order) %>%
    dplyr::distinct()

  # Handle addition of subsource for queries
  query_addition = " and qc_status NOT LIKE '%fail%'"
  if(!is.null(subsource)) {
    query_addition = paste0(query_addition, " and subsource='", subsource, "'")
  }
  # Store all missing RAC entries
  missing.all = data.frame()
  # Store all set RAC entries
  set.all = data.frame()

  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  slist = sort(slist)
  if(!is.null(source)) slist = source
  for(source in slist) {
    cat("----------------------------------------\n")
    cat(source,subsource,"\n")
    cat("----------------------------------------\n")

    if(restart & !report.only) {
      query = paste0("update toxval set risk_assessment_class = '-'  where source = '",source,"'")
      runQuery(query, toxval.db)
    }
    dict = conv[conv$source==source,]
    if(!nrow(dict) & !report.only){
      cat("\n\n>>> ",source,"\nStopping here means that new values need to be added to the risk_assessment_class dictionary:\n",file,
          "\nFind the unique values of study_type and enter them into the file.\nThen rerun the load.\n\n")
      browser()
    }
    n1.0 = runQuery(paste0("select count(*) from toxval where risk_assessment_class='-' and source = '",source,"'",query_addition) ,toxval.db)[1,1]
    if(n1.0 == 0 & !report.only){
      # Skip if not report.only and 0 of value "-"
      cat("...RAC set for all records for source ", source, "\n")
      next
    }
    cat("Initial rows:",n1.0,"\n")
    # Use dictionary to set appropriate RAC for each entry
    for(field in unique(dict$field)){
      cat("Setting RAC for field :", field,"\n")
      # Prepare dictionary for mapping by selecting the field terms
      f_dict = dict %>%
        dplyr::filter(field == !!field) %>%
        dplyr::select(term, risk_assessment_class) %T>% {
          names(.) <- c(field, "risk_assessment_class")
        }

      query = paste0("SELECT toxval_id, ", field,", source FROM toxval ",
                     "WHERE ", field, " in ('",
                     paste0(unique(dict$term[dict$field == field]), collapse = "', '"),
                     "') and source = '",source,"'")

      if(!report.only){
        query = paste0(query,
                       # Only update if RAC hasn't already been updated
                       " and risk_assessment_class = '-'")
      }

      # Query mapped field term entries
      rac_data = runQuery(query,
                          toxval.db) %>%
        # Join to dictionary to get RAC
        dplyr::left_join(f_dict,
                         by=field) %>%
        dplyr::filter(!is.na(risk_assessment_class))

      set.all = set.all %>%
        dplyr::bind_rows(rac_data %>%
                           dplyr::filter(!toxval_id %in% set.all$toxval_id))

      if (!report.only) {
        ##############################################################################
        ### Batch Update
        ##############################################################################
        batch_size <- 50000
        startPosition <- 1
        endPosition <- nrow(rac_data)
        incrementPosition <- batch_size
        if(incrementPosition > endPosition) incrementPosition = endPosition

        while(startPosition <= endPosition){
          message("...Inserting new data in batch: ", batch_size, " startPosition: ", startPosition," : incrementPosition: ", incrementPosition,
                  " (",round((incrementPosition/endPosition)*100, 3), "%)", " at: ", Sys.time())

          update_query <- paste0("UPDATE toxval a ",
                                 "INNER JOIN z_updated_df b ",
                                 "ON (a.toxval_id = b.toxval_id) ",
                                 "SET a.risk_assessment_class = b.risk_assessment_class ",
                                 "WHERE a.toxval_id in (",toString(rac_data$toxval_id[startPosition:incrementPosition]),")")

          runUpdate(table="toxval",
                    updateQuery = update_query,
                    updated_df = rac_data[startPosition:incrementPosition,],
                    db=toxval.db)

          startPosition <- startPosition + batch_size
          incrementPosition <- startPosition + batch_size - 1
        }
      }
      # Check if more to assign
      n0 = runQuery(paste0("select count(*) from toxval where source = '",source,"'",query_addition),toxval.db )[1,1]
      n1 = runQuery(paste0("select count(*) from toxval where risk_assessment_class='-' and source = '",source,"'",query_addition) ,toxval.db)[1,1]

      cat("RAC still missing: ",n1," out of ",n0," from original:",field, "\n")
      if(n1 == 0 & !report.only) break()
    }
    # for(i in 1:nrow(dict)){
    #   term = dict[i,"term"]
    #   rac = dict[i,"risk_assessment_class"]
    #   field = dict[i,"field"]
    #   source_c = dict[i,"source"]
    #   order = dict[i,"order"]
    #   query = paste0("update toxval set risk_assessment_class = '",rac,"' where ",field," = '",term,"' and source = '",source,"' and risk_assessment_class='-'",query_addition)
    #   #cat(query,"\n")
    #
    #   if (!report.only) runQuery(query,toxval.db,T,F)
    #   n0 = runQuery(paste0("select count(*) from toxval where source = '",source,"'",query_addition),toxval.db )[1,1]
    #   n1 = runQuery(paste0("select count(*) from toxval where risk_assessment_class='-' and source = '",source,"'",query_addition) ,toxval.db)[1,1]
    #
    #   cat("RAC still missing: ",order," : ",n1," out of ",n0," from original:",field,":",term," to rac:",rac,"\n")
    #   if(n1==0) break()
    # }
    # Handle DOD ERED edge case
    if(source=="DOD ERED") {
      dod_query_list = list(
        paste0("update toxval set risk_assessment_class = 'other' where toxval_type like 'ED%' and source = '",source,"' and risk_assessment_class='-'",query_addition),
        paste0("update toxval set risk_assessment_class = 'other' where toxval_type like 'EC%' and source = '",source,"' and risk_assessment_class='-'",query_addition),
        paste0("update toxval set risk_assessment_class = 'other' where toxval_type like 'IP%' and source = '",source,"' and risk_assessment_class='-'",query_addition),
        paste0("update toxval set risk_assessment_class = 'acute' where toxval_type like 'LD%' and source = '",source,"' and risk_assessment_class='-'",query_addition),
        paste0("update toxval set risk_assessment_class = 'acute' where toxval_type like 'LC%' and source = '",source,"' and risk_assessment_class='-'",query_addition),
        paste0("update toxval set risk_assessment_class = 'other' where toxval_type_original like 'N/R%' and source = '",source,"' and risk_assessment_class='-'",query_addition),
        paste0("update toxval set risk_assessment_class = 'other' where toxval_type_original like 'BIED%' and source = '",source,"' and risk_assessment_class='-'",query_addition)
      )
      for(query in dod_query_list){
        if (!report.only) {
          runQuery(query, toxval.db)
        }
        n0 = runQuery(paste0("select count(*) from toxval where source = '",source,"'",query_addition),toxval.db )[1,1]
        n1 = runQuery(paste0("select count(*) from toxval where risk_assessment_class='-' and source = '",source,"'",query_addition) ,toxval.db)[1,1]
        cat("RAC still missing: ",n1," out of ",n0,"\n")
      }

      if(report.only){
        rac_data = runQuery(paste0("SELECT toxval_id, source, toxval_type_original, toxval_type FROM toxval ",
                                   "WHERE source = '", source,
                                   "' and risk_assessment_class != '-'"), toxval.db)
        set.all = set.all %>%
          dplyr::bind_rows(rac_data %>%
                             dplyr::filter(!toxval_id %in% set.all$toxval_id))
      }
    }

    # Export missing assignments
    if(n1>0) {
      query = paste0("SELECT ",
                      "b.toxval_id,b.source_hash,b.source_table, ",
                      "a.dtxsid,a.casrn,a.name,b.chemical_id, ",
                      "b.source,b.subsource, ",
                      "b.source_url,b.subsource_url, ",
                      "b.qc_status, ",
                      "b.details_text, ",
                      "b.priority_id, ",
                      "b.risk_assessment_class, ",
                      "b.human_eco, ",
                      "b.toxval_type,b.toxval_type_original, ",
                      "b.toxval_subtype, ",
                      "b.toxval_numeric,b.toxval_units, ",
                      "b.toxval_numeric_original,b.toxval_units_original, ",
                      "b.toxval_numeric_standard,b.toxval_units_standard, ",
                      "b.toxval_numeric_human,b.toxval_units_human, ",
                      "b.study_type,b.study_type_original, ",
                      "b.study_duration_class,b.study_duration_class_original, ",
                      "b.study_duration_value,b.study_duration_value_original, ",
                      "b.study_duration_units,b.study_duration_units_original, ",
                      "b.species_id,b.species_original, ",
                      "b.strain,b.strain_group,b.strain_original, ",
                      "b.sex,b.sex_original, ",
                      "b.generation,b.lifestage, ",
                      "b.exposure_route,b.exposure_route_original, ",
                      "b.exposure_method,b.exposure_method_original, ",
                      "b.exposure_form,b.exposure_form_original, ",
                      "b.media,b.media_original, ",
                      "b.critical_effect, ",
                      "b.critical_effect_original, ",
                      "b.year, ",
                      "b.datestamp ",
                      "FROM ",
                      "toxval b ",
                      "INNER JOIN source_chemical a on a.chemical_id=b.chemical_id ",
                      "WHERE ",
                      "b.source='",source,"' ",
                      "and b.risk_assessment_class='-' ",
                      "and b.qc_status NOT LIKE '%fail%'")
      if(!is.null(subsource)) {
        query = paste0(query, " and b.subsource='",subsource,"'")
      }
      # Record missing RAC entries
      temp = runQuery(query,toxval.db)
      missing.all = dplyr::bind_rows(missing.all, temp)
      file = paste0(toxval.config()$datapath,"dictionary/missing/missing_rac/missing_RAC_",source, " ",subsource,".xlsx") %>%
        gsub(" \\.xlsx", ".xlsx", .)
      if (!report.only) {
        openxlsx::write.xlsx(temp,file)
        cat("\n\n>>> ",source,subsource,"\nStopping here means that new values need to be added to the risk_assessment_class dictionary:\n",file,
            "\nFind the unique values of temp$study_type and enter them into the file.\nThen rerun the load.\n\n")
        browser()
      }
    }
  }
  if (report.only) return(list(missing = missing.all, set = set.all))
}
