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
  conv = openxlsx::read.xlsx(file)
  print(dim(conv))
  conv = conv[conv$order>0,]
  conv = conv[order(conv$term),]
  conv = conv[order(conv$risk_assessment_class),]
  conv = conv[order(conv$order),]
  conv = conv[conv$useme==1,]
  conv = conv[!is.na(conv$source),]

  # Handle addition of subsource for queries
  query_addition = " and qc_status!='fail'"
  if(!is.null(subsource)) {
    query_addition = paste0(query_addition, " and subsource='", subsource, "'")
  }
  # Store all missing RAC entries
  missing.all = NULL

  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  slist = sort(slist)
  if(!is.null(source)) slist = source
  for(source in slist) {
    cat("----------------------------------------\n")
    cat(source,subsource,"\n")
    cat("----------------------------------------\n")

    if(restart & !report.only) {
      query = paste0("update toxval set risk_assessment_class = '-'  where source like '",source,"'",query_addition)
      runInsert(query,toxval.db,T,F,T)
    }
    dict = conv[conv$source==source,]
    if(!nrow(dict) & !report.only){
      cat("\n\n>>> ",source,"\nStopping here means that new values need to be added to the risk_assessment_class dictionary:\n",file,
          "\nFind the unique values of study_type and enter them into the file.\nThen rerun the load.\n\n")
      browser()
    }
    n1.0 = runQuery(paste0("select count(*) from toxval where risk_assessment_class='-' and source = '",source,"'",query_addition) ,toxval.db)[1,1]
    cat("Initial rows:",n1.0,"\n")
    # Use dictionary to set appropriate RAC for each entry
    for(i in 1:nrow(dict)){
      term = dict[i,"term"]
      rac = dict[i,"risk_assessment_class"]
      field = dict[i,"field"]
      source_c = dict[i,"source"]
      order = dict[i,"order"]
      query = paste0("update toxval set risk_assessment_class = '",rac,"' where ",field," = '",term,"' and source = '",source,"' and risk_assessment_class='-'",query_addition)
      #cat(query,"\n")

      if (!report.only) runQuery(query,toxval.db,T,F)
      n0 = runQuery(paste0("select count(*) from toxval where source = '",source,"'",query_addition),toxval.db )[1,1]
      n1 = runQuery(paste0("select count(*) from toxval where risk_assessment_class='-' and source = '",source,"'",query_addition) ,toxval.db)[1,1]

      cat("RAC still missing: ",order," : ",n1," out of ",n0," from original:",field,":",term," to rac:",rac,"\n")
      if(n1==0) break()
    }
    # Handle DOD ERED edge case
    if(source=="DOD ERED") {
      query = paste0("update toxval set risk_assessment_class = 'other' where toxval_type like 'ED%' and source = '",source,"' and risk_assessment_class='-'")
      if (!report.only) runQuery(query,toxval.db,T,F)
      n0 = runQuery(paste0("select count(*) from toxval where source = '",source,"'"),toxval.db )[1,1]
      n1 = runQuery(paste0("select count(*) from toxval where risk_assessment_class='-' and source = '",source,"'") ,toxval.db)[1,1]
      cat("RAC still missing: ",n1," out of ",n0,"\n")

      query = paste0("update toxval set risk_assessment_class = 'other' where toxval_type like 'EC%' and source = '",source,"' and risk_assessment_class='-'")
      if (!report.only) runQuery(query,toxval.db,T,F)
      n0 = runQuery(paste0("select count(*) from toxval where source = '",source,"'"),toxval.db )[1,1]
      n1 = runQuery(paste0("select count(*) from toxval where risk_assessment_class='-' and source = '",source,"'") ,toxval.db)[1,1]
      cat("RAC still missing: ",n1," out of ",n0,"\n")

      query = paste0("update toxval set risk_assessment_class = 'other' where toxval_type like 'IP%' and source = '",source,"' and risk_assessment_class='-'")
      if (!report.only) runQuery(query,toxval.db,T,F)
      n0 = runQuery(paste0("select count(*) from toxval where source = '",source,"'"),toxval.db )[1,1]
      n1 = runQuery(paste0("select count(*) from toxval where risk_assessment_class='-' and source = '",source,"'") ,toxval.db)[1,1]
      cat("RAC still missing: ",n1," out of ",n0,"\n")

      query = paste0("update toxval set risk_assessment_class = 'acute' where toxval_type like 'LD%' and source = '",source,"' and risk_assessment_class='-'")
      if (!report.only) runQuery(query,toxval.db,T,F)
      n0 = runQuery(paste0("select count(*) from toxval where source = '",source,"'"),toxval.db )[1,1]
      n1 = runQuery(paste0("select count(*) from toxval where risk_assessment_class='-' and source = '",source,"'") ,toxval.db)[1,1]
      cat("RAC still missing: ",n1," out of ",n0,"\n")

      query = paste0("update toxval set risk_assessment_class = 'acute' where toxval_type like 'LC%' and source = '",source,"' and risk_assessment_class='-'")
      if (!report.only) runQuery(query,toxval.db,T,F)
      n0 = runQuery(paste0("select count(*) from toxval where source = '",source,"'"),toxval.db )[1,1]
      n1 = runQuery(paste0("select count(*) from toxval where risk_assessment_class='-' and source = '",source,"'") ,toxval.db)[1,1]
      cat("RAC still missing: ",n1," out of ",n0,"\n")

      query = paste0("update toxval set risk_assessment_class = 'other' where toxval_type_original like 'N/R%' and source = '",source,"' and risk_assessment_class='-'")
      if (!report.only) runQuery(query,toxval.db,T,F)
      n0 = runQuery(paste0("select count(*) from toxval where source = '",source,"'"),toxval.db )[1,1]
      n1 = runQuery(paste0("select count(*) from toxval where risk_assessment_class='-' and source = '",source,"'") ,toxval.db)[1,1]
      cat("RAC still missing: ",n1," out of ",n0,"\n")

      query = paste0("update toxval set risk_assessment_class = 'other' where toxval_type_original like 'BIED%' and source = '",source,"' and risk_assessment_class='-'")
      if (!report.only) runQuery(query,toxval.db,T,F)
      n0 = runQuery(paste0("select count(*) from toxval where source = '",source,"'"),toxval.db )[1,1]
      n1 = runQuery(paste0("select count(*) from toxval where risk_assessment_class='-' and source = '",source,"'") ,toxval.db)[1,1]
      cat("RAC still missing: ",n1," out of ",n0,"\n")
    }
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
                      "and b.qc_status!='fail'")
      if(!is.null(subsource)) {
        query = paste0(query, " and b.subsource='",subsource,"'")
      }
      # Record missing RAC entries
      temp = runQuery(query,toxval.db)
      missing.all = rbind(missing.all, temp)
      file = paste0(toxval.config()$datapath,"dictionary/missing/missing_rac/missing_RAC_",source, " ",subsource,".xlsx") %>%
        gsub(" \\.xlsx", ".xlsx", .)
      if (!report.only) {
        openxlsx::write.xlsx(temp,file)
        cat("\n\n>>> ",source,subsource,"\nStopping here means that new values need to be added to the risk_assessment_class dictionary:\n",file,
            "\nFind the unique values of temp$study_type and enter them into the file.\nThen rerun the load.\n\n")
        browser()
      }
      #if(!is.element(source,c("DOD ERED","EFSA","HAWC","HPVIS","IRIS"))) browser()
    }
  }
  if (report.only) return(missing.all)
}
