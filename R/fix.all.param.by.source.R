#-------------------------------------------------------------------------------------
#' Alter the contents of toxval according to an excel dictionary file with fields -
#' exposure_method, exposure_route, sex,strain, study_duration_class, study_duration_units, study_type,
#' toxval_type, exposure_form, media, toxval_subtype
#' @param toxval.db The version of toxval in which the data is altered.
#' @param source The source to be fixed. If source=NULL, fix all sources
#' @param subsource The subsource to be fixed (NULL default)
#' @param fill.toxval_fix If TRUE (default) read the dictionaries into the toxval_fix table
#' @import magrittr
#' @return The database will be altered
#' @export
#-------------------------------------------------------------------------------------
fix.all.param.by.source <- function(toxval.db, source=NULL,subsource=NULL, fill.toxval_fix=TRUE) {
  printCurrentFunction(toxval.db)

  n = runQuery("select count(*) from toxval_fix",toxval.db)[1,1]
  if(n==0) fill.toxval_fix = TRUE
  if(fill.toxval_fix) {
    cat("load toxval_fix\n")
    runInsert("delete from toxval_fix",toxval.db)

    filenames <- list.files(path = paste0(toxval.config()$datapath,"dictionary/2021_dictionaries/"), pattern="_5.xlsx", full.names = T)

    full_dict <- lapply(filenames, function(x) openxlsx::read.xlsx(x, colNames = T)) %T>% {
      names(.) <- gsub("_5.xlsx", "", basename(filenames))
    }
    field <- names(full_dict)
    full_dict <- mapply(cbind, full_dict, "field"=field, SIMPLIFY=F)
    colnames <- c("term_final","term_original","field")
    #print(View(full_dict))
    full_dict <- lapply(full_dict, setNames, colnames)
    full_dict <- do.call(rbind,full_dict)
    rownames(full_dict) <- NULL
    full_dict <- lapply(full_dict, function(x) utils::type.convert(as.character(x), as.is = T))
    full_dict <- data.frame(full_dict, stringsAsFactors = F)
    full_dict = unique(full_dict)
    full_dict = full_dict[!is.na(full_dict$term_original),]
    full_dict["toxval_fix_id"] <- c(1:length(full_dict[,1]))
    full_dict <- full_dict[c("toxval_fix_id",names(full_dict[-4]))]
    full_dict$field <- gsub("^[^[:alnum:]]","",full_dict$field)
    full_dict = fix.trim_spaces(full_dict)

    runInsertTable(full_dict, "toxval_fix", toxval.db, verbose)
  } else {
    full_dict = runQuery("select * from toxval_fix",toxval.db)
  }
  full_dict = full_dict[!is.na(full_dict$term_original),]
  #print(View(full_dict))
  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  # slist = slist[!is.element(slist,c("ECOTOX","ECHA IUCLID"))]
  slist = sort(slist)

  for(source in slist) {
    cat("\n-----------------------------------------------------\n")
    cat(source,subsource,"\n")
    cat("-----------------------------------------------------\n")
    cat("perform extra processes that require matching between fields\n")
    fix.exposure.params(toxval.db, source,subsource)
    fix.study_duration.params(toxval.db, source,subsource)
    fix.generation.by.source(toxval.db, source,subsource)

    cat(" deal with quotes in strings\n")
    cat("   exposure_method\n")
    runQuery(paste0("update toxval SET exposure_method"," = ", "REPLACE", "( exposure_method",  ",\'\"\',", " \"'\" ) WHERE exposure_method"," LIKE \'%\"%\' and source = '",source,"'",query_addition),toxval.db)
    cat("   strain\n")
    runQuery(paste0("update toxval SET strain"," = ", "REPLACE", "( strain",  ",\'\"\',", " \"'\" ) WHERE strain"," LIKE \'%\"%\' and source = '",source,"'",query_addition),toxval.db)
    cat("   exposure_route\n")
    runQuery(paste0("update toxval SET exposure_route"," = ", "REPLACE", "( exposure_route",  ",\'\"\',", " \"'\" ) WHERE exposure_route"," LIKE \'%\"%\' and source = '",source,"'",query_addition),toxval.db)
    cat("   media\n")
    runQuery(paste0("update toxval SET media"," = ", "REPLACE", "( media",  ",\'\"\',", " \"'\" ) WHERE media"," LIKE \'%\"%\' and source = '",source,"'",query_addition),toxval.db)
    cat("   study_type\n")
    runQuery(paste0("update toxval SET study_type"," = ", "REPLACE", "( study_type",  ",\'\"\',", " \"'\" ) WHERE study_type"," LIKE \'%\"%\' and source = '",source,"'",query_addition),toxval.db)
    cat("   toxval_type\n")
    runQuery(paste0("update toxval SET toxval_type"," = ", "REPLACE", "( toxval_type",  ",\'\"\',", " \"'\" ) WHERE toxval_type"," LIKE \'%\"%\' and source = '",source,"'",query_addition),toxval.db)
    cat(" iterate through the full_dict\n")
    flist = unique(full_dict$field)
    for(field in flist) {
      cat("   ",field,"\n")
      sdict = full_dict[full_dict$field==field,]
      query = paste0("select distinct ",field,"_original from toxval where source='",source,"'",query_addition)
      terms = runQuery(query,toxval.db)[,1]
      if(length(terms)>0) {
        sdict = sdict[is.element(sdict$term_original,terms),]
        if(nrow(sdict)>0) {
          for(i in 1:nrow(sdict)) {
            original = sdict[i,"term_original"]
            final = sdict[i,"term_final"]
            #if(field=="toxval_units") cat(original,final,"\n")
            query = paste0("update toxval set ",field,"=\"",final,"\" where ",field,"_original=\"",original,"\" and source = '",source,"'",query_addition)
            runInsert(query,toxval.db,T,F,T)
          }
        }
      }
    }

    query <- paste0("update toxval set ",field,"='-' where ",field,"_original is NULL and source = '",source,"'",query_addition)
    runInsert(query,toxval.db,T,F,T)

    cat("  expoure route\n")
    query <- paste0("update toxval
    set exposure_route = 'inhalation'
    where toxval_type in ('RFCi', 'Inhalation Unit Risk', 'IUR', 'Inhalation UR', 'Inhalation TC', 'Inhalation SF') and source = '",source,"'",query_addition)
    runInsert(query,toxval.db,T,F,T)

    query = paste0("update toxval
    set exposure_route = 'oral'
    where toxval_type in ('RFDo', 'Oral Slope Factor', 'oral TDI', 'oral SF', 'oral ADI', 'LDD50 (Lethal Dietary Dose)') and source = '",source,"'",query_addition)
    runInsert(query,toxval.db,T,F,T)

    query = paste0("update toxval ",
                   "SET exposure_route = 'oral' ",
                   "WHERE (exposure_route = '-' or exposure_route_original = '-') and toxval_units = 'mg/kg-day' and ",
                   "(toxval_type in ('NEL', 'LEL', 'LOEL', 'NOEL', 'NOAEL', 'LOAEL') or toxval_type like 'BMD%') and ",
                   "source = '",source,"'",query_addition)
    runQuery(query, toxval.db)

    cat("  study_duration_class\n")
    query = paste0("update toxval
    set study_duration_class = 'acute'
    where toxval_type in ('ARFD', 'ARFD (group)', 'AAOEL') and source = '",source,"'",query_addition)

    query = paste0("update toxval
    set study_duration_class = 'chronic'
    where toxval_type like 'Chronic%' and source = '",source,"'",query_addition)
    runInsert(query,toxval.db,T,F,T)

    export.missing.dictionary.entries(toxval.db,source,subsource)
  }
}
