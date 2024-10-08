#-------------------------------------------------------------------------------------
#' standardize critical_effect in toxval table based on icf dictionary and toxval critical effects dictionary
#'
#' @param toxval.db The version of toxvaldb to use.
#' @param source THe source to be fixed
#' @param subsource The subsource to be fixed (NULL default)
#' @export
#-------------------------------------------------------------------------------------
fix.critical_effect.icf.by.source <- function(toxval.db, source, subsource=NULL) {
  printCurrentFunction(paste(toxval.db,":", source, subsource))

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  #####################################################################
  cat("extract dictionary info \n")
  #####################################################################

  file <- paste0(toxval.config()$datapath,"dictionary/icf_critical_effect.xlsx")
  dict <- openxlsx::read.xlsx(file)
  print(dim(dict))
  print(names(dict))
  #####################################################################
  cat("extract critical effect info from toxval \n")
  #####################################################################
  query <- paste0("select critical_effect_original,critical_effect,source from toxval where source like '",source,"'",query_addition)
  res <- runQuery(query,toxval.db)

  #####################################################################
  cat("find critical effect values from toxval that are missing from icf dict \n")
  #####################################################################

  x <- unique(res$critical_effect_original)
  x <- x[!x %in% dict$critical_effect_original]
  cat("   missing values in dictionary:",length(x),"\n")
  # file <- paste0(toxval.config()$datapath,"dictionary/critical_effect_icf_missing_",Sys.Date(),".xlsx")
  # write.xlsx(x, file)
  #
  #####################################################################
  cat("find critical effect values from toxval that are in icf dict \n")
  #####################################################################
  y <- unique(res$critical_effect_original)
  y <- y[y %in% dict$critical_effect_original]
  cat("   values in dictionary:",length(y),"\n")

  print(dim(dict))
  dict_new1 <- dict[dict$critical_effect_original %in% y,]

  # file <- paste0(toxval.config()$datapath,"dictionary/critical_effect_icf_in_toxval_",Sys.Date(),".xlsx")
  # write.xlsx(y, file)
  #
  #####################################################################
  cat("find missing icf dict values in existing critical effect dictionary \n")
  #####################################################################
  file <- paste0(toxval.config()$datapath,"dictionary/critical_effect_stage2_set_1234b2021-04-14.xlsx")
  dict_current <- openxlsx::read.xlsx(file)
  print(dim(dict_current))

  z <- unique(dict_current$critical_effect_original_0)
  z <- z[z %in% x]
  cat("   missing icf values found in dictionary:",length(z),"\n")
  # file <- paste0(toxval.config()$datapath,"dictionary/critical_effect_icf_missing_values_in_dict_",Sys.Date(),".xlsx")
  # write.xlsx(z, file)

  dict_new2 <- dict_current[dict_current$critical_effect_original_0 %in% z,]
  # subset only the new value column and the original value column
  dict_new2 <- dict_new2 %>% dplyr::select(critical_effect=critical_effect_stage2,
                                    critical_effect_original=critical_effect_original_0)

  # combine both dictionary(icf and current dictionary) values found in toxval to create new dictionary

  dict_new <- rbind(dict_new1, dict_new2)
  print(dim(dict_new))
  dict_new <- unique(dict_new)
  # file <- paste0(toxval.config()$datapath,"dictionary/critical_effect_dict6_",Sys.Date(),".xlsx")
  # write.xlsx(dict_new, file)

  # find missing toxval critical effect values from the new dictionary
  query <- paste0("select critical_effect_original,critical_effect,source from toxval where source like '",source,"'",query_addition)
  res <- runQuery(query,toxval.db)
  #print(View(res))
  x <- unique(res$critical_effect_original)
  x <- x[!x %in% dict_new$critical_effect_original]
  cat("   missing values in dictionary:",length(x),"\n")
  # file <- paste0(toxval.config()$datapath,"dictionary/critical_effect_missing6_",Sys.Date(),".xlsx")
  # write.xlsx(x, file)

  for(i in seq_len(nrow(dict_new))) {
    original <- dict_new$critical_effect_original[i]
    final <- dict_new$critical_effect[i]
    #cat(original,":",final,"\n"); flush.console()
    query <- paste0("update toxval set critical_effect =\"",final,"\" where critical_effect_original=\"",original,"\" and source like '",source,"'",query_addition)
    runInsert(query,toxval.db,T,F,T)
    if(i%%100==0) cat("finished ",i,"out of ",nrow(dict_new),"\n")
  }
  query <- paste0("update toxval set critical_effect ='-' where critical_effect_original is NULL and source like '",source,"'",query_addition)
  runInsert(query,toxval.db,T,F,T)
  query <- paste0("update toxval set critical_effect ='-' where critical_effect_original ='NA' and source like '",source,"'",query_addition)
  runInsert(query,toxval.db,T,F,T)
}
