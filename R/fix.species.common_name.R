#-------------------------------------------------------------------------------------
#' Fix issues with species common names
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @export
#--------------------------------------------------------------------------------------
fix.species.common_name <- function(toxval.db) {
  printCurrentFunction(toxval.db)

  # Remove whitespace from common_name values in species table
  runQuery("update species set common_name=TRIM(common_name)",toxval.db)

  # Get list of common_names values
  res = runQuery("select species_id, common_name from species",toxval.db)
  res$fixed = res$common_name

  # Clean/fix common names
  cat("Phase 1: ",nrow(res),"\n")
  for(i in seq_len(nrow(res))) {
    x = res[i,"fixed"]
    val = paste0("[",x,"]")
    tryCatch({
      x = stringr::str_trim(x)
    }, warning = function(w) {
      cat("WARNING:",x,"\n")
    }, error = function(e) {
      cat("ERROR:",x,"\n")
      x = iconv(x,from="UTF-8",to="ASCII//TRANSLIT")
      x = stringr::str_trim(stringi::stri_escape_unicode(x))
    })
    res[i,"fixed"] = x
  }
  res = res[res$common_name!=res$fixed,]
  cat("Phase 2: ",nrow(res),"\n")

  # Update species table with fixed common names
  for(i in seq_len(nrow(res))) {
    query = paste0("update species set common_name=TRIM(common_name) where species_id=",res[i,"species_id"])
    runQuery(query,toxval.db)
  }

  # Perform further cleaning operations on common names
  for(i in seq_len(nrow(res))) {
    x = res[i,"fixed"]
    val = paste0("[",x,"]")
    tryCatch({
      x = stringr::str_trim(x)
    }, warning = function(w) {
      cat("WARNING:",x,"\n")
    }, error = function(e) {
      cat("ERROR:",x,"\n")
      x = iconv(x,from="UTF-8",to="ASCII//TRANSLIT")
      x = stringr::str_trim(stringi::stri_escape_unicode(x))
    })
    res[i,"fixed"] = x
  }
  res = res[res$common_name!=res$fixed,]
  cat("Phase 3: ",nrow(res),"\n")

  # Set final common names
  for(i in seq_len(nrow(res))) {
    sid = res[i,"species_id"]
    cn = res[i,"fixed"]
    query = paste0("update species set common_name='",cn,"' where species_id=",sid)
    runQuery(query,toxval.db)
  }
}
