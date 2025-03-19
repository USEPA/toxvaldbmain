#--------------------------------------------------------------------------------------
#' Load the Skin eye data
#'
#' @param toxval.db Database version
#' @param verbose if TRUE, print diagnostic messages along the way
#' @export
#--------------------------------------------------------------------------------------
toxval.load.skin.eye <- function(toxval.db,source.db,verbose=F) {
  printCurrentFunction(toxval.db)
  runQuery("delete from skin_eye",toxval.db)

  cat("  load GHS data\n")
  file <- paste0(toxval.config()$datapath,"skin_eye/GHS skin and eye data v3.xlsx")
  mat <- read.xlsx(file)
  mat[is.element(mat[,"score"],"N/A"),"score"] <- "NC"
  mat$record_url <- "-"
  file <- paste0(toxval.config()$datapath,"skin_eye/GHS_sources.xlsx")
  dict <- read.xlsx(file)
  source.list <- unique(dict[,"Abbreviation"])
  for(source in source.list) {
    url <- dict[is.element(dict[,"Abbreviation"],source),"Source.URL"]
    mat[is.element(mat[,"source"],source),"record_url"] <- url
  }

  names(mat) <- c("casrn","name","endpoint","source","score","route","classification","hazard_code",
                  "hazard_statement","rationale","note","note2","valueMassOperator","valueMass","valueMassUnits","authority","record_url")
  mat$result_text <- paste(mat[,"hazard_statement"],mat[,"hazard_code"],mat[,"rationale"])
  mat <- mat[,c("casrn","name","endpoint","source","score","classification","result_text","authority","record_url")]
  mat2 = source_chemical.extra(toxval.db,source.db,mat,"Skin Eye")
  mat2 = subset(mat2,select=-c(casrn,name,chemical_index))
  runInsertTable(mat2, "skin_eye", toxval.db, verbose)

  cat("  load eChemPortal data\n")
  file <- paste0(toxval.config()$datapath,"skin_eye/echemportal_skin_eye_parsed.xlsx")
  mat <- read.xlsx(file)
  names(mat)[is.element(names(mat),"result")] <- "result_text"
  mat$source <- "ECHA eChemPortal"
  mat2 = source_chemical.extra(toxval.db,source.db,mat,"Skin Eye")
  mat2 = subset(mat2,select=-c(casrn,name,chemical_index))
  mat[is.element(mat[,"study_type"],"Eye irritation"),"study_type"] <- "eye irritation"
  mat[is.element(mat[,"study_type"],"Skin irritation / corrosion"),"study_type"] <- "skin irritation"
  mat[is.element(mat[,"study_type"],"Skin sensitisation"),"study_type"] <- "skin sensitization"
  mat$authority <- "Screening"
  file <- paste0(toxval.config()$datapath,"skin_eye/skin_eye raw.xlsx")
  write.xlsx(mat,file)
  runInsertTable(mat2, "skin_eye", toxval.db, verbose=verbose)
}
