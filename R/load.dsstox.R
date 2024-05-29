#-------------------------------------------------------------------------------------
#'
#' Load DSSTox if needed from a file into a global variables (DSSTOX)
#'
#-------------------------------------------------------------------------------------
load.dsstox <- function(reprocess = FALSE) {
  printCurrentFunction()

  # Check for existing DSSTOX data; otherwise, load from RData file
  if(!exists("DSSTOX")) {
    sys.date = "2022-07-20"
    if(!reprocess){
      file = paste0(toxval.config()$datapath,"/DSSTox/DSSTox_",sys.date,"_processed.RData")
      load(file)
      DSSTOX <<- DSSTOX
      return()
    }
    # Load file and reprocess
    file = paste0(toxval.config()$datapath,"/DSSTox/DSSTox_",sys.date,".RData")
    load(file)
    dsstox = DSSTOX
    rownames(dsstox) = dsstox$casrn
    dsstox = fix.non_ascii.v2(dsstox,"DSSTox")
    dsstox[is.na(dsstox$preferred_name),"preferred_name"] = "noname"
    names(dsstox)[is.element(names(dsstox),"dsstox_substance_id")] <- "dtxsid"
    DSSTOX <<- dsstox
  }
}
