#-------------------------------------------------------------------------------------
#' Extract the ECOTO species dictionary from the ECOTOX data
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source.db The version of toxval source - used to manage chemicals
#' @param verbose Whether the loaded rows should be printed to the console.
#' @param log If TRUE, send output to a log file
#' @param do.load If TRUE, load the data from the input file and put into a global variable
#' @export
#--------------------------------------------------------------------------------------
ecotox.species.dictionary <- function(toxval.db,do.load=F,sys.date="2023-05-03") {
  printCurrentFunction(toxval.db)
  source <- "ECOTOX"
  source_table = "direct_load"

  #####################################################################
  cat("load data to res\n")
  #####################################################################
  if(!exists("ECOTOX")) do.load=T
  if(do.load) {
    cat("load ECOTOX data\n")
    file = paste0(toxval.config()$datapath,"ecotox/ecotox_files/ECOTOX ",sys.date,".RData")
    #file = paste0(toxval.config()$datapath,"ecotox/ecotox_files/ECOTOX Dev small ",sys.date,".RData")
    print(file)
    load(file=file)
    print(dim(ECOTOX))
    ECOTOX <- unique(ECOTOX)
    print(dim(ECOTOX))
    ECOTOX <<- ECOTOX
    dict = unique(ECOTOX[,c("species_scientific_name","species_common_name","species_group","habitat")])
    file = paste0(toxval.config()$datapath,"ecotox/ecotox_files/ECOTOX_dictionary_",sys.date,".xlsx")
    openxlsx::write.xlsx(dict,file)
  }
  res = ECOTOX
  slist = runQuery("select distinct species_id from species",toxval.db)[,1]
  print(nrow(res))
  res = res[!is.element(res$species_number,slist),]
  print(nrow(res))
  dict = unique(res[,c("species_number","species_common_name","species_scientific_name","species_group")])
  file = paste0(toxval.config()$datapath,"species/ecotox_new_species ",sys.date,".xlsx")
  openxlsx::write.xlsx(dict,file)
}
