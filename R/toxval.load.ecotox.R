#-------------------------------------------------------------------------------------
#' Load ECOTOX from the datahub to toxval
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source.db The version of toxval source - used to manage chemicals
#' @param verbose Whether the loaded rows should be printed to the console.
#' @param log If TRUE, send output to a log file
#' @param do.load If TRUE, load the data from the input file and put into a global variable
#' @export
#--------------------------------------------------------------------------------------
toxval.load.ecotox <- function(toxval.db,source.db,log=FALSE,do.load=FALSE,sys.date="2023-08-01") {
  ##############################################################################
  ### Start of standard import.source logic
  ##############################################################################
  printCurrentFunction(toxval.db)
  source <- "ECOTOX"
  source_table = "direct_load"

  verbose = log
  #####################################################################
  cat("start output log, log files for each source can be accessed from output_log folder\n")
  #####################################################################
  if(log) {
    con1 = file.path(toxval.config()$datapath,paste0(source,"_",Sys.Date(),".log"))
    con1 = logr::log_open(con1)
    con = file(paste0(toxval.config()$datapath,source,"_",Sys.Date(),".log"))
    sink(con, append=TRUE)
    sink(con, append=TRUE, type="message")
  }
  #####################################################################
  cat("clean source_info by source\n")
  #####################################################################
  import.source.info.by.source(toxval.db, source)
  #####################################################################
  cat("clean by source\n")
  #####################################################################
  clean.toxval.by.source(toxval.db,source)

  #####################################################################
  cat("load data to res\n")
  #####################################################################
  if(!exists("ECOTOX")) do.load=TRUE
  if(do.load) {
    cat("load ECOTOX data\n")
    file = paste0(toxval.config()$datapath,"ecotox/ecotox_files/ECOTOX ",sys.date,".RData")
    #file = paste0(toxval.config()$datapath,"ecotox/ecotox_files/ECOTOX Dev small ",sys.date,".RData")
    print(file)
    load(file=file)
    print(dim(ECOTOX))
    ECOTOX <- distinct(ECOTOX)
    print(dim(ECOTOX))
    ECOTOX <<- ECOTOX
    dict = distinct(ECOTOX[,c("species_scientific_name","species_common_name","species_group","habitat")])
    file = paste0(toxval.config()$datapath,"ecotox/ecotox_files/ECOTOX_dictionary_",sys.date,".xlsx")
    write.xlsx(dict,file)
  }
  res = ECOTOX

  rename_list <- c(
    dtxsid = "dsstox_substance_id",
    casrn = "dsstox_casrn",
    name = "dsstox_pref_nm",
    species_id = "species_number",
    common_name = "species_common_name",
    latin_name = "species_scientific_name",
    habitat = "habitat",
    lifestage = "organism_lifestage",
    ecotox_group = "species_group",
    study_type = "effect",
    toxval_type = "endpoint",
    conc1_author = "conc1_author",
    conc1_units_author = "conc1_units_author",
    conc1_mean_op = "conc1_mean_op",
    conc1_mean = "conc1_mean",
    conc1_min_op = "conc1_min_op",
    conc1_min = "conc1_min",
    conc1_max_op = "conc1_max_op",
    conc1_max = "conc1_max",
    conc1_units_std = "conc1_units_std",
    conc1_mean_std = "conc1_mean_std",
    exposure_route = "exposure_group",
    exposure_method = "exposure_type",
    media = "media_type",
    long_ref = "source",
    author = "author",
    title = "title",
    year = "publication_year",
    pmid = "reference_number",
    observed_duration_std="observed_duration_std",
    observed_duration_units_std="observed_duration_units_std",
    observ_duration_mean_op="observ_duration_mean_op",
    observ_duration_mean="observ_duration_mean",
    observ_duration_min_op="observ_duration_min_op",
    observ_duration_min="observ_duration_min",
    observ_duration_max_op="observ_duration_max_op",
    observ_duration_max="observ_duration_max",
    observ_duration_unit="observ_duration_unit",
    observ_duration_unit_desc="observ_duration_unit_desc",
    effect_measurement = "effect_measurement"
  )

  res <- res %>%
    dplyr::rename(rename_list) %>%
    dplyr::select(dplyr::all_of(names(rename_list))) %>%
    dplyr::mutate(species_original = common_name) %>%
    # Replace NA values ("NA", "NR", "")
    dplyr::mutate(dplyr::across(where(is.character), na_if, "NA"),
                  dplyr::across(where(is.character), na_if, "NR"),
                  dplyr::across(where(is.character), na_if, ""))

  # Programmatically set toxval_numeric, units, and qualifier from conc1_* fields
  res <- ecotox.select.toxval.numeric(in_data = res)
  res$toxval_numeric_qualifier[is.na(res$toxval_numeric_qualifier) |
                                 res$toxval_numeric_qualifier == ""] <- "-"
  # Programmatically set study_duration, units, and qualifier from observ* fields
  res <- ecotox.select.study.duration(in_data = res)

  res$source = source

  cat(nrow(res),"\n")
  # Filter out NA toxval_type
  res = res[!is.na(res$toxval_type),]
  cat(nrow(res),"\n")

  res$study_duration_units[res$study_duration_units %in% c("d", "Day(s)")] = "days"
  res$toxval_type = res$toxval_type %>%
    gsub("\\/|\\*","", .) %>%
    gsub("--", "-", .)

  res$study_duration_value[is.na(res$study_duration_value)] <- -999

  res <- res %>%
    dplyr::mutate(exposure_route = dplyr::case_when(
      exposure_route == "ENV" ~ "environmental",
      exposure_route == "MULTIPLE" ~ "multiple",
      exposure_route == "IN VITRO" ~ "in vitro",
      exposure_route == "ORAL" ~ "oral",
      exposure_route == "INJECT" ~ "injection",
      exposure_route == "TOP" ~ "dermal",
      exposure_route == "AQUA" ~ "aquatic",
      exposure_route == "UNK" ~ "unknown",
      TRUE ~ exposure_route
    ))

  res$long_ref = paste(res$long_ref,res$author,res$title,res$year)
  res$critical_effect = paste(res$study_type, res$effect_measurement)
  # Remove effect_measurement field
  res$effect_measurement = NULL

  res = distinct(res)
  print(dim(res))

  tt.list <- res$toxval_type
  tt.list <- gsub("(^[a-zA-Z]+)([0-9]*)(.*)","\\1",tt.list)

  res1 <- res[tt.list!="LT",]
  res2 <- res[tt.list=="LT",]

  dose.units <- res2$toxval_units
  time.value <- res2$study_duration_value
  dose.value <- res2$toxval_numeric %>%
    signif(., digits=4)
  dose.qualifier <- res2$toxval_numeric_qualifier
  time.units <- res2$study_duration_units
  type <- res2$toxval_type

  new.type <- paste0(type,"@",dose.qualifier," ",dose.value," ",dose.units)
  res3 <- res2
  res3$toxval_type <- new.type
  res3$toxval_numeric <- time.value
  res3$toxval_units <- time.units
  res <- rbind(res1,res3)

  # Fix casrn
  x <- res[,"casrn"]
  for(i in 1:length(x)) x[i] <- fix.casrn(x[i])
  res[,"casrn"] <- x

  res <- res[!is.na(res$toxval_numeric),]
  res$quality <- paste("Control type:",res$quality)
  res <- res[res$toxval_numeric>=0,]

  res$exposure_method <- res$exposure_method %>%
    stringr::str_squish()
  message("Why is this line here? exposure_method was set to exposure_type originally...")
  browser(print("Why is this line here? exposure_method was set to exposure_type originally..."))
  res$exposure_method <- res$media

  #####################################################################
  cat("add other columns to res\n")
  #####################################################################
  #Get rid of wacko characters (seem to mean auto or AI generated)
  res$toxval_units <- res$toxval_units %>%
    # Starts with match to remove
    gsub("^AI |^ae |^ai ", "", .) %>%
    stringr::str_squish()

  res$toxval_type <- res$toxval_type %>%
    gsub("\\*|\\/|\\~", "", .)

  res$study_duration_class <- "chronic"
  res <- distinct(res)
  res <- fill.toxval.defaults(toxval.db,res)

  cat("set the source_hash\n")
  # res$source_hash = NA
  # # Sort by names so it's reproducible
  # res.temp = res[,sort(names(res))]
  # for (i in 1:nrow(res)){
  #   row <- res.temp[i,]
  #   res[i,"source_hash"] = digest(paste0(row,collapse=""), serialize = FALSE)
  #   if(i%%1000==0) cat(i," out of ",nrow(res),"\n")
  # }
  # Vectorized approach to source_hash generation
  non_hash_cols <- toxval.config()$non_hash_cols
  res = res %>%
    tidyr::unite(hash_col, all_of(sort(names(.)[!names(.) %in% non_hash_cols])), sep="", remove = FALSE) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(source_hash = digest(hash_col, serialize = FALSE)) %>%
    dplyr::ungroup() %>%
    select(-hash_col)

  res = source_chemical.ecotox(toxval.db,source.db,res,source,chem.check.halt=FALSE,
                               casrn.col="casrn",name.col="name",verbose=FALSE)

  ##############################################################################
  ### Start of standard toxval.load logic
  ##############################################################################

  #####################################################################
  cat("Add the code from the original version from Aswani\n")
  #####################################################################
  cremove = c(non_hash_cols, "chemical_index","common_name","latin_name","ecotox_group")
  res = res[ , !(names(res) %in% cremove)]

  #####################################################################
  cat("find columns in res that do not map to toxval or record_source\n")
  #####################################################################
  cols1 = runQuery("desc record_source",toxval.db)[,1]
  cols2 = runQuery("desc toxval",toxval.db)[,1]
  cols = unique(c(cols1,cols2))
  res = res[ , !(names(res) %in% c("record_url","short_ref"))]
  nlist = names(res)
  nlist = nlist[!is.element(nlist,c("casrn","name"))]
  nlist = nlist[!is.element(nlist,cols)]
  if(length(nlist)>0) {
    cat("columns to be dealt with\n")
    print(nlist)
    browser()
  }
  print(dim(res))

  #####################################################################
  cat("Generic steps \n")
  #####################################################################
  res = distinct(res)
  res = fill.toxval.defaults(toxval.db,res)
  res = res[!is.na(res$toxval_numeric),]
  res = res[res$toxval_numeric>0,]
  res = generate.originals(toxval.db,res)
  if("species_original" %in% names(res)) res$species_original = tolower(res$species_original)
  res$toxval_numeric = as.numeric(res$toxval_numeric)
  print(paste0("Dimensions of source data after originals added: ", toString(dim(res))))
  res=fix.non_ascii.v2(res,source)
  # Remove excess whitespace
  res = res %>%
    dplyr::mutate(dplyr::across(where(is.character), stringr::str_squish))
  res = distinct(res)
  res = res[,!is.element(names(res),c("casrn","name"))]
  print(paste0("Dimensions of source data after ascii fix and removing chemical info: ", toString(dim(res))))

  #####################################################################
  cat("add toxval_id to res\n")
  #####################################################################
  count = runQuery("select count(*) from toxval",toxval.db)[1,1]
  if(count==0) tid0 = 1
  else tid0 = runQuery("select max(toxval_id) from toxval",toxval.db)[1,1] + 1
  tids = seq(from=tid0,to=tid0+nrow(res)-1)
  res$toxval_id = tids
  print(dim(res))

  #####################################################################
  cat("pull out record source to refs\n")
  #####################################################################
  cols = runQuery("desc record_source",toxval.db)[,1]
  nlist = names(res)
  keep = nlist[is.element(nlist,cols)]
  refs = res[,keep]
  cols = runQuery("desc toxval",toxval.db)[,1]
  nlist = names(res)
  remove = nlist[!is.element(nlist,cols)]
  res = res[ , !(names(res) %in% c(remove))]
  print(dim(res))

  #####################################################################
  cat("add extra columns to refs\n")
  #####################################################################
  refs$record_source_type = "-"
  refs$record_source_note = "-"
  refs$record_source_level = "-"
  print(dim(res))

  #####################################################################
  cat("load res and refs to the database\n")
  #####################################################################
  res = distinct(res)
  refs = distinct(refs)
  res$datestamp = Sys.Date()
  res$source_table = source_table
  res$subsource <- "EPA ORD"
  res$source_url <- "https://cfpub.epa.gov/ecotox/"
  res$details_text = paste(source,"Details")
  runInsertTable(res, "toxval", toxval.db, verbose)
  runInsertTable(refs, "record_source", toxval.db, verbose)
  print(dim(res))
  #####################################################################
  cat("do the post processing\n")
  #####################################################################
  toxval.load.postprocess(toxval.db,source.db,source,do.convert.units=FALSE)

  if(log) {
    #####################################################################
    cat("stop output log \n")
    #####################################################################
    closeAllConnections()
    logr::log_close()
    output_message = read.delim(paste0(toxval.config()$datapath,source,"_",Sys.Date(),".log"), stringsAsFactors = FALSE, header = FALSE)
    names(output_message) = "message"
    output_log = read.delim(paste0(toxval.config()$datapath,"log/",source,"_",Sys.Date(),".log"), stringsAsFactors = FALSE, header = FALSE)
    names(output_log) = "log"
    new_log = log_message(output_log, output_message[,1])
    writeLines(new_log, paste0(toxval.config()$datapath,"output_log/",source,"_",Sys.Date(),".txt"))
  }
  #####################################################################
  cat("finish\n")
  #####################################################################
}
