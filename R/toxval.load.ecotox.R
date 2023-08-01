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
  nlist_old = c("dsstox_substance_id","dsstox_casrn","dsstox_pref_nm",
  "species_number","species_common_name","species_scientific_name","habitat","organism_lifestage","species_group",
  "effect",
  "endpoint","conc1_type_std","conc1_units_std","conc1_units_author",
  "conc1_author","conc1_mean_std","conc1_author",
  # "conc1_author","conc1_mean_op","conc1_mean","conc1_min_op","conc1_min","conc1_max_op","conc1_max",
  # "conc1_units_std","conc1_mean_std",
  "observed_duration_units_std","observed_duration_std",
  "exposure_group","exposure_type","media_type",
  "source","author","title","publication_year","reference_number",
  "effect_measurement", "species_common_name")

  nlist_new = c("dtxsid","casrn","name",
             "species_id","common_name","latin_name","habitat","lifestage","ecotox_group",
             "study_type",
             "toxval_type","toxval_subtype","toxval_units","toxval_units2",
             "toxval_numeric_qualifier","toxval_numeric","toxval_numeric2",
             # "conc1_author","conc1_mean_op","conc1_mean","conc1_min_op","conc1_min","conc1_max_op","conc1_max",
             # "conc1_units_std","conc1_mean_std",
             "study_duration_units","study_duration_value",
             "exposure_route","exposure_method","media",
             "long_ref","author","title","year","pmid",
             "effect_measurement", "species_original")

  # Code used to generate the rename parameters from old/new list
  # paste0(nlist_new, " = ", nlist_old, collapse = ", ")
  res <- res %>%
    dplyr::rename(dtxsid = dsstox_substance_id,
                  casrn = dsstox_casrn,
                  name = dsstox_pref_nm,
                  species_id = species_number,
                  common_name = species_common_name,
                  latin_name = species_scientific_name,
                  # habitat = habitat,
                  lifestage = organism_lifestage,
                  ecotox_group = species_group,
                  study_type = effect,
                  toxval_type = endpoint,
                  toxval_subtype = conc1_type_std,
                  toxval_units = conc1_units_std,
                  toxval_units2 = conc1_units_author,
                  toxval_numeric = conc1_mean_std,
                  # conc1_author = conc1_author,
                  # conc1_mean_op = conc1_mean_op,
                  # conc1_mean = conc1_mean,
                  # conc1_min_op = conc1_min_op,
                  # conc1_min = conc1_min,
                  # conc1_max_op = conc1_max_op,
                  # conc1_max = conc1_max,
                  # conc1_units_std = conc1_units_std,
                  # conc1_mean_std = conc1_mean_std,
                  study_duration_units = observed_duration_units_std,
                  study_duration_value = observed_duration_std,
                  exposure_route = exposure_group,
                  exposure_method = exposure_type,
                  media = media_type,
                  long_ref = source,
                  author = author,
                  title = title,
                  year = publication_year,
                  pmid = reference_number,
                  effect_measurement = effect_measurement,
                  species_original = species_common_name) %>%
    dplyr::mutate(toxval_numeric_qualifier = conc1_author,
                  toxval_numeric2 = conc1_author) %>%
    dplyr::select(dplyr::any_of(nlist_new)) %>%
    # Replace NA values ("NA", "NR", "")
    dplyr::mutate(dplyr::across(where(is.character), na_if, "NA"),
                  dplyr::across(where(is.character), na_if, "NR"),
                  dplyr::across(where(is.character), na_if, ""))

  # Fix decimal lists in toxval_numeric2
  #'@title Split Decimal List
  #'@description Helpter function to split a decimal list
  #'@details The assumption is the decimal list contains numeric values
  #'with the same significant figures per number. So, first split the list
  #'and get the length of the last string in the split list. This is the assumed
  #'length of the standard significant figures in the decimal list. Next, find the
  #'leading values before the decimal by using the found decimal place count (perl regex).
  #'Finally, get the tail values by substituting the leads before piecing the numerics
  #'back together.
  #'@param in_str Input decimal list string to split/fix.
  #'@param get_min Boolean to return the min of the split list. Default is TRUE.
  #'@examples
  #'   split_decimal_list(in_str = "1226.131118.211344.27") returns 1118.21
  #'   split_decimal_list(in_str = "40.5773.3", get_min=FALSE) returns c(40.5, 773.3)
  #'   split_decimal_list(in_str = "0.5840.1141.054") returns c(0.584, 0.114, 1.054")
  split_decimal_list <- function(in_str, get_min=TRUE){
    # Skip NULL or NA strings
    if(is.null(in_str) | is.na(in_str)){
      return(in_str)
    }
    # Ignore ranges, single decimals, or empty strings
    if(stringr::str_count(in_str, "[.]") <= 1 | grepl("-", in_str) | in_str %in% c("", " ")){
      return(in_str)
    }
    # message("Fixing: ", in_str)
    # Find decimal places (look at last in decimal list)
    sig_n = in_str %>%
      strsplit(split = "\\.") %>%
      unlist() %>%
      .[length(.)] %>%
      stringr::str_length()

    # Match leading numeric based on sig_n
    lead = in_str %>%
      strsplit(split = paste0("(?<=[.]).{1,",sig_n,"}"), perl = TRUE) %>%
      unlist()
    # Get decimal places (remove lead)
    tail = in_str %>%
      gsub(paste0(lead %>%
                    gsub("\\.", "[.]", .),
                  collapse="|"), ".", .) %>%
      strsplit(split = "[.]") %>%
      unlist() %>%
      # REmove empty strings
      .[!. %in% c("")]
    # Recombine
    out = paste0(lead, tail) %>%
      as.numeric()

    # Return a character string back
    if(get_min){
      return(min(out) %>% as.character())
    } else{
      return(out %>% as.character())
    }
  }

  # Attempt to clean up toxval_numeric field
  # Identify cases to handle
  # tn_fix <- res %>%
  #   dplyr::select(toxval_numeric) %>%
  #   unique() %>%
  #   dplyr::mutate(toxval_numeric_n = suppressWarnings(as.numeric(toxval_numeric))) %>%
  #   filter(is.na(toxval_numeric_n), !is.na(toxval_numeric))

  res$toxval_numeric = res$toxval_numeric %>%
    gsub("\\*", "", .) %>%
    as.numeric()

  # Attempt to clean up toxval_numeric2 field
  # Identify cases to handle
  # tn_fix <- res %>%
  #   dplyr::select(toxval_numeric2) %>%
  #   unique() %>%
  #   dplyr::mutate(toxval_numeric_n = suppressWarnings(as.numeric(toxval_numeric2))) %>%
  #   filter(is.na(toxval_numeric_n), !is.na(toxval_numeric2)) %>%
  #   # Temp filter out ranges
  #   filter(!grepl("-", toxval_numeric2)) # %>%
  #   # # Temp filter out multiple decimals
  #   # dplyr::mutate(multi_decimals = stringr::str_count(toxval_numeric2, "\\.")) %>%
  #   # filter(multi_decimals %in% c(0, 1))

  res$toxval_numeric2 = res$toxval_numeric2 %>%
    gsub("\\/NR|NR|\\/|\\*|>|<|>=|<=|=|~|ca|\\+", "", .)

  # Fix decimal lists, assumes standard decimal places, only applied to those
  # with multiple decimals, not ranges
  res = res %>%
    dplyr::rowwise() %>%
    dplyr::mutate(toxval_numeric2 = split_decimal_list(toxval_numeric2, get_min = TRUE)) %>%
    dplyr::ungroup()

  res$source = source
  # res[res=="NA"] = NA
  # res[res=="NR"] = NA
  # res[res==""] = NA
  # res$toxval_numeric_qualifier = "="

  cat("fix the toxval_numeric_qualifier ranges\n")
  x = res$toxval_numeric_qualifier
  x = str_replace_all(x,"NR","")
  x = str_replace_all(x,"/","")
  xq = x
  xq[] = "="
  hits = grep("-",x)
  if(length(hits)>0) {
    for(i in 1:length(hits)) x[hits[i]] = str_split(x[hits[i]],"-")[[1]][1]
    qlist = c("<=",">=","<",">","~","ca ")
    for(ql in qlist) {
      cat("  ",ql,"\n")
      ql2 = ql
      if(ql=="ca ") ql2 = "~"
      qhits = grep(ql,x)
      for(i in 1:length(qhits)) {
        x[qhits[i]] = str_replace(x[qhits[i]],ql,"")
        xq[qhits[i]] = ql2
      }
    }
  }
  res$toxval_numeric_qualifier = xq
  res1 = res[!is.na(res$toxval_numeric),]
  cat(nrow(res),"\n")
  # Filter out NA toxval_type
  res = res[!is.na(res$toxval_type),]
  cat(nrow(res),"\n")

  res$study_duration_units[res$study_duration_units == "d"] = "days"
  res$study_duration_units[res$study_duration_units == "Day(s)"] <- "Day"
  res$toxval_type = res$toxval_type %>%
    gsub("\\/|\\*","", .)

  res$study_duration_value = res$study_duration_value %>%
    gsub("\\/|\\~|\\*|>=|<=|>|<|=| ","", .) %>%
    gsub("NR", "-999", .) %>%
    as.numeric()

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
  res$toxval_units[is.na(res$toxval_units)] = res$toxval_units2[is.na(res$toxval_units)]
  res$toxval_numeric[is.na(res$toxval_numeric)] = res$toxval_numeric2[is.na(res$toxval_numeric)]

  res = res %>%
    dplyr::select(-c(effect_measurement,
                     # conc1_author,
                     # conc1_mean_op,
                     # conc1_mean,
                     # conc1_min_op,
                     # conc1_min,
                     # conc1_max_op,
                     # conc1_max,
                     # conc1_units_std,
                     # conc1_mean_std,
                     toxval_units2,
                     toxval_numeric2))

  res = distinct(res)
  print(dim(res))

  tt.list <- res$toxval_type
  tt.list <- gsub("(^[a-zA-Z]+)([0-9]*)(.*)","\\1",tt.list)

  res1 <- res[tt.list!="LT",]
  res2 <- res[tt.list=="LT",]

  dose.units <- res2$toxval_units
  time.value <- res2$study_duration_value
  dose.value <- res2$toxval_numeric
  dose.value <- gsub("NR",NA, dose.value)
  dose.qualifier <- as.data.frame(str_extract_all(dose.value, "[^0-9.E+-]+", simplify = TRUE))[,1]
  dose.value <- gsub("[^0-9.E+-]+", "", dose.value)
  time.units <- res2$study_duration_units
  type <- res2$toxval_type

  for(i in 1:length(dose.value)) {
    if(!is.na(as.numeric(dose.value[i]))) {
      dose.value[i] <- signif(as.numeric(dose.value[i]),digits=4)
    }
  }

  new.type <- paste0(type,"@",dose.qualifier," ",dose.value," ",dose.units)
  res3 <- res2
  res3$toxval_type <- new.type
  res3$toxval_numeric <- time.value
  res3$toxval_units <- time.units
  res <- rbind(res1,res3)
  x <- res[,"casrn"]
  for(i in 1:length(x)) x[i] <- fix.casrn(x[i])
  res[,"casrn"] <- x

  res <- res[!is.na(res$toxval_numeric),]
  res$quality <- paste("Control type:",res$quality)
  res <- res[res$toxval_numeric>=0,]
  res$toxval_numeric_qualifier[which(is.na(res$toxval_numeric_qualifier)|res$toxval_numeric_qualifier == "")] <- "-"
  message("Why is this line here? exposure_method was set to exposure_type originally...")
  browser(print("Why is this line here? exposure_method was set to exposure_type originally..."))
  res$exposure_method <- res$media

  #####################################################################
  cat("add other columns to res\n")
  #####################################################################
  #Get rid of wacko characters
  do.fix.units <- TRUE
  if(do.fix.units) {
    res$toxval_units <- str_replace_all(res$toxval_units,"AI ","")
    res$toxval_units <- str_replace_all(res$toxval_units,"ae ","")
    res$toxval_units <- str_replace_all(res$toxval_units,"ai ","")
  }
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
