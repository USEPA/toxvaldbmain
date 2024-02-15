#-------------------------------------------------------------------------------------
#' Load the uterotophic and Hershberger data
#' @param toxval.db The database version to use
#' @param source.db The source database
#' @param log If TRUE, send output to a log file
#' @param remove_null_dtxsid If TRUE, delete source records without curated DTXSID value
#' @export
#--------------------------------------------------------------------------------------
toxval.load.ut_hb <- function(toxval.db, source.db, log=FALSE, remove_null_dtxsid=TRUE){
  printCurrentFunction(toxval.db)
  source <- "Uterotrophic Hershberger DB"
  source_table = "direct load"
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
  # Read in Uterotrophic data
  file = paste0(toxval.config()$datapath,"ut_hb/UT for ToxValDB.xlsx")
  res0_ut = readxl::read_xlsx(file, col_types = "text")

  res_ut = res0_ut %>%
    # Keep only entries with "Active" response
    dplyr::filter(response == "Active") %>%

    # Drop unused columns
    dplyr::select(tidyselect::all_of(c("casrn",
                                       "name",
                                       "study_type",
                                       "toxval_type",
                                       "toxval_numeric",
                                       "toxval_numeric_qualifier",
                                       "toxval_units",
                                       "critical_effect",
                                       "species",
                                       "exposure_route",
                                       "study_duration_value",
                                       "study_duration_units",
                                       "pmid",
                                       "guideline"))) %>%

    # Add hard-coded information
    dplyr::mutate(
      long_ref = "Kleinstreuer et al., A Curated Database of Rodent Uterotrophic Bioactivity, EHP Vol 124 2106, https://doi.org/10.1289/ehp.1510183",
      url = "https://ntp.niehs.nih.gov/go/40658",
      exposure_method = "-",
      strain = "-"
    )

  # Read in Hershberger data
  file = paste0(toxval.config()$datapath,"ut_hb/HB for ToxValDB.xlsx")
  res0_hb = readxl::read_xlsx(file, col_types = "text")

  res_hb = res0_hb %>%
    # Select only relevant fields
    dplyr::select(tidyselect::all_of(c("casrn","name","long_ref","species","strain","study_duration_value",
                                       "study_duration_units","study_type","exposure_route","exposure_method",
                                       "toxval_units","critical_effect","Androgenic call","Antiandrogenic call",
                                       "HB total activity","other in vivo effects","In vivo NOEL for antiandrogenic",
                                        "in vivo LOEL antiandrogenic","HB NOEL","HB LOEL"))) %>%

    # Set hard-coded information
    dplyr::mutate(
      url = "https://www.sciencedirect.com/science/article/pii/S089062381830145X",
      toxval_numeric_qualifier = "=",
      pmid = NA,
      guideline = "-",
    ) %>%

    # Pivot to separate data on toxval_types
    tidyr::pivot_longer(
      cols = c("In vivo NOEL for antiandrogenic","in vivo LOEL antiandrogenic","HB NOEL","HB LOEL"),
      names_to = "toxval_study_type",
      values_to = "toxval_numeric",
    ) %>%

    # Set appropriate values after pivot
    dplyr::mutate(
      toxval_type = dplyr::case_when(
        grepl("NOEL", toxval_study_type) ~ "NOEL",
        grepl("LOEL", toxval_study_type) ~ "LOEL",
        TRUE ~ as.character(NA)
      ),
      study_type = dplyr::case_when(
        grepl("vivo", toxval_study_type) ~ "acute",
        grepl("HB", toxval_study_type) ~ "Hershberger",
        TRUE ~ as.character(NA)
      ),
      `Androgenic call` = dplyr::case_when(
        `Androgenic call` == "Positive" ~ "Androgenic",
        TRUE ~ `Androgenic call`
      ),
      `Antiandrogenic call` = dplyr::case_when(
        `Antiandrogenic call` == "Positive" ~ "Antiandrogenic",
        TRUE ~ `Antiandrogenic call`
      )
    ) %>%

    # Drop entries without required "active" value
    dplyr::filter(!(toxval_study_type == "HB NOEL" & `HB total activity` != "active"),
                  !(toxval_study_type == "in vivo LOEL antiandrogenic" & `HB total activity` != "active")) %>%

    # Compile critical_effect value from relevant fields
    tidyr::unite(
      "critical_effect",
      `HB total activity`,`Androgenic call`,`Antiandrogenic call`, critical_effect, `other in vivo effects`,
      sep = "|",
      remove = TRUE,
      na.rm = TRUE
    ) %>%

    # Remove fields to prep for rbind
    dplyr::select(tidyselect::all_of(c("casrn","name","toxval_type","toxval_numeric_qualifier","toxval_numeric","toxval_units",
                                       "study_type","exposure_method","exposure_route","study_duration_value","study_duration_units",
                                       "species","strain","critical_effect","long_ref","pmid","url","guideline")))

  # Combine UT and HB data
  res = rbind(res_ut, res_hb) %>%
    # Remove NA casrn entries (logic used in old script)
    dplyr::filter(!is.na(casrn) & casrn != "-" & casrn != "") %>%
    # Set literal "NA" values to NA
    dplyr::mutate(dplyr::across(where(is.character), ~na_if(., "NA"))) %>%

    # Perform final cleaning operations
    dplyr::mutate(
      source = !!source,
      species = tolower(species),
      toxval_numeric = as.numeric(toxval_numeric),

      # Fix study_duration values
      study_duration_value = study_duration_value %>%
        # Get first number or range
        stringr::str_extract("[0-9\\.,]+(?:\\s?\\-\\s?[0-9\\.,]+)?") %>% c() %>%
        stringr::str_squish(),
      # If invalid study_duration_value, set units to NA
      study_duration_units = dplyr::case_when(
        is.na(study_duration_value) ~ as.character(NA),
        TRUE ~ study_duration_units
      )
      # TODO Await team discussion on how to better clean critical_effect field
      # # Remove inappropriate critical_effect values
      # critical_effect_new = dplyr::case_when(
      #   # Search for terms related to actual critical effects
      #   grepl(paste0("androgenic|\\||increase|decrease|significant|conclude|effect|",
      #                "estradiol|response|gain|interaction|weight|deficient"),
      #         critical_effect, ignore.case=TRUE) ~ critical_effect,
      #   # Return NA if value is not actually critical_effect
      #   TRUE ~ as.character(NA)
      # )
    ) %>%

    # Drop entries without toxval cols
    tidyr::drop_na(toxval_numeric, toxval_units, toxval_type) %>%
    # Ensure toxval_numeric is greater than 0
    dplyr::filter(toxval_numeric > 0) %>%
    # Drop duplicate entries
    dplyr::distinct()

  # Maintained logic from old script - comment out if unnecessary
  cat("set the source_hash\n")
  res$source_hash = NA
  # Sort res columns before hashing for more consistent hashing
  res = res[,sort(names(res))]
  for (i in 1:nrow(res)){
    row <- res[i,]
    res[i,"source_hash"] = digest(paste0(row,collapse=""), serialize = FALSE)
    if(i%%1000==0) cat(i," out of ",nrow(res),"\n")
  }

  # Clean name/casrn and set chemical_id in source_chemical table
  res = source_chemical.extra(toxval.db,source.db,res,source,chem.check.halt=FALSE,casrn.col="casrn", name.col="name",verbose=FALSE)

  #####################################################################
  cat("Add the code from the original version from Aswani\n")
  #####################################################################

  # Data cleaned in above section

  #####################################################################
  cat("find columns in res that do not map to toxval or record_source\n")
  #####################################################################
  cols1 = runQuery("desc record_source",toxval.db)[,1]
  cols2 = runQuery("desc toxval",toxval.db)[,1]
  cols = unique(c(cols1,cols2))
  colnames(res)[which(names(res) == "species")] = "species_original"
  res = res[ , !(names(res) %in% c("record_url","short_ref"))]
  nlist = names(res)
  nlist = nlist[!is.element(nlist,c("casrn","name"))]
  nlist = nlist[!is.element(nlist,cols)]

  # Dynamically remove unused columns ("columns to be dealt with")
  res = res %>% dplyr::select(!tidyselect::any_of(nlist))

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
  res = generate.originals(toxval.db,res)
  res$toxval_numeric = as.numeric(res$toxval_numeric)
  print(paste0("Dimensions of source data after originals added: ", toString(dim(res))))
  res=fix.non_ascii.v2(res,source)
  # Remove excess whitespace
  res = res %>%
    dplyr::mutate(dplyr::across(where(is.character), stringr::str_squish))
  res = distinct(res)
  res = res[, !names(res) %in% c("casrn","name")]
  print(paste0("Dimensions of source data after ascii fix and removing chemical info: ", toString(dim(res))))

  #####################################################################
  cat("add toxval_id to res\n")
  #####################################################################
  count = runQuery("select count(*) from toxval",toxval.db)[1,1]
  if(count==0) {
    tid0 = 1
  } else {
    tid0 = runQuery("select max(toxval_id) from toxval",toxval.db)[1,1] + 1
  }
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
  print(paste0("Dimensions of references after adding ref columns: ", toString(dim(refs))))

  #####################################################################
  cat("load res and refs to the database\n")
  #####################################################################
  res = distinct(res)
  refs = distinct(refs)
  res$datestamp = Sys.Date()
  res$source_table = source_table
  res$source_url = "-"
  res$subsource_url = "-"
  res$details_text = paste(source,"Details")
  runInsertTable(res, "toxval", toxval.db, verbose)
  print(paste0("Dimensions of source data pushed to toxval: ", toString(dim(res))))
  runInsertTable(refs, "record_source", toxval.db, verbose)
  print(paste0("Dimensions of references pushed to record_source: ", toString(dim(refs))))

  #####################################################################
  cat("do the post processing\n")
  #####################################################################
  toxval.load.postprocess(toxval.db,source.db,source,do.convert.units=FALSE, remove_null_dtxsid=remove_null_dtxsid)

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
  return(0)

  #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  #++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
}

