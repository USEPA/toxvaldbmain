#--------------------------------------------------------------------------------------
#
#' Load ToxRefdb data to toxval
#' @param toxval.db The database version to use
#' @param source.db The source database
#' @param log If TRUE, send output to a log file
#' @param remove_null_dtxsid If TRUE, delete source records without curated DTXSID value
#' @export
#--------------------------------------------------------------------------------------
toxval.load.toxrefdb2.1 <- function(toxval.db, source.db, log=FALSE, remove_null_dtxsid=TRUE) {
  printCurrentFunction(toxval.db)
  source <- "ToxRefDB"
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
  # Local file provided by ToxRefDB team (v2.1) special query/R Script
  res0 = readxl::read_xlsx(paste0(toxval.config()$datapath, "toxrefdb/toxrefdb_files/toxref-toxval_pod_13AUG2024.xlsx"))

  res = res0 %>%
    dplyr::rename(name = preferred_name,
                  dtxsid = dsstox_substance_id,
                  critical_effect = toxval_effect_list,
                  toxval_type = calc_pod_type,
                  toxval_numeric_qualifier = qualifier,
                  toxval_numeric = mg_kg_day_value,
                  species = species,
                  strain = strain,
                  study_duration_value = dose_end,
                  study_duration_units = dose_end_unit,
                  year = study_year,
                  long_ref = study_citation,
                  exposure_route = admin_route,
                  exposure_method = admin_method
    ) %>%
    tidyr::separate(col = toxval_study_source_id,
                    into = c("external_source_id", "lifestage", "generation", "sex", "endpoint_category"),
                    sep = "_",
                    remove = FALSE) %>%
    dplyr::mutate(

      exposure_form = vehicle,
      source = !!source,
      toxval_units = "mg/kg-day",
      sex = dplyr::case_when(
        sex == "M" ~ "male",
        sex == "F" ~ "female",
        sex == "MF" ~ "male/female",
        TRUE ~ sex
      ),
      external_source_id = external_source_id %>%
        gsub("studyid", "", .) %>%
        as.numeric(),
      external_source_id_desc = "ToxRefDB Study ID",
      strain = dplyr::case_when(
        strain %in% c("[Other]") ~ "Other",
        TRUE ~ strain
      ),
      exposure_method = dplyr::case_when(
        exposure_method == "[Not Specified]" ~ "not specified",
        grepl("gavage", exposure_route) ~ "gavage",
        TRUE ~ exposure_method
      ) %>%
        tolower() %>%
        gsub("\\/intubation", "", .),
      exposure_route = dplyr::case_when(
        grepl("gavage", exposure_route) ~ "oral",
        exposure_route == "Direct" ~ "injection",
        TRUE ~ exposure_route
      ) %>%
        tolower(),
      exposure_form = dplyr::case_when(
        exposure_form %in% c("None", "none") ~ NA,
        TRUE ~ exposure_form
      ) %>%
        tolower(),
      study_duration_units = dplyr::case_when(
        # Keeping GD and PND as-is for clarity on the exposure (e.g., through mother if it's not clear elsewhere)
        # study_duration_units %in% c("GD", "PND") ~ "days",
        # grepl("day", study_duration_units) ~ "days",
        study_duration_units == "day (PND)" ~ "PND",
        grepl("week", study_duration_units) ~ "weeks",
        TRUE ~ study_duration_units
      ),
      critical_effect = critical_effect %>%
        dplyr::na_if("NA"),
      endpoint_category = endpoint_category %>%
        dplyr::na_if("NA"),

      # Set correct study_duration for DEV/MGR entries
      study_duration_value = dplyr::case_when(
        study_type %in% c("DEV", "MGR") ~ stringr::str_c(dose_start, "-", study_duration_value),
        TRUE ~ as.character(study_duration_value)
      ),
      study_duration_units = dplyr::case_when(
        study_type %in% c("DEV", "MGR") ~ stringr::str_c(dose_start_unit, "-", study_duration_units),
        TRUE ~ study_duration_units
      ),

      study_type = dplyr::case_when(
        study_type == "SUB" ~ "subchronic",
        study_type == "CHR" ~ "chronic",
        study_type == "DEV" ~ "developmental",
        study_type == "MGR" ~ "reproductive",
        study_type == "SAC" ~ "subacute",
        study_type == "DNT" ~ "developmental neurotoxicity",
        study_type == "NEU" ~ "neurotoxicity",
        study_type == "REP" ~ "reproductive",
        study_type == "OTH" ~ "OTH",
        study_type == "ACU" ~ "acute",
        TRUE ~ study_type
      ),
    ) %>%
    tidyr::drop_na(toxval_type, toxval_numeric, toxval_units) %>%
    tidyr::unite("critical_effect", endpoint_category, critical_effect, sep=": ", na.rm=TRUE, remove=FALSE) %>%
    dplyr::mutate(
      # Carry over dynamic generation value by using two separate assignments
      critical_effect = stringr::str_replace_all(critical_effect, "\\|", stringr::str_c("\\|", endpoint_category, ": ")),
      dplyr::across(dplyr::where(is.character), ~tidyr::replace_na(.x, '-') %>%
                      dplyr::na_if("NA") %>%
                      dplyr::na_if(""))
    )

  # Manually fix special case identified
  # Set study duration from "54465 weeks" to "104 weeks"
  res$study_duration_value[res$study_duration_value %in% c(54465)] = 104

  # View(res %>% select(admin_route, admin_method, vehicle, exposure_route, exposure_method, exposure_form) %>% distinct())
  cat("set the source_hash\n")
  # Add source_hash_temp column
  res[, toxval.config()$hashing_cols[!toxval.config()$hashing_cols %in% names(res)]] <- "-"

  # Perform deduping
  res = toxval.load.dedup(res)
  hashing_cols = c(toxval.config()$hashing_cols)
  res.temp = source_hash_vectorized(res, hashing_cols)
  res$source_hash = res.temp$source_hash

  chem_map = source_chemical.toxrefdb(toxval.db, source.db,
                                      res=res %>%
                                        dplyr::select(casrn, name, dtxsid) %>%
                                        dplyr::distinct(),
                                      source, chem.check.halt=FALSE,
                                 casrn.col="casrn", name.col="name", verbose=FALSE)

  # Map back chemical information to all records
  res <- res %>%
    dplyr::left_join(chem_map %>%
                dplyr::select(-chemical_index, -dtxsid),
              by = c("name", "casrn"))

  # Remove intermediate
  rm(chem_map)

  #####################################################################
  cat("Add the code from the original version from Aswani\n")
  #####################################################################

  cremove = c("study_source","chemical_index",
              "study_id", "toxval_study_source_id", "endpoint_category",
              "dose_level", "vehicle", "study_duration_qualifier")

  res = res[ , !(names(res) %in% cremove)]

  res = res %>%
    dplyr::mutate(
      study_duration_value_original = study_duration_value,
      study_duration_value = as.numeric(study_duration_value)
    )

  #####################################################################
  cat("find columns in res that do not map to toxval or record_source\n")
  #####################################################################
  cols1 = runQuery("desc record_source",toxval.db)[,1]
  cols2 = runQuery("desc toxval",toxval.db)[,1]
  cols = unique(c(cols1,cols2))
  colnames(res)[which(names(res) == "species")] = "species_original"
  res = res[ , !(names(res) %in% c("record_url","short_ref"))]
  nlist = names(res)
  nlist = nlist[!nlist %in% c("casrn","name",
                              # Do not remove fields that would become "_original" fields
                              unique(gsub("_original", "", cols)))]
  nlist = nlist[!nlist %in% cols]

  # Remove columns that are not used in toxval
  res = res %>% dplyr::select(!dplyr::any_of(nlist))

  # Check if any non-toxval column still remaining in nlist
  nlist = names(res)
  nlist = nlist[!nlist %in% c("casrn","name",
                              # Do not remove fields that would become "_original" fields
                              unique(gsub("_original", "", cols)))]
  nlist = nlist[!nlist %in% cols]

  if(length(nlist)>0) {
    cat("columns to be dealt with\n")
    print(nlist)
    browser()
  }
  print(dim(res))

  #####################################################################
  cat("Generic steps \n")
  #####################################################################
  res = dplyr::distinct(res)
  res = res[!is.na(res$toxval_numeric),]
  res = res[res$toxval_numeric>0,]
  res = fill.toxval.defaults(toxval.db,res)
  res = generate.originals(toxval.db,res)
  if("species_original" %in% names(res)) res$species_original = tolower(res$species_original)

  res$toxval_numeric = as.numeric(res$toxval_numeric)
  print(paste0("Dimensions of source data after originals added: ", toString(dim(res))))
  res=fix.non_ascii.v2(res,source)
  # Remove excess whitespace
  res = res %>%
    dplyr::mutate(dplyr::across(where(is.character),
                                ~stringr::str_squish(.) %>%
                                  dplyr::na_if("NA")
                                )
                  ) %>%
    dplyr::distinct()

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
  res = dplyr::distinct(res)
  refs = dplyr::distinct(refs)
  res$datestamp = Sys.Date()
  res$source_table = source_table
  res$source_url = "https://github.com/USEPA/CompTox-ToxRefDB"
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
}
