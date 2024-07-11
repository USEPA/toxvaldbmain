#--------------------------------------------------------------------------------------
#' Load DOE Wildlife Benchmarks data from toxval_source to toxval
#' @param toxval.db The database version to use
#' @param source.db The source database
#' @param log If TRUE, send output to a log file
#' @param remove_null_dtxsid If TRUE, delete source records without curated DTXSID value
#--------------------------------------------------------------------------------------
toxval.load.doe.benchmarks <- function(toxval.db, source.db, log=FALSE, remove_null_dtxsid=TRUE){
  source = "DOE Wildlife Benchmarks"
  source_table = "source_doe_benchmarks"
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
  # Whether to remove records with NULL DTXSID values
  if(!remove_null_dtxsid){
    query = paste0("select * from ",source_table)
  } else {
    query = paste0("select * from ",source_table, " ",
                   # Filter out records without curated chemical information
                   "WHERE chemical_id IN (SELECT chemical_id FROM source_chemical WHERE dtxsid is NOT NULL)")
  }
  res = runQuery(query,source.db,TRUE,FALSE)
  res = res[,!names(res) %in% toxval.config()$non_hash_cols[!toxval.config()$non_hash_cols %in%
                                                              c("chemical_id", "document_name", "source_hash", "qc_status")]]
  res$source = source
  res$details_text = paste(source,"Details")
  print(paste0("Dimensions of source data: ", toString(dim(res))))

  #####################################################################
  cat("Add code to deal with specific issues for this source\n")
  #####################################################################

  # Remove unnecessary cols (keep species_relationship_id for now)
  cremove = c("source_name_sid", "data_collection", "source_name_cid", "analyte",
              "form", "species_type", "source_field", "study_duration_qualifier")
  res = res[ , !(names(res) %in% cremove)]

  # Set redundant subsource_url values to "-"
  res = res %>%
    dplyr::mutate(
      subsource_url = dplyr::case_when(
        subsource_url == source_url ~ "-",
        TRUE ~ subsource_url
      )
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
  nlist = nlist[!is.element(nlist,c("casrn","name","species_relationship_id"))]
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
  res = dplyr::distinct(res)
  res = fill.toxval.defaults(toxval.db,res)
  res = generate.originals(toxval.db,res)
  res$toxval_numeric = as.numeric(res$toxval_numeric)
  print(paste0("Dimensions of source data after originals added: ", toString(dim(res))))
  res=fix.non_ascii.v2(res,source)
  # Remove excess whitespace
  res = res %>%
    dplyr::mutate(dplyr::across(tidyselect::where(is.character), stringr::str_squish))
  res = dplyr::distinct(res)
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
  cat("add record linkages to toxval_relationship\n")
  #####################################################################

  # # Initial approach. Works, but is slower
  # # Initialize empty tibble in format of toxval_relationship table
  # linkage_tibble = tibble::tibble(
  #   toxval_id_1 = numeric(),
  #   toxval_id_2 = numeric(),
  #   relationship = character()
  # )
  #
  # # Build tibble with linkage data
  # for (i in unique(res$species_relationship_id)) {
  #   # Filter out entries with different linkage id
  #   curr_linkage_tibble = dplyr::filter(res, species_relationship_id == i)
  #   # curr_linkage_tibble = dplyr::filter(res, species_relationship_id == 1)
  #
  #   # Check for relationships
  #   test_noael = dplyr::filter(curr_linkage_tibble, toxval_subtype == "Test Species NOAEL")
  #   test_loael = dplyr::filter(curr_linkage_tibble, toxval_subtype == "Test Species LOAEL")
  #   endpoint_noael = dplyr::filter(curr_linkage_tibble, grepl("Endpoint Species NOAEL", toxval_subtype))
  #   endpoint_loael = dplyr::filter(curr_linkage_tibble, grepl("Endpoint Species LOAEL", toxval_subtype))
  #
  #   # Check for relationships and add data to linkage_tibble
  #   # Test NOAEL to Test LOAEL
  #   if (nrow(test_noael) > 0 & nrow(test_loael) > 0) {
  #     linkage_tibble = linkage_tibble %>% tibble::add_row(toxval_id_1 = as.numeric(test_noael$toxval_id[1]),
  #                                                         toxval_id_2 = as.numeric(test_loael$toxval_id[1]),
  #                                                         relationship = "Test NOAEL to Test LOAEL")
  #   }
  #   # Endpoint NOAEL to Endpoint LOAEL
  #   if (nrow(endpoint_noael) > 0 & nrow(endpoint_loael) > 0) {
  #     linkage_tibble = linkage_tibble %>% tibble::add_row(toxval_id_1 = as.numeric(endpoint_noael$toxval_id[1]),
  #                                                         toxval_id_2 = as.numeric(endpoint_loael$toxval_id[1]),
  #                                                         relationship = "Endpoint NOAEL to Endpoint LOAEL")
  #   }
  #   # Test NOAEL to Endpoint NOAEL
  #   if (nrow(test_noael) > 0 & nrow(endpoint_noael) > 0) {
  #     linkage_tibble = linkage_tibble %>% tibble::add_row(toxval_id_1 = as.numeric(test_noael$toxval_id[1]),
  #                                                         toxval_id_2 = as.numeric(endpoint_noael$toxval_id[1]),
  #                                                         relationship = "Test NOAEL to Endpoint NOAEL")
  #   }
  #   # Test LOAEL to Endpoint LOAEL
  #   if (nrow(test_loael) > 0 & nrow(endpoint_loael) > 0) {
  #     linkage_tibble = linkage_tibble %>% tibble::add_row(toxval_id_1 = as.numeric(test_loael$toxval_id[1]),
  #                                                         toxval_id_2 = as.numeric(endpoint_loael$toxval_id[1]),
  #                                                         relationship = "Test LOAEL to Endpoint LOAEL")
  #   }
  # }

  # Faster dplyr/tidyr approach
  N2L = res %>%
    # Expand collapsed species_relationship_id
    tidyr::separate_rows(species_relationship_id, sep = ", ") %>%
    dplyr::mutate(species_relationship_id = species_relationship_id %>%
                    stringr::str_squish() %>%
                    as.numeric()) %>%
    dplyr::select(toxval_id, species_relationship_id, toxval_type, toxval_subtype) %>%
    dplyr::mutate(toxval_subtype = toxval_subtype %>%
                    gsub("NOAEL|LOAEL|Species|Food|Water|Piscivore|,", "", .) %>%
                    stringr::str_squish(),
                  toxval_type = toxval_type %>%
                    gsub("\\(ADJ\\)", "", .) %>%
                    stringr::str_squish()) %>%
    dplyr::group_by(species_relationship_id, toxval_subtype) %>%
    dplyr::mutate(toxval_relationship = paste0(toxval_id, collapse = ", "),
                  relationship = paste(toxval_subtype, "NOAEL", "to", toxval_subtype, "LOAEL", sep = " ") %>%
                    stringr::str_squish()) %>%
    dplyr::ungroup() %>%
    dplyr::filter(grepl(",", toxval_relationship)) %>%
    dplyr::select(toxval_relationship, relationship) %>%
    dplyr::distinct() %>%
    tidyr::separate(col="toxval_relationship", into=c("toxval_id_1", "toxval_id_2"), sep = ", ") %>%
    dplyr::mutate(dplyr::across(c("toxval_id_1", "toxval_id_2"), ~as.numeric(.)))

  N2NL2L = res %>%
    # Expand collapsed species_relationship_id
    tidyr::separate_rows(species_relationship_id, sep = ", ") %>%
    dplyr::mutate(species_relationship_id = species_relationship_id %>%
                    stringr::str_squish() %>%
                    as.numeric()) %>%
    dplyr::select(toxval_id, species_relationship_id, toxval_type, toxval_subtype) %>%
    dplyr::mutate(toxval_subtype = toxval_subtype %>%
                    gsub("NOAEL|LOAEL|Species|Food|Water|Piscivore|,", "", .) %>%
                    stringr::str_squish(),
                  toxval_type = toxval_type %>%
                    gsub("\\(ADJ\\)", "", .) %>%
                    stringr::str_squish()) %>%
    dplyr::group_by(species_relationship_id, toxval_type) %>%
    dplyr::mutate(toxval_relationship = paste0(toxval_id, collapse = ", "),
                  relationship = paste("Test", toxval_type, "to", "Endpoint", toxval_type, sep = " ") %>%
                    stringr::str_squish()
    ) %>%
    dplyr::ungroup() %>%
    dplyr::filter(grepl(",", toxval_relationship)) %>%
    dplyr::select(toxval_relationship, relationship) %>%
    dplyr::distinct() %>%
    tidyr::separate(col="toxval_relationship", into=c("toxval_id_1", "toxval_id_2"), sep = ", ") %>%
    dplyr::mutate(dplyr::across(c("toxval_id_1", "toxval_id_2"), ~as.numeric(.)))

  linkage_tibble2 = dplyr::bind_rows(N2L, N2NL2L) %>%
    # Add species terms to relationship text
    dplyr::mutate(
      relationship = relationship %>%
        gsub("Test", "Test Species", .) %>%
        gsub("(?:Target )?Endpoint", "Target Species Endpoint", .)
    )


  # Prove both methods produce the same results
  # identical(linkage_tibble %>%
  #             arrange(toxval_id_1, relationship),
  #           linkage_tibble2 %>%
  #             arrange(toxval_id_1, relationship))

  # # Delete earlier entries that should be overwritten
  # runQuery(query = paste0("DELETE FROM toxval_relationship WHERE toxval_id_1 in (",
  #                         toString(unique(linkage_tibble$toxval_id_1)),
  #                         ") AND toxval_id_2 in (",
  #                         unique(toString(linkage_tibble$toxval_id_2)), ")"),
  #          db = toxval.db)

  # Send linkage data to ToxVal
  runInsertTable(linkage_tibble2, "toxval_relationship", toxval.db)

  # Remove species_relationship_id column from res
  res = res %>% dplyr::select(-species_relationship_id)

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
  refs$record_source_type = "website"
  refs$record_source_note = "to be cleaned up"
  refs$record_source_level = "primary (risk assessment values)"
  print(paste0("Dimensions of references after adding ref columns: ", toString(dim(refs))))

  #####################################################################
  cat("load res and refs to the database\n")
  #####################################################################
  res = dplyr::distinct(res)
  refs = dplyr::distinct(refs)
  res$datestamp = Sys.Date()
  res$source_table = source_table
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
