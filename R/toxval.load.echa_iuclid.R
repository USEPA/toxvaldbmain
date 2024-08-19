#--------------------------------------------------------------------------------------
#' Loading the ECHA IUCLID data to toxval from toxval_source
#' This method is different from most because there are multiple tables (one per study
#' type) for this source
#' @param toxval.db The database version to use
#' @param source.db The source database
#' @param log If TRUE, send output to a log file
#' @param remove_null_dtxsid If TRUE, delete source records without curated DTXSID value
#' @export
#--------------------------------------------------------------------------------------
toxval.load.echa_iuclid <- function(toxval.db, source.db, log=FALSE, remove_null_dtxsid=TRUE) {
  source = "ECHA IUCLID"

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

  # Create translation dictionary to go from table name to human-readable name
  name_translator = c(
    # "source_iuclid_acutetoxicitydermal"="Acute Toxicity Dermal",
    # "source_iuclid_acutetoxicityinhalation"="Acute Toxicity Inhalation",
    # "source_iuclid_acutetoxicityoral"="Acute Toxicity Oral",
    # "source_iuclid_acutetoxicityotherroutes"="Acute Toxicity Other Routes",
    ## "source_iuclid_carcinogenicity"="Carcinogenicity",
    # "source_iuclid_repeateddosetoxicitydermal"="Repeated Dose Toxicity Dermal",
    # "source_iuclid_repeateddosetoxicityinhalation"="Repeated Dose Toxicity Inhalation",
    "source_iuclid_repeateddosetoxicityoral"="Repeated Dose Toxicity Oral"#,
    # "source_iuclid_repeateddosetoxicityother"="Repeated Dose Toxicity Other",
    # "source_iuclid_toxicitytoaquaticalgae"="Toxicity to Aquatic Algae",
    # "source_iuclid_toxicitytoaquaticplant"="Toxicity to Aquatic Plants",
    # "source_iuclid_toxicitytobirds"="Toxicity to Birds",
    # "source_iuclid_toxicitytomicroorganisms"="Toxicity to Microorganisms",
    # "source_iuclid_toxicitytootherabovegroundorganisms"="Toxicity to Other Aboveground Organisms",
    # "source_iuclid_toxicitytootheraqua"="Toxicity to Other Aquatic Organisms",
    # "source_iuclid_toxicitytosoilmacroorganismsexceptarthropods"="Toxicity to Soil Macroorganisms Except Arthropods",
    # "source_iuclid_toxicitytosoilmicroorganisms"="Toxicity to Soil Microorganisms",
    # "source_iuclid_toxicitytoterrestrialarthropods"="Toxicity to Terrestrial Arthropods",
    # "source_iuclid_toxicitytoterrestrialplants"="Toxicity to Terrestrial Plants",
    # "source_iuclid_endocrinedisruptermammalianscreening"="Endocrine Disrupter Mammalian Screening",
    # "source_iuclid_eyeirritation"="Eye Irritation",
    ## "source_iuclid_immunotoxicity"="Immunotoxicity",
    # "source_iuclid_longtermtoxicitytoaquainv"="Long Term Toxicity to Aquatic Invertebrates",
    # "source_iuclid_longtermtoxtofish"="Long Term Toxicity to Fish",
    ## "source_iuclid_neurotoxicity"="Neurotoxicity",
    # "source_iuclid_sedimenttoxicity"="Sediment Toxicity",
    # "source_iuclid_shorttermtoxicitytoaquainv"="Short Term Toxicity to Aquatic Invertebrates",
    # "source_iuclid_shorttermtoxicitytofish"="Short Term Toxicity to Fish",
    # "source_iuclid_skinirritationcorrosion"="Skin Irritation Corrosion",
    # "source_iuclid_skinsensitisation"="Skin Sensitisation",
    ## "source_iuclid_developmentaltoxicityteratogenicity"="Developmental Toxicity Teratogenicity",
    ## "source_iuclid_toxicityreproduction"="Toxicity Reproduction"
  )

  # Get list of IUCLID tables in toxval_source
  iuclid_source_tables = runQuery("SHOW TABLES", source.db) %>%
    dplyr::filter(Tables_in_res_toxval_source_v5 %in% names(name_translator)) %>%
    dplyr::pull(Tables_in_res_toxval_source_v5)

  # Get list of IUCLID OHTs represented in ToxVal
  iuclid_toxval_ohts = runQuery("SELECT DISTINCT source_table FROM toxval", toxval.db) %>%
    dplyr::filter(grepl("iuclid", source_table)) %>%
    dplyr::pull(source_table)

  # Load each source
  for(oht in iuclid_source_tables) {
    # Check if source has already been loaded
    # if(oht %in% iuclid_toxval_ohts) {
    #   # Get load/import dates
    #   source_date = as.POSIXct(unique(runQuery(paste0("SELECT create_time FROM ",
    #                                                   oht), source.db))$create_time[1])
    #   toxval_date = as.POSIXct(unique(runQuery(paste0("SELECT datestamp FROM toxval WHERE source_table='",
    #                                                   oht, "'"), toxval.db))$datestamp[1])
    #
    #   # If ToxVal data is newer than source data, skip loading this OHT
    #   if(toxval_date > source_date) {
    #     next
    #   }
    # }

    # Get human-readable name for OHT
    ohtname = name_translator[[oht]]

    #####################################################################
    cat("++++++++++++++++++++++++++++++++++++++++++++++\n")
    cat("OHT:",ohtname,"\n")
    cat("++++++++++++++++++++++++++++++++++++++++++++++\n\n")
    #####################################################################
    source_table = oht
    oht = gsub("source_iuclid_", "", oht)
    subsource = ohtname
    chem_source = paste0("IUCLID_",oht)

    count = runQuery(paste0("select count(*) from toxval where source='",source,"' and subsource='",subsource,"'"),toxval.db)[1,1]
    if(count>0) {
      runQuery(paste0("delete from toxval_notes where toxval_id in (select toxval_id from toxval where source='",source,"' and subsource='",subsource,"')"),toxval.db)
      runQuery(paste0("delete from toxval_qc_notes where toxval_id in (select toxval_id from toxval where source='",source,"' and subsource='",subsource,"')"),toxval.db)
      runQuery(paste0("delete from record_source where toxval_id in (select toxval_id from toxval where source='",source,"' and subsource='",subsource,"')"),toxval.db)
      runQuery(paste0("delete from toxval_uf where toxval_id in (select toxval_id from toxval where source='",source,"' and subsource='",subsource,"')"),toxval.db)
      runQuery(paste0("delete from toxval_uf where parent_id in (select toxval_id from toxval where source='",source,"' and subsource='",subsource,"')"),toxval.db)
      runQuery(paste0("delete from toxval_relationship where toxval_id_1 in (select toxval_id from toxval where source='",source,"' and subsource='",subsource,"')"),toxval.db)
      runQuery(paste0("delete from toxval_relationship where toxval_id_2 in (select toxval_id from toxval where source='",source,"' and subsource='",subsource,"')"),toxval.db)
      runQuery(paste0("delete from toxval where source='",source,"' and subsource='",subsource,"'"),toxval.db)
    }

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
    if(!nrow(res)) {
      cat(oht,"NO ENTRIES! Try querying without removing null dtxsid.\n\n")
      next
    }
    res = res[,!names(res) %in% toxval.config()$non_hash_cols[!toxval.config()$non_hash_cols %in%
                                                                c("chemical_id", "document_name", "source_hash", "qc_status")]]
    res$source = source
    res$subsource = subsource
    print(paste0("Dimensions of source data: ", toString(dim(res))))

    #####################################################################
    cat("Add code to deal with specific issues for this source\n")
    #####################################################################

    # Add subsource_url from source_url
    res = res %>%
      dplyr::rename(subsource_url = source_url)

    # Rename glp/guideline field to be pulled into record_source
    if("record_source_glp" %in% names(res)) {
      res = res %>%
        dplyr::rename(glp = record_source_glp)
    }
    if("record_source_guideline" %in% names(res)) {
      res = res %>%
        dplyr::rename(guideline = record_source_guideline)
    }

    #####################################################################
    cat("find columns in res that do not map to toxval or record_source\n")
    #####################################################################
    cols1 = runQuery("desc record_source",toxval.db)[,1]
    cols2 = runQuery("desc toxval",toxval.db)[,1]
    cols = unique(c(cols1,cols2))
    colnames(res)[which(names(res) == "species")] = "species_original"
    res = res[ , !(names(res) %in% c("record_url","short_ref"))]
    nlist = names(res)
    nlist = nlist[!nlist %in% c("casrn","name", "range_relationship_id",
                                # Do not remove fields that would become "_original" fields
                                unique(gsub("_original", "", cols)))]
    nlist = nlist[!nlist %in% cols]

    # Remove columns that are not used in toxval
    res = res %>% dplyr::select(!dplyr::any_of(nlist))

    # Check if any non-toxval column still remaining in nlist
    nlist = names(res)
    nlist = nlist[!nlist %in% c("casrn","name", "range_relationship_id",
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

    # Flag and filter out impossible toxval_numeric values
    maxval = max(res$toxval_numeric,na.rm=TRUE)
    cutoff = 1E8
    bad = res[res$toxval_numeric>cutoff,]
    cat("Max toxval_numeric: ",maxval," : Records above threshold - ",nrow(bad),"\n")
    if(nrow(bad)){
      file = paste0(toxval.config()$datapath,"echa_iuclid/bad values ",ohtname," ",Sys.Date(),".xlsx")
      openxlsx::write.xlsx(bad, file)
      # browser()
    }
    # Filter out
    res = res[res$toxval_numeric<cutoff,]

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
    cat("Set the toxval_relationship for separated toxval_numeric range records\n")
    #####################################################################
    if("range_relationship_id" %in% names(res)) {
      relationship_initial = res %>%
        dplyr::filter(grepl("Range", toxval_subtype),
                      !range_relationship_id %in% c("-"))

      # Add check for filtered values
      if(nrow(relationship_initial)) {
        relationship = relationship_initial %>%
          tidyr::separate_rows(
            range_relationship_id,
            sep = " \\|::\\| "
          ) %>%
          dplyr::select(toxval_id, range_relationship_id, toxval_subtype) %>%
          tidyr::pivot_wider(id_cols = "range_relationship_id", names_from=toxval_subtype, values_from = toxval_id) %>%
          dplyr::rename(toxval_id_1 = `Lower Range`,
                        toxval_id_2 = `Upper Range`) %>%
          dplyr::mutate(relationship = "toxval_numeric range") %>%
          dplyr::select(-range_relationship_id)

        # Insert range relationships into toxval_relationship table
        if(nrow(relationship)){
          runInsertTable(mat=relationship, table='toxval_relationship', db=toxval.db)
        }
      }

      # Remove range_relationship_id
      res <- res %>%
        dplyr::select(-range_relationship_id)
    }

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
    res$source_url = "https://echa.europa.eu/"
    res$details_text = paste(source,"Details")
    runInsertTable(res, "toxval", toxval.db, verbose)
    print(paste0("Dimensions of source data pushed to toxval: ", toString(dim(res))))
    runInsertTable(refs, "record_source", toxval.db, verbose)
    print(paste0("Dimensions of references pushed to record_source: ", toString(dim(refs))))

    #####################################################################
    cat("do the post processing\n")
    #####################################################################
    cat(ohtname,"\n")
    toxval.load.postprocess(toxval.db,source.db,source,do.convert.units=FALSE,chem_source,subsource)
  }
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
