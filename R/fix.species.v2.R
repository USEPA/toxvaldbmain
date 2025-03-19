#--------------------------------------------------------------------------------------
#'
#' Set the species_id column in toxval
#'
#' This function replaces fix.species
#' This function precedes toxvaldb.load.species
#'
#' @param toxval.db The version of the database to use
#' @param source The source to be fixed
#' @param subsource The subsource to be fixed (NULL default)
#' @param date_string The date version of the dictionary
#' @export
#--------------------------------------------------------------------------------------
fix.species.v2 <- function(toxval.db,source=NULL,subsource=NULL,date_string="2023-05-18") {
  printCurrentFunction()
  # Read species dictionary files
  file =paste0(toxval.config()$datapath,"species/ecotox_species_dictionary_",date_string,".xlsx")
  dict = openxlsx::read.xlsx(file)
  file = paste0(toxval.config()$datapath,"species/ecotox_species_synonyms_",date_string,".xlsx")
  synonyms = openxlsx::read.xlsx(file)
  file = paste0(toxval.config()$datapath,"species/toxvaldb_extra_species_",date_string,".xlsx")
  extra = openxlsx::read.xlsx(file)

  # Prepare species dictionary
  dict2 = extra[,c("species_id","common_name","latin_name","ecotox_group")]
  dict2 = dict2[!is.element(dict2$species_id,dict$species_id),]
  dict2$kingdom = NA
  dict2$phylum_division = NA
  dict2$subphylum_div = NA
  dict2$superclass = NA
  dict2$class = NA
  dict2$tax_order = NA
  dict2$family = NA
  dict2$genus = NA
  dict2$species = NA
  dict2$subspecies = NA
  dict2$variety = NA
  dict2 = dict2[,names(dict)]
  dict = rbind(dict,dict2)
  dict$latin_name = tolower(dict$latin_name)
  dict$common_name = tolower(dict$common_name)
  synonyms$latin_name = tolower(synonyms$latin_name)
  extra$common_name = tolower(extra$common_name)
  extra$latin_name = tolower(extra$latin_name)
  extra$species_original = tolower(extra$species_original)

  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  for(source in slist) {
    cat(">>> fix.species.v2: ",source,subsource,"\n")

    so.1 = runQuery(paste0("select distinct species_original from toxval where source='",source,"' and species_id in (-1,1000000)",query_addition),
                    toxval.db)[,1]
    so.2 = runQuery(paste0("select distinct species_original from toxval where source='",source,"' and species_id not in (select species_id from species)",query_addition)
                    ,toxval.db)[,1]
    so = c(so.1,so.2)

    count.good = 0
    cat("Start setting species_id:",length(so),"\n")
    if(length(so)>0) {
      # Identify appropriate species_id values
      for(i in 1:length(so)) {
        tag = so[i]
        tag = tolower(tag)
        #tag = str_replace_all(tag,"\'","\\\\'")
        tag0 = tag
        nc = nchar(tag)
        tagend = substr(tag,nc-2,nc)

        if(tagend %in% c(" sp")) {
          tag = paste0(tag,".")
        }

        slist = c("other aquatic arthropod ","other aquatic crustacea: ","other aquatic mollusc: ",
                  "other aquatic worm: ","other,","other,other algae: ","other: ")
        for(x in slist) {
          if(grepl(x, tag)) tag = stringr::str_replace(tag,x,"")
        }
        sid = -1
        if(is.element(tag,dict$common_name)) {
          sid = dict[is.element(dict$common_name,tag),"species_id"][1]
        } else if(is.element(tag,dict$latin_name)) {
          sid = dict[is.element(dict$latin_name,tag),"species_id"][1]
        } else if(is.element(tag,synonyms$latin_name)) {
          sid = dict[is.element(synonyms$latin_name,tag),"species_id"][1]
        } else if(is.element(tag,extra$species_original)) {
          sid = extra[is.element(extra$species_original,tag),"species_id"][1]
        } else if(is.element(tag0,dict$common_name)) {
          sid = dict[is.element(dict$common_name,tag0),"species_id"][1]
        } else if(is.element(tag0,dict$latin_name)) {
          sid = dict[is.element(dict$latin_name,tag0),"species_id"][1]
        } else if(is.element(tag0,synonyms$latin_name)) {
          sid = dict[is.element(synonyms$latin_name,tag0),"species_id"][1]
        } else if(is.element(tag0,extra$species_original)) {
          sid = extra[is.element(extra$species_original,tag0),"species_id"][1]
        } else if(is.element(tag,extra$species_original)) {
          sid = extra[is.element(extra$species_original,tag),"species_id"][1]
        }
        cat(tag,sid,"\n")
        # Update toxval entries with identified species_id values
        if(sid>=0) {
          count.good = count.good+1
          query = paste0("update toxval set species_id=",sid," where source='",source,"'",query_addition," and species_original='",stringr::str_replace_all(tag0,"\\\'","\\\\'"),"'")
          runQuery(query,toxval.db)
        } else {
          cat(tag,"\n")
          #browser()
        }
        if(i%%100==0) cat("finished",i,"out of",length(so),":",count.good,"\n")
      }
    }
    # Handle edge cases
    runQuery("update toxval set species_id=4510 where species_id=23410",toxval.db)
    runQuery("update toxval set species_id=1000000 where species_id=-1",toxval.db)
  }
  # Get species_id for 'Human'
  human_id = runQuery("SELECT DISTINCT species_id FROM species WHERE common_name='Human'", toxval.db)[[1]]

  # Handle ATSDR MRLs special human MRL case
  if(source == "ATSDR MRLs"){
    query = paste0("UPDATE toxval SET species_id = ", human_id, " ",
                   "WHERE source = 'ATSDR MRLs' AND toxval_type = 'MRL'")
    runQuery(query, toxval.db)
  }

  # Handle Health Canada special human case
  if(source == "Health Canada"){
    query = paste0("UPDATE toxval SET species_id = ", human_id, " ",
                   "WHERE source='Health Canada' ",
                   "AND toxval_type IN ",
                   "('TDI', 'ADI', 'cancer slope factor', 'cancer unit risk',",
                   " 'UL', 'tolerable concentration in air')")
    runQuery(query, toxval.db)
  }

  # Set species_id to human for specified sources
  human_source_list = c("EPA AEGL", "EPA OW NPDWR", "EPA OW NRWQC-HHC", "FDA CEDI",
                        "Mass. Drinking Water Standards", "NIOSH", "OSHA Air contaminants",
                        "OW Drinking Water Standards", "Pennsylvania DEP ToxValues", "RSL", "USGS HBSL",
                        "EPA OW NPDWR")

  if(source %in% human_source_list){
    query = paste0("UPDATE toxval SET species_id=", human_id, " ",
                   "WHERE source='", source, "'")
    runQuery(query, toxval.db)
  }

  # Set species_id to human for specified toxval_type
  human_tt_list = c("ADI", "air contaminant limit", "cancer slope factor", "cancer unit risk",
                    "carcinogenic HHBP", "CCC", "CMC", "DWEL", "HBSL", "HHBP", "HHBP (Organism)",
                    "HHBP (Water+Organism)", "IDLH", "Level of Distinct Odor Awareness (LOA)", "MCL",
                    "MCL-based SSL, groundwater", "MCLG", "MMCL", "MRDL", "MRL", "ORSG", "PMTDI",
                    "PTWI", "risk-based SSL, groundwater", "screening level (industrial air)",
                    "screening level (industrial soil)", "screening level (MCL)", "screening level (residential air)",
                    "screening level (residential soil)", "screening level (tap water)", "SMCL", "TDI",
                    "PAD (RfD)")

  query = paste0("UPDATE toxval SET species_id=", human_id, " ",
                 "WHERE source='", source, "' ",
                 "AND (toxval_type IN ('", paste0(human_tt_list, collapse="', '"), "') ",
                 "OR toxval_type_original IN ('", paste0(human_tt_list, collapse="', '"), "'))")
  runQuery(query, toxval.db)
  # Handle special AEGL case
  query = paste0("UPDATE toxval SET species_id=", human_id, " ",
                 "WHERE source='", source, "' ",
                 "AND (toxval_type LIKE 'AEGL%' OR toxval_type_original LIKE 'AEGL%')")
  runQuery(query, toxval.db)

  # QC fail entries with out of scope species
  out_of_scope = readxl::read_xlsx(paste0(toxval.config()$datapath, "species/out_of_scope_species_ToxValDB.xlsx")) %>%
    dplyr::pull(common_name) %>%
    unique() %>%
    paste0(collapse="', '")
  query = paste0("UPDATE toxval a LEFT JOIN species b ON a.species_id=b.species_id ",
                 "SET a.qc_status = CASE ",
                 "WHEN a.qc_status like '%species out of scope%' THEN a.qc_status ",
                 "WHEN a.qc_status LIKE '%fail%' THEN CONCAT(a.qc_status, '; species out of scope') ",
                 "ELSE 'fail: species out of scope' ",
                 "END ",
                 "WHERE b.common_name IN ('", out_of_scope, "') or ",
                 "a.species_original IN ('", out_of_scope, "') ",
                 "AND a.source='", source, "'",
                 query_addition %>% gsub("subsource", "a.subsource", .))
  runQuery(query, toxval.db)

  # Handle cases where two entries with the same species_original have different species_id values
  fix.species.duplicates(toxval.db, source, subsource)

  #####################################################################
  cat("generate export for entries with 'Not Specified' species\n")
  #####################################################################
  query = paste0("SELECT a.source, a.source_hash, a.species_original, ",
                 "a.toxval_type_original, a.study_type_original ",
                 "FROM toxval a INNER JOIN species b ON a.species_id=b.species_id ",
                 "WHERE b.common_name LIKE '%Not Specified%' ",
                 "AND a.source='", source, "' ",
                 "AND qc_status not like '%fail%' ",
                 query_addition %>% gsub("subsource", "a.subsource", .)
                 )
  not_specified = runQuery(query, toxval.db)

  if(nrow(not_specified)) {
    out_file = paste0("Repo/dictionary/missing/missing_", source, "_", subsource, "_species.xlsx") %>%
      gsub("__", "_", .)
    cat(nrow(not_specified), " entries have 'Not Specified' species\n")
    writexl::write_xlsx(not_specified, out_file)
  }
}
