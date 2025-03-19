
#--------------------------------------------------------------------------------------
#' Load new_caloehha from toxval_source to toxval
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source.db The source database to use.
#' @param log If TRUE, send output to a log file
#' @export
#--------------------------------------------------------------------------------------
toxval.load.caloehha <- function(toxval.db,source.db ,log=F){
  verbose = log
  printCurrentFunction(toxval.db)

  source <- "Cal OEHHA"
  source_table = "source_caloehha"
  #####################################################################
  cat("start output log, log files for each source can be accessed from output_log folder\n")
  #####################################################################
  if(log) {
    con1 = file.path(toxval.config()$datapath,paste0(source,"_",Sys.Date(),".log"))
    con1 = log_open(con1)
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
  query = paste0("select * from ",source_table)
  res = runQuery(query,source.db,T,F)
  res = res[ , !(names(res) %in% c("source_id","clowder_id","parent_hash","create_time","modify_time","created_by"))]
  res = res[ , !(names(res) %in% c("qc_flags","qc_notes","version"))]
  res$source = source
  res$details_text = paste(source,"Details")

  ####################################################################################
  ####################################################################################
  ####################################################################################
  ####################################################################################
  ####################################################################################
  ####################################################################################
  ####################################################################################
  ####################################################################################
  mat = res
  mat[is.null(mat)] <- NA
  name.list <- c("source_hash","casrn","name","toxval_type","toxval_numeric","toxval_units","chemical_id",
                 "species_original","critical_effect",
                 "risk_assessment_class","year","exposure_route",
                 "study_type","study_duration_value","study_duration_units","record_url","document_name")

  row <- as.data.frame(matrix(nrow=1,ncol=length(name.list)))
  names(row) <- name.list
  res <- NULL

  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "inhalation_unit_risk","inhalation_unit_risk_units",
            "inhalation_slope_factor","inhalation_slope_factor_units",
            "oral_slope_factor","oral_slope_factor_units",
            "acute_rel","acute_rel_units","acute_rel_species","acute_rel_critical_effect","acute_rel_target_organ","acute_rel_severity","acute_rel_year",
            "rel_8_hour_inhalation","rel_8_hour_inhalation_units",
            "chronic_inhalation_rel","chronic_inhalation_rel_units",
            "chronic_oral_rel","chronic_oral_rel_units","chronic_rel_critical_effect","chronic_rel_target_organ",
            "mcl","mcl_units",
            "phg","phg_units","phg_year",
            "nsrl_inhalation","nsrl_inhalation_units",
            "nsrl_oral","nsrl_oral_units",
            "madl_inhlation_reprotox","madl_inhlation_reprotox_units",
            "madl_oral_reprotox","madl_oral_reprotox_units","madl_nsrl_year",
            "chrfd","chrfd_units","chrd_year")

  #-------------------------------------------------------------------------------------------------------------
  # Inhalation unit risk
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "inhalation_unit_risk","inhalation_unit_risk_units")
  res1 = mat[,nlist]
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "toxval_numeric","toxval_units")
  names(res1) = nlist
  res1$year = "2022"
  res1 = res1[!is.na(res1$toxval_numeric),]
  res1$toxval_type = "cancer unit risk"
  res1$toxval_subtype = "inhalation unit rosk"
  res1$study_type = "carcinogenicity"
  res1$study_duration_class = "chronic"
  res1$exposure_route = "inhalation"
  res1$critical_effect = "-"
  res1$species_original = "-"

  #-------------------------------------------------------------------------------------------------------------
  # Inhalation slope factor
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "inhalation_slope_factor","inhalation_slope_factor_units")
  res2 = mat[,nlist]
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "toxval_numeric","toxval_units")
  names(res2) = nlist
  res2$year = "2022"
  res2 = res2[!is.na(res2$toxval_numeric),]
  res2$toxval_type = "cancer slope factor"
  res2$toxval_subtype = "inhalaiton slope factor"
  res2$study_type = "carcinogenicity"
  res2$study_duration_class = "chronic"
  res2$exposure_route = "inhalation"
  res2$critical_effect = "-"
  res2$species_original = "-"

  #-------------------------------------------------------------------------------------------------------------
  # oral slope factor
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "oral_slope_factor","oral_slope_factor_units")
  res3 = mat[,nlist]
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "toxval_numeric","toxval_units")
  names(res3) = nlist
  res3$year = "2022"
  res3 = res3[!is.na(res3$toxval_numeric),]
  res3$toxval_type = "cancer slope factor"
  res3$toxval_subtype = "oral slope factor"
  res3$study_type = "carcinogenicity"
  res3$study_duration_class = "chronic"
  res3$exposure_route = "oral"
  res3$critical_effect = "-"
  res3$species_original = "-"

  #-------------------------------------------------------------------------------------------------------------
  # acute REL
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "acute_rel","acute_rel_units","acute_rel_species","acute_rel_critical_effect","acute_rel_target_organ","acute_rel_severity","acute_rel_year")
  res4 = mat[,nlist]
  res4$critical_effect = paste0(res4$acute_rel_critical_effect,"|",res4$acute_rel_target_organ,"|",res4$acute_rel_severity)
  res4 = res4[,c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
                 "acute_rel","acute_rel_units","acute_rel_species","critical_effect","acute_rel_year")]
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "toxval_numeric","toxval_units","species_original","critical_effect","year")
  names(res4) = nlist
  res4 = res4[!is.na(res4$toxval_numeric),]
  res4$toxval_type = "REL"
  res4$toxval_subtype = "acute REL"
  res4$study_type = "acute"
  res4$study_duration_class = "acute"
  res4$exposure_route = "oral"

  #-------------------------------------------------------------------------------------------------------------
  # Inhalation 8 hour REL
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "rel_8_hour_inhalation","rel_8_hour_inhalation_units")
  res5 = mat[,nlist]
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "toxval_numeric","toxval_units")
  names(res5) = nlist
  res5$year = "2022"
  res5 = res5[!is.na(res5$toxval_numeric),]
  res5$toxval_type = "REL"
  res5$toxval_subtype = "8 hour inhalation REL"
  res5$study_type = "acute"
  res5$study_duration_class = "acute"
  res5$exposure_route = "inhalation"
  res5$critical_effect = "-"
  res5$species_original = "-"

  #-------------------------------------------------------------------------------------------------------------
  # Chronic inhalation REL
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "chronic_inhalation_rel","chronic_inhalation_rel_units")
  res6 = mat[,nlist]
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "toxval_numeric","toxval_units")
  names(res6) = nlist
  res6$year = "2022"
  res6 = res6[!is.na(res6$toxval_numeric),]
  res6$toxval_type = "REL"
  res6$toxval_subtype = "chronic inhalation REL"
  res6$study_type = "chronic"
  res6$study_duration_class = "chronic"
  res6$exposure_route = "inhalation"
  res6$critical_effect = "-"
  res6$species_original = "-"

  #-------------------------------------------------------------------------------------------------------------
  # Chronic Oral REL
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "chronic_oral_rel","chronic_oral_rel_units","chronic_rel_critical_effect","chronic_rel_target_organ")
  res7 = mat[,nlist]
  res7$critical_effect = paste0(res7$chronic_rel_critical_effect,"|",res7$chronic_rel_target_organ)
  res7 = res7[,c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
                 "chronic_oral_rel","chronic_oral_rel_units","critical_effect")]
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "toxval_numeric","toxval_units","critical_effect")
  names(res7) = nlist
  res7$year = "2022"
  res7 = res7[!is.na(res7$toxval_numeric),]
  res7$toxval_type = "REL"
  res7$toxval_subtype = "chronic oral REL"
  res7$study_type = "chronic"
  res7$study_duration_class = "chronic"
  res7$exposure_route = "oral"
  res7$species_original = "-"

  #-------------------------------------------------------------------------------------------------------------
  # MCL
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "mcl","mcl_units")
  res8 = mat[,nlist]
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "toxval_numeric","toxval_units")
  names(res8) = nlist
  res8$year = "2022"
  res8 = res8[!is.na(res8$toxval_numeric),]
  res8$toxval_type = "MCL"
  res8$toxval_subtype = "-"
  res8$study_type = "chronic"
  res8$study_duration_class = "chronic"
  res8$exposure_route = "oral"
  res8$critical_effect = "-"
  res8$species_original = "-"

  #-------------------------------------------------------------------------------------------------------------
  # PHG
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "phg","phg_units","phg_year")
  res9 = mat[,nlist]
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "toxval_numeric","toxval_units","year")
  names(res9) = nlist
  res9 = res9[!is.na(res9$toxval_numeric),]
  res9 = res9[res9$toxval_numeric!="-",]
  res9$toxval_numeric = as.numeric(res9$toxval_numeric)
  res9$toxval_type = "OEHHA PHG"
  res9$toxval_subtype = "-"
  res9$study_type = "chronic"
  res9$study_duration_class = "chronic"
  res9$exposure_route = "oral"
  res9$critical_effect = "-"
  res9$species_original = "-"
  #-------------------------------------------------------------------------------------------------------------
  # Inhalation NSRL
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "nsrl_inhalation","nsrl_inhalation_units")
  res10 = mat[,nlist]
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "toxval_numeric","toxval_units")
  names(res10) = nlist
  res10$year = "2022"
  res10 = res10[!is.na(res10$toxval_numeric),]
  res10$toxval_type = "OEHHA NSRL"
  res10$toxval_subtype = "-"
  res10$study_type = "chronic"
  res10$study_duration_class = "chronic"
  res10$exposure_route = "inhalation"
  res10$critical_effect = "-"
  res10$species_original = "-"

  #-------------------------------------------------------------------------------------------------------------
  # Oral NSRL
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
             "nsrl_oral","nsrl_oral_units")
  res11 = mat[,nlist]
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "toxval_numeric","toxval_units")
  names(res11) = nlist
  res11$year = "2022"
  res11 = res11[!is.na(res11$toxval_numeric),]
  res11$toxval_type = "OEHHA NSRL"
  res11$toxval_subtype = "-"
  res11$study_type = "chronic"
  res11$study_duration_class = "chronic"
  res11$exposure_route = "oral"
  res11$critical_effect = "-"
  res11$species_original = "-"

  #-------------------------------------------------------------------------------------------------------------
  # MADL Inhalation
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
             "madl_inhlation_reprotox","madl_inhlation_reprotox_units")
  res12 = mat[,nlist]
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "toxval_numeric","toxval_units")
  names(res12) = nlist
  res12$year = "2022"
  res12 = res12[!is.na(res12$toxval_numeric),]
  res12$toxval_type = "OEHHA MADL"
  res12$toxval_subtype = "-"
  res12$study_type = "chronic"
  res12$study_duration_class = "chronic"
  res12$exposure_route = "inhalation"
  res12$critical_effect = "-"
  res12$species_original = "-"

  #-------------------------------------------------------------------------------------------------------------
  # MADL oral
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
             "madl_oral_reprotox","madl_oral_reprotox_units","madl_nsrl_year")
  res13 = mat[,nlist]
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "toxval_numeric","toxval_units","year")
  names(res13) = nlist
  res13 = res13[!is.na(res13$toxval_numeric),]
  res13$toxval_type = "OEHHA MADL"
  res13$toxval_subtype = "-"
  res13$study_type = "chronic"
  res13$study_duration_class = "chronic"
  res13$exposure_route = "oral"
  res13$critical_effect = "-"
  res13$species_original = "-"

  #-------------------------------------------------------------------------------------------------------------
  # ChRfD
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "chrfd","chrfd_units","chrd_year")
  res14 = mat[,nlist]
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "toxval_numeric","toxval_units","year")
  names(res14) = nlist
  res14 = res14[!is.na(res14$toxval_numeric),]
  res14$toxval_type = "RfD"
  res14$toxval_subtype = "Child RfD"
  res14$study_type = "chronic"
  res14$study_duration_class = "chronic"
  res14$exposure_route = "oral"
  res14$critical_effect = "-"
  res14$species_original = "-"

  #-------------------------------------------------------------------------------------------------------------
  nlist = c("casrn","name","chemical_id","document_name","source", "source_hash","qc_status","details_text",
            "toxval_type","toxval_subtype","toxval_numeric","toxval_units",
            "study_type","study_duration_class","exposure_route","critical_effect","species_original","year")

  res1 = res1[,nlist]
  res2 = res2[,nlist]
  res3 = res3[,nlist]
  res4 = res4[,nlist]
  res5 = res5[,nlist]
  res6 = res6[,nlist]
  res7 = res7[,nlist]
  res8 = res8[,nlist]
  res9 = res9[,nlist]
  res10 = res10[,nlist]
  res11 = res11[,nlist]
  res12 = res12[,nlist]
  res13 = res13[,nlist]
  res14 = res14[,nlist]

  res = rbind(res1,res2,res3,res4,res5,res6,res7,res8,res9,res10,res11,res12,res13,res14)
  res$human_ra = "Y"
  res$target_species = "Human"
  res$species_original = "-"
  res$human_eco = "human health"
  ##########################################################
  cat("remove the redundancy from the source_hash\n")
  ##########################################################
  x=seq(from=1,to=nrow(res))
  y = paste0(res$source_hash,"_",x)
  res$source_hash = y

  res$toxval_numeric_qualifier = "="

  res[res$toxval_units==" (fibers/L water)-1","toxval_units"] = "(fibers/L water)-1"
  res[res$toxval_units==" pCi/L","toxval_units"] = "pCi/L"
  res[res$toxval_units==" million fibers/L","toxval_units"] = "million fibers/L"
  res[res$toxval_units=="mg.L (total chromium)","toxval_units"] = "mg/L (total chromium)"
  res[res$toxval_units==" ug/dL blood","toxval_units"] = "ug/dL blood"

  res = generate.originals(toxval.db,res)

  ##########################################################
  cat("Convert multiple date formats present in year field to the corresponding year value,
      then change the data type from character to integer \n ")
  ###########################################################
  date_fix = excel_numeric_to_date(as.numeric(as.character(res[grep("[0-9]{5}", res$year),'year'])), date_system = "modern")
  date_fix = format(date_fix, format = "%Y")

  res[grep("[0-9]{5}", res$year),'year'] = date_fix
  res[grep("[a-zA-Z]+", res$year),'year'] = gsub(".*\\,\\s+(\\d{4})", "\\1", grep("[a-zA-Z]+", res$year,value= T))
  res[which(res1$year == "-"), "year"] = NA
  res$year = as.integer(res$year)

   res = res[,(names(res) %in% c("source_hash","casrn","name","toxval_type","toxval_subtype","toxval_numeric","toxval_units","species_original","critical_effect",
                                   "risk_assessment_class","year","exposure_route","study_type","study_duration_class","study_duration_value", "study_duration_units",
                                   "record_url","toxval_type_original","toxval_subtype_original","toxval_numeric_original","toxval_units_original","critical_effect_original",
                                   "year_original","exposure_route_original","study_type_original","study_duration_value_original","study_duration_class_original",
                                   "study_duration_units_original","document_name","chemical_id"))]
  res = data.frame(lapply(res, function(x) if(class(x)=="character") trimws(x) else(x)), stringsAsFactors=F, check.names=F)
  res = res[!is.na(res[,"casrn"]),]
  res[is.element(res$species_original,"Human"),"species_original"] = "-"
  #####################################################################
  cat("add other columns to res\n")
  #####################################################################
  res$source = source
  res$human_eco = "human health"
  res$subsource = "California DPH"
  res$details_text = "Cal OEHHA Details"
  res$source_url = "https://oehha.ca.gov/chemicals"
  res$toxval_numeric_qualifier = "="
  res = res[!is.na(res[,"casrn"]),]
  res = fill.toxval.defaults(toxval.db,res)

  res = unique(res)
  #res = res[res$toxval_numeric != "-999",]
  res = unique(res)
  res$toxval_numeric_original = res$toxval_numeric
  ####################################################################################
  ####################################################################################
  ####################################################################################
  ####################################################################################
  ####################################################################################
  ####################################################################################
  ####################################################################################
  ####################################################################################

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
  if(length(nlist)>0) {
    cat("columns to be dealt with\n")
    print(nlist)
    browser()
  }

  #####################################################################
  cat("Generic steps \n")
  #####################################################################
  res = unique(res)
  res = fill.toxval.defaults(toxval.db,res)
  res = generate.originals(toxval.db,res)
  if(is.element("species_original",names(res))) res[,"species_original"] = tolower(res[,"species_original"])
  res$toxval_numeric = as.numeric(res$toxval_numeric)
  print(dim(res))
  res=fix.non_ascii.v2(res,source)
  res = data.frame(lapply(res, function(x) if(class(x)=="character") trimws(x) else(x)), stringsAsFactors=F, check.names=F)
  res = unique(res)
  res = res[,!is.element(names(res),c("casrn","name"))]
  print(dim(res))

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
  refs$record_source_type = "website"
  refs$record_source_note = "to be cleaned up"
  refs$record_source_level = "primary (risk assessment values)"
  print(dim(res))


  #####################################################################
  cat("load res and refs to the database\n")
  #####################################################################
  res = unique(res)
  refs = unique(refs)
  res$datestamp = Sys.Date()
  res$source_table = source_table
  res$source_url = "https://oehha.ca.gov/chemicals"
  res$subsource_url = "-"
  res$details_text = paste(source,"Details")
  #for(i in 1:nrow(res)) res[i,"toxval_uuid"] = UUIDgenerate()
  #for(i in 1:nrow(refs)) refs[i,"record_source_uuid"] = UUIDgenerate()
  runInsertTable(res, "toxval", toxval.db, verbose)
  runInsertTable(refs, "record_source", toxval.db, verbose)
  print(dim(res))

  #####################################################################
  cat("do the post processing\n")
  #####################################################################
  toxval.load.postprocess(toxval.db,source.db,source)

  if(log) {
    #####################################################################
    cat("stop output log \n")
    #####################################################################
    closeAllConnections()
    log_close()
    output_message = read.delim(paste0(toxval.config()$datapath,source,"_",Sys.Date(),".log"), stringsAsFactors = F, header = F)
    names(output_message) = "message"
    output_log = read.delim(paste0(toxval.config()$datapath,"log/",source,"_",Sys.Date(),".log"), stringsAsFactors = F, header = F)
    names(output_log) = "log"
    new_log = log_message(output_log, output_message[,1])
    writeLines(new_log, paste0(toxval.config()$datapath,"output_log/",source,"_",Sys.Date(),".txt"))
  }
  #####################################################################
  cat("finish\n")
  #####################################################################
}

