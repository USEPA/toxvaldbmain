#--------------------------------------------------------------------------------------
#' Load Wignall from toxval_source to toxval
#'
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source.db The version of toxval_source from which the tables are loaded.
#' @param log If TRUE, send output to a log file
#--------------------------------------------------------------------------------------
toxval.load.wignall <- function(toxval.db,source.db, log=F){
  printCurrentFunction(toxval.db)
  source <- "Wignall"
  source_table = "source_wignall"
  verbose = log
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
  res = res[ , !(names(res) %in% c("qc_flags","qc_notes","version","parent_chemical_id"))]
  res$source = source
  res$details_text = paste(source,"Details")
  print(dim(res))

  #####################################################################
  cat("Add the code from the original version from Aswani\n")
  #####################################################################
  res$critical_effect <- paste(res$effect, res$effect_description, sep = ";")
  res$toxval_type <- gsub("\\s+$","", res$toxval_type)
  names(res)[names(res) == "reference"] <- "long_ref"
  names.list <- c("casrn","name","original_toxval_type","original_toxval_units","organ","effect_description",
                  "dose_number","dose_values","dose_units","dose_converted",
                  "dr_type","mean_response","response_units","sd_of_response",
                  "total_number_of_animals","incidence_in_number_of_animals","bmr",
                  "bmdl_wizard_notes","action_taken",
                  "bmdl_prime_wizard_notes","comments","hyperlink",
                  "details_text","bmd_prime","bmdl_prime")
  res <- res[,!(names(res)%in% names.list)]

  res1 = res[,c("chemical_id","document_name","source_hash","source","qc_status","subsource","uf","critical_effect","long_ref",
                "toxval_type","toxval_numeric","toxval_units")]
  res2 = res[,c("chemical_id","document_name","source_hash","source","qc_status","subsource","uf","critical_effect","long_ref",
                "pod_type","pod_numeric","pod_units")]
  res3 = res[,c("chemical_id","document_name","source_hash","source","qc_status","subsource","uf","critical_effect","long_ref",
                "toxval_type","bmd","pod_units")]
  res4 = res[,c("chemical_id","document_name","source_hash","source","qc_status","subsource","uf","critical_effect","long_ref",
                "toxval_type","bmdl","pod_units")]

  res3$pod_units = "mg/kg-day"
  res4$pod_units = "mg/kg-day"

  res3$toxval_type = "BMD"
  res4$toxval_type = "BMDL"
  names(res2) = names(res1)
  names(res3) = names(res1)
  names(res4) = names(res1)
  res = rbind(res1,res2,res3,res4)
  res = res[is.element(res$toxval_type,c("BMD","BMC","BMDL","BMCL")),]
  ##########################################################
  cat("remove the redundancy from the source_hash\n")
  ##########################################################
  x=seq(from=1,to=nrow(res))
  y = paste0(res$source_hash,"_",x)
  res$source_hash = y

  for(i in 1:nrow(res)) {
    x = res[i,"toxval_numeric"]
    if(contains(x,";")) {
      y = str_split(x,";")[[1]]
      z = min(y)
      #cat(x,z,"\n")
      res[i,"toxval_numeric"] = z
      #browser()
    }
  }
  res = res[!is.na(res$toxval_numeric),]
  res = res[res$toxval_numeric!="-",]
  #####################################################################
  cat("checks, finds and replaces non ascii characters in res with XXX\n")
  #####################################################################
  res$year <- -1
  res$species <- "-"
  res$strain <- "-"
  res$sex <- "-"
  res$exposure_route <- "-"
  res$exposure_method <- "-"
  res$study_duration_value <- ""
  res$study_duration_units <- "-"
#  res <- generate.originals(toxval.db,res)
  for(i in 1:nrow(res)) {
    val <- res[i,"long_ref"]
    for(year in 1980:2018) {
      syear <- as.character(year)
      if(contains(val,syear)) res[i,"year"] <- year
    }
  }
  #fix species
  # v8 has multiple field information that were not defined in source document, these field info extracted from long_ref
  # key word vector created based on v8
  res$species <- "-"
  sps <- c("rat","mouse","rabbit","dog","monkey","non-human primate","hamster","mice","rats","rabbits","dogs","hamsters")
  res$species <- str_extract_all(res$long_ref, regex(paste(sps, collapse = "|"), ignore_case = T)) %>%
    sapply(., paste, collapse = ", ")
  res$species <- tolower(res$species)
  res[grep("mouse", res$species),"species"] <- "mice"

  for (i in 1:nrow(res)){
    res$species[i] <- paste(unique(unlist(str_split(res$species[i], ", "))), collapse = ", ")
  }

  res[grep("^rat, mice$", res$species),"species"] <- "mice, rat"
  res[grep("^rat, dog$", res$species),"species"] <- "dog, rat"
  res[grep("^rat, rabbit$", res$species),"species"] <- "rabbit, rat"
  res[which(res$species == ""),"species"] <- "-"
  res$species_original <- res$species

  # fix strain
  res$strain <- "-"
  strain <- c("CD","Fischer 344","Sprague-Dawley","B6C3F1","Wistar","Albino","New Zealand","Beagle","CD-1","Long-Evans","Osborne-Mendel",
              "ICR","Crl:CD","Rhesus","JCL-ICR","CFE","NMRI","Sherman","Carworth farm","FDRL","CF-1","F344/N","F344/N/N",
              "B6C3F1/CrlBR","BDF1","C57/B1","CF-N","C57BL/6","Golden Syrian","Swiss","Balb/c","Harlan","Dutch","Sprague-Dawley (CD)")
  res$strain <- str_extract_all(res$long_ref, regex(paste(strain, collapse = "|"), ignore_case = T)) %>% sapply(., paste, collapse = ", ")
  res[which(res$strain == ""),"strain"] <- "-"

  # fix sex
  res$sex <- "-"
  res[grep("\\bFemale\\b", res$long_ref, ignore.case = T),"sex"] <- "Female"
  res[grep("\\bMale\\b", res$long_ref, ignore.case = T),"sex"] <- paste("Male", res[grep("\\bMale\\b", res$long_ref, ignore.case = T),"sex"] , sep = '/')
  res$sex <- gsub("Male\\/NA","Male",res$sex)
  res$sex <- tolower(res$sex)

  # fix exposure_route
  res$exposure_route <- "-"
  exp_route <- c("oral","inhalation")
  res$exposure_route <- str_extract_all(res$long_ref, regex(paste(exp_route, collapse = "|"), ignore_case = T)) %>%
    sapply(., paste, collapse = ", ")
  res$exposure_route <- tolower(res$exposure_route)
  res[grep("^oral\\, oral$", res$exposure_route),"exposure_route"] <- "oral"
  res[grep("^inhalation\\, inhalation$", res$exposure_route),"exposure_route"] <- "inhalation"
  res[which(res$exposure_route == ""),"exposure_route"] <- "-"

  # fix exposure_method
  res$exposure_method <- "-"
  exp_method <- c("\\bgavage\\b","\\bdiet\\b","\\bdrinking water\\b","\\bvapor\\b","\\bvapors\\b")
  res$exposure_method <- str_extract_all(res$long_ref, regex(paste(exp_method, collapse = "|"), ignore_case = T)) %>% sapply(., paste, collapse = ", ")
  res$exposure_method <- tolower(res$exposure_method)
  res[grep("^diet\\, diet$", res$exposure_method),"exposure_method"] <- "diet"
  res[grep("^vapors$", res$exposure_method),"exposure_method"] <- "vapor"
  res[which(res$exposure_method == ""),"exposure_method"] <- "-"

  # fix study_duration value and study_duration_units
  res$study_duration_value <- "-"
  res$study_duration_units <- "-"
  dur <- c("[a-zA-Z0-9]*\\-*\\s*[a-zA-Z0-9]+\\s+year","[a-zA-Z0-9]*\\-*\\s*[a-zA-Z0-9]+\\s+month","[a-zA-Z0-9]*\\-*\\s*[a-zA-Z0-9]+\\s+week","[a-zA-Z0-9]*\\-*\\s*[a-zA-Z0-9]+\\s+day","[a-zA-Z0-9]*\\-*\\s*[a-zA-Z0-9]+\\s+generations")
  res$study_duration_value <- str_extract_all(res$long_ref, regex(paste(dur, collapse = "|"), ignore_case = T)) %>% sapply(., paste, collapse = ", ")
  res$study_duration_value <- tolower(res$study_duration_value)
  res$study_duration_value <- gsub("(^for\\s+|^or\\s+|^to\\s+|^fore\\s+|^after\\s+|^a\\s+)(.*)","\\2",res$study_duration_value)
  res$study_duration_value <- gsub("(^\\s+)(.*)","\\2",res$study_duration_value)
  res$study_duration_units <- gsub("(.*\\s+)(\\w+$)","\\2", res$study_duration_value)
  res[which(res$study_duration_units == ""),"study_duration_units"] <- "-"
  res$study_duration_value <- gsub("(.*)(\\s+\\w+$)","\\1",res$study_duration_value)
  res$study_duration_value <- gsub("(.*)(\\s+consecutive$)","\\1",res$study_duration_value)
  res$study_duration_value <- mgsub(res$study_duration_value, replace_number(seq_len(100)), seq_len(100))
  res$study_duration_value <- as.numeric(res$study_duration_value)

  res = res[!is.na(res$toxval_numeric),]
  res[res$exposure_method=="gavage","exposure_route"] = "oral"
  res[res$toxval_type=="RfD","exposure_route"] = "oral"
  res[res$toxval_type=="RfC","exposure_route"] = "inhalation"
  res[res$toxval_type=="Oral Slope Factor","exposure_route"] = "oral"
  res[res$toxval_units=="mg/kg/day","exposure_route"] = "oral"
  res[res$toxval_units=="mg/m3","exposure_route"] = "inhalation"
  res[res$toxval_type=="Inhalation Unit Risk","exposure_route"] = "inhalation"
  res[res$toxval_type=="Chronic Inhalation REL","exposure_route"] = "inhalation"
  res[res$toxval_units=="mg/kg-d","exposure_route"] = "oral"
  res[res$toxval_units=="ug/kg/day","exposure_route"] = "oral"
  res[res$toxval_units=="mg/kg","exposure_route"] = "oral"
  res[res$toxval_units=="mg/kg-day","exposure_route"] = "oral"
  res[res$exposure_method=="vapor","exposure_route"] = "inhalation"
  res[res$toxval_units=="1/mg/kg-day","exposure_route"] = "oral"
  res[res$toxval_units=="mg/kg-day-1","exposure_route"] = "oral"
  res[res$toxval_units=="1/?g/m3","exposure_route"] = "inhalation"
  res[res$toxval_units=="microgram/m3","exposure_route"] = "inhalation"

   cremove = c("uf","species_original.1","species")
  res = res[ , !(names(res) %in% cremove)]

  #####################################################################
  cat("find columns in res that do not map to toxval or record_source\n")
  #####################################################################
  cols1 = runQuery("desc record_source",toxval.db)[,1]
  cols2 = runQuery("desc toxval",toxval.db)[,1]
  cols = unique(c(cols1,cols2))
  #colnames(res)[which(names(res) == "species")] = "species_original"
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

  # examples ...
  # names(res)[names(res) == "source_url"] = "url"
  # colnames(res)[which(names(res) == "phenotype")] = "critical_effect"

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
  refs$record_source_type = "-"
  refs$record_source_note = "-"
  refs$record_source_level = "-"
  print(dim(res))
  #####################################################################
  cat("load res and refs to the database\n")
  #####################################################################
  res = unique(res)
  refs = unique(refs)
  res$datestamp = Sys.Date()
  res$source_table = source_table
  res$source_url = "https://ehp.niehs.nih.gov/doi/full/10.1289/ehp.1307539"
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
  toxval.load.postprocess(toxval.db,source.db,source,do.convert.units=F)

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
