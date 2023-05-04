#-----------------------------------------------------------------------------------
#
#' Build a data frame of the PODs and exports as xlsx
#'
#' @param toxval.db Database version
#' @param human_eco Either 'human health' or 'eco'
#' @param file.name If not NA, this is a file containing chemicals, and only those chemicals will be exported
#'
#'
#' @return writes an Excel file with the name
#'  ../export/toxval_pod_summary_[human_eco]_Sys.Date().xlsx
#'
#-----------------------------------------------------------------------------------
export.all.with.references.v93 <- function(toxval.db="res_toxval_v94",file.name=NA,pfas=T) {
  printCurrentFunction(toxval.db)
  dir = paste0(toxval.config()$datapath,"export/")
  if(!is.na(file.name)) {
    file = paste0(toxval.config()$datapath,"chemicals/for_load/",file.name,".xlsx")
    print(file)
    chems = read.xlsx(file)
    rownames(chems) = chems$dtxsid
    dlist = unique(chems$dtxsid)
  }
  if(pfas) {
    file = paste0(toxval.config()$datapath,"chemicals/for_load/PFAS synonyms.xlsx")
    print(file)
    synonyms = read.xlsx(file)
    rownames(synonyms) = synonyms$dtxsid
  }
  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  res = NULL
  for(src in slist) {
    cat(src,"\n")
    query = paste0("SELECT
                    a.dtxsid,a.casrn,a.name,
                    b.source,b.subsource,
                    b.qc_status,
                    b.risk_assessment_class,
                    b.toxval_type,
                    b.toxval_subtype,
                    e.toxval_type_supercategory,
                    b.toxval_numeric_qualifier,
                    b.toxval_numeric,
                    b.toxval_units,
                    b.study_type,
                    b.study_type as study_type_corrected,
                    b.study_duration_class,
                    b.study_duration_value,
                    b.study_duration_units,
                    d.species_id,d.common_name,d.latin_name,d.ecotox_group,
                    b.strain,
                    b.strain_group,
                    b.sex,
                    b.generation,
                    b.exposure_route,
                    b.exposure_method,
                    b.critical_effect,
                    b.year,
                    f.long_ref,
                    f.title,
                    f.author,
                    f.journal,
                    f.volume,
                    f.year as ref_year,
                    f.issue,
                    f.url,
                    b.source_hash,
                    a.cleaned_casrn,a.cleaned_name
                    FROM
                    toxval b
                    INNER JOIN source_chemical a on a.chemical_id=b.chemical_id
                    LEFT JOIN species d on b.species_id=d.species_id
                    INNER JOIN toxval_type_dictionary e on b.toxval_type=e.toxval_type
                    INNER JOIN record_source f on b.toxval_id=f.toxval_id
                    WHERE
                    b.source='",src,"'
                    and human_eco='human health'
                    and toxval_type_supercategory in ('Point of Departure','Lethality Effect Level','Toxicity Value')")

    query = paste0("SELECT
                    a.dtxsid,a.casrn,a.name,
                     b.source,b.subsource,
                    b.qc_status,
                    b.risk_assessment_class,
                    b.toxval_type,
                    b.toxval_subtype,
                    e.toxval_type_supercategory,
                    b.toxval_numeric_qualifier,
                    b.toxval_numeric,
                    b.toxval_units,
                    b.study_type,
                    b.study_duration_class,
                    b.study_duration_value,
                    b.study_duration_units,
                    d.species_id,d.common_name,d.latin_name,d.ecotox_group,
                    b.strain,
                    b.strain_group,
                    b.sex,
                    b.generation,
                    b.exposure_route,
                    b.exposure_method,
                    b.critical_effect,
                    b.year,
                    f.long_ref,
                    f.title,
                    f.author,
                    f.journal,
                    f.volume,
                    f.year as ref_year,
                    f.issue,
                    f.url,
                    f.guideline,
                    b.source_hash,
                    a.cleaned_casrn,a.cleaned_name
                    FROM
                    toxval b
                    INNER JOIN source_chemical a on a.chemical_id=b.chemical_id
                    LEFT JOIN species d on b.species_id=d.species_id
                    INNER JOIN toxval_type_dictionary e on b.toxval_type=e.toxval_type
                    INNER JOIN record_source f on b.toxval_id=f.toxval_id
                    WHERE
                    b.source='",src,"'
                    and toxval_type_supercategory in ('Point of Departure','Lethality Effect Level','Toxicity Value')")
    query = paste0("SELECT
                    a.dtxsid,a.casrn,a.name,
                    a.name as nickname,
                    a.name as category,
                    b.source,b.subsource,
                    b.qc_status,
                    b.risk_assessment_class,
                    b.toxval_type,
                    b.toxval_subtype,
                    e.toxval_type_supercategory,
                    b.toxval_numeric_qualifier,
                    b.toxval_numeric,
                    b.toxval_units,
                    b.study_type,
                    b.study_duration_class,
                    b.study_duration_value,
                    b.study_duration_units,
                    d.species_id,d.common_name,d.latin_name,d.ecotox_group,
                    b.strain,
                    b.strain_group,
                    b.sex,
                    b.habitat,
                    b.generation,
                    b.lifestage,
                    b.population,
                    b.exposure_route,
                    b.exposure_method,
                    b.exposure_form,
                    b.media,
                    b.media_original,
                    b.critical_effect,
                    b.year,
                    f.record_source_type,
                    f.record_source_level,
                    f.long_ref,
                    f.title,
                    f.author,
                    f.journal,
                    f.volume,
                    f.year as ref_year,
                    f.issue,
                    f.pmid,
                    f.guideline,
                    f.glp,
                    f.quality,
                    f.url,
                    b.source_hash,
                    a.cleaned_casrn,a.cleaned_name
                    FROM
                    toxval b
                    INNER JOIN source_chemical a on a.chemical_id=b.chemical_id
                    LEFT JOIN species d on b.species_id=d.species_id
                    INNER JOIN toxval_type_dictionary e on b.toxval_type=e.toxval_type
                    INNER JOIN record_source f on b.toxval_id=f.toxval_id
                    WHERE
                    b.source='",src,"'
                    and human_eco='human health'
                    and toxval_type_supercategory in ('Point of Departure','Lethality Effect Level','Toxicity Value')")

    mat = runQuery(query,toxval.db,T,F)
    mat = unique(mat)
    mat[is.na(mat$casrn),"casrn"] = mat[is.na(mat$casrn),"cleaned_casrn"]
    mat[is.na(mat$name),"name"] = mat[is.na(mat$name),"cleaned_name"]
    mat[mat$casrn=='-',"casrn"] = mat[mat$casrn=='-',"cleaned_casrn"]
    mat[mat$name=='-',"name"] = mat[mat$name=='-',"cleaned_name"]
    cremove = c("cleaned_name","cleaned_casrn")
    mat = mat[ , !(names(mat) %in% cremove)]
    if(!is.null(file.name)) mat = mat[is.element(mat$dtxsid,dlist),]
    if(nrow(mat)>0) {
      mat$nickname = NA
      mat$category = NA
      nlist = c("dtxsid","source","subsource","risk_assessment_class","toxval_units","study_type","study_duration_class",
                "study_duration_value","study_duration_units","common_name","strain","sex","exposure_route",
                "exposure_method","year","long_ref","ref_year","title","author","journal","volume","issue")
      temp = mat[,nlist]
      mat$hashkey = NA
      mat$study_group = NA
      for(i in 1:nrow(mat)) mat[i,"hashkey"] = digest(paste0(temp[i,],collapse=""), serialize = FALSE)
      hlist = unique(mat$hashkey)
      for(i in 1:length(hlist)) {
        sg = paste0(src,"_",i)
        mat[mat$hashkey==hlist[i],"study_group"] = sg
      }

      if(pfas) {
        d2 = unique(mat$dtxsid)
        for(dtxsid in d2) {
          if(is.element(dtxsid,synonyms$dtxsid)) mat[is.element(mat$dtxsid,dtxsid),"nickname"] = synonyms[dtxsid,"nickname"]
          mat[is.element(mat$dtxsid,dtxsid),"category"] = chems[dtxsid,"category"]
        }
      }
      res = rbind(res,mat)
      cat("   ",nrow(mat),length(hlist),nrow(res),"\n")
    }
  }
  #res = subset(res,select= -c(hashkey))
  file = paste0(dir,"/toxval_all_with_references_",toxval.db,"_",Sys.Date(),".xlsx")
  if(!is.na(file.name))
    file <- paste0(dir,"/toxval_all_with_references_",file.name,"_",toxval.db,"_",Sys.Date(),".xlsx")
  sty <- createStyle(halign="center",valign="center",textRotation=90,textDecoration = "bold")
  write.xlsx(res,file,firstRow=T,headerStyle=sty)

}
