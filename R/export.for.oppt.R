#-----------------------------------------------------------------------------------
#
#' Build a data frame of the PODs and exports as xlsx
#'
#' @param toxval.db Database version
#' @param file.name If not NA, this is a file containing chemicals, and only those chemicals will be exported
#'
#'
#' @return writes an Excel file with the name
#'  ../export/toxval_pod_summary_[human_eco]_Sys.Date().xlsx
#'
#-----------------------------------------------------------------------------------
export.for.oppt <- function(toxval.db, file.name="TSCA PICS") {
  printCurrentFunction(toxval.db)
  dir = paste0(toxval.config()$datapath,"export_subset/")
  if(!is.na(file.name)) {
    file = paste0(dir,file.name,".xlsx")
    print(file)
    chems = openxlsx::read.xlsx(file)
    dlist = unique(chems$dtxsid)
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
                    b.toxval_type_original,
                    b.toxval_subtype,
                    e.toxval_type_supercategory,
                    b.toxval_numeric_qualifier,
                    b.toxval_numeric,
                    b.toxval_numeric_original,
                    b.toxval_units,
                    b.toxval_units_original,
                    b.study_type,
                    b.study_type_original,
                    b.study_type as study_type_corrected,
                    b.study_duration_class,
                    b.study_duration_class_original,
                    b.study_duration_value,
                    b.study_duration_value_original,
                    b.study_duration_units,
                    b.study_duration_units_original,
                    d.species_id,d.common_name,d.latin_name,d.ecotox_group,
                    b.strain,
                    b.strain_original,
                    b.strain_group,
                    b.sex,
                    b.sex_original,
                    b.habitat,
                    b.generation,
                    b.generation_original,
                    b.lifestage,
                    b.lifestage_original,
                    b.population,
                    b.population_original,
                    b.exposure_route,
                    b.exposure_route_original,
                    b.exposure_method,
                    b.exposure_method_original,
                    b.exposure_form,
                    b.exposure_form_original,
                    b.media,
                    b.media_original,
                    b.critical_effect,
                    b.critical_effect_original,
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

    # query = paste0("SELECT
    #                 a.dtxsid,a.casrn,a.name,
    #                 b.source,b.subsource,
    #                 b.qc_status,
    #                 b.risk_assessment_class,
    #                 b.toxval_type,
    #                 b.toxval_subtype,
    #                 e.toxval_type_supercategory,
    #                 b.toxval_numeric_qualifier,
    #                 b.toxval_numeric,
    #                 b.toxval_units,
    #                 b.study_type,
    #                 b.study_duration_class,
    #                 b.study_duration_value,
    #                 b.study_duration_units,
    #                 d.species_id,d.common_name,d.latin_name,d.ecotox_group,
    #                 b.strain,
    #                 b.strain_group,
    #                 b.sex,
    #                 b.generation,
    #                 b.exposure_route,
    #                 b.exposure_method,
    #                 b.critical_effect,
    #                 b.year,
    #                 f.long_ref,
    #                 f.title,
    #                 f.author,
    #                 f.journal,
    #                 f.volume,
    #                 f.year as ref_year,
    #                 f.issue,
    #                 f.url,
    #                 b.source_hash,
    #                 a.cleaned_casrn,a.cleaned_name
    #                 FROM
    #                 toxval b
    #                 INNER JOIN source_chemical a on a.chemical_id=b.chemical_id
    #                 LEFT JOIN species d on b.species_id=d.species_id
    #                 INNER JOIN toxval_type_dictionary e on b.toxval_type=e.toxval_type
    #                 INNER JOIN record_source f on b.toxval_id=f.toxval_id
    #                 WHERE
    #                 b.source='",src,"'
    #                 and toxval_type_supercategory in ('Point of Departure','Lethality Effect Level','Toxicity Value')")

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
      mat0 = mat
      nlist = c("dtxsid","source","subsource","risk_assessment_class","toxval_units","study_type","study_duration_class",
                "study_duration_value","study_duration_units","common_name","strain","sex","exposure_route",
                "exposure_method","year","long_ref","ref_year","title","author","journal","volume","issue")
      temp = mat[,nlist]
      mat$hashkey = NA
      mat$study_group = NA
      for(i in 1:nrow(mat)) mat[i,"hashkey"] = digest::digest(paste0(temp[i,],collapse=""), serialize = FALSE)
      hlist = unique(mat$hashkey)
      for(i in 1:length(hlist)) {
        sg = paste0(src,"_",i)
        mat[mat$hashkey==hlist[i],"study_group"] = sg
      }
      res = rbind(res,mat)
      cat("   ",nrow(mat),length(hlist),nrow(res),"\n")
    }
  }
  #res = subset(res,select= -c(hashkey))
  file <- paste0(dir,"/toxval_all_with_references_",toxval.db,"_",Sys.Date(),".xlsx")
  if(!is.na(file.name))
    file <- paste0(dir,"/toxval_all_with_references_",file.name,"_",toxval.db,"_",Sys.Date(),".xlsx")
  sty <- openxlsx::createStyle(halign="center",valign="center",textRotation=90,textDecoration = "bold")
  openxlsx::write.xlsx(res,file,firstRow=T,headerStyle=sty)

}
