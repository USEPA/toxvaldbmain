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
export.for.toxval.qc <- function(toxval.db, source=NULL) {
  printCurrentFunction(toxval.db)
  dir = paste0(toxval.config()$datapath,"export_for_qc/")

  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source
  res = NULL
  for(src in slist) {
    cat(src,"\n")
    query = paste0("SELECT
                    a.dtxsid,a.casrn,a.name,
                    b.source,b.subsource,
                    b.qc_status,
                    b.risk_assessment_class,
                    b.toxval_type,b.toxval_type_original,
                    b.toxval_subtype,b.toxval_subtype_original,
                    e.toxval_type_supercategory,
                    b.toxval_numeric_qualifier,b.toxval_numeric_qualifier_original,
                    b.toxval_numeric,b.toxval_numeric_original,
                    b.toxval_units,b.toxval_units_original,
                    b.study_type,b.study_type_original,
                    b.study_duration_class,b.study_duration_class_original,
                    b.study_duration_value,b.study_duration_value_original,
                    b.study_duration_units,b.study_duration_units_original,
                    d.species_id,d.common_name,d.latin_name,d.ecotox_group,
                    b.species_original,
                    b.strain,b.strain_original,
                    b.strain_group,
                    b.sex,b.sex_original,
                    b.generation,b.generation_original,
                    b.exposure_route,b.exposure_route_original,
                    b.exposure_method,b.exposure_method_original,
                    b.critical_effect,b.critical_effect_original,
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
                    b.study_group,
                    a.cleaned_casrn,a.cleaned_name
                    FROM
                    toxval b
                    INNER JOIN source_chemical a on a.chemical_id=b.chemical_id
                    LEFT JOIN species d on b.species_id=d.species_id
                    INNER JOIN toxval_type_dictionary e on b.toxval_type=e.toxval_type
                    INNER JOIN record_source f on b.toxval_id=f.toxval_id
                    WHERE
                    b.source='",src,"'
                   ")


    mat = runQuery(query,toxval.db,T,F)
    mat = unique(mat)
    mat[is.na(mat$casrn),"casrn"] = mat[is.na(mat$casrn),"cleaned_casrn"]
    mat[is.na(mat$name),"name"] = mat[is.na(mat$name),"cleaned_name"]
    mat[mat$casrn=='-',"casrn"] = mat[mat$casrn=='-',"cleaned_casrn"]
    mat[mat$name=='-',"name"] = mat[mat$name=='-',"cleaned_name"]
    cremove = c("cleaned_name","cleaned_casrn")
    mat = mat[ , !(names(mat) %in% cremove)]
    file = paste0(dir,"/toxval_for_qc_",src," ",toxval.db,"_",Sys.Date(),".xlsx")
    sty = openxlsx::createStyle(halign="center",valign="center",textRotation=90,textDecoration = "bold")
    openxlsx::write.xlsx(mat,file,firstRow=T,headerStyle=sty)
  }
}
