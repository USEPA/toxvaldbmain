library(openxlsx)
library(digest)
#-----------------------------------------------------------------------------------
#' Produce a view of the ToxValDB Data
#'
#' `toxvaldb.view` Produces a view for ToxValDB by merging specified tables
#'
#' @param toxval.db Database version
#' @param user The username for the MySQL database. The database instance is
#' hard-coded in the function setDBConn().
#' @param password The user's MySQL database password.
#' @param count If count>0, only select this number of records from each source, used for debugging
#' @return Write a file with the results: data/view/ToxValDB View {toxval.db} {Sys.Date()}.xlsx
#' @export
#-----------------------------------------------------------------------------------
toxvaldb.view <- function(toxval.db="res_toxval_v95",user="_dataminer",password="pass",
                          count=10) {
  printCurrentFunction(toxval.db)
  dir = "data/view/"
  setDBConn(user=user,password=password)
  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  res = NULL
  for(src in slist) {
    n = runQuery(paste0("select count(*) from toxval where source='",src,"'"),toxval.db)[1,1]
    cat(src,":",n,"\n")
    query = paste0("SELECT
                      a.dtxsid,a.casrn,a.name,
                      b.toxval_id,
                      b.source_hash,
                      b.source,
                      b.toxval_type,
                      e.toxval_type_supercategory,
                      b.toxval_subtype,
                      b.toxval_numeric_qualifier,
                      b.toxval_numeric,
                      b.toxval_units,
                      b.study_type,
                      b.study_duration_value,
                      b.study_duration_units,
                      b.study_duration_class,
                      b.sex,
                      b.exposure_route,
                      b.exposure_method,
                      b.exposure_form,
                      b.critical_effect,
                      b.generation,
                      b.media,
                      b.lifestage,
                      b.population,
                      b.strain,
                      b.strain_group,
                      b.year,
                      b.risk_assessment_class,

                      b.toxval_type_original,
                      b.toxval_subtype_original,
                      b.toxval_numeric_qualifier_original,
                      b.toxval_numeric_original,
                      b.toxval_units_original,
                      b.study_type_original,
                      b.study_duration_value_original,
                      b.study_duration_units_original,
                      b.study_duration_class_original,
                      b.sex_original,
                      b.exposure_route_original,
                      b.exposure_method,
                      b.exposure_form,
                      b.critical_effect_original,
                      b.generation_original,
                      b.media_original,
                      b.lifestage_original,
                      b.population_original,
                      b.strain_original,
                      b.year_original,

                      b.species_original,
                      d.common_name,d.latin_name,d.ecotox_group,d.habitat,
                      b.human_eco,
                      b.experimental_record,
                      b.source_url,
                      b.subsource_url,
                      f.long_ref,
                      f.url,
                      f.title,
                      f.author,
                      f.journal,
                      f.volume,
                      f.year as ref_year,
                      f.issue,
                      f.page,
                      f.doi,
                      f.pmid,
                      f.guideline,
                      g.glp,
                      f.quality,
                      f.record_source_level,
                      f.record_source_type,
                      f.priority,
                      f.clowder_doc_id,
                      f.clowder_doc_metadata,
                      f.qa_status,
                      b.source_hash,
                      b.study_group,
                      b.qc_status
                      FROM
                      toxval b
                      INNER JOIN source_chemical a on a.chemical_id=b.chemical_id
                      LEFT JOIN species d on b.species_id=d.species_id
                      INNER JOIN toxval_type_dictionary e on b.toxval_type=e.toxval_type
                      INNER JOIN record_source f on b.toxval_id=f.toxval_id
                      WHERE
                      b.source='",src,"'
                      and f.priority=1
                     ")

    if(count>0) query = paste(query," limit ",count)
    mat = runQuery(query,toxval.db,T,F)

    res = rbind(res,mat)
  }
  sty = createStyle(halign="center",valign="center",textRotation=90,textDecoration = "bold")
  file = paste0(dir,"ToxValDB View ",toxval.db," ",Sys.Date(),".xlsx")
  openxlsx::write.xlsx(res,file,firstRow=T,headerStyle=sty)
}
