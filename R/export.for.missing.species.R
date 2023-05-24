#-----------------------------------------------------------------------------------
#' Export the records with missing species and point of departure values
#'
#' @param toxval.db Database version
#' @param source The source to be updated
#' #' @return for each source writes an Excel file with the name
#'  ../export/export_by_source_{data}/toxval_all_{toxval.db}_{source}.xlsx
#'
#-----------------------------------------------------------------------------------
export.for.missing.species <- function(toxval.db, source=NULL) {
  printCurrentFunction(toxval.db)

  dir = paste0(toxval.config()$datapath,"species/POD missing species/export_by_source_",toxval.db,"_",Sys.Date(),"/")
  if(!dir.exists(dir)) dir.create(dir)

  slist = sort(runQuery("select distinct source from toxval",toxval.db)[,1])
  if(!is.null(source)) slist=source

  for(src in slist) {
    query = paste0("SELECT
                    a.dtxsid,a.casrn,a.name,a.cleaned_casrn, a.cleaned_name,
                    b.source,b.subsource,
                    b.qc_status,
                    b.human_eco,
                    b.toxval_type,b.toxval_type_original,
                    b.toxval_subtype,
                    e.toxval_type_supercategory,
                    b.toxval_numeric_qualifier,b.toxval_numeric,b.toxval_units,
                    b.toxval_numeric_original,b.toxval_units_original,
                    b.study_type,b.study_type_original,
                    b.study_duration_value,b.study_duration_value_original,
                    b.study_duration_units,b.study_duration_units_original,
                    b.species_id,b.species_original,d.common_name,d.latin_name,d.ecotox_group,
                    b.strain,b.strain_group,b.strain_original,
                    b.sex,b.sex_original,
                    b.exposure_route,b.exposure_route_original,
                    b.year,
                    b.datestamp,
                    f.long_ref,
                    f.title,
                    f.author,
                    f.journal,
                    f.volume,
                    f.year,
                    f.issue,
                    f.url,
                    f.document_name,
                    b.toxval_id,b.source_hash,b.source_table
                    FROM
                    toxval b
                    INNER JOIN source_chemical a on a.chemical_id=b.chemical_id
                    LEFT JOIN species d on b.species_id=d.species_id
                    INNER JOIN toxval_type_dictionary e on b.toxval_type=e.toxval_type
                    INNER JOIN record_source f on b.toxval_id=f.toxval_id
                    WHERE
                    b.source='",src,"'
                    and e.toxval_type_supercategory in ('Point of Departure','Lethality Effect Level')
                    and d.species_id=1000000")
    mat = runQuery(query,toxval.db,T,F)
    mat[is.na(mat$casrn),"casrn"] = mat[is.na(mat$casrn),"cleaned_casrn"]
    mat[mat$casrn=='-',"casrn"] = mat[mat$casrn=='-',"cleaned_casrn"]
    mat[is.na(mat$name),"name"] = mat[is.na(mat$name),"cleaned_name"]
    mat[mat$name=='-',"name"] = mat[mat$name=='-',"cleaned_name"]
    cremove = c("cleaned_name","cleaned_casrn")
    mat = mat[ , !(names(mat) %in% cremove)]
    mat = unique(mat)
    cat(src,nrow(mat),"\n")
    if(nrow(mat)>0) {
      file = paste0(dir,"toxval_missing_species_",toxval.db,"_",src,".xlsx")
      sty = createStyle(halign="center",valign="center",textRotation=90,textDecoration = "bold")
      openxlsx::write.xlsx(mat,file,firstRow=T,headerStyle=sty)
    }
  }
}
