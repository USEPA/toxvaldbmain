library(digest)
#-----------------------------------------------------------------------------------
#' Find species and toxval_type mismaps, e.g. species other than human fro RfD, RfC, cancer sloper, MSL, etc.
#'
#' @param toxval.db Database version
#' @param source The source to be updated
#' @return Write a file with the results
#'
#-----------------------------------------------------------------------------------
toxval_type.species.mismap <- function(toxval.db) {
  printCurrentFunction(toxval.db)
  dir = "data/input/" # paste0(toxval.config()$datapath,"manuscript_data")
  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  res = NULL
  for(src in slist) {
    cat(src,"\n")
    query = paste0("SELECT
                    a.dtxsid,a.casrn,a.name,
                    b.source,
                    b.toxval_type,
                    b.toxval_subtype,
                    e.toxval_type_supercategory,
                    b.species_original,
                    d.species_id,d.common_name,d.latin_name,d.ecotox_group,
                    b.source_hash,
                    a.cleaned_casrn,a.cleaned_name
                    FROM
                    toxval b
                    INNER JOIN source_chemical a on a.chemical_id=b.chemical_id
                    LEFT JOIN species d on b.species_id=d.species_id
                    INNER JOIN toxval_type_dictionary e on b.toxval_type=e.toxval_type
                    WHERE
                    b.source='",src,"'
                    and human_eco='human health'
                    and toxval_type_supercategory in ('Toxicity Value')")
    mat = runQuery(query,toxval.db,T,F)
    mat = unique(mat)
    mat[is.na(mat$casrn),"casrn"] = mat[is.na(mat$casrn),"cleaned_casrn"]
    mat[mat$casrn=='-',"casrn"] = mat[mat$casrn=='-',"cleaned_casrn"]
    mat[is.na(mat$name),"name"] = mat[is.na(mat$name),"cleaned_name"]
    mat[mat$name=='-',"name"] = mat[mat$name=='-',"cleaned_name"]

    cremove = c("cleaned_name","cleaned_casrn")
    mat = mat[ , !(names(mat) %in% cremove)]
    mat = unique(mat)
    res = rbind(res,mat)
  }

  file = paste0(dir,"/toxval_type.species.mismap ",toxval.db," ",Sys.Date(),".xlsx")
  sty = createStyle(halign="center",valign="center",textRotation=90,textDecoration = "bold")
  openxlsx::write.xlsx(res,file,firstRow=T,headerStyle=sty)
}
