#-----------------------------------------------------------------------------------
#' Build a data frame of the data from toxval for a subset of chemicals
#'
#' @param toxval.db Database version
#' @param filename The name of the file the be imported - should be a short name that
#' will be used in the output filename. This is an xlsx file and needs a column labeled dtxsid
#' @return Write a file with the results
#'
#-----------------------------------------------------------------------------------
export.toxvaldb.subset <- function(toxval.db,filename) {
  printCurrentFunction(toxval.db)
  dir = paste0(toxval.config()$datapath,"export_subset/")
  file = paste0(dir,filename)
  prefix = substr(filename,1,nchar(filename)-5)
  chems = openxlsx::read.xlsx(file)
  dlist = chems$dtxsid
  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  res = NULL
  for(src in slist) {
    cat(src,"\n")
    query = paste0("SELECT
                    a.dtxsid,a.casrn,a.name,
                    b.source,b.subsource,
                    b.qc_status,
                    b.toxval_type,
                    b.toxval_subtype,
                    e.toxval_type_supercategory,
                    b.toxval_numeric_qualifier,
                    b.toxval_numeric,
                    b.toxval_units,
                    b.mw,
                    b.toxval_numeric_original,
                    b.toxval_units_original,
                    b.risk_assessment_class,
                    b.study_type,
                    b.study_type as study_type_corrected,
                    b.study_type_original,
                    b.study_duration_value,
                    b.study_duration_units,
                    b.study_duration_class,
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
                    and qc_status='pass'
                    ")
    mat = runQuery(query,toxval.db,T,F)
    mat = unique(mat)
    mat = mat[is.element(mat$dtxsid,dlist),]
    mat[is.na(mat$casrn),"casrn"] = mat[is.na(mat$casrn),"cleaned_casrn"]
    mat[mat$casrn=='-',"casrn"] = mat[mat$casrn=='-',"cleaned_casrn"]
    mat[is.na(mat$name),"name"] = mat[is.na(mat$name),"cleaned_name"]
    mat[mat$name=='-',"name"] = mat[mat$name=='-',"cleaned_name"]

    cremove = c("cleaned_name","cleaned_casrn")
    mat = mat[ , !(names(mat) %in% cremove)]
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
      mat = mat[!is.element(mat$dtxsid,c("-","none","NODTXSID")),]
      res = rbind(res,mat)
      cat("   ",nrow(mat),length(hlist),nrow(res),"\n")
    }
  }

  res = subset(res,select= -c(hashkey))
  file = paste0(dir,prefix,"_toxval_",toxval.db," ",Sys.Date(),".xlsx")
  sty = openxlsx::createStyle(halign="center",valign="center",textRotation=90,textDecoration = "bold")
  openxlsx::write.xlsx(res,file,firstRow=T,headerStyle=sty)
}
