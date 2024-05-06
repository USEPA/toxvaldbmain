#-----------------------------------------------------------------------------------
#' Build a data frame of the data for the toxval manuscript
#'
#' @param toxval.db Database version
#' @param source The source to be updated
#' @return Write a file with the results
#'
#-----------------------------------------------------------------------------------
export.for.toxvaldb.manuscript <- function(toxval.db) {
  printCurrentFunction(toxval.db)
  dir = paste0(toxval.config()$datapath,"manuscript_data")
  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  res = NULL
  for(src in slist) {
    cat(src,"\n")
    query = paste0("SELECT
                    a.dtxsid,a.casrn,a.name,
                    b.source,b.subsource,
                    b.qc_status,
                    b.study_group,
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
                    b.human_ra,
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
                    and human_eco='human health'
                    and toxval_type_supercategory in ('Point of Departure','Lethality Effect Level','Toxicity Value')")
    mat = runQuery(query,toxval.db,T,F)
    mat = unique(mat)
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
      mat$study_group_2 = NA
      for(i in 1:nrow(mat)) mat[i,"hashkey"] = digest(paste0(temp[i,],collapse=""), serialize = FALSE)
      hlist = unique(mat$hashkey)
      for(i in 1:length(hlist)) {
        sg = paste0(src,"_",i)
        mat[mat$hashkey==hlist[i],"study_group_2"] = sg
      }
      mat = mat[!is.element(mat$dtxsid,c("-","none","NODTXSID")),]
      res = rbind(res,mat)
      cat("   ",nrow(mat),length(hlist),nrow(res),"\n")
    }
  }
  res = subset(res,select= -c(hashkey))
  file = paste0(dir,"/toxval_all_for_manuscript_PODs_",toxval.db," ",Sys.Date(),".RData")
  save(res,file=file)
  resa = res[is.element(res$toxval_type_supercategory,c("Point of Departure","Lethality Effect Level")),]
  resb = res[is.element(res$toxval_type_supercategory,c("Toxicity Value")),]

  sty = createStyle(halign="center",valign="center",textRotation=90,textDecoration = "bold")
  file = paste0(dir,"/toxval_all_for_manuscript_PODs_",toxval.db," ",Sys.Date(),".xlsx")
  openxlsx::write.xlsx(resa,file,firstRow=T,headerStyle=sty)
  file = paste0(dir,"/toxval_all_for_manuscript_RAvals_",toxval.db," ",Sys.Date(),".xlsx")
  openxlsx::write.xlsx(resb,file,firstRow=T,headerStyle=sty)

  file = paste0(dir,"/toxval_all_for_manuscript_PODs_",toxval.db," ",Sys.Date(),".csv")
  write.csv(resa,file=file,row.names=F)
  file = paste0(dir,"/toxval_all_for_manuscript_RAvals_",toxval.db," ",Sys.Date(),".csv")
  write.csv(resb,file=file,row.names=F)
}
