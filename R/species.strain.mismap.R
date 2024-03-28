#-----------------------------------------------------------------------------------
#' Find species and strain mismaps
#'
#' @param toxval.db Database version
#' @return Write a file with the results
#'
#-----------------------------------------------------------------------------------
species.strain.mismap <- function(toxval.db) {
  printCurrentFunction(toxval.db)
  dir = "data/input/" # paste0(toxval.config()$datapath,"manuscript_data")

  query = paste0("SELECT
                  a.dtxsid,a.name,
                  b.source,
                  b.species_original,b.strain_original,
                  d.common_name,
                  b.strain, b.strain_group
                  FROM
                  toxval b
                  INNER JOIN source_chemical a on a.chemical_id=b.chemical_id
                  LEFT JOIN species d on b.species_id=d.species_id
                  WHERE b.human_eco='human health'
                  and d.common_name in ('Rat','Rabbit','Dog','Mouse','Human','Cat','Pig','Guinea Pig','Gerbil','Hamster','Macaques','Mink','Monkey','Pikas','Prairie Dog','Primate','Sheep')")
  mat = runQuery(query,toxval.db,T,F)
  mat = unique(mat)
  mat = mat[!is.element(mat$source,c("EPA OPPT")),]
  mat$mismap = NA
  cremove = c("name","dtxsid")
  mat = mat[ , !(names(mat) %in% cremove)]
  mat = unique(mat)
  file = paste0(dir,"/species.strain.mismap ",toxval.db," ",Sys.Date(),".xlsx")
  sty = createStyle(halign="center",valign="center",textRotation=90,textDecoration = "bold")
  openxlsx::write.xlsx(mat,file,firstRow=T,headerStyle=sty)
}
