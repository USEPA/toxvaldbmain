#-------------------------------------------------------------------------------------
#' Load the Arnot BAF / BCF data
#'
#' @param toxval.db The database to use.
#' @param verbose If TRUE, print out extra diagnostic messages
#' @export
#--------------------------------------------------------------------------------------
toxval.load.bcfbaf <- function(toxval.db, source.db, verbose=FALSE) {
  printCurrentFunction(toxval.db)

  name.list <- c(
    "casrn","name",
    "units",
    "species_supercategory",
    "species_scientific",
    "species_common",
    "author",
    "title",
    "year",
    "journal",
    "logbaf",
    "logbcf",
    "tissue",
    "calc_method",
    "comments",
    "logkow",
    "logkow_reference",
    "water_conc",
    "radiolabel",
    "exposure_duration",
    "exposure_type",
    "temperature",
    "exposure_route",
    "media",
    "pH",
    "total_organic_carbon",
    "wet_weight",
    "lipid_content")
  file <- paste0(toxval.config()$datapath,"PB/Arnot_Gobas_supplementary information_open.xls_20190328.xlsx")
  res <- read.xlsx(file)
  res$units <- "L/kg"
  # res <- res[,name.list]
  # for(i in 1:nrow(res)) res[i,"casrn"] <- fix.casrn(res[i,"casrn"])
  # res <- res[,name.list]
  # res <- res[!is.na(res[,"casrn"]),]
  # cas.list = res[,1:2]
  # cid.list = get.cid.list.toxval(toxval.db, cas.list,"bcfbaf")
  # res$chemical_id <- cid.list$chemical_id
  # #res <- merge(res,cid.list)
  # res <- res[,!is.element(names(res),c("casrn","name"))]
  # res <- unique(res)
  res2 = source_chemical.extra(toxval.db,source.db,res,"BCF BAF")
  res2 = subset(res2,select=-c(casrn,name,chemical_index))
  res2 = unique(res2)
  n1 = names(res2)
  n2 = runQuery("desc bcfbaf",toxval.db)[,1]
  n3 = n1[is.element(n1,n2)]
  res2 = res2[,n3]
  #browser()

  # res$bcfbaf_uuid <- '-'
  # for(i in 1:nrow(res)) {
  #   row <- res[i,2:ncol(res)]
  #   res[i,"bcfbaf_uuid"] <- digest(paste0(row,collapse=""), serialize = FALSE)
  #   res[i,"qa_level"] <- -1
  # }
  runQuery("delete from bcfbaf", toxval.db)
  # for(i in 1:nrow(res)) res[i,"bcfbaf_uuid"] <- UUIDgenerate()
  runInsertTable(res2, "bcfbaf", toxval.db,verbose)
}
