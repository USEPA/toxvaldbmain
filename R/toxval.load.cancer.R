#--------------------------------------------------------------------------------------
#
#' prepare the cancer call data. The data comes form a series of files
#' ../NIOSH/NIOSH_CARC_2018.xlsx
#' ../IRIS/iris_cancer_call_2018-10-03.xlsx
#' ../PPRTV_ORNL/PPRTV_ORNL cancer calls 2018-10-25.xlsx
#' ../cancer_summary/cancer/NTP/NTP cancer clean.xlsx
#' ../cancer_summary/cancer/IARC/IARC cancer 2018-10-29.xlsx
#' ../cancer_summary/cancer/HealthCanada/HealthCanada_TRVs_2010_AppendixA v2.xlsx
#' ../cancer_summary/cancer/EPA_OPP_CARC/EPA_CARC.xlsx
#' ../cancer_summary/cancer/CalEPA/calepa_p65_cancer_only.xlsx
#'
#' extract all of the chemicals with cancer slope factor or unit risk with appropriate units
#'
#' @param toxval.db The version of the database to use
#-----------------------------------------------------------------------------------
toxval.load.cancer <- function(toxval.db,source.db) {
  # verbose = log
  printCurrentFunction(toxval.db)

  runQuery("delete from cancer_summary",toxval.db)

  file = paste0(toxval.config()$datapath,"niosh/niosh_files/NIOSH Carcinogens.xlsx")
  print(file)
  niosh <- read.xlsx(file)
  niosh$exposure_route <- "inhalation"
  niosh$source <- "NIOSH"
  niosh$cancer_call <- "potential occupational carcinogen"
  niosh <- niosh[,c("casrn","name","source","exposure_route","cancer_call","url")]

  file <- paste0(toxval.config()$datapath,"cancer_summary/cancer/IRIS/IRIS cancer calls 2022-10-21.xlsx")
  print(file)
  iris <- read.xlsx(file)
  iris$source <- "IRIS"
  iris$exposure_route <- "-"
  iris <- iris[,c("casrn","name","source","exposure_route","cancer_call","url")]
  iris = iris[!is.na(iris$casrn),]
  for(i in 1:nrow(iris)) iris[i,"casrn"] = fix.casrn(iris[i,"casrn"])

  file <- paste0(toxval.config()$datapath,"cancer_summary/cancer/NTP/NTP ROC 2021.xlsx")
  print(file)
  ntp <- read.xlsx(file)
  ntp$exposure_route <- "-"
  ntp = ntp[!is.na(ntp$casrn),]
  ntp <- ntp[,c("casrn","name","source","exposure_route","cancer_call","url")]

  file <- paste0(toxval.config()$datapath,"cancer_summary/cancer/IARC/IARC cancer 2022-10-21.xlsx")
  print(file)
  iarc <- read.xlsx(file)
  name.list <- c("casrn","name","cancer_call")
  iarc <- iarc[,name.list]
  iarc$exposure_route <- "-"
  iarc$source <- "IARC"
  iarc$url <- "https://monographs.iarc.who.int/list-of-classifications/"
  iarc = iarc[!is.na(iarc$casrn),]
  iarc = iarc[!is.na(iarc$cancer_call),]
  iarc <- iarc[,c("casrn","name","source","exposure_route","cancer_call","url")]

  file <- paste0(toxval.config()$datapath,"pprtv_ornl/pprtv_ornl_files/PPRTV_ORNL cancer calls 2018-10-25.xlsx")
  print(file)
  pprtv_ornl <- read.xlsx(file)
  name.list <- c("casrn","name","cancer_call")
  pprtv_ornl <- pprtv_ornl[,name.list]
  pprtv_ornl$exposure_route <- "-"
  pprtv_ornl$source <- "PPRTV (ORNL)"
  pprtv_ornl$url <- "https://hhpprtv.ornl.gov/quickview/pprtv.php"
  pprtv_ornl = pprtv_ornl[!is.na(pprtv_ornl$casrn),]
  pprtv_ornl <- pprtv_ornl[,c("casrn","name","source","exposure_route","cancer_call","url")]

  file <- paste0(toxval.config()$datapath,"cancer_summary/cancer/HealthCanada/HealthCanada_TRVs_2010_AppendixA v2.xlsx")
  print(file)
  hc <- read.xlsx(file)
  hc$source <- "Health Canada"
  hc$url <- "http://publications.gc.ca/collections/collection_2012/sc-hc/H128-1-11-638-eng.pdf"
  hc = hc[!is.na(hc$casrn),]
  hc <- hc[,c("casrn","name","source","exposure_route","cancer_call","url")]

  file <- paste0(toxval.config()$datapath,"cancer_summary/cancer/EPA_OPP_CARC/EPA_CARC.xlsx")
  print(file)
  opp <- read.xlsx(file)
  name.list <- c("casrn","name","cancer_call")
  opp <- opp[,name.list]
  opp$exposure_route <- "-"
  opp$source <- "EPA OPP"
  opp$url <- "http://www.epa.gov/pesticides/carlist/"
  opp = opp[!is.na(opp$casrn),]
  opp <- opp[,c("casrn","name","source","exposure_route","cancer_call","url")]

  file <- paste0(toxval.config()$datapath,"cancer_summary/cancer/CalEPA/calepa_p65_cancer_only 2022-10-21.xlsx")
  print(file)
  calepa <- read.xlsx(file)
  name.list <- c("casrn","name","cancer_call")
  calepa <- calepa[,name.list]
  calepa$exposure_route <- "-"
  calepa$source <- "CalEPA"
  calepa$url <- "https://oehha.ca.gov/proposition-65/proposition-65-list"
  calepa = calepa[!is.na(calepa$casrn),]
  calepa <- calepa[,c("casrn","name","source","exposure_route","cancer_call","url")]

  mat <- rbind(niosh,iris,pprtv_ornl,ntp,iarc,hc,opp,calepa)
  mat <- mat[!is.na(mat[,"casrn"]),]
  for(i in 1:nrow(mat)) mat[i,"casrn"] <- fix.casrn(mat[i,"casrn"])
  file <- paste0(toxval.config()$datapath,"/cancer_summary/cancer_summary_",Sys.Date(),".xlsx")
  sty <- createStyle(halign="center",valign="center",textRotation=90,textDecoration = "bold")
  write.xlsx(mat,file,firstRow=T,headerStyle=sty)

  mat2 = source_chemical.extra(toxval.db,source.db,mat,"Cancer Summary")
  mat2 = subset(mat2,select=-c(casrn,name,chemical_index))

  runInsertTable(mat2, "cancer_summary", toxval.db)
}
