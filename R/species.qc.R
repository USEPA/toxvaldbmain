#-------------------------------------------------------------------------------------
#' Run some checks on the species information
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @export
#--------------------------------------------------------------------------------------
species.qc <- function(toxval.db) {
  printCurrentFunction(toxval.db)
  dir = paste0(toxval.config()$datapath,"species/QC/")
  slist = runQuery("select distinct source from toxval",toxval.db)[,1]

  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  splist = runQuery("select distinct human_eco from toxval",toxval.db)[,1]
  res = as.data.frame(matrix(nrow=length(splist),ncol=length(slist)))
  names(res) = slist
  rownames(res) = splist
  res[] = 0
  for(i in 1:length(slist)) {
    source = slist[i]
    query = paste0("select distinct human_eco from toxval where source='",source,"'")
    x = runQuery(query,toxval.db)[,1]
    res[x,i] = 1
  }
  res = as.data.frame(t(res))
  for(i in 1:ncol(res)) res[,i] = as.numeric(res[,i])
  res = cbind(rownames(res),res)
  names(res)[1] = "source"
  file = paste0(dir,"source x human_eco.xlsx")
  openxlsx::write.xlsx(res,file)

  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  splist = runQuery("select distinct common_name from species where species_id in (select distinct species_id from toxval)",toxval.db)[,1]
  res = as.data.frame(matrix(nrow=length(splist),ncol=length(slist)))
  names(res) = slist
  rownames(res) = splist
  res[] = 0
  for(i in 1:length(slist)) {
    source = slist[i]
    query = paste0("select distinct common_name from species where species_id in (select distinct species_id from toxval where source='",source,"')")
    x = runQuery(query,toxval.db)[,1]
    res[x,i] = 1
  }
  res = cbind(rownames(res),res)
  names(res)[1] = "species"
  file = paste0(dir,"source x species.xlsx")
  openxlsx::write.xlsx(res,file)
}


