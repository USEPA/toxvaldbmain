#--------------------------------------------------------------------------------------
#' special process to deal with source chemicals for extra source (cancer, genetox, skin_eye, etc)
#' @param toxval.db The version of toxval into which the source info is loaded.
#' @param source.db The source database version
#' @param source The source to be processed #' @param chem.check.halt If TRUE, halt if there are errors in the chemical checking
#' @param casrn.col  Name of the column containing the CASRN
#' @param name.col Name of the column containing chemical names
#' @param verbose If TRUE, output extra diagnostics information
#--------------------------------------------------------------------------------------
source_chemical.extra <- function(toxval.db,
                                  source.db,
                                   res,
                                   source,
                                   chem.check.halt=FALSE,
                                   casrn.col="casrn",
                                   name.col="name",
                                   verbose=FALSE) {
  printCurrentFunction(paste0(toxval.db,"\n",source))
  if(!exists("DSSTOX")) load.dsstox()

  #####################################################################
  cat("Do the chemical checking\n")
  #####################################################################
  res = res %>%
    tidyr::unite(col="chemical_index", all_of(casrn.col), all_of(name.col), sep=" ", remove=FALSE) %>%
    # Replace unicode in raw values (database constraint for utf8 strings)
    dplyr::mutate(dplyr::across(all_of(c(casrn.col, name.col)),
                                # Replace zero width space unicode
                                ~gsub("\u200b|\u00a0", "", .)),
                  dplyr::across(all_of(c(casrn.col, name.col)),
                                # Replace prime symbol/apostrophe unicode
                                ~gsub("\u2019|\u00b4", "'", .)),
                    dplyr::across(all_of(c(casrn.col, name.col)),
                                  # Replace soft hyphen
                                  ~gsub("\u00ad", "-", .)),
                  dplyr::across(all_of(c(casrn.col, name.col)),
                                # Replace Registered symbol
                                ~gsub("\u00ae", "R", .)),
                  dplyr::across(all_of(c(casrn.col, name.col)),
                                # Replace micro greek symbol
                                ~gsub("\u00b5", "u", .)),
                  dplyr::across(all_of(c(casrn.col, name.col)),
                                # Replace small a with circumflex (â)
                                ~gsub("\u00e2", "a", .)),
                  dplyr::across(all_of(c(casrn.col, name.col)),
                                # Replace dagger (†) unicode (typically a footnote symbol)
                                ~gsub("\u2020", "+", .)))

  # gsub("\u2019", "'", .) %>%
  result = chem.check.v2(res,verbose=verbose,source)
  if(chem.check.halt) if(!result$name.OK || !result$casrn.OK || !result$checksum.OK) browser()

  #####################################################################
  cat("Build the chemical table\n")
  #####################################################################
  chems = cbind(res[,c(casrn.col,name.col)],result$res0[,c(casrn.col,name.col)])
  names(chems) = c("raw_casrn","raw_name","cleaned_casrn","cleaned_name")
  chems = distinct(chems)
  chems$source = source

  prefix = runQuery(paste0("select chemprefix from chemical_source_index where source='",source,"'"),source.db)[1,1]
  ilist = seq(from=1,to=nrow(chems))
  chems$chemical_id = "-"
  for(i in 1:nrow(chems)) {
    chems[i,"chemical_id"] = paste0(prefix,"_",digest(paste0(chems[i,c("raw_casrn","raw_name")],collapse=""),algo="xxhash64", serialize = FALSE))
  }
  # check for duplicates
  x = chems$chemical_id
  y=sum(duplicated(x))
  if(y>0) {
    cat("******************************************************************\n")
    cat("some chemical hash keys are duplicated for ",source,"\n")
    cat("******************************************************************\n")
    browser()
  }
  chems$chemical_index = paste(chems$raw_casrn,chems$raw_name)
  cat("add the dtxsid\n")
  # file = paste0(toxval.config()$datapath,"/ecotox/ecotox_files/ECOTOX chemical mapping 2022-07-21.xlsx")
  # echems = read.xlsx(file)
  # rownames(echems) = as.character(echems$casrn)
  dsstox = DSSTOX[is.element(DSSTOX$casrn,chems$cleaned_casrn),]

  chems$dtxsid = "NODTXSID"
  for(i in 1:nrow(chems)) {
    casrn = as.character(chems[i,"raw_casrn"])
    ccasrn = chems[i,"cleaned_casrn"]
    # if(is.element(casrn,echems$casrn)) {
    #   chems[i,"cleaned_name"] = echems[casrn,"name"]
    #   chems[i,"name"] = echems[casrn,"name"]
    #   chems[i,"casrn"] = ccasrn
    #   chems[i,"dtxsid"] = echems[casrn,"dtxsid"]
    # }
    # else {
      if(is.element(ccasrn,dsstox$casrn)) {
        chems[i,"cleaned_name"] = dsstox[ccasrn,"preferred_name"]
        chems[i,"casrn"] = ccasrn
        chems[i,"name"] = dsstox[ccasrn,"preferred_name"]
        chems[i,"dtxsid"] = dsstox[ccasrn,"dtxsid"]
      }
    # }
  }

  cat("add the chemical_id to res\n")
  res$chemical_id = NA
  res$dtxsid = NA
  for(i in 1:nrow(chems)) {
    indx = chems[i,"chemical_index"]
    cid = chems[i,"chemical_id"]
    dtxsid = chems[i,"dtxsid"]
    res[is.element(res$chemical_index,indx),"chemical_id"]=cid
    res[is.element(res$chemical_index,indx),"dtxsid"]=dtxsid
    if(i%%1000==0) cat(" finished ",i," out of ",nrow(chems),"\n")
  }
  chems = subset(chems,select=-c(chemical_index))
  cids = runQuery(paste0("select distinct chemical_id from source_chemical where source='",source,"'"),source.db)[,1]
  chems.new = chems[!is.element(chems$chemical_id,cids),]
  n0 = length(cids)
  n1 = nrow(chems)
  n01 = nrow(chems.new)
  newfrac = 100*(n01)/n1
  cat("**************************************************************************\n")
  cat(source,"\n")
  cat("chem matching: original,new,match:",n0,n1,n01," new percent: ",format(newfrac,digits=2),"\n")
  cat("**************************************************************************\n")

  runInsertTable(chems.new,"source_chemical",source.db,do.halt=T,verbose=F)
  cids = runQuery(paste0("select distinct chemical_id from source_chemical where source='",source,"'"),toxval.db)[,1]
  chems.new = chems[!is.element(chems$chemical_id,cids),]
  runInsertTable(chems.new,"source_chemical",toxval.db,do.halt=T,verbose=F)
  return(res)
}
