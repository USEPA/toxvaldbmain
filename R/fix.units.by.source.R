#-------------------------------------------------------------------------------------
#' Do all of the fixes to units
#'
#' \enumerate{
#'   \item All of these steps operate on the toxval_units column.
#'   \item Replace variant unit names with standard ones, running fix.single.param.new.by.source.R
#'   This fixes issues like variant names for mg/kg-day and uses the dictionary
#'   file dictionary/toxval_units_5.xlsx
#'   \item Fix special characters in toxval_units
#'   \item Fix issues with units containing extra characters for some ECOTOX records
#'   \item Convert units that are multiples of standard ones (e.g. ppb to ppm). This
#'   uses the dictionary file dictionary/toxval_units conversions 2018-09-12.xlsx
#'   \item Run conversions from molar to mg units, using MW. This uses the dictionary file
#'    dictionary/MW conversions.xlsx
#'   \item Convert ppm to mg/m3 for inhalation studies. This uses the conversion Concentration
#'   (mg/m3) = 0.0409 x concentration (ppm) x molecular weight. See
#'   https://cfpub.epa.gov/ncer_abstracts/index.cfm/fuseaction/display.files/fileID/14285.
#'   This function requires htat the DSSTox external chemical_id be set
#'   \item Convert ppm to mg/kg-day in toxval according to a species-specific
#'   conversion factor for oral exposures. This uses the dictionary file
#'   dictionary/ppm to mgkgday by animal.xlsx
#'   See: www10.plala.or.jp/biostatistics/1-3.doc
#'   This probbaly assumes feed rather than water
#'   \item Make sure that eco studies are in mg/L and human health in mg/m3
#' }
#'
#' @param toxval.db The version of toxvaldb to use.
#' @param source Source to be fixed
#' @param do.convert.units If TRUE, so unit conversions, as opposed to just cleaning
#' @export
#-------------------------------------------------------------------------------------
fix.units.by.source <- function(toxval.db,source=NULL, subsource=NULL,do.convert.units=F) {
  printCurrentFunction(paste(toxval.db,":", source))
  dsstox.db <- toxval.config()$dsstox.db

  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source
  for(source in slist) {
    if(source=="ECOTOX") do.convert.units=F
    if(source=="ECOTOX") {
      runQuery("update toxval set toxval_numeric_original=1E10 where toxval_numeric_original>1E10 and source='ECOTOX'",toxval.db)
    }
    cat("===============================================\n")
    cat(source,"\n")
    if(!is.null(subsource)) cat(subsource,"\n")
    cat("===============================================\n")
    cat("update 1\n")
    runQuery(paste0("update toxval set toxval_units_original='-' where toxval_units_original='' and source = '",source,"'"),toxval.db)
    cat("update 2\n")
    runQuery(paste0("update toxval set toxval_units=toxval_units_original where source = '",source,"'"),toxval.db)
    cat("update 3\n")
    runQuery(paste0("update toxval set toxval_numeric=toxval_numeric_original where source = '",source,"'"),toxval.db)

    # Remove special characters
    cat(">>> Fix special characters in units\n")
    unit.list = sort(runQuery(paste0("select distinct toxval_units_original from toxval where source = '",source,"' group by toxval_units_original"),toxval.db)[,1])
    for(i in seq_len(length(unit.list))) {
      input = unit.list[i]
      output = input
      temp = str_replace_all(output,"\uFFFD","u")
      #if(temp!=output) cat(output,":",temp,"\n")
      output = temp
      output = str_trim(output)
      query <- paste0("update toxval set toxval_units_original='",output,"' where toxval_units_original='",input,"' and source = '",source,"'")
      runInsert(query,toxval.db,T,F,T)
    }

    # Replace variant unit names with standard ones,
    cat(">>> Transform variant unit names to standard ones\n")
    fix.single.param.by.source(toxval.db,"toxval_units", source, ignore = F)

    # Replace mg/kg with mg/kg-day where toxval type is NOAEL or NOEL
    cat(">>> Replace mg/kg with mg/kg-day where toxval type is NOAEL or NOEL\n")
    query <- paste0("update toxval set toxval_units='mg/kg-day' where toxval_type in ('BMDL','BMDL05','BMDL10','HNEL','LOAEC','LOAEL','LOEC','LOEL','NEL','NOAEC','NOAEL','NOEC','NOEL') and toxval_units='mg/kg' and source = '",source,"'")
    runInsert(query,toxval.db,T,F,T)

    # Convert units to standard denominator (e.g. ppb to ppm by dividing by 1000)
    cat(">>> Convert units that are simple multiples of standard units\n")
    convos = read.xlsx(paste0(toxval.config()$datapath,"dictionary/toxval_units conversions 2022-08-22.xlsx"))
    #browser()
    query = paste0("select distinct toxval_units from toxval where source='",source,"'")
    if(!is.null(subsource)) query = paste0("select distinct toxval_units from toxval where subsource='",subsource,"'")
    tulist = runQuery(query,toxval.db)[,1]
    convos = convos[is.element(convos$toxval_units,tulist),]
    nrows = dim(convos)[1]
    if(nrows>0) {
      for (i in seq_len(nrows)){
        cat("  ",convos[i,1],convos[i,2],convos[i,3],"\n")
        query = paste0("update toxval set toxval_units = '",convos[i,2],"', toxval_numeric = toxval_numeric*",convos[i,3]," where toxval_units = '",convos[i,1],"' and source = '",source,"'")
        if(!is.null(subsource))
          query = paste0("update toxval set toxval_units = '",convos[i,2],"', toxval_numeric = toxval_numeric*",convos[i,3]," where toxval_units = '",convos[i,1],"' and subsource = '",subsource,"'")
        runInsert(query,toxval.db,T,F,T)
      }
    }

    # Run conversions from molar to mg units, using MW
    cat(">>> Run conversions from molar to mg units, using MW\n")
    convos <- read.xlsx(paste0(toxval.config()$datapath,"dictionary/MW conversions.xlsx"))
    query = paste0("select distinct toxval_units from toxval where source='",source,"'")
    if(!is.null(subsource)) query = paste0("select distinct toxval_units from toxval where subsource='",subsource,"'")
    tulist = runQuery(query,toxval.db)[,1]
    convos = convos[is.element(convos$toxval_units,tulist),]
    nrows = dim(convos)[1]
    for (i in seq_len(nrows)){
      units = convos[i,1]
      units.new = convos[i,2]
      cat("  ",convos[i,1],convos[i,2],"\n")
      query = paste0("update toxval set toxval_units = '",convos[i,2],"', toxval_numeric = toxval_numeric*mw
                         where mw>0 and toxval_units = '",convos[i,1],"' and source = '",source,"' ")
      if(!is.null(subsource))
        query = paste0("update toxval set toxval_units = '",convos[i,2],"', toxval_numeric = toxval_numeric*mw
                           where mw>0 and toxval_units = '",convos[i,1],"' and subsource = '",subsource,"' ")
      runInsert(query,toxval.db,T,F,T)
    }

    # Convert ppm to mg/m3 for inhalation studies
    # https://cfpub.epa.gov/ncer_abstracts/index.cfm/fuseaction/display.files/fileID/14285
    cat(">>> Convert ppm to mg/m3 for inhalation studies\n")
    query = paste0("update toxval
                      set toxval_units = 'mg/m3', toxval_numeric = toxval_numeric*mw*0.0409
                      where mw>0 and toxval_units like 'ppm%' and exposure_route = 'inhalation'
                      and human_eco='human health' and source = '",source,"' ")
    if(!is.null(subsource))
      query = paste0("update toxval
                        set toxval_units = 'mg/m3', toxval_numeric = toxval_numeric*mw*0.0409
                        where mw>0 and toxval_units like 'ppm%' and exposure_route = 'inhalation' and
                        human_eco='human health' and subsource = '",subsource,"' ")
    runInsert(query,toxval.db)


    # Do the conversion from ppm to mg/kg-day on a species-wise basis for oral exposures
    cat(">>> Do the conversion from ppm to mg/kg-day on a species-wise basis\n")
    conv = read.xlsx(paste0(toxval.config()$datapath,"dictionary/ppm to mgkgday by animal.xlsx"))
    for (i in seq_len(nrow(conv))){
      species <- conv[i,1]
      factor <- conv[i,2]
      sid.list <- runQuery(paste0("select species_id from species where common_name ='",species,"'"),toxval.db)[,1]
      for(sid in sid.list) {
        cat(species, sid,":",length(sid.list),"\n")
        query = paste0("update toxval
                         set toxval_numeric = toxval_numeric_original*",factor,", toxval_units = 'mg/kg-day'
                         where exposure_route='oral'
                         and toxval_units='ppm'
                         and species_id = ",sid," and source = '",source,"'")
        if(!is.null(subsource))
          query = paste0("update toxval
                           set toxval_numeric = toxval_numeric_original*",factor,", toxval_units = 'mg/kg-day'
                           where exposure_route='oral'
                           and toxval_units='ppm'
                           and species_id = ",sid," and subsource = '",subsource,"'")
        runInsert(query,toxval.db,T,F,T)
      }
    }
  }
}
