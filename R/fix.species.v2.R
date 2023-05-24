#--------------------------------------------------------------------------------------
#'
#' Set the species_id column in toxval
#'
#' This function replaces fix.species
#' This function precedes toxvaldb.load.species
#'
#' @param toxval.db The version of the database to use
#' @param source The source to be fixed
#' @param date_string The date version of the dictionary
#' @export
#--------------------------------------------------------------------------------------
fix.species.v2 <- function(toxval.db,source=NULL,date_string="2023-05-18") {
  printCurrentFunction()
  file =paste0(toxval.config()$datapath,"species/ecotox_species_dictionary_",date_string,".xlsx")
  dict = read.xlsx(file)
  file = paste0(toxval.config()$datapath,"species/ecotox_species_synonyms_",date_string,".xlsx")
  synonyms = read.xlsx(file)
  file = paste0(toxval.config()$datapath,"species/toxvaldb_extra_species_",date_string,".xlsx")
  extra = read.xlsx(file)

  dict2 = extra[,c("species_id","common_name","latin_name","ecotox_group")]
  dict2 = dict2[!is.element(dict2$species_id,dict$species_id),]
  dict2$kingdom = NA
  dict2$phylum_division = NA
  dict2$subphylum_div = NA
  dict2$superclass = NA
  dict2$class = NA
  dict2$tax_order = NA
  dict2$family = NA
  dict2$genus = NA
  dict2$species = NA
  dict2$subspecies = NA
  dict2$variety = NA
  dict2 = dict2[,names(dict)]
  dict = rbind(dict,dict2)
  dict$latin_name = tolower(dict$latin_name)
  dict$common_name = tolower(dict$common_name)
  synonyms$latin_name = tolower(synonyms$latin_name)
  extra$common_name = tolower(extra$common_name)
  extra$latin_name = tolower(extra$latin_name)
  extra$species_original = tolower(extra$species_original)

  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source

  for(source in slist) {
    cat(">>> fix.species.v2: ",source,"\n")

    so.1 = runQuery(paste0("select distinct species_original from toxval where source='",source,"' and species_id in (-1,1000000)"),toxval.db)[,1]
    so.2 = runQuery(paste0("select distinct species_original from toxval where source='",source,"' and species_id not in (select species_id from species)"),toxval.db)[,1]
    so = c(so.1,so.2)

    count.good = 0
    cat("Start setting species_id:",length(so),"\n")
    if(length(so)>0) {
      for(i in 1:length(so)) {
        tag = so[i]
        tag = tolower(tag)
        #tag = str_replace_all(tag,"\'","\\\\'")
        tag0 = tag
        nc = nchar(tag)
        tagend = substr(tag,nc-2,nc)
        if(tagend==" sp") {
          tag = paste0(tag,".")
        }
        slist = c("other aquatic arthropod ","other aquatic crustacea: ","other aquatic mollusc: ",
                  "other aquatic worm: ","other,","other,other algae: ","other: ")
        for(x in slist) {
          if(contains(tag,x)) tag = str_replace(tag,x,"")
        }
        sid = -1
        if(is.element(tag,dict$common_name)) {
          sid = dict[is.element(dict$common_name,tag),"species_id"][1]
        }
        else if(is.element(tag,dict$latin_name)) {
          sid = dict[is.element(dict$latin_name,tag),"species_id"][1]
        }
        else if(is.element(tag,synonyms$latin_name)) {
          sid = dict[is.element(synonyms$latin_name,tag),"species_id"][1]
        }
        else if(is.element(tag,extra$species_original)) {
          sid = extra[is.element(extra$species_original,tag),"species_id"][1]
        }
        else if(is.element(tag0,dict$common_name)) {
          sid = dict[is.element(dict$common_name,tag0),"species_id"][1]
        }
        else if(is.element(tag0,dict$latin_name)) {
          sid = dict[is.element(dict$latin_name,tag0),"species_id"][1]
        }
        else if(is.element(tag0,synonyms$latin_name)) {
          sid = dict[is.element(synonyms$latin_name,tag0),"species_id"][1]
        }
        else if(is.element(tag0,extra$species_original)) {
          sid = extra[is.element(extra$species_original,tag0),"species_id"][1]
        }
        else if(is.element(tag,extra$species_original)) {
          sid = extra[is.element(extra$species_original,tag),"species_id"][1]
        }
        cat(tag,sid,"\n")
        if(sid>=0) {
          count.good = count.good+1
          query = paste0("update toxval set species_id=",sid," where source='",source,"' and species_original='",str_replace_all(tag0,"\\\'","\\\\'"),"'")
          runQuery(query,toxval.db)
        }
        else {
          cat(tag,"\n")
          #browser()
        }
        if(i%%100==0) cat("finished",i,"out of",length(so),":",count.good,"\n")
      }
    }
    runQuery("update toxval set species_id=4510 where species_id=23410",toxval.db)
    runQuery("update toxval set species_id=1000000 where species_id=-1",toxval.db)
  }
}
