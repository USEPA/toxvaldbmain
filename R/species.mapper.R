#--------------------------------------------------------------------------------------
#'
#' Map the species to the ECOTOX species dictionary and export the missing species to
#' add to the dictionary
#'
#' This function replaces fix.species
#' This function precedes toxvaldb.load.species
#'
#' @param toxval.db The version of the database to use
#' @param date_string The date of the dictionary versions
#' @export
#--------------------------------------------------------------------------------------
species.mapper <- function(toxval.db,date_string="2023-02-14") {
  printCurrentFunction()
  file =paste0(toxval.config()$datapath,"species/ecotox_species_dictionary_",date_string,".xlsx")
  print(file)
  dict = read.xlsx(file)
  file = paste0(toxval.config()$datapath,"species/ecotox_species_synonyms_",date_string,".xlsx")
  print(file)
  synonyms = read.xlsx(file)
  file = paste0(toxval.config()$datapath,"species/toxvaldb_extra_species_",date_string,".xlsx")
  print(file)
  extra = read.xlsx(file)

  dict$latin_name = tolower(dict$latin_name)
  dict$common_name = tolower(dict$common_name)
  synonyms$latin_name = tolower(synonyms$latin_name)
  extra$common_name = tolower(extra$common_name)
  extra$latin_name = tolower(extra$latin_name)
  extra$species_original = tolower(extra$species_original)

  so = runQuery("select distinct species_original from toxval",toxval.db)
  so$species_id = -1
  so[,1] = tolower(so[,1])
  for(i in 1:nrow(so)) {
    tag = so[i,1]
    tag = tolower(tag)
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
      #cat("common:",tag,sid,"\n")
      #browser()
    }
    else if(is.element(tag,dict$latin_name)) {
      sid = dict[is.element(dict$latin_name,tag),"species_id"][1]
      #cat("latin:",tag,sid,"\n")
      #browser()
    }
    else if(is.element(tag,synonyms$latin_name)) {
      sid = dict[is.element(synonyms$latin_name,tag),"species_id"][1]
      #cat("synonym:",tag,sid,"\n")
      #browser()
    }
    else if(is.element(tag,extra$species_original)) {
      sid = extra[is.element(extra$species_original,tag),"species_id"][1]
      #cat("synonym:",tag,sid,"\n")
      #browser()
    }
    else if(is.element(tag0,dict$common_name)) {
      sid = dict[is.element(dict$common_name,tag0),"species_id"][1]
      #cat("common:",tag,sid,"\n")
      #browser()
    }
    else if(is.element(tag0,dict$latin_name)) {
      sid = dict[is.element(dict$latin_name,tag0),"species_id"][1]
      #cat("latin:",tag,sid,"\n")
      #browser()
    }
    else if(is.element(tag0,synonyms$latin_name)) {
      sid = dict[is.element(synonyms$latin_name,tag0),"species_id"][1]
      #cat("synonym:",tag,sid,"\n")
      #browser()
    }
    else if(is.element(tag0,extra$species_original)) {
      sid = extra[is.element(extra$species_original,tag0),"species_id"][1]
      #cat("synonym:",tag,sid,"\n")
      #browser()
    }
    # if(tag=="mouse") {
    #   cat(tag,"\n")
    #   browser()
    # }

    so[i,"species_id"] = sid
    if(i%%100==0) {
      nadd = nrow(so[so$species_id>=0,])
      cat("finished",i,"out of",nrow(so),":",nadd,"\n")
    }

  }
  file = paste0(toxval.config()$datapath,"species/all_species.xlsx")
  write.xlsx(so,file)
  so = so[so$species_id<0,]
  cat("Number of missing species:",nrow(so),"\n")
  file = paste0(toxval.config()$datapath,"species/missing_species.xlsx")
  write.xlsx(so,file)
}
