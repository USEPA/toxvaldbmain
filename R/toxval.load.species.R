#-------------------------------------------------------------------------------------
#' Load the species table
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param date_string The date string for the dictionary files
#' @export
#--------------------------------------------------------------------------------------
toxval.load.species <- function(toxval.db,date_string="2023-05-18") {
  printCurrentFunction(toxval.db)

  #################################################################
  cat("load the species table\n")
  #################################################################
  file = paste0(toxval.config()$datapath,"species/ecotox_species_dictionary_",date_string,".xlsx")
  print(file)
  dict = openxlsx::read.xlsx(file)
  file = paste0(toxval.config()$datapath,"species/ecotox_species_synonyms_",date_string,".xlsx")
  print(file)
  synonyms = openxlsx::read.xlsx(file)
  file = paste0(toxval.config()$datapath,"species/toxvaldb_extra_species_",date_string,".xlsx")
  print(file)
  extra = openxlsx::read.xlsx(file)

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
  count = runQuery("select count(*) from species where species_id=-1",toxval.db)[1,1]
  if(count==0) {
    runInsert("insert into species (species_id,common_name,latin_name) values (-1,'Not Specified','Not Specified')",toxval.db,do.halt=T)
  }
  slist = runQuery("select species_id from species",toxval.db)[,1]
  dict3 = dict[!is.element(dict$species_id,slist),]
  cat("Number of species records to be entered:",nrow(dict3),"\n")
  #
  # runQuery("delete from species where species_id>=0",toxval.db)
  runInsertTable(dict3,"species",toxval.db)
  d2 = unique(dict[,c("species_id","ecotox_group")])
  d3 = runQuery("select species_id,ecotox_group from species",toxval.db)
  d2$index = paste(d2$species_id,d2$ecotox_group)
  d3$index = paste(d3$species_id,d3$ecotox_group)
  d2 = d2[!is.element(d2$index,d3$index),]
  if(nrow(d2)>0) {
    cat("update the ecotox group",nrow(d2),"\n")
    for(i in 1:nrow(d2)) {
      query = paste0("update species set ecotox_group='",d2[i,"ecotox_group"],"' where species_id=",d2[i,"species_id"])
      runQuery(query,toxval.db)
      if(i%%1000==0) cat("finished ",i," out of ",nrow(dict),"\n")
    }
  }
  runQuery("update species set common_name='Rat' where species_id=4510",toxval.db)
  runQuery("update species set common_name='Mouse' where species_id=4913",toxval.db)
  runQuery("update species set common_name='Dog' where species_id=4928",toxval.db)
  runQuery("update species set common_name='Dog' where species_id=7630",toxval.db)
  runQuery("update species set common_name='Rabbit' where species_id=22808",toxval.db)
  runQuery("update species set common_name='Cat' where species_id=7378",toxval.db)
  runQuery("update toxval set species_id=4510 where species_id=23410",toxval.db)
  runQuery("update toxval set species_id=7378 where species_original='cat'",toxval.db)
  runQuery("update toxval set species_id=11181 where species_original='cattle'",toxval.db)
  runQuery("update toxval set species_id=7630 where species_original='dog'",toxval.db)
  runQuery("update toxval set species_id=4988 where species_original='guinea pig'",toxval.db)
  runQuery("update toxval set species_id=21216 where species_original='hamster'",toxval.db)
  runQuery("update toxval set species_id=2000000 where species_original='human'",toxval.db)
  runQuery("update toxval set species_id=2000004 where species_original='monkey'",toxval.db)
  runQuery("update toxval set species_id=4913 where species_original='mouse'",toxval.db)
  runQuery("update toxval set species_id=4946 where species_original='pig'",toxval.db)
  runQuery("update toxval set species_id=22808 where species_original='rabbit'",toxval.db)
  runQuery("update toxval set species_id=4510 where species_original='rat'",toxval.db)
  fix.species.common_name(toxval.db)
  fix.species.duplicates(toxval.db)
}
