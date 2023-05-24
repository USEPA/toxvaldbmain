#-----------------------------------------------------------------------------------
#' Set the study_group field
#' series of xlsx files
#'
#' @param toxval.db Database version
#' @param source The source to be updated
#' #' @return for each source writes an Excel file with the name
#'  ../export/export_by_source_{data}/toxval_all_{toxval.db}_{source}.xlsx
#'
#-----------------------------------------------------------------------------------
fix.study_group <- function(toxval.db, source=NULL,reset=F) {
  printCurrentFunction(toxval.db)

  if(reset) runQuery("update toxval set study_group='-'",toxval.db)
  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source
  slist = slist[!is.element(slist,c("ECOTOX"))]
  for(source in slist) {
    sglist = runQuery(paste0("select distinct study_group from toxval where source='",source,"'"),toxval.db)[,1]
    doit = F
    if(is.element("-",sglist)) doit = T
    if(doit) {
      cat(source,"\n")
      runQuery(paste0("update toxval set study_group='-' where source='",source,"'"),toxval.db)

      query = paste0("select a.toxval_id,a.dtxsid,c.common_name, a.toxval_units,
                      a.target_species, a.study_type, a.exposure_route,a.exposure_method,
                      a.study_duration_value, a.study_duration_units,
                      a.strain,
                      b.year, b.long_ref, b.title, b.author
                      from toxval a, record_source b, species c where a.species_id=c.species_id and a.toxval_id=b.toxval_id and a.source='",source,"'")
      temp = runQuery(query,toxval.db)
      temp$key = NA
      temp$study_group = NA
      gtemp = subset(temp, select = -c(toxval_id) )
      for(i in 1:nrow(gtemp)) temp[i,"key"] = digest(paste0(gtemp[i,],collapse=""), serialize = FALSE)
      hlist = unique(temp$key)
      for(i in 1:length(hlist)) {
        sg = paste0(source,"_dup_",i)
        temp[temp$key==hlist[i],"study_group"] = sg
      }

      nr = nrow(temp)
      nsg = length(unique(temp$study_group))
      cat("  nrow:",nr," unique values:",nsg,"\n")
      query = paste0("update toxval set study_group=CONCAT(source,'_',toxval_id) where source='",source,"'")
      runQuery(query,toxval.db)

      if(nsg!=nr) {
        dups = sort(unique(temp[duplicated(temp$study_group),"study_group"]))
        cat("   Number of dups:",length(dups),"\n")
        chunk = ""
        for(i in 1:length(dups)) {
          sg = dups[i]
          tids = temp[is.element(temp$study_group,sg),"toxval_id"]
          for(tid in tids) {
            chunk = paste0(chunk,"(",tid,",'",sg,"'),")
          }
        }
        chunk = substr(chunk,1,(nchar(chunk)-1))
        query = paste0("INSERT INTO toxval (toxval_id,study_group) VALUES ",chunk," ON DUPLICATE KEY UPDATE study_group=VALUES(study_group)")
        #browser()
        runQuery(query,toxval.db)
        #browser()
        # INSERT INTO students
        # (id, score1, score2)
        # VALUES
        # (1, 5, 8),
        # (2, 10, 8),
        # (3, 8, 3),
        # (4, 10, 7)
        # ON DUPLICATE KEY UPDATE
        # score1 = VALUES(score1),
        # score2 = VALUES(score2);


        # for(i in 1:length(dups)) {
        #   sg = dups[i]
        #   tids = temp[is.element(temp$study_group,sg),"toxval_id"]
        #   for(tid in tids) {
        #     query = paste0("update toxval set study_group='",sg,"' where toxval_id='",tid,"'")
        #     runQuery(query,toxval.db)
        #   }
        #   if(i%%1000==0) cat("   finished",i," out of",length(dups),"\n")
        # }
      }
    }
  }
}
