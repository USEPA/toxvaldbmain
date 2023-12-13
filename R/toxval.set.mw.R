#--------------------------------------------------------------------------------------
#' Set the molecular weight in the toxval table, for use in unit conversions
#' @param toxval.db The database version to use
#' @param source The source
#--------------------------------------------------------------------------------------
toxval.set.mw <- function(toxval.db, source=NULL){
  printCurrentFunction(toxval.db)
  dsstox.db <- toxval.config()$dsstox.db
  if(!is.null(source)) {
    slist = source
  } else {
    slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  }
  # Loop through each source
  for(source in slist) {
    cat(source,"\n")
    runQuery(paste0("update toxval set mw=-1 where mw is null and source='",source,"'"),toxval.db)
    # Pull list of DTXSID values as a vector
    dlist = runQuery(paste0("select distinct dtxsid from toxval where source='",source,"' and mw<0"),toxval.db)[,1]
    # Check if any dtxsid values returned
    if(!length(dlist) | is.na(dlist)){
      return("No mapped dtxsid values to use to set mw...returning...")
    }
    # Test of API is up and running
    api_test <- httr::GET("https://api-ccte.epa.gov/docs/chemical.html") %>%
      httr::content()

    # Use bulk DTXSID CCTE Chemicals API pull (limit 200 per call)
    if(!is.null(API_AUTH) & !grepl("404 Not Found", api_test)){
      cat("...Pulling DSSTox mw using CCTE API...\n")
      # Split list into subsets of 200
      mw <- dlist %>%
        split(., rep(1:ceiling(length(.)/200), each=200, length.out=length(.)))
      # Loop through the groups of 200 DTXSID values
      for(i in seq_along(mw)){
        # Wait between calls for API courtesy
        Sys.sleep(0.25)
        cat("...Pulling DSSTox mw ", i , " of ", length(mw), "\n")
        mw[[i]] <- httr::POST(
          "https://api-ccte.epa.gov/chemical/detail/search/by-dtxsid/",
          httr::accept_json(),
          httr::content_type_json(),
          # Use API Key for authorization
          httr::add_headers(`x-api-key` = API_AUTH),
          encode = "json",
          body=as.list(mw[[i]])
        ) %>%
          httr::content() %>%
          dplyr::bind_rows() %>%
          dplyr::select(dtxsid, mw=averageMass)
      }
      # Combine all results
      mw = dplyr::bind_rows(mw)
    } else {
      cat("...Pulling DSSTox mw using DSSTox Database...\n")
      # Access directly from DSSTox (if user has SELECT access)
      # Query DSSTox once for source dlist
      query = paste0(
        "select ",
        dsstox.db,".generic_substances.dsstox_substance_id as dtxsid, ",
        dsstox.db,".compounds.mol_weight as mw
        from ",
        dsstox.db,".compounds, ",
        dsstox.db,".generic_substance_compounds, ",
        dsstox.db,".generic_substances
        where ",
        dsstox.db,".compounds.id = ",dsstox.db,".generic_substance_compounds.fk_compound_id
        and ",
        dsstox.db,".generic_substance_compounds.fk_generic_substance_id = ",dsstox.db,".generic_substances.id
        and ",dsstox.db,".generic_substances.dsstox_substance_id IN ('",
        paste0(dlist, collapse = "', '"),
        "')"
      )
      mw = runQuery(query,toxval.db) %>%
        filter(!is.na(mw))
    }

    # Only run if mw values are present for selected DTXSID values
    if(nrow(mw)){
      # Query to inner join and make updates with mw dataframe (temp table added/dropped)
      updateQuery = paste0("UPDATE toxval a INNER JOIN z_updated_df b ",
                           "ON (a.dtxsid = b.dtxsid) SET a.mw = b.mw ",
                           "WHERE a.mw < 0")
      # Run update query
      runUpdate(table="toxval", updateQuery=updateQuery, updated_df=mw, db=toxval.db)
    }
  }
}
