#' @title fix.rename.source.table
#' @description Function to help rename a source within the toxval_source database
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param old_name Old source name (e.g., DOE ECORISK)
#' @param old_table Old source table name (e.g., source_lanl)
#' @param new_name New source name (e.g., DOE LANL ECORISK)
#' @param new_table New source table name (e.g., source_doe_lanl_ecorisk)
#' @return None. SQL statements are carried out.
fix.rename.source.table <- function(toxval.db, old_name, old_table, new_name, new_table){

  message("Renaming ", old_name , " (", old_table,") to ", new_name, " (", new_table,")")
  # Drop UPDATE triggers (not auditing a name change)
  # Drop trigger if exists already
  # runQuery(query= "DROP TRIGGER IF EXISTS source_table_audit_bu" %>%
  #            gsub("source_table", old_table, .),
  #          db=db)
#
#   # Drop trigger if exists already
#   runQuery(query= "DROP TRIGGER IF EXISTS source_table_update_bu" %>%
#              gsub("source_table", old_table, .),
#            db=db)

  # record_source
  tmp <- runQuery(paste0("SELECT * FROM record_source WHERE source = '", old_name,"'"), toxval.db)
  if(nrow(tmp)){
    runQuery(paste0("UPDATE record_source SET source = '", new_name, "' ",
                    "WHERE source = '", old_name, "'"),
             toxval.db)
  }

  # source_chemical
  tmp <- runQuery(paste0("SELECT * FROM source_chemical WHERE source = '", old_name,"'"), toxval.db)
  if(nrow(tmp)){
    runQuery(paste0("UPDATE source_chemical SET source = '", new_name, "' ",
                    "WHERE source = '", old_name, "'"),
             toxval.db)
  }

  # source_info
  tmp <- runQuery(paste0("SELECT * FROM source_info WHERE source = '", old_name,"'"), toxval.db)
  if(nrow(tmp)){
    runQuery(paste0("UPDATE source_info SET source = '", new_name, "' ",
                    "WHERE source = '", old_name, "'"),
             toxval.db)
  }

  # toxval
  tmp <- runQuery(paste0("SELECT * FROM toxval WHERE source = '", old_name,"'"), toxval.db)
  if(nrow(tmp)){
    runQuery(paste0("UPDATE toxval SET source = '", new_name, "', ",
                    "source_table = '", new_table,"', details_text = '",
                    new_name," Details' WHERE source = '", old_name, "'"),
             toxval.db)
  }

  message("Done.")

}
