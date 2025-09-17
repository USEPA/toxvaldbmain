#--------------------------------------------------------------------------------------
#' Load and process all information into ToxValDB. The entire process can be run with
#' one command: toxval.load.all(toxval.db=...,source.db=..., do.all=TRUE)
#' It can also be run in stages, but needs to be run in the order of the do.X parameters
#' listed here. If any earlier step is run, all of the subsequent steps need to be rerun.
#'
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source.db The version of toxval_source database from which information is pulled.
#' @param jira_access_token A personal access token for authentication in Jira
#' @param confluence_access_token A personal access token for authentication in Confluence
#' @param log If TRUE write the output from each load script to a log file
#' @param do.init If True, clean out all of the database tables
#' @param do.reset If TRUE, empty the database to restart
#' @param do.load If TRUE, load all of the source
#' @param do.post If TRUE, do th post-processingwork of fixing study type and risk_assessment_class
#' @param do.extra If TRUE, load the non-toxval data (genetox, bcfbaf, skin/eye)
#' @return Nothing is returned
#' @export
#'
toxval.load.all <- function(toxval.db,
                            source.db,
                            confluence_access_token,
                            jira_access_token,
                            log=FALSE,
                            do.init=FALSE,
                            do.reset=FALSE,
                            do.load=FALSE,
                            do.post=FALSE,
                            do.extra=FALSE) {
  printCurrentFunction(toxval.db)

  if(do.init)  {
    toxval.init.db(toxval.db, reset=do.reset)
  }

  # Load package and then list out the current load functions
  # ls("package:toxvaldbmain") %>%
  #   .[grepl("^toxval.load", .)] %>%
  #   # Remove select functions
  #   .[!grepl("all$|initial$|postprocess$|bcfbaf$|cancer$|genetox\\.all$|skin\\.eye$|dedup$|generic$|source_chemical$|species$", .)] %>%
  #   paste0(., "(toxval.db, source.db, log)") %>%
  #   cat(sep="\n")

  if(do.load)  {

    toxval.load.alaska_dec(toxval.db, source.db, log)
    toxval.load.atsdr.pfas.2021(toxval.db, source.db, log)
    toxval.load.atsdr_mrls(toxval.db, source.db, log)
    toxval.load.caloehha(toxval.db, source.db, log)
    toxval.load.chemidplus(toxval.db, source.db, log)
    toxval.load.copper(toxval.db, source.db, log)
    toxval.load.dod(toxval.db, source.db, log)
    toxval.load.doe.benchmarks(toxval.db, source.db, log)
    toxval.load.doe.pac(toxval.db, source.db, log)
    toxval.load.echa_iuclid(toxval.db, source.db, log)
    toxval.load.ecotox(toxval.db, source.db, log)
    toxval.load.efsa(toxval.db, source.db, log)
    # Special case for EFSA that needs postprocessing twice
    # due to study_type and human_eco reassignment
    toxval.load.postprocess(toxval.db, source.db, "EFSA")
    toxval.load.epa_aegl(toxval.db, source.db, log)
    toxval.load.epa_hhtv(toxval.db, source.db, log)
    toxval.load.epa_ow_npdwr(toxval.db, source.db, log)
    toxval.load.epa_ow_nrwqc_hhc(toxval.db, source.db, log)
    toxval.load.gestis.dnel(toxval.db, source.db, log)
    toxval.load.hawc(toxval.db, source.db, log)
    toxval.load.hawc_pfas_150(toxval.db, source.db, log)
    toxval.load.hawc_pfas_430(toxval.db, source.db, log)
    toxval.load.healthcanada(toxval.db, source.db, log)
    toxval.load.heast(toxval.db, source.db, log)
    toxval.load.hess(toxval.db, source.db, log)
    toxval.load.hpvis(toxval.db, source.db, log)
    toxval.load.iris(toxval.db, source.db, log)
    toxval.load.mass_mmcl(toxval.db, source.db, log)
    toxval.load.niosh(toxval.db, source.db, log)
    toxval.load.ntp.pfas(toxval.db, source.db, log)
    toxval.load.opp(toxval.db, source.db, log)
    toxval.load.osha_air_limits(toxval.db, source.db, log)
    toxval.load.ow_dwsha(toxval.db, source.db, log)
    toxval.load.penn(toxval.db, source.db, log)
    toxval.load.penn_dep_mscs(toxval.db, source.db, log)
    toxval.load.pfas_150_sem_v2(toxval.db, source.db, log)
    toxval.load.pprtv.cphea(toxval.db, source.db, log)
    toxval.load.rsl(toxval.db, source.db, log)
    toxval.load.test(toxval.db, source.db, log)
    toxval.load.toxrefdb2.1(toxval.db, source.db, log)
    toxval.load.usgs_hbsl(toxval.db, source.db, log)
    toxval.load.ut_hb(toxval.db, source.db, log)
    toxval.load.who_ipcs(toxval.db, source.db, log)
    toxval.load.who_jecfa_adi(toxval.db, source.db, log)
    toxval.load.who_jecfa_tox_studies(toxval.db, source.db, log)
    toxval.load.epa_dcap(toxval.db, source.db, log)
    toxval.load.epa_etap(toxval.db, source.db, log)
    
    toxval.load.epa_ncel(toxval.db, source.db, log)
    toxval.load.epa_ecel(toxval.db, source.db, log)
    toxval.load.il_epa(toxval.db, source.db, log)
    toxval.load.echa_rac_oel(toxval.db, source.db, log)
  }

  if(do.post) {
    fix.study_type.by.source(toxval.db, mode="import", source=NULL)
    fix.risk_assessment_class.by.source(toxval.db, restart=TRUE)
    # load.dsstox()
  }

  if(do.extra) {
    toxval.load.bcfbaf(toxval.db, source.db, verbose=FALSE)
    toxval.load.cancer(toxval.db, source.db)
    toxval.load.genetox.all(toxval.db, source.db, sys.date="2024-04-01", verbose=FALSE)
    toxval.load.skin.eye(toxval.db, source.db, verbose=FALSE)
  }

  #####################################################################
  cat("fix deduping hierarchy by source\n")
  #####################################################################
  fix.dedup.hierarchy.by.source(toxval.db=toxval.db)
  #####################################################################
  cat("Delete qc_status fail records after final dedup hierarchy check\n")
  #####################################################################
  export.delete.qc_status.fail.by.source(toxval.db)

  # Set QC Category - need Jira and Confluence API tokens
  set.qc.category.by.source(toxval.db, source.db,
                            confluence_access_token=confluence_access_token,
                            jira_access_token=jira_access_token)
  # Set extraction documents
  set_extraction_doc_clowder_id(toxval.db, source.db)
}

