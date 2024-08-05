#--------------------------------------------------------------------------------------
#' Check the chemicals from a file
#' Names with special characters are cleaned and trimmed
#' CASRN are fixed (dashes put in, trimmed) and check sums are calculated
#' The output is sent to a file called chemcheck.xlsx in the source data file
#' One option for using this is to edit the source file until no errors are found
#'
#' @param res0  The data frame in which chemicals names and CASRN will be replaced
#' @param in_source The source to be processed. If source=NULL, process all sources
#' @param verbose If TRUE, print diagnostic messages
#' @return Return a list with fixed CASRN and name and flags indicating if fixes were made:
#' res0=res0,name.OK=name.OK,casrn.OK=casrn.OK,checksum.OK=checksum.OK
#'
#--------------------------------------------------------------------------------------
chem.check.v2 <- function(res0, in_source=NULL,verbose=FALSE) {
  printCurrentFunction(in_source)
  name.OK = TRUE
  casrn.OK = TRUE
  checksum.OK = TRUE

  cat(">>> Deal with name\n")
  chem.check.name <- function(in_name, in_source, verbose){
    n0 = in_name %>%
      # Replace zero width space unicode
      gsub("\u200b", "", .)

    if(is.na(n0)) {
      cat("NA name found...\n")
      return(n0)
    }
    n1 = n0 %>%
      iconv(.,from="UTF-8",to="ASCII//TRANSLIT")
    n2 <- n1 %>%
      stringi::stri_escape_unicode() %>%

      stringr::str_replace_all("\\\\'","\'") %>%
      stringr::str_squish()

    if(in_source %in% c("Alaska DEC",
                        "EPA AEGL",
                        "Mass. Drinking Water Standards",
                        "OSHA Air contaminants",
                        "OW Drinking Water Standards",
                        "Pennsylvania DEP MSCs",
                        "USGS HBSL",
                        "WHO IPCS",
                        "ATSDR MRLs",
                        "Cal OEHHA",
                        "COSMOS",
                        "DOD ERED",
                        "DOE Wildlife Benchmarks",
                        "DOE Protective Action Criteria",
                        "IRIS",
                        "EPA OPP",
                        "Pennsylvania DEP ToxValues",
                        "EnviroTox_v2",
                        "HEAST")) {
      # Only take first name stem before ";"
      if(grepl(";", n2)) {
        n2 = sub(';.*', '', n2)
      }
      # Remove trailing abbreviation (ex. "DI(2-ETHYLHEXYL)PHTHALATE (DEHP)" to "DI(2-ETHYLHEXYL)PHTHALATE")
      if(grepl(" \\(", n2)) {
        n2 = sub(' \\(.*', '', n2)
      }
    }
    n2 = clean.last.character(n2)
    if(verbose) cat("1>>> ",n0,n1,n2,"\n")
    return(paste(n0, n1, n2, sep="||"))
  }

  res0 = res0 %>%
    dplyr::rowwise() %>%
    dplyr::mutate(name_check = chem.check.name(in_name=name,
                                               in_source=in_source,
                                               verbose=verbose)) %>%
    dplyr::ungroup() %>%
    tidyr::separate(name_check,
                    into=c("n0", "n1", "n2"),
                    sep="\\|\\|")

  ccheck_name = res0 %>%
    dplyr::filter(n2 != n0) %>%
    dplyr::select(n0, n1, n2) %>%
    dplyr::mutate(cs = NA)

  if(nrow(ccheck_name)) {
    name.OK = FALSE
  }

  # Set name as cleaned n2, remove intermediates
  res0 = res0 %>%
    dplyr::select(-name, -n0, -n1) %>%
    dplyr::rename(name = n2)

  cat("\n>>> Deal with CASRN\n")
  chem.check.casrn <- function(in_cas, verbose){
    n0 = in_cas
    if(!is.na(n0)) {
      n0 = n0 %>%
        # Replace NO-BREAK SPACE unicode
        gsub("\u00a0", "", .)
      n1 = iconv(n0,from="UTF-8",to="ASCII//TRANSLIT")
      n2 = n1 %>%
        stringi::stri_escape_unicode() %>%
        fix.casrn()
      cs = cas_checkSum(n2)
      if(is.na(cs)) cs = 0
      if(verbose) cat("2>>> ",n0,n1,n2,cs,"\n")
      return(paste(n0, n1, n2, cs, sep="||"))
    } else {
      return(paste(NA, NA, NA, NA, sep="||"))
    }
  }

  res0 <- res0 %>%
    dplyr::rowwise() %>%
    dplyr::mutate(cas_check = chem.check.casrn(in_cas=casrn,
                                               verbose=verbose)) %>%
    dplyr::ungroup() %>%
    tidyr::separate(cas_check,
                    into=c("n0", "n1", "n2", "cs"),
                    sep="\\|\\|")

  ccheck_cas = res0 %>%
    dplyr::filter(n2 != n0) %>%
    dplyr::select(n0, n1, n2, cs)

  if(nrow(ccheck_cas)) {
    casrn.OK = FALSE
    if(any(ccheck_cas$cs == 0)){
      checksum.OK = FALSE
      cat("bad checksum present\n")
    }
  }

  # Set name as cleaned n2, remove intermediates
  res0 = res0 %>%
    dplyr::select(-casrn, -n0, -n1, -cs) %>%
    dplyr::rename(casrn = n2)

  # Prep check export
  ccheck = rbind(ccheck_name,
                 ccheck_cas) %>%
    dplyr::rename(original=n0,
                  escaped=n1,
                  cleaned=n2,
                  checksum=cs) %>%
    dplyr::distinct()

  indir = paste0(toxval.config()$datapath,"chemcheck/")
  if(is.null(in_source)) {
    file = paste0(indir,"chemcheck no source.xlsx")
  } else {
    file = paste0(indir,"chemcheck ",in_source,".xlsx")
  }
  if(!is.null(ccheck)) if(nrow(ccheck)>0) writexl::write_xlsx(ccheck,file)

  if(!name.OK) { cat("Some names fixed\n") } else { cat("All names OK\n") }
  if(!casrn.OK) { cat("Some casrn fixed\n") } else { cat("All casrn OK\n") }
  if(!checksum.OK) { cat("Some casrn have bad checksums\n") } else { cat("All checksums OK\n") }
  return(list(res0=res0,name.OK=name.OK,casrn.OK=casrn.OK,checksum.OK=checksum.OK))
}
