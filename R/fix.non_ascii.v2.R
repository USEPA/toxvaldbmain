#-------------------------------------------------------------------------------------
#' @description Flag and fix non-ascii characters in the database
#' @param df The dataframe to be processed
#' @param The source to be fixed
#' @return The dataframe with non ascii characters replaced with cleaned versions
#' @export
#' @title fix.non_ascii.v2
#' @param source Current ToxVal source
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[openxlsx]{read.xlsx}}, \code{\link[openxlsx]{write.xlsx}}
#'  \code{\link[stringr]{str_trim}}
#'  \code{\link[stringi]{stri_escape_unicode}}
#' @rdname fix.non_ascii.v2
#' @importFrom openxlsx read.xlsx write.xlsx
#' @importFrom stringr str_trim
#' @importFrom stringi stri_escape_unicode
#' @importFrom dplyr select distinct mutate filter
#' @importFrom rlang sym
#-------------------------------------------------------------------------------------
fix.non_ascii.v2 <- function(df,source){
  printCurrentFunction(source)
  non_ascii_check = rep(NA, ncol(df))
  #check for non ascii characters
  for (i in seq_len(ncol(df))){
    non_ascii_check[i] <- any(grepl("NON_ASCII", iconv(df[[names(df)[i]]], "UTF-8", "ASCII", sub="NON_ASCII")))
    #cat("Dataframe Column having non_ascii characters :", i, "\n")
    #print( non_ascii_check[i] )
  }
  clist = names(df)[non_ascii_check==TRUE]
  # Fix non-ASCII characters if identified
  if(length(clist)>0) {
    # file = paste0(toxval.config()$datapath,"dictionary/2021_dictionaries/unicode map.xlsx")
    # map = readxl::read_xlsx(file) %>%
    #   dplyr::select(unicode, replacement)
    #
    # rownames(map) = map$unicode
    # row = data.frame(matrix(nrow=1,ncol=2))
    # res = NULL
    # names(row) = c("raw","converted")
    # missing = NA
    for(col in clist) {
      # Create list of all non-ASCII characters
      non_ascii_find = unique(df[[col]])[grep("NON_ASCII",
                                              iconv(unique(df[[col]]), "UTF-8", "ASCII", sub="NON_ASCII"))]
      # Create list of Unicode characters identified
      unicode_find <- df %>%
        dplyr::select(!!col) %>%
        dplyr::distinct() %>%
        dplyr::mutate(uni_check = stringi::stri_escape_unicode(!!rlang::sym(col))) %>%
        dplyr::filter(!!rlang::sym(col) != uni_check) %>%
        dplyr::select(!!col) %>% unlist %>% unname()

      # Create combined list of every non-ASCII character encountered
      non_ascii_find <- unique(c(non_ascii_find, unicode_find))

      for(i in non_ascii_find) {
        # Fix each non-ASCII character, if possible
        tryCatch({
          n0 = i
          n1 = iconv(n0,from="UTF-8",to="ASCII//TRANSLIT")
          n2 = stringr::str_trim(stringi::stri_escape_unicode(n1))
          # row[1,"raw"] = n0
          # row[1,"converted"] = n1
          # res = rbind(res,row)
          # Replace all instances
          df[[col]][df[[col]] == n0] = n2
        }, warning = function(w) {
          cat("fix.non_ascii.v2 WARNING:",n1,"\n")
          browser()
        }, error = function(e) {
          cat("fix.non_ascii.v2 ERROR:",n1,"\n")
          browser()
        })
      }
    }
    # missing = sort(unique(missing))
    # if(length(missing)>0) {
    #   print(missing)
    #   file = paste0(toxval.config()$datapath,"chemcheck/non_ascii ",source,".xlsx")
    #   writexl::write_xlsx(missing,file)
    # }
  }

  return(df)
}
