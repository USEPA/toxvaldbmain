#-------------------------------------------------------------------------------------
#' Flag and fix non-ascii characters in the database
#' @param df The dataframe to be processed
#' @param The source to be fixed
#' @return The dataframe with non ascii characters replaced with cleaned versions
#' @export
#-------------------------------------------------------------------------------------
fix.non_ascii.v2 <- function(df,source){
  printCurrentFunction(source)
  non_ascii_check = ""
  #check for non ascii characters
  for (i in 1:ncol(df)){
    non_ascii_check[i] <- any(grepl("NON_ASCII", iconv(df[,names(df)[i]], "UTF-8", "ASCII", sub="NON_ASCII")))
    #cat("Dataframe Column having non_ascii characters :", i, "\n")
    #print( non_ascii_check[i] )
  }
  clist = names(df)[non_ascii_check==T]
  if(length(clist)>0) {
    file = paste0(toxval.config()$datapath,"dictionary/2021_dictionaries/unicode map.xlsx")
    map = openxlsx::read.xlsx(file)
    map$unicode = toupper(map$unicode)
    x = map$unicode

    rownames(map) = map$unicode
    row = as.data.frame(matrix(nrow=1,ncol=2))
    res = NULL
    names(row) = c("raw","converted")
    missing = NA
    for(col in clist) {
      non_ascii_find = list(grep("NON_ASCII", iconv(df[,col], "UTF-8", "ASCII", sub="NON_ASCII")))[[1]]
      for(i in 1:length(non_ascii_find)) {
        tryCatch({
          n0 = df[non_ascii_find[i],col]
          n1 = iconv(n0,from="UTF-8",to="ASCII//TRANSLIT")
          n2 = str_trim(stri_escape_unicode(n1))
          row[1,"raw"] = n0
          row[1,"converted"] = n1
          res = rbind(res,row)
          df[non_ascii_find[i],col] = n1[1]
        }, warning = function(w) {
          cat("fix.non_ascii.v2 WARNING:",n1,"\n")
          browser()
        }, error = function(e) {
          cat("fix.non_ascii.v2 ERROR:",n1,"\n")
          browser()
        })
      }
    }
    missing = sort(unique(missing))
    if(length(missing)>0) {
      print(missing)
      file = paste0(toxval.config()$datapath,"chemcheck/non_ascii ",source,".xlsx")
      openxlsx::write.xlsx(missing,file)
    }
  }

  return(df)
}
