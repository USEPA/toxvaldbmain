jira_load_file_from_api <- function(url, headers, file_type, mode = "w"){
  temp_in <- tempfile(fileext = paste0(".", file_type))
  out <- tryCatch({
    utils::download.file(url = url,
                         destfile = temp_in,
                         headers = headers,
                         mode = mode)
    if(file_type == "csv"){
      readr::read_csv(temp_in,
                      col_types = readr::cols()) %>%
        dplyr::mutate(dplyr::across(where(is.character), stringr::str_squish)) %>%
        return()
    } else if(file_type == "xlsx"){
      sheet_ls = readxl::excel_sheets(temp_in)
      lapply(sheet_ls, function(s_name){
        readxl::read_xlsx(temp_in,
                          sheet=s_name) %>%
          dplyr::mutate(dplyr::across(where(is.character), stringr::str_squish))
      }) %T>% {
        names(.) <- sheet_ls
      }
    } else {
      stop("'load_file_from_api()' unsupported file_type '", file_type,"'")
    }
  }, error=function(e) {
    message(e)
    return(NULL)
  },
  finally = { unlink(temp_in) }
  )
  return(out)
}
