#' @title export.input.dict.file.list
#' @description Pull list of directories, files, and dictionaries used throughout the ToxValDB workflow.

#' @return Dataframe with "folder_name" field of files and folders in the toxval.config()$datapath directory.
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[readxl]{read_excel}}
#'  \code{\link[writexl]{write_xlsx}}
#'  \code{\link[dplyr]{filter}}, \code{\link[dplyr]{pull}}, \code{\link[dplyr]{bind_rows}}, \code{\link[dplyr]{distinct}}
#' @rdname export.input.dict.file.list
#' @export
#' @importFrom readxl read_xlsx
#' @importFrom writexl write_xlsx
#' @importFrom dplyr filter pull bind_rows distinct
export.input.dict.file.list <- function(){

  # Pull first layer of folders
  dir_list = list.dirs(toxval.config()$datapath,
                      full.names = TRUE,
                      recursive = FALSE)

  # Load dictionary of folders to include
  release_files_dict = readxl::read_xlsx(paste0(toxval.config()$datapath, "release_files/toxval_release_files_dict.xlsx"),
                                         sheet = "folder_list")

  files_exclude = readxl::read_xlsx(paste0(toxval.config()$datapath, "release_files/toxval_release_files_dict.xlsx"),
                                    sheet = "exclude_file_list")

  # Check if any new folders are present
  to_classify = dir_list[!dir_list %in% release_files_dict$folder_name]

  if(length(to_classify)){
    writexl::write_xlsx(data.frame(folder_name = to_classify),
                        paste0(toxval.config()$datapath, "release_files/toxval_release_files_dict_to_classify.xlsx"))
  }

  include_dirs = release_files_dict %>%
    dplyr::filter(include == 1) %>%
    dplyr::pull(folder_name)

  dir_list = dir_list[dir_list %in% include_dirs]

  # List of directories to exclude
  exclude_dirs = c("archive", "old", "drafts", "draft", "missing",
                   "old files", "replaced 2024-02-22", "export_temp",
                   "document_cataloging")

  out = lapply(dir_list, function(f){
    cat("Pulling directory ", f, "(", which(f == dir_list), " of ", length(dir_list), ")\n")
    dir_0 = list.files(f, full.names = TRUE) %>%
      # Filter out excluded directories
      .[!grepl(paste0("/", exclude_dirs, "/", collapse = "|"), .)] %>%
      .[!grepl(paste0("/", exclude_dirs, "$", collapse = "|"), .)]

    dir_out = lapply(dir_0, function(f2){
      cat("...pulling subdirectory ", f2, "(", which(f2 == dir_0), " of ", length(dir_0), ")\n")
      data.frame(
        folder_name = list.files(f2, full.names = TRUE, recursive = TRUE) %>%
          # Filter out excluded directories
          .[!grepl(paste0("/", exclude_dirs, "/", collapse = "|"), .)] %>%
          .[!grepl(paste0("/", exclude_dirs, "$", collapse = "|"), .)] %>%
          .[!grepl(paste0(files_exclude$file_name, collapse = "|"), .)]
        )
    }) %>%
      dplyr::bind_rows() %>%
      return()

  }) %>%
    dplyr::bind_rows() %>%
    dplyr::distinct()

  # Filter out nested exclude_dirs beyond first layers
  out = out %>%
    dplyr::filter(!grepl(paste0("/", exclude_dirs, "/", collapse = "|"), folder_name),
                  !grepl(paste0("/", exclude_dirs, "$", collapse = "|"), folder_name))

  # Export
  writexl::write_xlsx(out, paste0(toxval.config()$datapath,
                                  "release_files/input_file_dict_list_", Sys.Date(), ".xlsx"))

  return(out)
}
