#--------------------------------------------------------------------------------------
#' @description Generate the hash key for a source table based on hashing columns
#'
#' @param res The data frame to be processed
#' @param hashing_cols Optional list of columns to use for generating source_hash
#' @title source_hash_vectorized
#' @return Input dataframe with new source_hash field
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[digest]{digest}}
#'  \code{\link[dplyr]{distinct}}
#' @rdname toxval_source.hash.and.load
#' @export
#' @importFrom digest digest
#' @importFrom dplyr distinct mutate ungroup rowwise
#' @importFrom tidyr unite
source_hash_vectorized <- function(res, hashing_cols){
  if(is.null(hashing_cols)){
    hashing_cols = names(res)
  }
  # New hashing system (2023/10/26)
  # Vectorized hash instead of for-loop
  # Different from previous in that Date columns aren't converted to numerics
  cat("Using vectorized hashing! \n")
  res %>%
    tidyr::unite(hash_col, tidyselect::any_of(sort(names(.)[names(.) %in% hashing_cols])), sep="-") %>%
    dplyr::rowwise() %>%
    dplyr::mutate(source_hash = paste0("ToxValhc_", digest::digest(hash_col, serialize = FALSE))) %>%
    dplyr::ungroup() %>%
    return()
}
