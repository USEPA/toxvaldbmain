#' @title cas_checkSum
#' @description Check CAS RN validity via checksum method
#'
#' For a suspected CAS RN, determine validity by calculating final digit checksum
#'
#' @importFrom stringr str_pad
#' @family cas_functions
#'
#' @param x chr. Input vector of values to check. Standard CAS notation using hyphens is fine, as #' all non-digit characters are stripped for checksum calculation. Each element of \emph{x} should contain #' only one suspected CAS RN to check.
#' @param checkLEN logi. Should the function check that the non-digit characters of \emph{x} are at least 4, but no #' more than 10 digits long? Defaults to TRUE. #'
#' @details 
#' This function performs a very specific type of check for CAS validity, namely whether the final digit checksum follows
#' the CAS standard. By default, it also ensures that the digit length is compatible with CAS standards. It does nothing
#' more.
#'
#' This means that there is no check for valid CAS format. Use the \code{\link{cas_detect}} function to check CAS
#' format beforehand, or write your own function if necessary.
#'
#' @note 
#' This is a vectorized, reasonably high-performance version of the \link[webchem]{is.cas} function found
#' in the \link[webchem]{webchem} package. The functionality encompasses only the actual checksum checking of \code{webchem::is.cas};
#' as mentioned in \code{details}, use \code{\link{cas_detect}} to recreate the CAS format + checksum checking in
#' \code{webchem::is.cas}. See examples.
#'
#' Short of looking up against the CAS registry, there is no way to be absolutely sure that even inputs that pass
#' the checksum test are actually registered CAS RNs. The short digit length of CAS IDs combined with the modulo 10 single-
#' digit checksum means that even within a set of randomly generated validly-formatted CAS entities, ~10\% will pass checksum.
#'
#' @return 
#' A \code{logical} vector of length \emph{x} denoting whether each \emph{x} is a valid CAS by the checksum method. \code{NA}
#' input values will remain \code{NA}.
#' @export 
#' @examples 
#' cas_good <- c("71-43-2", "18323-44-9", "7732-18-5") #benzene, clindamycin, water
#' cas_bad  <- c("61-43-2", "18323-40-9", "7732-18-4") #single digit change from good
#' cas_checkSum(c(cas_good, cas_bad))
#' @seealso 
#'  \code{\link[stringr]{str_detect}}, \code{\link[stringr]{str_pad}}
#' @rdname cas_checkSum
cas_checkSum <- function(x, checkLEN = TRUE) {

  if(!is.character(x)) {
    stop("input must be of class character")
  }

  if(!stringr::str_detect(x,"[0-9]")) {
    return(0)
  }

  x_clean  <- gsub("\\D", "", x)
  if(checkLEN) {
    x_clean[nchar(x_clean)>10 | nchar(x_clean)<4] <- NA
  }
  # If no valid formats, exit early
  if(all(is.na(x_clean))) {
    warning("No validly formatted inputs detected")
    return(rep(NA, length(x)))
  }

  checksum <- as.integer(substr(x_clean, nchar(x_clean), nchar(x_clean)))

  maxlen  <- max(nchar(x_clean), na.rm = TRUE)
  x_clean <- stringr::str_pad(x_clean, width = maxlen, side = "left", pad = "0")

  x_split <- do.call(
    cbind,
    strsplit(substr(x_clean, 1, nchar(x_clean)-1), "")
  )
  mode(x_split) <- "integer"

  x_pos  <- rev(seq_len(nrow(x_split)))
  x_calc <- colSums(x_split * x_pos)
  x_modulo10 <- x_calc %% 10

  checksum == x_modulo10
}
