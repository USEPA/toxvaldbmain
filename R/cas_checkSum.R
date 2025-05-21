#' @title cas_checkSum
#' @description Check CAS RN validity via checksum method
#'
#' For a suspected CAS RN, determine validity by calculating final digit checksum
#'
#' @param x chr. Input vector of values to check. Standard CAS notation using hyphens is fine, as #' all non-digit characters are stripped for checksum calculation. Each element of \emph{x} should contain #' only one suspected CAS RN to check.
#' @details
#' This function performs a very specific type of check for CAS validity, namely whether the final digit checksum follows
#' the CAS standard. By default, it also ensures that the digit length is compatible with CAS standards.
#'
#' @return
#' A \code{logical} vector of length \emph{x} denoting whether each \emph{x} is a valid CAS by the checksum method. \code{NA}
#' input values will be set 0 (FAIL).
#' @export
#' @examples
#' cas_good <- c("71-43-2", "18323-44-9", "7732-18-5") # benzene, clindamycin, water
#' cas_bad  <- c("61-43-2", "18323-40-9", "7732-18-4") # single digit change from good
#' cas_checkSum(c(cas_good, cas_bad))
#' @seealso
#'  \code{\link[stringr]{str_detect}}, \code{\link[stringr]{str_pad}}
#' @rdname cas_checkSum
cas_checkSum <- function(x) {
  # Ensure input values are of type character and contains digits
  if(!is.character(x)) {
    stop("input must be of class character")
  }

  check_df = data.frame(casrn = x,
                        # Default TRUE - passed checksum
                        checksum = TRUE) %>%
    # Temp ID to maintain vector order
    dplyr::mutate(tmp_id = 1:dplyr::n()) %>%
    tidyr::separate_wider_delim(
      cols = casrn,
      delim = "-",
      names = c("digit_1", "digit_2", "digit_3"),
      too_few = "align_start",
      too_many = "merge",
      cols_remove = FALSE
    ) %>%
    dplyr::mutate(
      # Set at NA if casrn input value did not split
      digit_1 = dplyr::case_when(
        digit_1 == casrn ~ NA,
        TRUE ~ digit_1
      ),
      checksum = dplyr::case_when(
        # If no numeric values in string, fail checksum
        !stringr::str_detect(casrn, "[0-9]") ~ FALSE,
        # Case where it's just a number, but could be a casrn without formatting
        !is.na(suppressWarnings(as.numeric(casrn))) ~ TRUE,
        is.na(digit_1) ~ FALSE,
        # If last digit group isn't singular, fail
        nchar(digit_3) != 1 ~ FALSE,
        # If middle digit group isn't 2-7 digits, fail
        nchar(digit_2) != 2 ~ FALSE,
        # If first digit group isn't 2-7 digits, fail
        nchar(digit_1) < 2 | nchar(digit_1) > 7 ~ FALSE,
        TRUE ~ checksum
      ),
      casrn_clean = dplyr::case_when(
        checksum == FALSE ~ NA,
        TRUE ~ casrn %>%
          gsub("\\D", "", .) %>%
          # Pad to 10 digits
          stringr::str_pad(width = 10,
                           side = "left",
                           pad = "0")
      ),
      digit_3 = dplyr::case_when(
        # Extract the last digit and fill in digit 3
        is.na(digit_3) & !is.na(casrn_clean) ~ stringr::str_extract(casrn_clean, "\\d$"),
        TRUE ~ digit_3
      ),
      # Reverse the string for calculation steps
      casrn_clean = stringi::stri_reverse(casrn_clean)
    ) %>%
    # Split numerics across 10 columns into individual digits
    tidyr::separate(
      col = casrn_clean,
      into = as.character(0:10),
      sep = "",
      remove = FALSE,
      fill = "left",
      extra = "merge"
    ) %>%
    # Pivot longer to get digit position and digit value
    tidyr::pivot_longer(
      cols = as.character(0:10),
      names_to = "pos",
      values_to = "digit_value"
    ) %>%
    # Multiple digit value by position
    dplyr::mutate(
      # Subtract 1 from pos so the positions are correct (base 0)
      pos = suppressWarnings(as.numeric(pos)) - 1,
      digit_pos = suppressWarnings(as.numeric(digit_value)) *
        pos) %>%
    # Sum up digit_pos values and get the modulus (remainder) of 10
    dplyr::group_by(tmp_id, casrn, checksum, digit_3) %>%
    dplyr::summarise(modulo_10 = sum(digit_pos, na.rm = TRUE) %% 10,
                     .groups = "drop") %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      checksum = dplyr::case_when(
        # Stay FALSE
        checksum == FALSE ~ 0,
        modulo_10 != digit_3 ~ 0,
        # modulo_10 == digit_3, return TRUE
        TRUE ~ 1
      )
    )

  # Check that returned order is the same
  if(!identical(check_df$casrn, x)){
    stop("cas_checkSum check_df order not the same as input...")
  }

  check_df %>%
    # Return checksum logical vector vector (0, 1)
    dplyr::pull(checksum) %>%
    return()

}
