#-------------------------------------------------------------------------------------
#'
#' Function to combine output log with output message
#' @param log_df Dataframe to which the log information will be appended
#' @param message_df_col New message to add
#-------------------------------------------------------------------------------------
log_message <- function(log_df,message_df_col){
  printCurrentFunction()
  # start logging message after 2nd occurance of line breaker(===)
  start_log <-  log_df[1:grep("^\\=",log_df[,1])[2],]
  # stop log after the occurance of last line breaker(===)
  stop_log <- log_df[grep("^\\=",log_df[,1])[2] : grep("^Log Elapsed Time|^NOTE\\: Elapsed Time",log_df[,1])+1,]
  return(c(start_log, message_df_col, stop_log))
}


