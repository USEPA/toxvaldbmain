# Helper function for use with ToxValDPScript.R to perform numeric profiling
# on combinations of fields.
# params: field_list - list of fields to be used in profiling
# returns: list of large outlier, medium outliers, spreads, and repeats

norm_by_fields <- function(field_list){
  largedf<-NULL
  mediumdf<-NULL
  spreadconcerndf<-NULL
  repeatconcerndf<-NULL
  #MT: Iterate over the list, print i as a time tracker.
  for (i in 1:length(field_list)) {
    if (i %% 100 == 0) {print(i)}
    #MT: Pull numerics from the list, take a log, and handle 0's/NAs by moving them
    #to -10
    temp<-field_list[[i]]$toxval_numeric
    temp<-log10(temp)
    temp[which(temp == -Inf)]<- -10
    temp[which(is.na(temp))]<- -10
    #MT: Check for repeat issues - if everyone's the same, or if some value is over
    #half of the records, then flag all of those records and move on. I do this in
    #two separate if statements to catch nas, which shouldn't be an issue anymore
    #due to above. These might be combinable into one if statement, I haven't tested
    #it yet.
    if (length(unique(temp)) <= 2) {
      repeatconcerndf<-rbind(repeatconcerndf,field_list[[i]])
      final[which(final$toxval_id %in% field_list[[i]]$toxval_id),]$outliers<-"R"
      final[which(final$toxval_id %in% field_list[[i]]$toxval_id),]$round<-"F"
      next
    }
    if (max(table(temp)) >= length(temp)/2) {
      repeatconcerndf<-rbind(repeatconcerndf,field_list[[i]])
      final[which(final$toxval_id %in% field_list[[i]]$toxval_id),]$outliers<-"R"
      final[which(final$toxval_id %in% field_list[[i]]$toxval_id),]$round<-"F"
      next
    }
    #MT: Do a normality test if the record count isn't too large for shapiro test.
    if (length(temp) < 100) {
      p<-shapiro.test(temp)$p
    } else {p<-1}
    #MT: Pull center and spread based on normality.
    if (p < 0.05) {
      spread<-mad(temp,constant = 1)
      center<-median(temp)
    } else {
      center<-mean(temp)
      spread<-sd(temp)
    }
    #MT: Standardize all records, and track large and medium outliers.  Rephrase temp
    #with the remaining records.
    vals<-abs((temp - center)/spread)
    large <- which(vals > 5)
    medium <- which(vals < 5 & vals > 3)
    temp <- temp[which(vals < 3)]
    #MT: Recalculate spread and center, for large-spread check.
    spread<-mad(temp,constant = 1)
    center<-median(temp)
    #MT: Time for the loading of records!  Large spreads, large outliers, and medium
    #outliers.
    if (spread > 2) {
      spreadconcerndf<-rbind(spreadconcerndf,field_list[[i]])
      final[which(final$toxval_id %in% field_list[[i]]$toxval_id),]$outliers<-"S"
      final[which(final$toxval_id %in% field_list[[i]]$toxval_id),]$round<-"F"
      next
    }
    if (length(large) != 0){
      largedf<-rbind(largedf,field_list[[i]][c(large),])
      final[which(final$toxval_id %in% field_list[[i]][c(large),]$toxval_id),]$outliers<-"L"
      final[which(final$toxval_id %in% field_list[[i]][c(large),]$toxval_id),]$round<-"F"
      final[which(final$toxval_id %in% field_list[[i]][c(large),]$toxval_id),]$amount<-vals[large]
    }
    if (length(medium) != 0) {
      mediumdf<-rbind(mediumdf,cbind(field_list[[i]][c(medium),]))
      final[which(final$toxval_id %in% field_list[[i]][c(medium),]$toxval_id),]$outliers<-"M"
      final[which(final$toxval_id %in% field_list[[i]][c(medium),]$toxval_id),]$round<-"F"
      final[which(final$toxval_id %in% field_list[[i]][c(medium),]$toxval_id),]$amount<-vals[medium]
    }
  }
  return(list(largedf,mediumdf,spreadconcerndf,repeatconcerndf))
}
