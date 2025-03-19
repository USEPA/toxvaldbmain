#'ToxVal Data Profiling Script! This script was/is used to perform data profiling
#'on ToxValDB. This script includes duplicate checking, normalization issue checking,
#'numeric profiling, and a number of small issues. For this script to work, please
#'preload ToxValDB into a dataframe called "df."
#'
#'This script was first created by Mitchell Tague (MT), and is currently being
#'maintained by Anthony Brito (AB). Please leave any comments with your initials
#'attached, to track overall changes to the process.

#MT: Package initialization.
library(writexl)
library(dplyr)
library(data.table)
library(stringdist)
library(readxl)
library(ggplot2)
library(forcats)
library(pointblank)
library(validate)
library(RMySQL)

#devtools::load_all()
df = runQuery("select * from toxval",
              toxval.config()$toxval.db)

# MG: pull from source_chemical so we can compare certain fields when
# checking for duplicates
df2 = runQuery("select * from source_chemical where chemical_id in (select distinct chemical_id from toxval)",
              toxval.config()$toxval.db)

# MG: Left outer join. Many flagged duplicates were determined non-dups
# because they had different casrn's, so we can compare these values
# in the check for duplicates
df3 <- merge(x=df, y = df2, by="chemical_id", all.x = TRUE)

#write_xlsx(df3,"joined_toxval_92523.xlsx")

#MT: Asked to exclude Ecotox with the latest process
ecotox<-df3[which(df3$source.x=="ECOTOX"),]
rest<-df3[which(df3$source.x != "ECOTOX"),]

rest2 <- df[which(df$source!="ECOTOX"),]

write_xlsx(rest,"toxval_no_ecotox_92523.xlsx")
write_xlsx(ecotox,"toxval_ecotox_only_92523.xlsx")


##MT: This code chunk was used to identify a certain subsection of the dataset with
##no exposure route, no human eco, and definining a PoD or Lethality effect.  It
##is not used in the overall ToxVal data profiling process now.

#route<-rest[which(rest$exposure_route == "-" & rest$human_eco != "not specified"),]
#tracker<-c()
#yes<-c("Point of Departure","Lethality Effect Level")
#for (i in 1:nrow(route)) {
#  supertype<-temp[which(temp$toxval_type==route[i,]$toxval_type),]$toxval_type_supercategory
#  if (length(supertype) == 0) {tracker[i] <- FALSE} else if (supertype %in% yes) {tracker[i]<-TRUE} else {tracker[i]<-FALSE}
#}
#route<-route[tracker,]
#write_xlsx(route,"Missing Exposure Route.xlsx")

#MT: Remove QC fails.  The last process of this code was asked to ignore human_eco
#and risk_assessment_class as qc fails.  Note that the numbers of the first line
#may need to be adjusted, should this ignoring still be requested.
clear<-unique(rest2$qc_status)[c(13,14)]
for (i in 1:nrow(rest2)) {
  if (i %% 100 == 0) {print(i)}
  if (rest2[i,]$qc_status %in% clear) {
    rest2[i,]$qc_status<-"pass"
  }
}
fail<-rest2[-which(rest2$qc_status == "pass"),]
rest<-rest2[which(rest2$qc_status == "pass"),]
#write_xlsx(fail,"QC Fail Identified.xlsx")
#write_xlsx(rest,"QC Fail Identified2.xlsx")



##MT: Used these to test whether they can be used as "identifier" columns.
head(sort(table(rest$toxval_id), decreasing = TRUE))
head(sort(table(rest$source_hash), decreasing = TRUE))
head(sort(table(rest$toxval_uuid), decreasing = TRUE))

##MT: Toxval_id can be used as identifiers cleanly.  Toxval_uuid has a problem of
##only being present for about 1/3rd of the data set.  Source hashes, meanwhile, were
##assumed to potentially be unique, but clearly are not.  Based on the below, about
##7.5% of the dataset has a repeated source hash.

## AB: source_hash are all unique in new version of toxval, res_toxval_v94
hashes<-table(rest$source_hash)
sum(hashes[which(hashes > 1)])/nrow(rest)

##Investigating what's going on with repeated source hashes.
#hashes<-hashes[which(hashes > 1)]
##Split apart records with more than one hash.
#hashdf<-rest[which(rest$source_hash %in% names(hashes)),]
#hashlist<-split(data.table(hashdf),by="source_hash")
##Set up matrix to track what's different for each hash.
#results<-matrix(0,nrow=length(hashlist),ncol=ncol(hashlist[[1]]))
##Pull each data table from the resulting list.
#for (i in 1:length(hashlist)) {
#  this.df<-hashlist[[i]]
#  this.df<-data.frame(this.df)
#  #Find the length of unique entries from each column in the df.
#  for (j in 1:ncol(this.df)) {
#    results[i,j] = length(unique(this.df[,j]))
#  }
#}
##Find averages throughout columns have cases of differences.
#resultavg<-apply(results,2,mean)
#nons<-which(resultavg > 1)
#nons<-colnames(rest)[nons]

#MT: Removing variables for memory concerns.
rm(hashes)
rm(hashlist)
rm(hashdf)
rm(results)
rm(this.df)
rm(nons)


#MT: This section exists to prepare the "certain dups" - cases where all columns
#aside from the three identifiers above are duplicated.  We want to identify this
#data, attach the toxval_ids so duplications can be tracked as one, and then remove
#the dups EXCEPT for the first instance of each duplication.

#MT: Use group by & filter to find all "certain dups".
cert <- data.table(rest) %>% group_by_at(c(3:67)) %>% filter(n() > 1)
#MT: Split across these duplications.
cert <- data.table(cert)
certlist<-split(cert,by=colnames(cert)[3:67])
#MT: Iterate over the split list.
for (i in 1:length(certlist)) {
  if (i %% 100 == 0) {print(i)}
  #MT: Take the unique vector of toxval ids.
  #AB: Take the unique vector of source_hash.
  temp<-unique(certlist[[i]]$toxval_id)
  #MT: Turn this vector into a pasted together string of toxval ids.
  templist<-as.character(temp[1])
  for (j in 2:length(temp)) {
    templist<-paste(templist,temp[j],sep=", ")
  }
  #MT: Repeat the string for however many rows exist, to create a column.
  tempr<-c()
  for (j in 1:length(temp)) {
    tempr[j] <- templist
  }
  #MT: Attach the column to the original data frame.
  certlist[[i]]<-cbind(certlist[[i]],tempr)
}
#MT: Use rbind to unsplit the list.
cert<-do.call("rbind",certlist)


#MT: Iterate over the split list.
for (i in 1:length(certlist)) {
  if (i %% 100 == 0) {print(i)}
  #MT: Take the unique vector of toxval ids.
  #AB: Take the unique vector of source_hash.
  temp2<-unique(certlist[[i]]$source_hash)
  #MT: Turn this vector into a pasted together string of toxval ids.
  templist2<-as.character(temp2[1])
  for (j in 2:length(temp2)) {
    templist2<-paste(templist2,temp2[j],sep=", ")
  }
  #MT: Repeat the string for however many rows exist, to create a column.
  tempr2<-c()
  for (j in 1:length(temp2)) {
    tempr2[j] <- templist2
  }
  #MT: Attach the column to the original data frame.
  certlist[[i]]<-cbind(certlist[[i]],tempr2)
}
#MT: Use rbind to unsplit the list.
cert<-do.call("rbind",certlist)
#MT: Give the new column a name, and push the results out.
#AB: added column Duplicate Hashes since source_hash is an unique identifier now
colnames(cert)[70]<-"Duplicate IDs"
colnames(cert)[71]<-"Duplicate Hash"
write_xlsx(cert,"Final Certain Dups.xlsx")


#MT: Remove the second+ iterants of these certain dups.
fronttruedup<-duplicated(rest[,-c(1,2,68)])
rest<-rest[!fronttruedup,]

#MT: Memory time, more time for a memory time.
rm(cert)
rm(certlist)
rm(temp)
rm(templist)
rm(tempr)
rm(fronttruedup)


#MT: Next, we need to investigate "possible dups" - records that do not duplicate in
#every single column, but duplicate the columns that are vital to the record, and are
#likely to be organized the same way (without as many normalization errors,
#differences in curator commands, et cetera.)  We need to especially consider the
#possibility of dups from different sources, so this investigation is done both
#considering and excluding sources as a requirement for duplication.

#MT: Set up the data tables using group_by and filter, as well as vectors of the
#relevant columns.
poss <- data.table(rest) %>% group_by(toxval_type, toxval_numeric, toxval_units,
                                      species_id, exposure_route, dtxsid, source,
                                      study_type, sex, critical_effect,
                                      risk_assessment_class,toxval_subtype) %>% filter(n() > 1)
possnos <- data.table(rest) %>% group_by(toxval_type, toxval_numeric, toxval_units,
                                         species_id, exposure_route, dtxsid, study_type,
                                         sex, critical_effect, risk_assessment_class,
                                         toxval_subtype) %>% filter(n() > 1)
full = c("toxval_type","toxval_numeric","toxval_units","species_id","exposure_route",
         "study_type","dtxsid","source","sex","critical_effect",
         "risk_assessment_class","toxval_subtype")
fullnos = c("toxval_type","toxval_numeric","toxval_units","species_id","exposure_route",
            "study_type","dtxsid","sex","critical_effect","risk_assessment_class","toxval_subtype")

##MT: Investigate other records for the potential of being dup-vital.
##This segment has been used iteratively to determine the above column set, and
##should only be needed again if you wish to update the columns by which new
##possible dups should be identified.

##MT: Split the data table apart into dups.
poss<-data.table(poss)
posslist <- split(poss,by=full)
##MT: Identify which columns are already considered by duplicates.  Add toxval_uuid -
##reasoning discussed later.
humph<-(colnames(rest) %in% full)
humph[68]<-TRUE
##MT: Set up matrices to investigate "how often are other columns duplicated in this set"
##as well as to check how many times each column is the ONLY non-duped column.
invest<-matrix(data=0,nrow=1000,ncol=ncol(rest)-length(full)-1)
check<-matrix(data=0,nrow=length(posslist),ncol=ncol(rest)-length(full)-1)
for (i in 1:length(posslist)) {
  #MT: Set up trackers to see if something is a true duplicate, how many non-duplicates
  #are found, and the last j shown to be non-dup.
  nondup<-FALSE
  tracker = 0
  jtrack = 0
  #MT: Pull data frame, and remove columns already identified as poss to dups.
  temp<-data.frame(posslist[[i]])
  temp<-temp[,!humph]
  #MT: Determine row of invest based on size of temp, and add to first column of invest.
  n<-nrow(temp)-1
  invest[n,1] = invest[n,1] + 1
  #MT: Measure from column 3 - this excludes toxval_id & source_hash.  This, along with
  #the exclusion of toxval_uuid, ensures that they do not mess with "true dup" id.
  for (j in 3:ncol(temp)) {
    #MT: If the length of uniques of that column is 1, then the column is duplicated.
    #Add it to invest for whichever column it is.
    if (length(unique(temp[,j])) == 1) {
      invest[n,j] = invest[n,j] + 1
      #MT:Otherwise, mark that the data frame is not a true dup, track how many times
      #a non-dup have been found in this df, and track j.
    } else {
      nondup<-TRUE
      tracker = tracker + 1
      jtrack = j
    }
  }
  #  #MT: Add true dups to column 2, and add "only one" dups to the check matrix.
  if (!nondup) {invest[n,2] = invest[n,2] + 1}
  if (tracker == 1) {check[i,jtrack] == 1}
}
##MT: Use humph to identify the colnames of invest, and set up the first two colnames.
colnames(invest)<-colnames(rest[,!humph])
colnames(invest)[1:2]<-c("total","true")

#MT: Get ToxVal ids for the possible dups, and connect them to the dataset.
#This is the same process as for certain dups (lines 100-127), hence the lack
#of comments.
for (i in 1:length(posslist)) {
  if (i %% 100 == 0) {print(i)}
  temp<-unique(posslist[[i]]$toxval_id)
  templist<-as.character(temp[1])
  for (j in 2:length(temp)) {
    templist<-paste(templist,temp[j],sep=", ")
  }
  tempr<-c()
  for (j in 1:length(temp)) {
    tempr[j] <- templist}
  posslist[[i]]<-cbind(posslist[[i]],tempr)
}
poss<-do.call("rbind",posslist)
for (i in 1:length(posslist)) {
  if (i %% 100 == 0) {print(i)}
  temp2<-unique(posslist[[i]]$source_hash)
  templist2<-as.character(temp2[1])
  for (j in 2:length(temp2)) {
    templist2<-paste(templist2,temp2[j],sep=", ")
  }
  tempr2<-c()
  for (j in 1:length(temp2)) {
    tempr2[j] <- templist2}
  posslist[[i]]<-cbind(posslist[[i]],tempr2)
}
poss<-do.call("rbind",posslist)

#MT: Do the exact same thing for the non-sourced dups.
possnos<-data.table(possnos)
possnoslist<-split(possnos,by=fullnos)
for (i in 1:length(possnoslist)) {
  if (i %% 100 == 0) {print(i)}
  temp<-unique(possnoslist[[i]]$toxval_id)
  templist<-as.character(temp[1])
  for (j in 2:length(temp)) {
    templist<-paste(templist,temp[j],sep=", ")
  }
  tempr<-c()
  for (j in 1:length(temp)) {
    tempr[j] <- templist
  }
  possnoslist[[i]]<-cbind(possnoslist[[i]],tempr)
}
possnos<-do.call("rbind",possnoslist)
for (i in 1:length(possnoslist)) {
  if (i %% 100 == 0) {print(i)}
  temp2<-unique(possnoslist[[i]]$source_hash)
  templist2<-as.character(temp2[1])
  for (j in 2:length(temp2)) {
    templist2<-paste(templist2,temp2[j],sep=", ")
  }
  tempr2<-c()
  for (j in 1:length(temp2)) {
    tempr2[j] <- templist2
  }
  possnoslist[[i]]<-cbind(possnoslist[[i]],tempr2)
}
possnos<-do.call("rbind",possnoslist)


#MT: Remove records in poss from possnos, to keep them separated.
possnos<-possnos[-which(possnos$toxval_id %in% poss$toxval_id),]

#MT: Push the results to some excel files.
write_xlsx(poss,"Possible Duplicates expanded v2.xlsx")
write_xlsx(possnos,"Possible, No Sources expanded v2.xlsx")

#MT: Pull the toxval ids of dups after the first, from poss, and remove from rest.
duplist<-c()
for (i in 1:length(possnoslist)) {
  for (j in 2:nrow(possnoslist[[i]])) {
    duplist<-c(duplist,possnoslist[[i]]$toxval_id[j])
  }
}
rest<-rest[-which(rest$toxval_id %in% duplist),]

#MT: Remove some things for memory concerns now that dups are completed
rm(poss)
rm(posslist)
rm(possnos)
rm(possnoslist)
rm(duplist)
#rm(full)
rm(fullnos)
rm(temp)
rm(templist)
rm(tempr)
rm(check)
rm(invest)
rm(humph)

#MT: Beginning the search for normalization errors. Investigating cases where the
#original column is different, but the normal column is the same, OR vice versa.
#You don't need to do this with every run, but it may be worthwhile after an
#update to any normalization procedures or a new source. Note that the columns
#have to be in a matched up order.
orig<-c("toxval_type_original","toxval_subtype_original","toxval_units_original",
        "toxval_numeric_qualifier_original","study_type_original","species_id_original","strain_original","critical_effect_original",
        "population_original","exposure_route_original","exposure_method_original","exposure_form_original","media_original",
        "lifestage_original","generation_original","year_original")
norm<-c("toxval_type","toxval_subtype","toxval_units", "toxval_numeric_qualifier",
        "study_type","species_id","strain","critical_effect","population",
        "exposure_route","exposure_method","exposure_form","media","lifestage","generation","year")

#MT: Set up columns for tracking issues
normed<-NULL
denormed<-NULL

#MT: Isolate each column pair.
for (i in 1:length(orig)) {
  this.orig<-orig[i]
  this.norm<-norm[i]
  #MT: Remove the columns in question from full, to dup check and sort outside of these conditions.
  these.full<-full[!full %in% c(this.orig, this.norm)]
  #MT: Get a dup check based on these.full, and split into a list.
  big.df<-data.table(rest) %>% group_by(across(all_of(these.full))) %>% filter(n() > 1)
  dflist <- split(data.table(big.df), by=these.full)
  for(j in 1:length(dflist)) {
    #MT: Pace check.
    if (j %% 100 == 0) {
      print(paste(i, j))
    }
    #MT: Pull one split df at a time, skip over it if it's too big (toxval_numeric),
    #and get col indices.
    this.df<-data.frame(dflist[[j]])
    if (nrow(this.df) > 1000) {next}
    orig.ind<-which(colnames(this.df) == this.orig)
    norm.ind<-which(colnames(this.df) == this.norm)
    for(k in 1:(nrow(this.df)-1)) {
      for (l in k:nrow(this.df)) {
        #MT: Combine ids and the column name, and check if original column
        #and normed column are equal.
        temp <- c(this.df[k,1], this.df[l,1], this.norm)
        origcheck <- (this.df[k,orig.ind] == this.df[l,orig.ind])
        normcheck <- (this.df[k,norm.ind] == this.df[l,norm.ind])
        #MT: If either is length 0, then some NAs are happening and we don't care.
        if (length(origcheck) == 0 | length(normcheck) == 0) {next}
        #MT: Find cases where they differ on one or not the other.  Attach the differing
        #values, and connect them to the dataframe they need.
        if (origcheck & !normcheck) {
          temp <- c(temp, this.df[k,norm.ind], this.df[l,norm.ind])
          denormed<-rbind(denormed, temp)
        }
        if (!origcheck & normcheck) {
          temp <- c(temp, this.df[k,orig.ind], this.df[l,orig.ind])
          normed <- rbind(normed, temp)
        }
      }
    }
  }
}
#MT: Memory Handling
rm(big.df)
rm(dflist)
rm(this.df)

#MT: Provide colnames, rownames, and push to excel files.
colnames(normed)<-c("First ToxVal ID", "Second ToxVal ID", "Column",
                    "First Orig Value", "Second Orig Value")
colnames(denormed)<-c("First ToxVal ID", "Second ToxVal ID", "Column",
                      "First Normed Value", "Second Normed Value")
rownames(normed)<-NULL
rownames(denormed)<-NULL
write_xlsx(data.frame(normed), "Normalized Final.xlsx")
write_xlsx(data.frame(denormed), "Denormalized Final.xlsx")

#MT: The AEGL individuals are formatted medium garbage. Let's toss them for now.
AEGL<-NULL
AEGLind<-c()
for (i in 1:nrow(rest)) {
  if (grepl("AEGL",rest[i,]$toxval_type)) {
    AEGL<-rbind(AEGL,rest[i,])
    AEGLind<-c(AEGLind,i)
  }
}
rest<-rest[-AEGLind,]
write_xlsx(AEGL,"AEGL is Weird.xlsx")
rm(AEGL)
rm(AEGLind)

#MT: Levenshtein Distance investigation.  Find values within columns with Levenshtein
#distance of 1, to check if they should be normalized together. Run this any time you
#run the normalization check.
#First, find how many unique values there are for each column - we don't care about
#columns that we can look at directly, or that have so many individuals it's
#impossible to investigate.
colchecks<-matrix(0,nrow=ncol(rest),ncol=2)
for (i in 1:ncol(rest)) {
  colchecks[i,1] = colnames(rest)[i]
  colchecks[i,2] = length(unique(rest[,i]))
}
colchecks

#MT: Based on that, select columns to investigate specific values for. Consider
#columns that have a reasonable number of values to process (25-1000-ish), ignore
#numeric columns and original columns of normalized fields.
check<-c("source","subsource","details_text","toxval_type","toxval_subtype",
         "toxval_units","study_duration_class","study_duration_units","population",
         "media","risk_assessment_class")
#MT: Set up matrix to store results, iterate over the columns of rest.
disttracker<-matrix(ncol=3)
for (i in 1:ncol(rest)) {
  #MT: Only consider columns within check, and get the list of unique columns.
  if (!(colnames(rest)[i] %in% check)) {next}
  this.col<-unique(rest[,i])
  #MT: Create a matrix of the Lev distances, and store size for weird index
  #math trick.
  n<-length(this.col)
  dist<-stringdistmatrix(this.col,method="lv")
  #MT: Iterate over distance, find values where distance is 1.
  for (j in 1:length(dist)) {
    if (is.na(dist[j])) {next}
    if (dist[j] == 1) {
      #MT: Find the values of the two records based on j, using an iterative loop.
      tempj<-j
      ind1<-1
      ind2<-2
      while (tempj > 1) {
        if (ind2 < n) {
          ind2 = ind2 + 1
        } else {
          ind1 = ind1 + 1
          ind2 = ind1 + 1
        }
        tempj = tempj - 1
      }
      #MT: Use found indices to create the row for this match, add to the results.
      temprow <- c(colnames(rest)[i],this.col[ind1],this.col[ind2])
      disttracker<-rbind(disttracker,temprow)
    }
  }
}
#MT: Export results to excel.
disttracker<-disttracker[-1,]
disttracker<-data.frame(disttracker)
write_xlsx(disttracker,"Near Matched Values.xlsx")

#MT: Find ToxVal types and units with less than 25 instances, and boot them for
#their own separate checkup.
lowftypes<-rest[rest$toxval_type %in% names(table(rest$toxval_type))[
  table(rest$toxval_type)<=25],]
write_xlsx(lowftypes, "Low Frequency Types.xlsx")
rest<-rest[rest$toxval_type %in% names(table(rest$toxval_type))[
  table(rest$toxval_type)>25],]

lowfunits<-rest[rest$toxval_units %in% names(table(rest$toxval_units))[
  table(rest$toxval_units)<=25],]
write_xlsx(lowfunits, "Low Frequency Units.xlsx")
rest<-rest[rest$toxval_units %in% names(table(rest$toxval_units))[
  table(rest$toxval_units)>25],]

##MT: This segment is based on the "unrealistic" values idea from early on in the
##script.  It's being included for posterity, but is not useful.

vbadmgL<-which(rest$toxval_units %in% c("mg/L", "mg/L serum", "mg/L urine")
               & rest$toxval_numeric >= 1000000)
vbadmgkg<-which((rest$toxval_units %in% c("mg/kg", "mg/kg-day", "(mg/kg-day)-1",
                                          "mg/kg ash femur", "mg/kg dry soil weight",
                                          "mg/kg diet", "mg/kg TAD", "mg ion/kg egg"))
                & rest$toxval_numeric >= 1000000)
vbadmgm3<-which(rest$toxval_units %in% c("mg As/m3", "mg Ba/m3", "mg Co/m3",
                                         "mg Cr/m3", "mg Cu/m3", "mg Hf/m3",
                                         "mg Os/m3", "mg Pb/m3", "mg Sb/m3", "mg/m3",
                                         "mg Se/m3", "mg Te/m3", "mg V/m3", "mg Y/m3")
                & rest$toxval_numeric >= 1000000000)

vbadmgLdf<-rest[vbadmgL,]
vbadmgkgdf<-rest[vbadmgkg,]
vbadmgm3df<-rest[vbadmgm3,]

write_xlsx(vbadmgLdf,"Clearly Bad mg per Liter.xlsx")
write_xlsx(vbadmgkgdf,"Clearly Bad mg per kg.xlsx")
write_xlsx(vbadmgm3df,"Clearly Bad mg per m3.xlsx")

vbad<-c(vbadmgL,vbadmgkg,vbadmgm3)
rest<-rest[-vbad,]

#'MT: Now it's finally time for the proper numeric profiling. We will group all
#'records based on field combinations - first on full combinations of toxval type,
#'subtype, units, chemical, and species, then on last combinations of just type
#'and units. We will track outliers, splitting them between large (>5 spreads from
#'center) and medium (3-5 spreads), as well as groups with a high amount of repeat
#'values and groups that remain very high spread after removing any possible outliers.
#'
#'Set up a matrix to track numeric profiling - why it was screened, which combination
#'it ws a part of, and if it was an outlier, the scale of that outlier.
outliers<-rep("",nrow(rest))
round<-rep("",nrow(rest))
amount<-rep(0,nrow(rest))
final<-cbind(rest,outliers,round,amount)


#MT: Use a count to find combinations containing at least 5 records.
distrocheck<-rest %>% count(dtxsid,toxval_type,toxval_subtype,toxval_units,species_id)
distrogood<-distrocheck[which(distrocheck$n >= 5),]
#MT: Strip the n off, and split the data set between these combinations and other
#records.
# largedf<-NULL
# mediumdf<-NULL
# spreadconcerndf<-NULL
# repeatconcerndf<-NULL
distrogood<-distrogood[,-6]
distro<-rest %>% inner_join(distrogood)
rest<-rest %>% anti_join(distrogood)

#MT: Create list by combination, and set up the four data frames for flagged records
distro<-data.table(distro)
distrolist<-split(distro,by=c("dtxsid","toxval_type","toxval_subtype","toxval_units","species_id"))

#MG: Uses the helper function instead allowing for testing of different fields
values <- profile_by_fields(distrolist)
write_xlsx(values[0],"DistroLargeOutliers.xlsx")
write_xlsx(values[1],"DistroMediumOutliers.xlsx")
write_xlsx(values[2],"DistroPostSpreads.xlsx")
write_xlsx(values[3],"DistroRepeats.xlsx")



#MT: Iterate over the list, print i as a time tracker.
# for (i in 1:length(distrolist)) {
#   if (i %% 100 == 0) {print(i)}
#   #MT: Pull numerics from the list, take a log, and handle 0's/NAs by moving them
#   #to -10
#   temp<-distrolist[[i]]$toxval_numeric
#   temp<-log10(temp)
#   temp[which(temp == -Inf)]<- -10
#   temp[which(is.na(temp))]<- -10
#   #MT: Check for repeat issues - if everyone's the same, or if some value is over
#   #half of the records, then flag all of those records and move on. I do this in
#   #two separate if statements to catch nas, which shouldn't be an issue anymore
#   #due to above. These might be combinable into one if statement, I haven't tested
#   #it yet.
#   if (length(unique(temp)) <= 2) {
#     repeatconcerndf<-rbind(repeatconcerndf,distrolist[[i]])
#     final[which(final$toxval_id %in% distrolist[[i]]$toxval_id),]$outliers<-"R"
#     final[which(final$toxval_id %in% distrolist[[i]]$toxval_id),]$round<-"F"
#     next
#   }
#   if (max(table(temp)) >= length(temp)/2) {
#     repeatconcerndf<-rbind(repeatconcerndf,distrolist[[i]])
#     final[which(final$toxval_id %in% distrolist[[i]]$toxval_id),]$outliers<-"R"
#     final[which(final$toxval_id %in% distrolist[[i]]$toxval_id),]$round<-"F"
#     next
#   }
#   #MT: Do a normality test if the record count isn't too large for shapiro test.
#   if (length(temp) < 100) {
#     p<-shapiro.test(temp)$p
#   } else {p<-1}
#   #MT: Pull center and spread based on normality.
#   if (p < 0.05) {
#     spread<-mad(temp,constant = 1)
#     center<-median(temp)
#   } else {
#     center<-mean(temp)
#     spread<-sd(temp)
#   }
#   #MT: Standardize all records, and track large and medium outliers.  Rephrase temp
#   #with the remaining records.
#   vals<-abs((temp - center)/spread)
#   large <- which(vals > 5)
#   medium <- which(vals < 5 & vals > 3)
#   temp <- temp[which(vals < 3)]
#   #MT: Recalculate spread and center, for large-spread check.
#   spread<-mad(temp,constant = 1)
#   center<-median(temp)
#   #MT: Time for the loading of records!  Large spreads, large outliers, and medium
#   #outliers.
#   if (spread > 2) {
#     spreadconcerndf<-rbind(spreadconcerndf,distrolist[[i]])
#     final[which(final$toxval_id %in% distrolist[[i]]$toxval_id),]$outliers<-"S"
#     final[which(final$toxval_id %in% distrolist[[i]]$toxval_id),]$round<-"F"
#     next
#   }
#   if (length(large) != 0){
#     largedf<-rbind(largedf,distrolist[[i]][c(large),])
#     final[which(final$toxval_id %in% distrolist[[i]][c(large),]$toxval_id),]$outliers<-"L"
#     final[which(final$toxval_id %in% distrolist[[i]][c(large),]$toxval_id),]$round<-"F"
#     final[which(final$toxval_id %in% distrolist[[i]][c(large),]$toxval_id),]$amount<-vals[large]
#   }
#   if (length(medium) != 0) {
#     mediumdf<-rbind(mediumdf,cbind(distrolist[[i]][c(medium),]))
#     final[which(final$toxval_id %in% distrolist[[i]][c(medium),]$toxval_id),]$outliers<-"M"
#     final[which(final$toxval_id %in% distrolist[[i]][c(medium),]$toxval_id),]$round<-"F"
#     final[which(final$toxval_id %in% distrolist[[i]][c(medium),]$toxval_id),]$amount<-vals[medium]
#   }
# }
#
# #MT: And then just send them all into excel files.
# write_xlsx(largedf,"DistroLargeOutliers.xlsx")
# write_xlsx(mediumdf,"DistroMediumOutliers.xlsx")
# write_xlsx(spreadconcerndf,"DistroPostSpreads.xlsx")
# write_xlsx(repeatconcerndf,"DistroRepeats.xlsx")

#MT: Honestly, the profiling above and below could probably be combined as a helper
#function, and that's a really good next step, to help make "checking different
#combinations of fields" easier. I'm not commenting because it's the exact same
#process as above.
lastcheck<-rest %>% count(toxval_type,toxval_units)
lastgood<-lastcheck[which(lastcheck$n >= 5),]
lastgood<-lastgood[,-3]
last<-rest %>% inner_join(lastgood)
rest<-rest %>% anti_join(lastgood)

write_xlsx(rest,"Not Enough Data.xlsx")

last<-data.table(last)
lastlist<-split(last,by=c("toxval_type","toxval_units"))
#MG: Using helper function instead. same process just in a function
values <- profile_by_fields(lastlist)
#print(values)
write_xlsx(values[0],"LastLargeOutliers.xlsx")
write_xlsx(values[1],"LastMediumOutliers.xlsx")
write_xlsx(values[2],"LastPostSpreads.xlsx")
write_xlsx(values[3],"LastRepeats.xlsx")



# largedf<-NULL
# mediumdf<-NULL
# spreadconcerndf<-NULL
# repeatconcerndf<-NULL
# preplots<-data.frame(Center = c(), Spread = c(), Index = c())
# postplots<-data.frame(Center = c(), Spread = c(), Index = c())
# for (i in 1:length(lastlist)) {
#   if (i %% 100 == 0) {print(i)}
#   temp<-lastlist[[i]]$toxval_numeric
#   temp<-log10(temp)
#   temp[which(temp == -Inf)]<- -10
#   temp[which(is.na(temp))]<- -10
#   if (length(unique(temp)) <= 2) {
#     repeatconcerndf<-rbind(repeatconcerndf,lastlist[[i]])
#     final[which(final$toxval_id %in% lastlist[[i]]$toxval_id),]$outliers<-"R"
#     final[which(final$toxval_id %in% lastlist[[i]]$toxval_id),]$round<-"S"
#     next
#   }
#   if (max(table(temp)) >= length(temp)/2) {
#     repeatconcerndf<-rbind(repeatconcerndf,lastlist[[i]])
#     final[which(final$toxval_id %in% lastlist[[i]]$toxval_id),]$outliers<-"R"
#     final[which(final$toxval_id %in% lastlist[[i]]$toxval_id),]$round<-"S"
#     next
#   }
#   if (length(temp) < 100) {
#     p<-shapiro.test(temp)$p
#   } else {p<-1}
#   if (p < 0.05) {
#     spread<-mad(temp,constant = 1)
#     center<-median(temp)
#     preplots<-rbind(preplots,c(center,spread,i))
#   } else {
#     center<-mean(temp)
#     spread<-sd(temp)
#     preplots<-rbind(preplots,c(center,spread,i))
#   }
#   vals<-abs((temp - center)/spread)
#   large <- which(vals > 5)
#   medium <- which(vals < 5 & vals > 3)
#   temp <- temp[which(vals < 3)]
#   spread<-mad(temp,constant = 1)
#   center<-median(temp)
#   postplots<-rbind(postplots,c(center,spread,i))
#   if (spread > 2) {
#     spreadconcerndf<-rbind(spreadconcerndf,distrolist[[i]])
#     final[which(final$toxval_id %in% lastlist[[i]]$toxval_id),]$outliers<-"S"
#     final[which(final$toxval_id %in% lastlist[[i]]$toxval_id),]$round<-"S"
#     next
#   }
#   if (length(large) != 0){
#     largedf<-rbind(largedf,distrolist[[i]][c(large),])
#     final[which(final$toxval_id %in% lastlist[[i]][c(large),]$toxval_id),]$outliers<-"L"
#     final[which(final$toxval_id %in% lastlist[[i]][c(large),]$toxval_id),]$round<-"S"
#     final[which(final$toxval_id %in% lastlist[[i]][c(large),]$toxval_id),]$amount<-vals[large]
#   }
#   if (length(medium) != 0) {
#     mediumdf<-rbind(mediumdf,cbind(distrolist[[i]][c(medium),]))
#     final[which(final$toxval_id %in% lastlist[[i]][c(medium),]$toxval_id),]$outliers<-"M"
#     final[which(final$toxval_id %in% lastlist[[i]][c(medium),]$toxval_id),]$round<-"S"
#     final[which(final$toxval_id %in% lastlist[[i]][c(medium),]$toxval_id),]$amount<-vals[medium]
#   }
# }
# colnames(preplots)<-c("Center","Spread", "Index")
# colnames(postplots)<-c("Center","Spread", "Index")
#
# write_xlsx(largedf,"LastLargeOutliers.xlsx")
# write_xlsx(mediumdf,"LastMediumOutliers.xlsx")
# write_xlsx(spreadconcerndf,"LastPostSpreads.xlsx")
# write_xlsx(repeatconcerndf,"LastRepeats.xlsx")
