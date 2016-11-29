# R Script - dplyr predominant
# Author: leerssej
# Date: Updated 28 Nov 2016
# Desc: Bucketize names by geoClusters, shard name words, 
# Desc: Norm and exclude name memberships
# Desc: string cluster names, select most representative name

options(stringsAsFactors = FALSE)
library(magrittr) 
library(tidyverse)

###### 95. REM2 Transform SwpGroup Names into concatenateGroups ## ##
# Same as newnewcoded 16 REM2 but with substitute
# Clear the decks
rm(list = ls())
#1 REM2 = REM2
## Name membership testing
load("REM2fullData")
# REM2fullData <- read.csv("REM2fullData.csv", stringsAsFactors = F)
REM2fullData %<>% rename(m1 = gc1mID, GID = GLDNID)
glimpse(REM2fullData)

# # Just REM2 Subset
REM2Clstr <- select(REM2fullData, m1, GID, ACCT_ID, ADDR_ID, swpCNTRBTR_ACCT_NAME) %>% distinct
# REM2Clstr
glimpse(REM2Clstr) #cnt = 38,715

# Cluster all the name data up without uniquing the data
cluster <- 
    group_by(REM2Clstr, m1) %>%
    summarise(clstrName = paste(swpCNTRBTR_ACCT_NAME, collapse = " "))
glimpse(cluster)

# # # Testing to see if the upstream WS cleaning took care of this need
# #Eliminate the extraneous whiteREM2ce that later turns into separate values that get counted
# cluster$clustername2 <- gsub("\\s\\s+"," ", cluster$clstrName)
# # Test for whiteREM2ce removal
# write.csv(cluster, "cluster.csv")

# Join Cluster back up to the original data
clusterREM2 <- 
    left_join(REM2Clstr, cluster, by = "m1")
# CheckStructure
glimpse(clusterREM2)

##RowCounting
# count of rows to clusterREM2 - summarise with n()
clusterCount <-  group_by(clusterREM2, m1) %>% summarise(rows=n())
# Rejoin the Row Counts to the table
clusterREM2 <- left_join(REM2Clstr, clusterCount, by = "m1") %>% 
    left_join(cluster, by = "m1")
glimpse(clusterREM2)

##SplitName column
#add new column with separated values from simple name 
clusterREM2$Nameshards <- sapply(clusterREM2$swpCNTRBTR_ACCT_NAME, function(x) strsplit(x,"\\s"))
# check the changed clustername to list
# head(clusterREM2)
glimpse(clusterREM2)

##ClusterName management into split values column
#add new column with separated values from name concatenate (need to unlist as it is multivector)
clusterREM2$clstrName <- sapply(clusterREM2$clstrName, function(x) unlist(strsplit(x,"\\s")))
glimpse(clusterREM2)
# Countnumber of names in clusters and then make into new column
clusterREM2$ClstrNameCnt <- sapply(clusterREM2$clstrName, function(cnu) length(unlist(cnu)))
glimpse(clusterREM2)
# Countnumber of names in nameshards and then make into new column
clusterREM2$NameCnt <- sapply(clusterREM2$Nameshards, function(ns) length(unlist(ns)))
glimpse(clusterREM2)

# ShardCount is quantity of namewords that are found in group name
ShardCount <- function(cnu, ns){
    length(which(cnu %in% ns))
}

# mapply ShardCount to the clusterREM2 Data
clusterREM2$ShardCount <- mapply(ShardCount, clusterREM2$clstrName, clusterREM2$Nameshards)
glimpse(clusterREM2)

# Compare Ns to Nb with rows
clusterREM2aa <- 
    clusterREM2 %>% 
    mutate(NameQNrmd = signif(NameCnt/ShardCount,digits = 5),
           AvgCt = signif(ClstrNameCnt/rows, digits = 0))
glimpse(clusterREM2aa)

## Rule as an OverMerge based on 1 word sharing rule
# generate flag for overmerge from Logic of when get NameQnrmd value of 1 and rows >1 & use this flag to generate new ID
clusterREM2a <- 
    clusterREM2aa %>% 
    rowwise %>%
    mutate(mg = ifelse(NameQNrmd == 1 & rows > 1, 1, 0),
           OvrMrg = ifelse(mg != 0 ,"OverMerged",""))
glimpse(clusterREM2a) 
    
clusterREM2ab <- 
    clusterREM2a %>% 
    mutate(m1mg = as.numeric(m1) + signif(mg*(runif(1)), digits = 5)) 
glimpse(clusterREM2ab) 
## Generate data about the groups formed based on 1, 1.5 and 2 SD distributions
clusterREM2b <- 
    clusterREM2ab %>% 
    group_by(m1mg) %>% 
    mutate(grpSDnameQnrmd = ifelse(rows == 1 | mg == 1 , 0, signif(sd(NameQNrmd), digits = 3)),
           grpMnNameQnrmd = signif(mean(NameQNrmd), digits = 3),
           grpMinNameQnrm = min(NameQNrmd),
           grpMaxNameQnrm = max(NameQNrmd),
           grpSD1 = signif(grpMnNameQnrmd-grpSDnameQnrmd, digits = 3),
           grpSD1p5 = signif(grpMnNameQnrmd-(1.5*grpSDnameQnrmd), digits = 3),
           grpSD2 = signif(grpMnNameQnrmd-(2*grpSDnameQnrmd), digits = 3),
           wgthdNameQnrmd = signif(NameQNrmd/ClstrNameCnt, digits = 2),
           NameDensity = signif(ShardCount/ClstrNameCnt, digits = 3)
    )

# for whole M1MG numbers Generate flag for ouside of 2+ StdDev for Qnrmd value
clusterREM2c <-  
clusterREM2b %>%  
    rowwise %>%
    mutate(mgSDaccpt = ifelse(((m1mg %% 1) == 0 & NameQNrmd < grpSD2), 1, 0),
            SDOvrMrg = ifelse(mgSDaccpt != 0 ,"SD2overMerged","")) %>% 
    mutate(m1mg = ifelse((m1mg %% 1) == 0, as.numeric(m1) + signif(mgSDaccpt*(runif(1)), digits = 5), m1mg)) 
glimpse(clusterREM2c) 
cat(names(clusterREM2c))
# Reorgview for easier review
clusterREM2d <- 
clusterREM2c %>%
    select(ACCT_ID, ADDR_ID, m1mg, OvrMrg, GID, ShrdCt = ShardCount, NameCnt, NameQNrmd,
           ClstCt = ClstrNameCnt, AvgCt, rows, swpCNTRBTR_ACCT_NAME, clstrName, Nameshards, grpSDnameQnrmd, grpMnNameQnrmd, grpMinNameQnrm, grpMaxNameQnrm, grpSD1, grpSD1p5, grpSD2, wgthdNameQnrmd, NameDensity, mgSDaccpt, SDOvrMrg) %>% 
    distinct
glimpse(clusterREM2d)

save(clusterREM2d, file="clusterREM2d")
#Summarise out the winners for the ID per m1mg(overmerge proofed) group
m1mgGeoClusterREM2NameWinners <- 
    group_by(clusterREM2d , m1mg) %>% 
    filter(NameQNrmd == min(NameQNrmd)) %>% 
    arrange(m1mg) %>% 
    select(m1mg, gcWinningName = swpCNTRBTR_ACCT_NAME) %>% 
    distinct
glimpse(m1mgGeoClusterREM2NameWinners)

# # Get unique list of Names by reducing on ID
# m1mgGeoClusterREM2nameWinners <-
#     
#     distinct
# glimpse(m1mgGeoClusterREM2nameWinners)

# Review results directly and Check for dupes
write.csv(m1mgGeoClusterREM2NameWinners, "m1mgGeoClusterREM2NameWinners.csv")

# ClusterREM2 joined with gcWinningNames
clusterREM2wgcWinNames <- 
    left_join(clusterREM2d, m1mgGeoClusterREM2NameWinners)
glimpse(clusterREM2wgcWinNames)

# ClusterREM2 joined with gcWinningNames to ALL Data
glimpse(REM2fullData)
glimpse(clusterREM2wgcWinNames)
clusterREM2RejoinedFull <-
    left_join(REM2fullData, clusterREM2wgcWinNames) 
#Distinct
glimpse(clusterREM2RejoinedFull)

# Tie the NameQnormingData back into the table
# clusterREM2 for rejoining - dropping lists into single vectors
glimpse(clusterREM2RejoinedFull)
clusterREM2RejoinedFull$clstrName <- sapply(clusterREM2RejoinedFull $clstrName, function(cn) paste(cn, collapse = " "))
clusterREM2RejoinedFull$Nameshards <- sapply(clusterREM2RejoinedFull $Nameshards, function(ns) paste(ns, collapse = " "))
glimpse(clusterREM2RejoinedFull)

write.csv(clusterREM2RejoinedFull, file = "clusterREM2RejoinedFull.csv", na = "", row.names = F)

## Same Group - Different Address Flag
# draw out count of Addresses per m1
glimpse(clusterREM2RejoinedFull)
clusterREM2RFAddrFlag <- 
    select(clusterREM2RejoinedFull, m1, rctGmStreet_number) %>%
    mutate(gmStrt_nmbrChar = as.character(rctGmStreet_number)) %>% 
    distinct %>% 
    group_by(m1) %>% 
    summarise(m1addrCnt = n()) %>% 
    #convert all the ones to ""
    mutate(m1addrCnt = gsub("1","",m1addrCnt))
glimpse(clusterREM2RFAddrFlag)

# Tie the count back up to the clusterREM2RejoinedFull table
clusterREM2RejoinedFullPlus <- 
    left_join(clusterREM2RejoinedFull, clusterREM2RFAddrFlag)
glimpse(clusterREM2RejoinedFullPlus)

#checkit in xlsx
write.csv(clusterREM2RejoinedFullPlus, "clusterREM2RejoinedFullPlus.csv", row.names = F, na = "")##Name membership testing
save(clusterREM2RejoinedFullPlus, file = "clusterREM2RejoinedFullPlus")

