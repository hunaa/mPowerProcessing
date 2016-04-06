# tests for utilities in process_mpower_data
# 
# Author: bhoff
###############################################################################

library(testthat)

context("test_unit_process_mpower_data")

df<-data.frame("TABLE_ID"=c("syn101", "syn202", "syn303"), "LAST_VERSION"=as.integer(c(3,2,1)), stringsAsFactors=F)
rownames(df)<-c("1_2", "2_0", "3_1")
lastProcessedVersion <- getLastProcessedVersion(df)

# Note: This doesn't work: expect_equal(as.integer(1), lastProcessedVersion["syn101"], tolerance=.000001)
expect_lt(abs(lastProcessedVersion["syn101"]-3), .00001)

expect_true(is.na(lastProcessedVersion["syn404"]))

# now let's check the conversion back to a data frame
expect_equal(df, mergeLastProcessVersionIntoToDF(lastProcessedVersion, df))

# should also be able to insert new rows:
df<-data.frame("TABLE_ID"=c("syn101", "syn202", "syn303"), "LAST_VERSION"=as.integer(c(3,2,1)), stringsAsFactors=F)
rownames(df)<-c("1_2", "2_0", "3_1")
lastProcessedVersion <- getLastProcessedVersion(df)
lastProcessedVersion["syn404"]<-10
mergeLastProcessVersionIntoToDF(lastProcessedVersion, df)

 
# test getMaxRowVersion()
df<-data.frame(A=c(1,2,3), B=c(4,5,6))
rownames(df)<-c("1_10", "2_4", "3_1")
expect_equal(getMaxRowVersion(df), 10)


