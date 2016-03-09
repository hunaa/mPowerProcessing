# tests for utilities in process_mpower_data
# 
# Author: bhoff
###############################################################################

library(testthat)

context("test_process_mpower_data")

df<-data.frame("LAST_VERSION"=as.integer(c(1,2,3)), "TABLE_ID"=c("syn101", "syn202", "syn303"))
lastProcessedVersion <- getLastProcessedVersion(df)

# Note: This doesn't work: expect_equal(as.integer(1), lastProcessedVersion["syn101"], tolerance=.000001)
expect_lt(abs(lastProcessedVersion["syn101"]-1), .00001)

expect_true(is.na(lastProcessedVersion["syn404"]))

# test getMaxRowVersion()
df<-data.frame(A=c(1,2,3), B=c(4,5,6))
rownames(df)<-c("1_10", "2_4", "3_1")
expect_equal(getMaxRowVersion(df), 10)

