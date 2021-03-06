# Test for cleanup_missing_med_data
# 
# Author: bhoff
###############################################################################

library(testthat)
library(synapseClient)

context("test_unit_cleanup_missing_med_data")

testDataFolder<-system.file("testdata", package="mPowerProcessing")

# load mDat:
mDatFilePath<-file.path(testDataFolder, "mDatExpected.RData")
load(mDatFilePath) # creates 'expected'
mResults<-expected
mDat<-mResults$mDat
mFilehandleCols<-mResults$mFilehandleCols

# load tDat:
tDatFilePath<-file.path(testDataFolder, "tDatExpected.RData")
load(tDatFilePath) # creates 'expected'
tResults<-expected
tDat<-tResults$tDat
tFilehandleCols<-tResults$tFilehandleCols

# load vDat:
vDatFilePath<-file.path(testDataFolder, "vDatExpected.RData")
load(vDatFilePath) # creates 'expected'
vResults<-expected
vDat<-vResults$vDat
vFilehandleCols<-vResults$vFilehandleCols

# load wDat:
wDatFilePath<-file.path(testDataFolder, "wDatExpected.RData")
load(wDatFilePath) # creates 'expected'
wResults<-expected
wDat<-wResults$wDat

# method under test:
cmm<-cleanup_missing_med_data(mDat, tDat, vDat, wDat)

cmmFilePath<-file.path(testDataFolder, "cmmExpected.RData")
# Here's how we created the 'expected' data frame:
if (createTestData()) {
	expected<-cmm
	save(expected, file=cmmFilePath, ascii=TRUE)
}
load(cmmFilePath) # creates 'expected'
expect_equal(cmm, expected)


# make sure it works for missing data
cleanup_missing_med_data(mDat[NULL,], tDat, vDat, wDat)
cleanup_missing_med_data(mDat, tDat[NULL,], vDat, wDat)
cleanup_missing_med_data(mDat, tDat, vDat[NULL,], wDat)
cleanup_missing_med_data(mDat, tDat, vDat, wDat[NULL,])
cleanup_missing_med_data(mDat[NULL,], tDat[NULL,], vDat[NULL,], wDat[NULL,])


