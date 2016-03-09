# Test for store_cleaned_data
# 
# Author: bhoff
###############################################################################

library(testthat)
library(synapseClient)

context("test_store_cleaned_data")

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

cmm<-cleanup_missing_med_data(mDat, tDat, vDat, wDat)

eDatFilePath<-file.path(testDataFolder, "eDatExpected.RData")
load(eDatFilePath) # creates 'expected'
eDat<-expected

uDatFilePath<-file.path(testDataFolder, "uDatExpected.RData")
load(uDatFilePath) # creates 'expected'
uDat<-expected

pDatFilePath<-file.path(testDataFolder, "pDatExpected.RData")
load(pDatFilePath) # creates 'expected'
pDat<-expected

# this is the parent project of all the tables
newParent<-"syn4993293"

# To generate qqFilePath:
# qq <- synQuery(paste0('SELECT id, name FROM table WHERE parentId=="', newParent, '"'))

qqFilePath<-file.path(testDataFolder, "qq.RData")
# save(qq, file=qqFilePath, ascii=TRUE)
load(qqFilePath)

with_mock(
		synQuery=function(q) qq,
		as.tableColumns=function(x) list(fileHandleId="123"),
		synGet=function(id) id,
		synStore=function(table) table,
		{
			store_cleaned_data(newParent, eDat, uDat, pDat, mDat, tDat, vDat, wDat, 
					mFilehandleCols, tFilehandleCols, vFilehandleCols)
		}
)

