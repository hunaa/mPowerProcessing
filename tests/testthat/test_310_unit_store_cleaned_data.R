# Test for store_cleaned_data
# 
# Author: bhoff
###############################################################################

library(testthat)
library(synapseClient)

context("test_unit_store_cleaned_data")

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
eDat<-expected$eDat

uDatFilePath<-file.path(testDataFolder, "uDatExpected.RData")
load(uDatFilePath) # creates 'expected'
uDat<-expected$uDat

pDatFilePath<-file.path(testDataFolder, "pDatExpected.RData")
load(pDatFilePath) # creates 'expected'
pDat<-expected$pDat

# this is the parent project of all the tables
outputProjectId<-"syn4993293"

qqFilePath<-file.path(testDataFolder, "qq.RData")

# To generate qqFilePath:
if (createTestData()) {
	qq <- synQuery(paste0('SELECT id, name FROM table WHERE parentId=="', outputProjectId, '"'))
	save(qq, file=qqFilePath, ascii=TRUE)
}

load(qqFilePath)

# we use this to capture the table values that 'store_cleaned_data' stores
storedResults<-NULL

tDatId<- qq[which(qq$table.name=="Tapping Activity"), "table.id"]

with_mock(
		synQuery=function(q) qq,
		synTableQuery=function(sql) {
			id<-getIdFromSql(sprintf("%s where", sql))
			if (id==tDatId) {
				values=tDat # mock the case in which the data is already in the table
			} else {
				values=data.frame()
			}
			Table(tableSchema=id, values=values)
		},
		synGet=function(id) id,
		synStore=function(table) {storedResults[[table@schema]]<<-table@values; table},
		{
			store_cleaned_data(outputProjectId, eDat, uDat, pDat, mDat, tDat, vDat, wDat, 
					mFilehandleCols, tFilehandleCols, vFilehandleCols)
		}
)


# 'expect_equal' fails if I don't do this:
rownames(storedResults[[tDatId]])<-NULL; rownames(tDat)<-NULL
expect_equal(storedResults[[tDatId]], tDat)
