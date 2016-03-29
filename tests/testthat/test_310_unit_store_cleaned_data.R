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
mDat<-cmm$mDat
tDat<-cmm$tDat
vDat<-cmm$vDat
wDat<-cmm$wDat

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

eDatId<- qq[which(qq$table.name=="Demographics Survey"), "table.id"]
uDatId<- qq[which(qq$table.name=="UPDRS Survey"), "table.id"]
pDatId<- qq[which(qq$table.name=="PDQ8 Survey"), "table.id"]
mDatId<- qq[which(qq$table.name=="Memory Activity"), "table.id"]
tDatId<- qq[which(qq$table.name=="Tapping Activity"), "table.id"]
vDatId<- qq[which(qq$table.name=="Voice Activity"), "table.id"]
wDatId<- qq[which(qq$table.name=="Walking Activity"), "table.id"]

validateColumnTypes<-function(schema, dataframe) {
	schemaColumns<-schema@columns
	schemaColumnMap<-list()
	for (column in schemaColumns@content) schemaColumnMap[[column@name]]<-column
	for (dfColumnName in names(dataframe)) {
		schemaColumn<-schemaColumnMap[[dfColumnName]]
		if (is.null(schemaColumn)) stop(sprintf("Data frame has column %s but schema has no such column.", dfColumnName))
		dfColumnType<-class(dataframe[[dfColumnName]])[1]
		expectedTableColumnTypes<-synapseClient:::getTableColumnTypeForDataFrameColumnType(dfColumnType)
		tableColumnType<-schemaColumn@columnType
		if (!any(tableColumnType==expectedTableColumnTypes)) {
			stop(sprintf("Column %s has type %s in Synapse but %s in the data frame. Allowed data frame columns types: %s.", 
							dfColumnName, tableColumnType, dfColumnType, 
							paste(expectedTableColumnTypes, collapse=" or ")))
		}
	}
}

getSchemaForId<-function(id) {
	if (id==eDatId) {
		demographicSurveySchema(id)
	} else if (id==uDatId) {
		updrsSurveySchema(id)
	} else if (id==pDatId) {
		pdq8SurveySchema(id)
	} else if (id==mDatId) {
		memoryActivitySchema(id)
	} else if (id==tDatId) {
		tappingActivitySchema(id)
	} else if (id==vDatId) {
		voiceActivitySchema(id)
	} else if (id==wDatId) {
		walkingActivitySchema(id)
	} else {
		stop("Unexpected table ID", id)
	}
	
}

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
		synGet=function(id) {getSchemaForId(id)},
		synStore=function(table) {
			id<-table@schema
			if (id==tDatId) {
				# 'expect_equal' fails if I don't do this:
				rownames(table@values)<-NULL; rownames(tDat)<-NULL
				expect_equal(table@values, tDat)
			}
			validateColumnTypes(getSchemaForId(id), table@values)
			table
		},
		{
			store_cleaned_data(outputProjectId, eDat, uDat, pDat, mDat, tDat, vDat, wDat, 
					mFilehandleCols, tFilehandleCols, vFilehandleCols)
		}
)
