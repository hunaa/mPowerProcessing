# Test for process_tapping_activity
# 
# Author: bhoff
###############################################################################

library(testthat)
library(synapseClient)

context("test_unit_process_tapping_activity")

testDataFolder<-system.file("testdata", package="mPowerProcessing")
tDataExpectedFile<-file.path(testDataFolder, "tappingTaskInput.RData")

ids<-c("syn4961463", "syn4961465", "syn4961484")

# This is run once, to create the data used in the test
createtExpected<-function() {
	schemaAndQuery<-sapply(ids, function(id) {
				schema<-synGet(id)
				query<-synTableQuery(paste0("SELECT * FROM ", id, " WHERE appVersion NOT LIKE '%YML%'"))
				vals <- query@values
				vals <- permuteMe(vals)
				vals <- prependHealthCodes(vals, "test-")
				query@values <- vals[1:min(nrow(vals), 100), ]
				c(schema=schema, query=query)
			})
	save(schemaAndQuery, file=tDataExpectedFile, ascii=TRUE)
}

if (createTestData()) createtExpected()

# Mock the schema and table content
expect_true(file.exists(tDataExpectedFile))
load(tDataExpectedFile)

with_mock(
		synGet=function(id) {schemaAndQuery["schema", id][[1]]},
		synTableQuery=function(sql) {schemaAndQuery["query", mPowerProcessing:::getIdFromSql(sql)][[1]]},
		{
			tResults<-process_tapping_activity(ids, NA)
			tDatFilePath<-file.path(testDataFolder, "tDatExpected.RData")
			# Here's how we created the 'expected' data frame:
			if (createTestData()) {
				expected<-tResults
				save(expected, file=tDatFilePath, ascii=TRUE)
			}
			load(tDatFilePath) # creates 'expected'
			expect_equal(tResults, expected)
		}
)

load(tDataExpectedFile)
# now add a duplicate row (repeat the last row)
dfRef<-schemaAndQuery["query", "syn4961463"][[1]]@values
schemaAndQuery["query", "syn4961463"][[1]]@values<-dfRef[c(1:nrow(dfRef),nrow(dfRef)),]
row.names(schemaAndQuery["query", "syn4961463"][[1]]@values)<-c(row.names(dfRef), sprintf("%s_0", nrow(dfRef)))

with_mock(
		synGet=function(id) {schemaAndQuery["schema", id][[1]]},
		synTableQuery=function(sql) {schemaAndQuery["query", mPowerProcessing:::getIdFromSql(sql)][[1]]},
		{
			tResults<-process_tapping_activity(ids, NA)
			tDatFilePath<-file.path(testDataFolder, "tDatExpected.RData")
			load(tDatFilePath) # creates 'expected'
			expect_equal(tResults, expected)
		}
)

# test the case that there's no new data:
lastMaxRowVersion<-c("syn4961463"="5", "syn4961465"="6", "syn4961484"="7")

load(tDataExpectedFile)

with_mock(
		synGet=function(id) {schemaAndQuery["schema", id][[1]]},
		synTableQuery=function(sql) {
			truncatedQuery<-schemaAndQuery["query", mPowerProcessing:::getIdFromSql(sql)][[1]]
			truncatedQuery@values<-truncatedQuery@values[NULL,]
			truncatedQuery
		},
		{
			tResults<-process_tapping_activity(ids, lastMaxRowVersion)
			expect_equal(nrow(tResults$tDat), 0)
			expect_equal(tResults$maxRowProcessed, lastMaxRowVersion)
		}
)


