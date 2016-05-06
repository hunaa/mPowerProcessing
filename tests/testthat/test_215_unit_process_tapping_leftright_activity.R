# Test for process_tapping_activity
# 
# Author: bhoff
###############################################################################

library(testthat)
library(synapseClient)

context("test_unit_process_tapping_leftright_activity")

testDataFolder<-system.file("testdata", package="mPowerProcessing")
tlrDataInputFile<-file.path(testDataFolder, "tappingLeftrightTaskInput.RData")

ids<-c("syn5556502")

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
	save(schemaAndQuery, file=tlrDataInputFile, ascii=TRUE)
}

if (createTestData()) createtExpected()

# Mock the schema and table content
expect_true(file.exists(tlrDataInputFile))
load(tlrDataInputFile)

with_mock(
		synGet=function(id) {schemaAndQuery["schema", id][[1]]},
		synTableQuery=function(sql) {schemaAndQuery["query", mPowerProcessing:::getIdFromSql(sql)][[1]]},
		{
			tlrResults<-process_tapping_leftright_activity(ids, NA)
			tlrDataExpectedPath<-file.path(testDataFolder, "tlrDatExpected.RData")
			# Here's how we created the 'expected' data frame:
			if (createTestData()) {
				expected<-tlrResults
				save(expected, file=tlrDataExpectedPath, ascii=TRUE)
			}
			load(tlrDataExpectedPath) # creates 'expected'
			expect_equal(tlrResults, expected)
		}
)

load(tlrDataInputFile)
# now add a duplicate row (repeat the last row)
dfRef<-schemaAndQuery["query", "syn5556502"][[1]]@values
schemaAndQuery["query", "syn5556502"][[1]]@values<-dfRef[c(1:nrow(dfRef),nrow(dfRef)),]
row.names(schemaAndQuery["query", "syn5556502"][[1]]@values)<-c(row.names(dfRef), sprintf("%s_0", nrow(dfRef)))

with_mock(
		synGet=function(id) {schemaAndQuery["schema", id][[1]]},
		synTableQuery=function(sql) {schemaAndQuery["query", mPowerProcessing:::getIdFromSql(sql)][[1]]},
		{
			tlrResults<-process_tapping_leftright_activity(ids, NA)
			tlrDataExpectedPath<-file.path(testDataFolder, "tlrDatExpected.RData")
			load(tlrDataExpectedPath) # creates 'expected'
			expect_equal(tlrResults, expected)
		}
)

# test the case that there's no new data:
lastMaxRowVersion<-c("syn5556502"="99")

load(tlrDataInputFile)

with_mock(
		synGet=function(id) {schemaAndQuery["schema", id][[1]]},
		synTableQuery=function(sql) {
			truncatedQuery<-schemaAndQuery["query", mPowerProcessing:::getIdFromSql(sql)][[1]]
			truncatedQuery@values<-truncatedQuery@values[NULL,]
			truncatedQuery
		},
		{
			tlrResults<-process_tapping_leftright_activity(ids, lastMaxRowVersion)
			expect_equal(nrow(tlrResults$tlrDat), 0)
			expect_equal(tlrResults$maxRowVersion, lastMaxRowVersion)
		}
)


