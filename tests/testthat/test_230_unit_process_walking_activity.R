# Test for process_walking_activity
# 
# Author: bhoff
###############################################################################

library(testthat)
library(synapseClient)

context("test_unit_process_walking_activity")

testDataFolder<-system.file("testdata", package="mPowerProcessing")
wDataExpectedFile<-file.path(testDataFolder, "walkingTaskInput.RData")
wIds <- c("syn4961452", "syn4961466", "syn4961469")

# This is run once, to create the data used in the test
createWExpected<-function() {
	schemaAndQuery<-sapply(wIds, function(id) {
				schema<-synGet(id)
				query<-synTableQuery(paste0("SELECT * FROM ", id, " WHERE appVersion NOT LIKE '%YML%'"))
				vals <- query@values
				vals <- permuteMe(vals)
				vals <- prependHealthCodes(vals, "test-")
				query@values <- vals[1:min(nrow(vals), 100), ]
				c(schema=schema, query=query)
			})
	save(schemaAndQuery, file=wDataExpectedFile, ascii=TRUE)
	
}

if (createTestData()) createWExpected()

# Mock the schema and table content
expect_true(file.exists(wDataExpectedFile))
load(wDataExpectedFile)

with_mock(
		synGet=function(id) {schemaAndQuery["schema", id][[1]]},
		synTableQuery=function(sql) {schemaAndQuery["query", mPowerProcessing:::getIdFromSql(sql)][[1]]},
		{
			wResults<-process_walking_activity(wIds, NA)
			wDatFilePath<-file.path(testDataFolder, "wDatExpected.RData")
			# Here's how we created the 'expected' data frame:
			if (createTestData()) {
				expected<-wResults
				save(expected, file=wDatFilePath, ascii=TRUE)
			}
			load(wDatFilePath) # creates 'expected'
			expect_equal(wResults, expected)
		}
)


