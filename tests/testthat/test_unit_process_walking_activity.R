# Test for process_walking_activity
# 
# Author: bhoff
###############################################################################

library(testthat)
library(synapseClient)

context("test_unit_process_walking_activity")

testDataFolder<-system.file("testdata", package="mPowerProcessing")
wDataExpectedFile<-file.path(testDataFolder, "walkingTaskInput.RData")

# This is run once, to create the data used in the test
createWExpected<-function() {
	id<-"syn4961452"
	schema<-synGet(id)
	query<-synTableQuery(paste0("SELECT * FROM ", id, " WHERE appVersion NOT LIKE '%YML%' LIMIT 100 OFFSET 500"))
	save(schema, query, file=wDataExpectedFile, ascii=TRUE)
}

# Mock the schema and table content
expect_true(file.exists(wDataExpectedFile))
load(wDataExpectedFile)

with_mock(
		synGet=function(id) {schema},
		synTableQuery=function(sql) {query},
		{
			wResults<-process_walking_activity("syn101")
			wDatFilePath<-file.path(testDataFolder, "wDatExpected.RData")
			# Here's how we created the 'expected' data frame:
			# expected<-wResults
			# save(expected, file=wDatFilePath, ascii=TRUE)
			load(wDatFilePath) # creates 'expected'
			expect_equal(wResults, expected)
		}
)


