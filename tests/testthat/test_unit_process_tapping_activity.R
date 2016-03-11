# Test for process_tapping_activity
# 
# Author: bhoff
###############################################################################

library(testthat)
library(synapseClient)

context("test_process_tapping_activity")

testDataFolder<-system.file("testdata", package="mPowerProcessing")
tDataExpectedFile<-file.path(testDataFolder, "tappingTaskInput.RData")

# This is run once, to create the data used in the test
createtExpected<-function() {
	id<-"syn4961463"
	schema<-synGet(id)
	query<-synTableQuery(paste0("SELECT * FROM ", id, " WHERE appVersion NOT LIKE '%YML%' LIMIT 100 OFFSET 500"))
	save(schema, query, file=tDataExpectedFile, ascii=TRUE)
}

# Mock the schema and table content
expect_true(file.exists(tDataExpectedFile))
load(tDataExpectedFile)

with_mock(
		synGet=function(id) {schema},
		synTableQuery=function(sql) {query},
		{
			tResults<-process_tapping_activity("syn101")
			tDatFilePath<-file.path(testDataFolder, "tDatExpected.RData")
			# Here's how we created the 'expected' data frame:
			# expected<-tResults
			# save(expected, file=tDatFilePath, ascii=TRUE)
			load(tDatFilePath) # creates 'expected'
			expect_equal(tResults, expected)
		}
)


