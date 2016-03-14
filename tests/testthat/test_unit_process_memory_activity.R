# Test for process_memory_activity
# 
# Author: bhoff
###############################################################################

library(testthat)
library(synapseClient)

context("test_unit_process_memory_activity")

testDataFolder<-system.file("testdata", package="mPowerProcessing")
mDataExpectedFile<-file.path(testDataFolder, "memTaskInput.RData")

# This is run once, to create the data used in the test
createmExpected<-function() {
	id<-"syn4961459"
	schema<-synGet(id)
	query<-synTableQuery(paste0("SELECT * FROM ", id, " WHERE appVersion NOT LIKE '%YML%'"))
	vals <- query@values
	vals <- mPowerProcessing:::permuteMe(vals)
	query@values <- vals[1:min(nrow(vals), 100), ]
	save(schema, query, file=mDataExpectedFile, ascii=TRUE)
}

# Mock the schema and table content
expect_true(file.exists(mDataExpectedFile))
load(mDataExpectedFile)


with_mock(
		synGet=function(id) {schema},
		synTableQuery=function(sql) {query},
		{
			mResults<-process_memory_activity("syn101")
			mDatFilePath<-file.path(testDataFolder, "mDatExpected.RData")
			# Here's how we created the 'expected' data frame:
			#expected<-mResults
			#save(expected, file=mDatFilePath, ascii=TRUE)
			load(mDatFilePath) # creates 'expected'
			expect_equal(mResults, expected)
		}
)


