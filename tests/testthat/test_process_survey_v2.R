# Test for process_survey_v2
# 
# Author: bhoff
###############################################################################

library(testthat)
library(synapseClient)

context("test_process_survey_v2")

testDataFolder<-system.file("testdata", package="mPowerProcessing")
v2DataExpectedFile<-file.path(testDataFolder, "v2SurveyInput.RData")

# This is run once, to create the data used in the test
createV2Expected<-function() {
	id<-"syn4961480"
	schema<-synGet(id)
	query<-synTableQuery(paste0("SELECT * FROM ", id, " WHERE appVersion NOT LIKE '%YML%'"))
	vals <- query@values
	vals <- mPowerProcessing:::permuteMe(vals)
	query@values <- vals[1:min(nrow(vals), 100), ]
	save(schema, query, file=v2DataExpectedFile, ascii=TRUE)
}

# Mock the schema and table content
expect_true(file.exists(v2DataExpectedFile))
load(v2DataExpectedFile)

with_mock(
		synGet=function(id) {schema},
		synTableQuery=function(sql) {query},
		{
			uDat<-process_survey_v2("syn101")
			uDatFilePath<-file.path(testDataFolder, "uDatExpected.RData")
			# Here's how we created the 'expected' data frame:
			# expected<-uDat
			# save(expected, file=uDatFilePath, ascii=TRUE)
			load(uDatFilePath) # creates 'expected'
			expect_equal(uDat, expected)
		}
)



