# Test for process_survey_v3
# 
# Author: bhoff
###############################################################################

library(testthat)
library(synapseClient)

context("test_unit_process_survey_v3")

testDataFolder<-system.file("testdata", package="mPowerProcessing")
v3DataExpectedFile<-file.path(testDataFolder, "v3SurveyInput.RData")

# This is run once, to create the data used in the test
createV3Expected<-function() {
	id<-"syn4961472"
	schema<-synGet(id)
	query<-synTableQuery(paste0("SELECT * FROM ", id, " WHERE appVersion NOT LIKE '%YML%'"))
	vals <- query@values
	vals <- mPowerProcessing:::permuteMe(vals)
	query@values <- vals[1:min(nrow(vals), 100), ]
	save(schema, query, file=v3DataExpectedFile, ascii=TRUE)
}

# Mock the schema and table content
expect_true(file.exists(v3DataExpectedFile))
load(v3DataExpectedFile)

with_mock(
		synGet=function(id) {schema},
		synTableQuery=function(sql) {query},
		{
			pDat<-process_survey_v3("syn101")
			pDatFilePath<-file.path(testDataFolder, "pDatExpected.RData")
			# Here's how we created the 'expected' data frame:
			# expected<-pDat
			# save(expected, file=pDatFilePath, ascii=TRUE)
			load(pDatFilePath) # creates 'expected'
			expect_equal(pDat, expected)
		}
)


