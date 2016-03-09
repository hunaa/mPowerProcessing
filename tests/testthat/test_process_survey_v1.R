# Test for process_survey_v1
# 
# Author: bhoff
###############################################################################

library(testthat)
library(synapseClient)

context("test_process_survey_v1")

testDataFolder<-system.file("testdata", package="mPowerProcessing")
v1SurveyInputFile<-file.path(testDataFolder, "v1SurveyInput.RData")

# This is run once, to create the data used in the test
createV1Expected<-function() {
	id<-"syn4961453"
	schema<-synGet(id)
	query<-synTableQuery(paste0("SELECT * FROM ", id, " WHERE appVersion NOT LIKE '%YML%' LIMIT 100 OFFSET 500"))
	eComFiles<-synDownloadTableColumns(query, "health-history") # maps filehandleId to file path
	eComContent <- sapply(eComFiles, readLines, warn=F)
	names(eComContent)<-eComFiles # maps file path to file content
	save(schema, query, eComFiles, eComContent, file=v1SurveyInputFile, ascii=TRUE)
}

# Mock the schema and table content
load(v1SurveyInputFile)

with_mock(
		synGet=function(id) {schema},
		synTableQuery=function(sql) {query},
		synDownloadTableColumns=function(synTable, tableColumns) {eComFiles},
		readLines=function(x) { # x is a file path with fileHandleId as name
			result<-eComContent[x] # this gets the file content.  The name is the file path
			names(result)<-names(x) # result must map fileHandleId to file content
			result
		}, 
		{
			eDat<-process_survey_v1("syn101")
			eDatFilePath<-file.path(testDataFolder, "eDatExpected.RData")
			# Here's how we created the 'expected' data frame:
			# expected<-eDat
			# save(expected, file=eDatFilePath, ascii=TRUE)
			load(eDatFilePath) # creates 'expected'
			expect_equal(eDat, expected)
		}
)
