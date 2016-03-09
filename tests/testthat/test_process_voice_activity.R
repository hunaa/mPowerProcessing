# Test for process_voice_activity
# 
# Author: bhoff
###############################################################################

library(testthat)
library(synapseClient)

context("test_process_voice_activity")

testDataFolder<-system.file("testdata", package="mPowerProcessing")
voiceDataExpectedFile<-file.path(testDataFolder, "voiceTaskInput.RData")

# This is run once, to create the data used in the test
createVoiceExpected<-function() {
	id1<-"syn4961455"
	schema1<-synGet(id1)
	query1<-synTableQuery(paste0("SELECT * FROM ", id1, " WHERE appVersion NOT LIKE '%YML%' LIMIT 100 OFFSET 100"))
	id2<-"syn4961456"
	schema2<-synGet(id2)
	query2<-synTableQuery(paste0("SELECT * FROM ", id2, " WHERE appVersion NOT LIKE '%YML%' LIMIT 100 OFFSET 500"))
	
	vMap<-synDownloadTableColumns(query1, "momentInDayFormat.json") # maps filehandleId to file path
	fileContent <- sapply(vMap[query1@values[, "momentInDayFormat.json"]], read_json_from_file)
	names(fileContent)<-vMap # maps file path to file content
	save(schema1, query1, schema2, query2, vMap, fileContent, file=voiceDataExpectedFile, ascii=TRUE)
}

# Mock the schema and table content
expect_true(file.exists(voiceDataExpectedFile))
load(voiceDataExpectedFile)
with_mock(
		synGet=function(id) {
			if (id=="syn101") {
				schema1
			} else {
				schema2
			}
		},
		synTableQuery=function(sql) {
			if (length(grep("syn101", sql))>0) {
				query1
			} else {
				query2
			}
		},
		read_json_from_file=function(file) {# file is a file path with fileHandleId as name
			result<-fileContent[file] # this gets the file content.  The name is the file path
			names(result)<-names(file) # result must map fileHandleId to file content
			result
		},
		{
			vResults<-process_voice_activity("syn101", "syn102", "1", "2")
			vDatFilePath<-file.path(testDataFolder, "vDatExpected.RData")
			# Here's how we created the 'expected' data frame:
			# expected<-vResults
			# save(expected, file=vDatFilePath, ascii=TRUE)
			load(vDatFilePath) # creates 'expected'
			expect_equal(vResults, expected)
		}
)


