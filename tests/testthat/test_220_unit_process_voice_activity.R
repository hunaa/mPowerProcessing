# Test for process_voice_activity
# 
# Author: bhoff
###############################################################################

library(testthat)
library(synapseClient)
library(RJSONIO)

context("test_unit_process_voice_activity")

testDataFolder<-system.file("testdata", package="mPowerProcessing")
voiceDataExpectedFile<-file.path(testDataFolder, "voiceTaskInput.RData")

vId1 <- c("syn4961455", "syn4961457", "syn4961464")
vId2 <- c("syn4961456")

# This is run once, to create the data used in the test
createVoiceExpected<-function() {
	mockFiles<-mockFileAttachments(system.file("testdata/moment-in-day-json", package="mPowerProcessing"))
	vFiles<-mockFiles$mockFiles
	fileContent<-mockFiles$fileContent
	
	schemaAndQuery<-sapply(vId1, function(id) {
				schema<-synGet(id)
				query<-synTableQuery(paste0("SELECT * FROM ", id, " WHERE appVersion NOT LIKE '%YML%'"))
				vals <- query@values
				vals <- prependHealthCodes(vals, "test-")
				vals <- permuteMe(vals)
				query@values <- vals[1:min(nrow(vals), 100), ]
				# Now update the file Handle IDs in the data frame to match the fake ones
				query@values$`momentInDayFormat.json`[which(!is.na(query@values$`momentInDayFormat.json`))]<-
						sample(names(vFiles), size=length(which(!is.na(query@values$`momentInDayFormat.json`))), replace=T)
				list(schema=schema, query=query)
			})
	
	schema2<-synGet(vId2)
	query2<-synTableQuery(paste0("SELECT * FROM ", vId2, " WHERE appVersion NOT LIKE '%YML%'"))
	vals <- query2@values
	vals <- mPowerProcessing:::permuteMe(vals)
	query2@values <- vals[1:min(nrow(vals), 100), ]
	
	save(schemaAndQuery, schema2, query2, fileContent, vFiles, file=voiceDataExpectedFile, ascii=TRUE)
}

if (createTestData()) createVoiceExpected()

# Mock the schema and table content
expect_true(file.exists(voiceDataExpectedFile))
load(voiceDataExpectedFile)
with_mock(
		synGet=function(id) {
			if (id==vId2) {
				schema2
			} else {
				schemaAndQuery["schema", id][[1]]
			}
		},
		synTableQuery=function(sql) {
			id<-mPowerProcessing:::getIdFromSql(sql)
			if (id==vId2) {
				query2
			} else {
				schemaAndQuery["query", id][[1]]
			}
		},
		read_json_from_file=function(file) {# file is a file path with fileHandleId as name
			result<-fromJSON(fileContent[file]) # this gets the file content.  The name is the file path
			names(result)<-names(file) # result must map fileHandleId to file content
			result
		},
		synDownloadTableColumns=function(synTable, tableColumns) {
			vFiles
		},
		
		{
			vResults<-process_voice_activity(vId1, vId2, "1", "2")
			vDatFilePath<-file.path(testDataFolder, "vDatExpected.RData")
			# Here's how we created the 'expected' data frame:
			if (createTestData()) {
				expected<-vResults
				save(expected, file=vDatFilePath, ascii=TRUE)
			}
			load(vDatFilePath) # creates 'expected'
			expect_equal(vResults, expected)
		}
)


