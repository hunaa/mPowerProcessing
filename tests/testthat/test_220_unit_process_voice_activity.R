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
				vals <- permuteMe(vals)
				vals <- prependHealthCodes(vals, "test-")
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

load(voiceDataExpectedFile)
# now add a duplicate row (repeat the last row)
dfRef<-schemaAndQuery["query", "syn4961455"][[1]]@values
schemaAndQuery["query", "syn4961455"][[1]]@values<-dfRef[c(1:nrow(dfRef),nrow(dfRef)),]
row.names(schemaAndQuery["query", "syn4961455"][[1]]@values)<-c(row.names(dfRef), sprintf("%s_0", nrow(dfRef)))

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
			load(vDatFilePath) # creates 'expected'
			expect_equal(vResults, expected)
		}
)


# test the case that there's no new data:
lastMaxRowVersion1<-c("syn4961455"=5, "syn4961457"=6, "syn4961464"=7)
lastMaxRowVersion2<-c("syn4961456"=8)

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
				truncatedQuery<-query2
			} else {
				truncatedQuery<-schemaAndQuery["query", id][[1]]
			}
			truncatedQuery@values<-truncatedQuery@values[NULL,]
			truncatedQuery
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
			vResults<-process_voice_activity(vId1, vId2, lastMaxRowVersion1, lastMaxRowVersion2)
			expect_equal(nrow(vResults$vDat), 0)
			expect_equal(vResults$maxRowProcessed[vId1], lastMaxRowVersion1)
			expect_equal(vResults$maxRowProcessed[vId2], lastMaxRowVersion2)
		}
)
