# End-to-end integration test
# To run:
# library(synapseClient)
# synapseLogin()
# source(system.file("integration_tests/process_mpower_data_integration_test.R", package="mPowerProcessing"))
# 
# Author: bhoff
###############################################################################
library(testthat)
library(synapseClient)

context("test_integration_process_mpower_data")


library(synapseClient)

username<-Sys.getenv("SYNAPSE_USERNAME")
if (nchar(username)==0) stop("Cannot run integration test.  Environment variable USERNAME is missing.")
apiKey<-Sys.getenv("SYNAPSE_APIKEY")
if (nchar(apiKey)==0) stop("Cannot run integration test.  Environment variable USERNAME is missing.")

synapseLogin(username=username, apiKey=apiKey, rememberMe=F)

message("Creating project ...")
project<-Project()
project<-synStore(project)
outputProjectId<-propertyValue(project, "id")
message("...done.  Project ID is ", outputProjectId)

## create and populate the source tables
testDataFolder<-system.file("testdata", package="mPowerProcessing")

# I encapsulate parts of the script as functions in order to limit the scope of the variables used
createSurveyV1Table<-function(project) {
	message("Creating Survey V1 table ...")
	v1SurveyInputFile<-file.path(testDataFolder, "v1SurveyInput.RData")
	load(v1SurveyInputFile) # brings into namespace: schema, query, eComFiles, eComContent
	# create the file handles
	tableData<-query@values
	#	for each in eComContent:  write to disk, upload to file handle, replace fh-id in tableData
	for (n in names(eComContent)) { # n is the file path
		fileContent<-eComContent[n]
		origFileHandleId<-names(eComFiles[which(eComFiles==n)])
		# upload a file and receive the file handle
		filePath<-tempfile()
		connection<-file(filePath)
		writeChar(fileContent, connection, eos=NULL)
		close(connection)  
		fileHandle<-synapseClient:::chunkedUploadFile(filePath)
		newFileHandleId<-fileHandle$id
		tableData[["health-history"]][which(tableData[["health-history"]]==origFileHandleId)]<-newFileHandleId
	}
	# create the schema
	columns<-list()
	for (column in schema@columns@content) {
		column@id<-character(0)
		columns<-append(columns, column)
	}
	v1Schema<-TableSchema("Survey V1 Raw Input", project, columns)
	v1Schema<-synStore(v1Schema)
	# create the v1 survey content
	v1Table<-Table(v1Schema, tableData)
	synStore(v1Table)
	eId<-propertyValue(v1Schema, "id")
	message("...done.")
	eId
}
#TODO reenable when TEST IS FIXED eId<-createSurveyV1Table(project)

createTable<-function(project, rDataFileName, tableName, message) {
	message(message)
	inputFile<-file.path(testDataFolder, rDataFileName)
	load(inputFile) # brings into namespace: schema, query
	tableData<-query@values
	columns<-list()
	for (column in schema@columns@content) {
		column@id<-character(0)
		columns<-append(columns, column)
	}
	newSchema<-TableSchema(tableName, project, columns)
	newSchema<-synStore(newSchema)
	# create the table content
	table<-Table(newSchema, tableData)
	synStore(table)
	message("...done.")
	propertyValue(newSchema, "id")
}

#TODO reenable when data is available uId<-createTable(project, "v2SurveyInput.RData", "Survey V2 Raw Input", "Creating Survey V2 table ...")

#TODO reenable when data is available pId<-createTable(project, "v3SurveyInput.RData", "Survey V3 Raw Input", "Creating Survey V3 table ...")

#TODO reenable when data is available mId<-createTable(project, "memTaskInput.RData", "Memory Task Raw Input", "Creating Memory Task table ...")

# TODO create copies of  "syn4961465", "syn4961484", in addition to "syn4961463"
#TODO reenable when data is available tId<-createTable(project, "tappingTaskInput.RData", "Tapping Task Raw Input", "Creating Tapping Task table ...")


createVoiceTaskTable1<-function(project) {
	message("Creating Voice Task table ...")
	voiceTaskInputFile<-file.path(testDataFolder, "voiceTaskInput.RData")
	load(voiceTaskInputFile) # brings into namespace: schema1, query1, schema2, query2, vMap, fileContent
	# create the file handles
	tableData<-query1@values
	#	for each in eComContent:  write to disk, upload to file handle, replace fh-id in tableData
	for (n in names(fileContent)) { # is the file path
		fileContentAsRObject<-fileContent[n]
		origFileHandleId<-names(vMap[which(vMap==n)])
		# upload a file and receive the file handle
		filePath<-tempfile()
		connection<-file(filePath)
		writeChar(toJSON(fileContentAsRObject), connection, eos=NULL)
		close(connection)  
		fileHandle<-synapseClient:::chunkedUploadFile(filePath)
		newFileHandleId<-fileHandle$id
		tableData[["momentInDayFormat.json"]][which(tableData[["momentInDayFormat.json"]]==origFileHandleId)]<-newFileHandleId
	}
	# create the schema
	columns<-list()
	for (column in schema1@columns@content) {
		column@id<-character(0)
		columns<-append(columns, column)
	}
	voiceSchema1<-TableSchema("Voice Raw Input 1", project, columns)
	voiceSchema1<-synStore(voiceSchema1)
	# create the v1 survey content
	voiceTable1<-Table(voiceSchema1, tableData)
	synStore(voiceTable1)
	vId1<-propertyValue(voiceSchema1, "id")
	
	# now create that other voice table
	columns<-list()
	for (column in schema2@columns@content) {
		column@id<-character(0)
		columns<-append(columns, column)
	}
	voiceSchema2<-TableSchema("Voice Raw Input 2", project, columns)
	voiceSchema2<-synStore(voiceSchema2)
	# create the table content
	voiceTable2<-Table(voiceSchema2, query2@values)
	synStore(voiceTable2)
	vId2<-propertyValue(voiceSchema2, "id")
	
	message("...done.")
	list(vId1=vId1, vId2=vId2)
}
#TODO reenable when data is available vResult<-createVoiceTaskTable1(project)
# TODO create copies of "syn4961457", "syn4961464" in addition to "syn4961455"
#TODO reenable when data is available vId1<-vResult$vId1
#TODO reenable when data is available vId2<-vResult$vId2

# TODO create copies of  "syn4961466", "syn4961469", in addition to "syn4961452"
#TODO reenable when data is available wId<-createTable(project, "walkingTaskInput.RData", "Walking Task Raw Input", "Creating Walking Task table ...")

# create 'lastProcessedVersion' table
createLastProcessedVersionTable<-function() {
	columns<-list(
			TableColumn(name="TABLE_ID", columnType="ENTITYID"), 
			TableColumn(name="LAST_VERSION", columnType="INTEGER"))
	schema<-TableSchema("Last Processed Table", project, columns)
	schema<-synStore(schema)
	# no content initially
	propertyValue(schema, "id")
}
lastProcessedVersionTableId <- createLastProcessedVersionTable()

# TODO set up bridgeStatusId, mPowerBatchStatusId

# TODO reenable when data is available process_mpower_data(eId, uId, pId, mId, tId, vId1, vId2, wId, 
# outputProjectId, bridgeStatusId, mPowerBatchStatusId, lastProcessedVersionTableId)

# TODO verify content

synDelete(project)


