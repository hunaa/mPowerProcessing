# End-to-end integration test
# 
# To run the environment variables SYNAPSE_USERNAME, SYNAPSE_APIKEY must be set.
# 
# Author: bhoff
###############################################################################
library(testthat)
library(synapseClient)

context("test_integration_process_mpower_data")


library(synapseClient)
library(bridger)

username<-Sys.getenv("SYNAPSE_USERNAME")
if (nchar(username)==0) {
	message("WARNING: Cannot run integration test.  Environment variable SYNAPSE_USERNAME is missing.")
	q("no")
}
apiKey<-Sys.getenv("SYNAPSE_APIKEY")
if (nchar(apiKey)==0) {
	message("WARNING: Cannot run integration test.  Environment variable SYNAPSE_APIKEY is missing.")
	q("no")
}

synapseLogin(username=username, apiKey=apiKey, rememberMe=F)

# TODO log in to Bridge
bridgeLogin(email='myEmail@awesome.com', password='password', study='parkinsons')

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
eId<-createSurveyV1Table(project)

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

uId<-createTable(project, "v2SurveyInput.RData", "Survey V2 Raw Input", "Creating Survey V2 table ...")

pId<-createTable(project, "v3SurveyInput.RData", "Survey V3 Raw Input", "Creating Survey V3 table ...")

mId<-createTable(project, "memTaskInput.RData", "Memory Task Raw Input", "Creating Memory Task table ...")

createMultipleTables<-function(project, rDataFileName, tableName, message) {
	message(message)
	inputFile<-file.path(testDataFolder, rDataFileName)
	load(inputFile) # brings into namespace: schemaAndQuery
	tableIds<-NULL
	for (i in 1:(dim(schemaAndQuery)[2])) {
		schema<-schemaAndQuery["schema", id][[1]]
		query<-schemaAndQuery["query", mPowerProcessing:::getIdFromSql(sql)][[1]]
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
		table<-synStore(table)
		tableIds<-append(tableIds, propertyValue(newSchema, "id"))
	}
	message("...done.")
	tableIds
}
tIds<-createMultipleTables(project, "tappingTaskInput.RData", "Tapping Task Raw Input", "Creating Tapping Task table ...")


createVoiceTaskTable1<-function(project) {
	message("Creating Voice Task table ...")
	voiceTaskInputFile<-file.path(testDataFolder, "voiceTaskInput.RData")
	load(voiceTaskInputFile) # brings into namespace: schemaAndQuery, schema2, query2, cumFileContent, cumVFiles
	tableData<-sapply(schemaAndQuery["query",], function(q) {q[[1]]@values})
	# create the file handles
	#	for each in cumFileContent:  write to disk, upload to file handle, replace fh-id in tableData
	for (n in names(cumFileContent)) { # is the file path
		fileContentAsRObject<-cumFileContent[n]
		origFileHandleId<-names(cumVFiles[which(cumVFiles==n)])
		# upload a file and receive the file handle
		filePath<-tempfile()
		connection<-file(filePath)
		writeChar(toJSON(fileContentAsRObject), connection, eos=NULL)
		close(connection)  
		fileHandle<-synapseClient:::chunkedUploadFile(filePath)
		newFileHandleId<-fileHandle$id
		for (i in 1:length(tableData)) {
			tableData[i][["momentInDayFormat.json"]][which(tableData[i][["momentInDayFormat.json"]]==origFileHandleId)]<-newFileHandleId
		}
	}
	vId1<-NULL
	for (i in 1:length(tableData)) {
		schema1<-schemaAndQuery["schema",i][[1]]
		# create the schema
		columns<-list()
		for (column in schema1@columns@content) {
			column@id<-character(0)
			columns<-append(columns, column)
		}
		voiceSchema1<-TableSchema(paste0("Voice Raw Input 1 ", i), project, columns)
		voiceSchema1<-synStore(voiceSchema1)
		# create the v1 survey content
		voiceTable1<-Table(voiceSchema1, tableData[i])
		synStore(voiceTable1)
		vId1<-append(vId1, propertyValue(voiceSchema1, "id"))
	}
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
vResult<-createVoiceTaskTable1(project)
vId1<-vResult$vId1
vId2<-vResult$vId2

wIds<-createMultipleTables(project, "walkingTaskInput.RData", "Walking Task Raw Input", "Creating Walking Task table ...")

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

# set up bridgeStatusId
column<-TableColumn(name="uploadDate", columnType="DATE")
bridgeStatusSchema<-TableSchema("Bridge Status Schema", project, list(column))
bridgeStatusSchema<-synStore(bridgeStatusSchema)
bridgeStatusId<-propertyValue(bridgeStatusSchema, "id")

# set up  mPowerBatchStatusId
c1<-TableColumn(name="bridgeUploadDate", columnType="DATE")
c2<-TableColumn(name="mPowerBatchStart", columnType="DATE")
c3<-TableColumn(name="hostName", columnType="STRING")
c4<-TableColumn(name="batchStatus", columnType="STRING")
mPowerBatchStatusSchema<-TableSchema("mPower Batch Status Schema", project, list(c1,c2,c3,c4))
mPowerBatchStatusSchema<-synStore(mPowerBatchStatusSchema)
mPowerBatchStatusId<-propertyValue(mPowerBatchStatusSchema, "id")

# TODO write a row into the bridgeStatusId table to kick off the job

process_mpower_data(eId, uId, pId, mId, tIds, vId1, vId2, wIds, 
	outputProjectId, bridgeStatusId, mPowerBatchStatusId, lastProcessedVersionTableId)

# TODO verify content

# TODO check that the batch has been marked 'complete'

synDelete(project)
message("Deleted project ", outputProjectId)


