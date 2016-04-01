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
library(RCurl)

canExecute<-TRUE

username<-Sys.getenv("SYNAPSE_USERNAME")
if (nchar(username)==0) {
	message("WARNING: Cannot run integration test.  Environment variable SYNAPSE_USERNAME is missing.")
	canExecute<-FALSE
}
apiKey<-Sys.getenv("SYNAPSE_APIKEY")
if (nchar(apiKey)==0) {
	message("WARNING: Cannot run integration test.  Environment variable SYNAPSE_APIKEY is missing.")
	canExecute<-FALSE
}
bridgeUsername<-Sys.getenv("BRIDGE_USERNAME")
if (nchar(bridgeUsername)==0) {
	message("WARNING: Cannot run integration test.  Environment variable BRIDGE_USERNAME is missing.")
	canExecute<-FALSE
}
bridgePassword<-Sys.getenv("BRIDGE_PASSWORD")
if (nchar(bridgePassword)==0) {
	message("WARNING: Cannot run integration test.  Environment variable BRIDGE_PASSWORD is missing.")
	canExecute<-FALSE
}

if (canExecute) {
	# log in to Synapse
	synapseLogin(username=username, apiKey=apiKey, rememberMe=F)
	
	# log in to Bridge
	bridgeLogin(email=bridgeUsername, password=bridgePassword, study='parkinson')
	
	message("\nCreating project ...")
	project<-Project()
	project<-synStore(project)
	outputProjectId<-propertyValue(project, "id")
	message("...done.  Project ID is ", outputProjectId)
	
	message("Creating output tables...")
	createOutputTables(outputProjectId)
	message("...done.")
	
	## create and populate the source tables
	testDataFolder<-system.file("testdata", package="mPowerProcessing")
	
	createTable<-function(project, schema, query, tableName, mockAttachmentFolders) {
		message("Creating ", tableName, "...")
		tableData<-query@values
		fileHandleColumns<-whichFilehandle(schema@columns)
		for (fhc in fileHandleColumns) {
			mockFiles<-mockFileAttachments(system.file(file.path("testdata", mockAttachmentFolders[fhc]), package="mPowerProcessing"))
			fileContent<-mockFiles$fileContent
			newFileHandleIds<-NULL
			for (origFileHandleId in names(mockFiles$mockFiles)) {
				origFilePath<-mockFiles$mockFiles[origFileHandleId]
				# upload a file and receive the file handle
				filePath<-tempfile()
				connection<-file(filePath)
				writeChar(fileContent[origFilePath], connection, eos=NULL)
				close(connection)  
				fileHandle<-synapseClient:::chunkedUploadFile(filePath)
				newFileHandleIds<-append(newFileHandleIds, fileHandle$id)
			}
			tableData[[fhc]][which(!is.na(tableData[[fhc]]))]<-
					sample(newFileHandleIds, size=length(which(!is.na(tableData[[fhc]]))), replace=T)
		}
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
	
	createTableFromRDataFile<-function(project, rDataFileName, tableName, mockAttachmentFolders) {
		inputFile<-file.path(testDataFolder, rDataFileName)
		load(inputFile) # brings into namespace: schema, query
		createTable(project, schema, query, tableName, mockAttachmentFolders)
	}
	
	eId<-createTableFromRDataFile(project, "v1SurveyInput.RData", "Survey V1 Raw Input", 
			c("health-history"="health-history"))
	
	uId<-createTableFromRDataFile(project, "v2SurveyInput.RData", "Survey V2 Raw Input")
	
	pId<-createTableFromRDataFile(project, "v3SurveyInput.RData", "Survey V3 Raw Input")
	
	mId<-createTableFromRDataFile(project, "memTaskInput.RData", "Memory Task Raw Input", 
			c("MemoryGameResults.json.MemoryGameGameRecords"="default-json-files"))
	
	createMultipleTables<-function(project, schemaAndQuery, tableName, mockAttachmentFolders) {
		message("Creating ", tableName, "...")
		tableIds<-NULL
		for (i in 1:(dim(schemaAndQuery)[2])) {
			schema<-schemaAndQuery["schema", i][[1]]
			query<-schemaAndQuery["query", i][[1]]
			tableData<-query@values
			fileHandleColumns<-whichFilehandle(schema@columns)
			for (fhc in fileHandleColumns) {
				mockFiles<-mockFileAttachments(system.file(file.path("testdata", mockAttachmentFolders[fhc]), package="mPowerProcessing"))
				fileContent<-mockFiles$fileContent
				newFileHandleIds<-NULL
				for (origFileHandleId in names(mockFiles$mockFiles)) {
					origFilePath<-mockFiles$mockFiles[origFileHandleId]
					# upload a file and receive the file handle
					filePath<-tempfile()
					connection<-file(filePath)
					writeChar(fileContent[origFilePath], connection, eos=NULL)
					close(connection)  
					fileHandle<-synapseClient:::chunkedUploadFile(filePath)
					newFileHandleIds<-append(newFileHandleIds, fileHandle$id)
				}
				tableData[[fhc]][which(!is.na(tableData[[fhc]]))]<-
						sample(newFileHandleIds, size=length(which(!is.na(tableData[[fhc]]))), replace=T)
			}
			columns<-list()
			for (column in schema@columns@content) {
				column@id<-character(0)
				columns<-append(columns, column)
			}
			newSchema<-TableSchema(paste0(tableName, " ", i), project, columns)
			newSchema<-synStore(newSchema)
			# create the table content
			table<-Table(newSchema, tableData)
			table<-synStore(table)
			tableIds<-append(tableIds, propertyValue(newSchema, "id"))
		}
		message("...done.")
		tableIds
	}
	
	createMultipleTablesFromRDataFile<-function(project, rDataFileName, tableName, mockAttachmentFolders) {
		inputFile<-file.path(testDataFolder, rDataFileName)
		load(inputFile) # brings into namespace: schemaAndQuery
		createMultipleTables(project, schemaAndQuery, tableName, mockAttachmentFolders)
	}
	
	# maps column name in Table to the folder name where mock files are found
	tappingAttachments<-c(
	"accel_tapping.json.items"="default-json-files",
	"accelerometer_tapping.items"="default-json-files",
	"tapping_results.json.TappingSamples"="default-json-files")
	tIds<-createMultipleTablesFromRDataFile(project, "tappingTaskInput.RData", "Tapping Task Raw Input", tappingAttachments)
	
	voiceTaskInputFile<-file.path(testDataFolder, "voiceTaskInput.RData")
	load(voiceTaskInputFile) # brings into namespace: schemaAndQuery, schema2, query2, fileContent, vFiles
	v1MockAttachmentFolders<-c(
			"audio_audio.m4a"="default-json-files",
			"audio_countdown.m4a"="default-json-files",  
			"momentInDayFormat.json"="moment-in-day-json"
			)
	vId1<-createMultipleTables(project, schemaAndQuery, "Voice Raw Input 1", v1MockAttachmentFolders)
	v2MockAttachmentFolders<-c(
			"audio_audio.m4a"="default-json-files",
			"audio_countdown.m4a"="default-json-files")
	vId2<-createTable(project, schema2, query2, "Voice Raw Input 2", v2MockAttachmentFolders)
	
	walkingAttachments<-c("accel_walking_outbound.json.items"="default-json-files",      
		"deviceMotion_walking_outbound.json.items"="default-json-files",
		"pedometer_walking_outbound.json.items"="default-json-files",
		"pedometer_walking.outbound.items"="default-json-files",
		"accel_walking_return.json.items"="default-json-files",
		"deviceMotion_walking_return.json.items"="default-json-files",
		"pedometer_walking_return.json.items"="default-json-files",
		"accel_walking_rest.json.items"="default-json-files",    
		"accelerometer_walking.rest.items"="default-json-files",    
		"deviceMotion_walking_rest.json.items"="default-json-files", 
		"deviceMotion_walking.rest.items"="default-json-files")
	wIds<-createMultipleTablesFromRDataFile(project, "walkingTaskInput.RData", "Walking Task Raw Input", walkingAttachments)
	
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
	
	# to test MPOW-14, part (3) write an extra row to the mPowerBatchStatus table
	mPowerBatchStatusValues<-data.frame(Sys.time()-24*3600, Sys.time()-24*3600, "test", "complete", stringsAsFactors=F)
	names(mPowerBatchStatusValues)<-c("bridgeUploadDate", "mPowerBatchStart",	"hostName", "batchStatus")
	statusTable<-Table(mPowerBatchStatusSchema, mPowerBatchStatusValues)
	synStore(statusTable)

	# write a row into the bridgeStatusId table to kick off the job
	trigger<-Table( bridgeStatusId, data.frame(uploadDate=Sys.time()) )
	trigger<-synStore(trigger)
	
	bridgeExportQueryResult<-checkForAndLockBridgeExportBatch(bridgeStatusId, mPowerBatchStatusId, "", Sys.time())
	expect_true(!(is.null(bridgeExportQueryResult) || nrow(bridgeExportQueryResult@values)==0))
		
	process_mpower_data_bare(eId, uId, pId, mId, tIds, vId1, vId2, wIds, outputProjectId, 
						bridgeStatusId, mPowerBatchStatusId, lastProcessedVersionTableId)
	markProcesingComplete(bridgeExportQueryResult, "complete")
	
	# TODO verify content
	
	# check that the batch has been marked 'complete'
	jobStatus<-synTableQuery(paste0("select * from ", mPowerBatchStatusId))
	expect_equal(nrow(jobStatus@values), 1)
	expect_equal(jobStatus@values[1,"batchStatus"], "complete")
	
	synDelete(project)
	message("Deleted project ", outputProjectId)
}

