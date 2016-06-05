# End-to-end integration test
# 
# To run the environment variables SYNAPSE_USERNAME, SYNAPSE_APIKEY must be set.
# 
# Author: bhoff
###############################################################################
library(testthat)
library(synapseClient)
library(mPowerProcessing)
library(bridger)
library(RJSONIO)
library(RCurl)

context("test_integration_process_mpower_data")

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
	
	message("Creating feature tables...")
	featureTableIds<-createFeatureTables(outputProjectId)
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

	## A specialized version of createTable/createMultipleTables that uploads binary voice data files
	createVoiceActivityTables <- function(project, tables, tableName, mockAttachmentFolders) {

		## return new table IDs
		tableIds <- NULL

		for (i in seq(along=tables)) {
			newTableName <- paste0(tableName, " ", i)
			message("Creating ", newTableName, "...")

			schema    <- tables[[i]]$schema
			tableData <- tables[[i]]$query@values

			## upload mock data files and attach to file handle columns
			fileHandleColumns<-whichFilehandle(schema@columns)
			for (fhc in fileHandleColumns) {
				mockFileFolder <- system.file("testdata", mockAttachmentFolders[fhc], package="mPowerProcessing")
				mockFiles <- list.files(mockFileFolder, full.names=TRUE)
				newFileHandleIds<-NULL
				for (filePath in mockFiles) {
					fileHandle <- synapseClient:::chunkedUploadFile(filePath)
					newFileHandleIds<-append(newFileHandleIds, fileHandle$id)
				}
				j <- which(!is.na(tableData[[fhc]]))
				tableData[[fhc]][j] <- sample(newFileHandleIds, size=length(j), replace=TRUE)
			}

			## create new table schema
			newSchema <- TableSchema(newTableName, project, schema@columns@content)
			newSchema <- synStore(newSchema)

			# create table content
			table <- synStore(Table(newSchema, tableData))

			## add ID of newly created table to list
			tableIds <- append(tableIds, propertyValue(newSchema, "id"))
		}

		tableIds
	}

	## Given a schemaAndQuery data.frame, convert it into a list.
	##
	## schemaAndQuery data.frames look like this:
	##
	##        syn4961455 syn4961457 syn4961464
	## schema tdf1       tdf2       tdf3
	## query  ts1        ts2        ts3
	##
	## tdf = An object of class "TableDataFrame"
	## ts  = An object of class "TableSchema"
	##
	## The results of schemaAndQueryToList(schemaAndQuery) look like:
	##
	## $syn4961455$schema = tdf1
	## $syn4961455$query  = ts1
	##
	## $syn4961457$schema = tdf2
	## $syn4961457$query  = ts2
	##
	## $syn4961464$schema = tdf3
	## $syn4961464$query  = ts3
	##
	schemaAndQueryToList <- function(schemaAndQuery) {
		tables <- list()
		for (j in seq(length.out=ncol(schemaAndQuery))) {
			tables[[j]] <- list()
			tables[[j]]['schema'] <- schemaAndQuery[[1,j]]
			tables[[j]]['query']  <- schemaAndQuery[[2,j]]
		}
		names(tables) <- colnames(schemaAndQuery)
		return(tables)
	}

	# maps column name in Table to the folder name where mock files are found
	tappingAttachments<-c(
	"accel_tapping.json.items"="default-json-files",
	"accelerometer_tapping.items"="default-json-files",
	"tapping_results.json.TappingSamples"="tapping_results.json.TappingSamples")
	tIds<-createMultipleTablesFromRDataFile(project, "tappingTaskInput.RData", "Tapping Task Raw Input", tappingAttachments)
	
	tlrId<-createMultipleTablesFromRDataFile(project, "tappingLeftrightTaskInput.RData", "Tapping Left Right Task Raw Input",
	                                         c("accel_tapping_right.json.items"="default-json-files",
	                                           "accel_tapping_left.json.items"="default-json-files",
	                                           "tapping_left.json.TappingSamples"="tapping_results.json.TappingSamples",
	                                           "tapping_right.json.TappingSamples"="tapping_results.json.TappingSamples"))
	
	voiceTaskInputFile<-file.path(testDataFolder, "voiceTaskInput.RData")
	load(voiceTaskInputFile) # brings into namespace: schemaAndQuery, schema2, query2, fileContent, vFiles


	# ------------------------------------------------------------
	#   Create mock voice tables
	# ------------------------------------------------------------
	v1MockAttachmentFolders<-c(
			"audio_audio.m4a"="audio_audio.m4a",
			"audio_countdown.m4a"="default-json-files",
			"momentInDayFormat.json"="moment-in-day-json")
	tables <- schemaAndQueryToList(schemaAndQuery)
	vId1 <- createVoiceActivityTables(project, tables, "Voice Raw Input 1", v1MockAttachmentFolders)
	message("vId1: ", paste(vId1, collapse=" "))

	v2MockAttachmentFolders <- c(	"audio_audio.m4a"="audio_audio.m4a",
																"audio_countdown.m4a"="default-json-files")
	tables <- list(list(schema=schema2, query=query2))
	vId2 <- createVoiceActivityTables(project, tables, "Voice Raw Input 2", v2MockAttachmentFolders)
	message("vId2: ", paste(vId2, collapse=" "))
	# ------------------------------------------------------------

	walkingAttachments<-c("accel_walking_outbound.json.items"="default-json-files",      
		"deviceMotion_walking_outbound.json.items"="deviceMotion_walking_outbound.json.items",
		"pedometer_walking_outbound.json.items"="default-json-files",
		"pedometer_walking.outbound.items"="default-json-files",
		"accel_walking_return.json.items"="default-json-files",
		"deviceMotion_walking_return.json.items"="default-json-files",
		"pedometer_walking_return.json.items"="default-json-files",
		"accel_walking_rest.json.items"="default-json-files",    
		"accelerometer_walking.rest.items"="default-json-files",    
		"deviceMotion_walking_rest.json.items"="deviceMotion_walking_rest.json.items", 
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
	
	lastProcessedFeatureVersionTableId <- createLastProcessedFeatureVersionTable(propertyValue(project, "id"))
	
	# set up bridgeStatusId
	column<-TableColumn(name="uploadDate", columnType="STRING")
	bridgeStatusSchema<-TableSchema("Bridge Status Schema", project, list(column))
	bridgeStatusSchema<-synStore(bridgeStatusSchema)
	bridgeStatusId<-propertyValue(bridgeStatusSchema, "id")
	
	# set up mPowerBatchStatusId
	c1<-TableColumn(name="bridgeUploadDate", columnType="STRING")
	c2<-TableColumn(name="mPowerBatchStart", columnType="DATE")
	c3<-TableColumn(name="hostName", columnType="STRING")
	c4<-TableColumn(name="batchStatus", columnType="STRING")
	c5<-TableColumn(name="reportsSentCount", columnType="INTEGER")
	mPowerBatchStatusSchema<-TableSchema("mPower Batch Status Schema", project, list(c1,c2,c3,c4,c5))
	mPowerBatchStatusSchema<-synStore(mPowerBatchStatusSchema)
	mPowerBatchStatusId<-propertyValue(mPowerBatchStatusSchema, "id")
	
	# to test MPOW-14, part (3) write an extra row to the mPowerBatchStatus table
	mPowerBatchStatusValues<-data.frame(Sys.time()-24*3600, Sys.time()-24*3600, "test", "complete", stringsAsFactors=F)
	names(mPowerBatchStatusValues)<-c("bridgeUploadDate", "mPowerBatchStart",	"hostName", "batchStatus")
	statusTable<-Table(mPowerBatchStatusSchema, mPowerBatchStatusValues)
	synStore(statusTable)

	# write a row into the bridgeStatusId table to kick off the job
	trigger<-Table( bridgeStatusId, data.frame(uploadDate=as.character(Sys.Date())) )
	trigger<-synStore(trigger)
	
	bridgeExportQueryResult<-checkForAndLockBridgeExportBatch(bridgeStatusId, mPowerBatchStatusId, "", Sys.time())
	expect_true(!(is.null(bridgeExportQueryResult) || nrow(bridgeExportQueryResult@values)==0))
		
	reportsSentCount<-
		process_mpower_data_bare(eId, uId, pId, mId, tIds, tlrId, vId1, vId2, wIds, outputProjectId, 
			featureTableIds$tfSchemaId, featureTableIds$tlfSchemaId, featureTableIds$trfSchemaId, featureTableIds$vfSchemaId, featureTableIds$bfSchemaId, featureTableIds$gfSchemaId, 
			bridgeStatusId, mPowerBatchStatusId, lastProcessedVersionTableId, lastProcessedFeatureVersionTableId)
	markProcesingComplete(bridgeExportQueryResult, "complete", reportsSentCount)
	
	lastProcessedVersion<-synTableQuery(sprintf("select * from %s", lastProcessedVersionTableId))@values
	expect_equal(nrow(lastProcessedVersion), 15)
	
	tappingFeatures<-synTableQuery(sprintf("select * from %s", featureTableIds$tfSchemaId))@values
	# verify content
	expect_true(all(tappingFeatures[['is_computed']]==TRUE))
	expect_true(all(tappingFeatures[['tap_count']]==as.integer(155)))
	
	tappingLeftFeatures<-synTableQuery(sprintf("select * from %s", featureTableIds$tlfSchemaId))@values
	# verify content
	expect_true(all(tappingLeftFeatures[['is_computed']]==TRUE))
	expect_true(all(tappingLeftFeatures[['tap_count']]==as.integer(155)))
	
	tappingRightFeatures<-synTableQuery(sprintf("select * from %s", featureTableIds$trfSchemaId))@values
	# verify content
	expect_true(all(tappingRightFeatures[['is_computed']]==TRUE))
	expect_true(all(tappingRightFeatures[['tap_count']]==as.integer(155)))

	voiceFeatures<-synTableQuery(sprintf("select * from %s", featureTableIds$vfSchemaId))@values
	message("Verifying voice features:")
	medianF0s <- sort(unique(voiceFeatures$medianF0), na.last=TRUE)
	message("found medianF0s = ", paste(medianF0s, collapse=", "))
	expected_medianF0s <- c(100, 107, 178)
	expect_equal(medianF0s, expected_medianF0s, tolerance=1.0)

	gaitFeatures<-synTableQuery(sprintf("select * from %s", featureTableIds$gfSchemaId))@values
	# TODO verify content
	balanceFeatures<-synTableQuery(sprintf("select * from %s", featureTableIds$bfSchemaId))@values
	# TODO verify content
	
	# check that the batch has been marked 'complete'
	jobStatus<-synTableQuery(paste0("select * from ", mPowerBatchStatusId,
					" where bridgeUploadDate='", as.character(Sys.Date()), "'"))
	expect_equal(nrow(jobStatus@values), 1)
	expect_equal(jobStatus@values[1,"batchStatus"], "complete")
	
	synDelete(project)
	message("Deleted project ", outputProjectId)
}

