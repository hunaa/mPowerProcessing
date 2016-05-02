# integration test for parallel processing voice data
# 
# Author: bhoff
###############################################################################
library(testthat)
library(synapseClient)
library(mPowerProcessing)
library(RJSONIO)

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


if (canExecute) {
	tryCatch({
		# log in to Synapse
		synapseLogin(username=username, apiKey=apiKey, rememberMe=F)
		
		message("\nCreating project ...")
		project<-Project()
		project<-synStore(project)
		outputProjectId<-propertyValue(project, "id")
		message("...done.  Project ID is ", outputProjectId)
		
		message("Creating  tables...")
		voiceInputTableId<-propertyValue(synStore(voiceActivitySchema(outputProjectId)), "id")
		voiceFeatureTableId<-propertyValue(synStore(voiceFeatureSchema(outputProjectId)), "id")
		voiceBatchTableId<-createVoiceBatchTable(outputProjectId)
		message("...done.")
		
		recordCount<-6
		
		# populate recordId and mock audio file in voiceInputTableId
		for (i in 1:recordCount) {
			# upload a file and receive the file handle
			filePath<-tempfile()
			connection<-file(filePath)
			writeChar(toJSON(list(medianF0=i)), connection, eos=NULL)
			close(connection)  
			fileHandle<-synapseClient:::chunkedUploadFile(filePath)
			if (i==1) {
				df<-data.frame(recordId=as.character(i), audio_audio.m4a=fileHandle$id, stringsAsFactors=FALSE)
			} else {
				df<-rbind(df, list(recordId=as.character(i), `audio_audio.m4a`=fileHandle$id))
			}
		}
		synStore(Table(voiceInputTableId, df))
		
		batchSize<-2
		
		with_mock(
				computeMedianF0=function(file){
					contents<-fromJSON(file)
					return(contents['medianF0'])
				},
				batchVoiceProcess(voiceInputTableId, voiceFeatureTableId, voiceBatchTableId, batchSize)
		)
		
		features<-synTableQuery(paste0("SELECT * FROM ", voiceFeatureTableId))
		
		expect_equal(2, nrow(features@values))
		# the dummy 'medianF0' function returns a value equals to the recordId
		expect_equal(features@values$recordId, as.character(features@values$medianF0))
		expect_equal(features@values$is_computed, rep(TRUE, 2))
		
		# now do it again!
		with_mock(
				computeMedianF0=function(file){
					contents<-fromJSON(file)
					return(contents['medianF0'])
				},
				batchVoiceProcess(voiceInputTableId, voiceFeatureTableId, voiceBatchTableId, batchSize)
		)

		features<-synTableQuery(paste0("SELECT * FROM ", voiceFeatureTableId))
		
		expect_equal(4, nrow(features@values))
		# the dummy 'medianF0' function returns a value equals to the recordId
		expect_equal(features@values$recordId, as.character(features@values$medianF0))
		expect_equal(features@values$is_computed, rep(TRUE, 4))
		
		# second batch should be different from first
		expect_equal(features@values$recordId, unique(features@values$recordId))
	
	},
	finally={
		synDelete(project)
		message("Deleted project ", outputProjectId)
	})
}

