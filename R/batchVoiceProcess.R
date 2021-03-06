# Process a batch of voice data
# return TRUE iff all batches have been processed
# 
# Author: bhoff
###############################################################################


batchVoiceProcess<-function(voiceInputTableId, voiceFeatureTableId, batchTableId, batchSize, hostName="UNK") {
	recordCountQuery<-synTableQuery(paste0("SELECT COUNT(*) FROM ", voiceInputTableId))
	recordCount <- recordCountQuery@values[1,1]
	
	# clean up any 'stale' batches
	batchTimeOutEpochMillis<-as.numeric(Sys.time()-as.difftime(2, units="hours"))*1000
	staleBatchQueryResult<-synTableQuery(paste0("SELECT batchNumber, batchStart, hostName, batchStatus FROM ", 
					batchTableId, " where batchStatus='PROCESSING' and batchStart<",  batchTimeOutEpochMillis))
	if (nrow(staleBatchQueryResult@values)>0) synDeleteRows(staleBatchQueryResult)
	
	for (i in 1:10) { # try the following 10 times, retrying if there is a concurrency error
		# find a batch
		batchQueryResult<-synTableQuery(paste0("SELECT batchNumber, batchStart, hostName, batchStatus FROM ", batchTableId))
		processedBatches<-batchQueryResult@values$batchNumber
		totalBatches<-(1:(ceiling(recordCount/batchSize)))
		if (length(processedBatches)==0) {
			availableBatches<-totalBatches
		} else {
			availableBatches<-totalBatches[-processedBatches]
		}
		if (length(availableBatches)==0) {
			print("All batches have been (or are being) processed!")
			return(TRUE)
		}
		# the next batch
		batchToProcess<-min(availableBatches)
		# lock it
		batchQueryResult@values<-data.frame(batchNumber=batchToProcess, batchStart=Sys.time(), 
				hostName=hostName, batchStatus="PROCESSING", stringsAsFactors=FALSE)
		synStoreResult<-try(synStore(batchQueryResult))
		if (!is(synStoreResult, "try-error")) break
		print(synStoreResult[[1]])
		
	}
	if (is(synStoreResult, "try-error")) stop("Failed to select a batch to process.")
	cat("batchVoiceProcess: batchToProcess: ", batchToProcess, "\n")
	# process the batch from batchSize*(batchToProcess-1) to batchSize*batchToProcess-1
	dataColName<-"audio_audio.m4a"
	recordIdIndex<-1 # i.e. the record IDs are in the first column of the query result
  audioIndex<-2 # i.e. the audio file handles are in the second column of the query result
	batchQueryString<-paste0('SELECT "recordId", "', dataColName, '" FROM ', 
			voiceInputTableId, ' LIMIT ', batchSize, 
			' OFFSET ', as.integer(batchSize*(batchToProcess-1)))
	voiceBatch<-synTableQuery(batchQueryString)
	recordIds<-voiceBatch@values[[recordIdIndex]]
	n<-length(recordIds)
	
	# create a new data frame to hold the computed feature(s)
	featureDataFrame<-data.frame(
			recordId=recordIds, 
			"is_computed"=rep(FALSE, n), 
			"medianF0"=rep(NA, n),
			stringsAsFactors=FALSE)
	
	# now compute the features
	dataFiles<-synDownloadTableColumns(voiceBatch, names(voiceBatch@values[audioIndex]))
	for (i in 1:n) {
		fileHandleId<-voiceBatch@values[i,audioIndex]
		if (length(fileHandleId)==0 || is.na(fileHandleId) || is.null(fileHandleId)) next
		medianF0<-try({
					file<-dataFiles[[fileHandleId]]
					computeMedianF0(file)
				}, silent=T)
		if (is(medianF0, "try-error")) {
			cat("batchVoiceProcess:  medianF0 failed for i=", i, ", fileHandleId=", fileHandleId, ".  Error is ", medianF0[[1]], "\n")
		} else {
			featureDataFrame[i,"medianF0"] <- medianF0
			featureDataFrame[i,"is_computed"] <- TRUE
		}
	}
	# upload the results
	voiceFeatures<-Table(voiceFeatureTableId, featureDataFrame)
	synStore(voiceFeatures)
	# mark the batch as complete
	batchQueryResult<-synTableQuery(paste0("SELECT * FROM ", batchTableId, " WHERE batchNumber=", batchToProcess))
	if (nrow(batchQueryResult@values)!=1) stop("Expected 1 row for batch ", batchToProcess, " but found ", nrow(batchQueryResult@values))
	batchQueryResult@values[1, 'batchStatus']<-"COMPLETED"
	synStore(batchQueryResult)
	return(FALSE)
}


computeMedianF0<-function(file) {
	medianF0(convert_to_wav(file))
}
