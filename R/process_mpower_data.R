
bridgeUploadDateColumnName<-"bridgeUploadDate"
mPowerBatchStartColumnName<-"mPowerBatchStart"
hostNameColumnName<-"hostName"
batchStatusColumnName<-"batchStatus"
inProgressStatusValue<-"inProgress"

# check for new data to process:  
# The latest export is the last row in the 'parkinson-status' table.
# The 'mPowerBatchStatus' table tracks the processing of each batch:
# the first column is the bridgeUploadDate date, treated as an ID for the batch
# the second column is the date-time when processing began
# the third column is the ID of the host/server processing the batch
# the fourth column is the state of the processing (inProgress, completed, or failed)
#
# A host may start processing a batch if (1) there is no row in the mPowerBatchStatus
# table for the batch; or (2) there is a row, the processing is 'inProgress' but the  
# date-time has timed out, and 'expireLease' is TRUE
#
# returns: NULL (if no new batch) or a query result with one row, locked for processing
#
checkForAndLockBridgeExportBatch<-function(bridgeStatusId, mPowerBatchStatusId, hostname, now, leaseTimeOut) {
	bridgeStatusSql<-paste0("select uploadDate from ", bridgeStatusId, " ORDER BY uploadDate DESC LIMIT 1 OFFSET 0")
	bridgeStatusValues<-synTableQuery(bridgeStatusSql)@values
	if (nrow(bridgeStatusValues)==0) return(NULL) # an unusual case
	if (nrow(bridgeStatusValues)!=1) stop(paste0("Expected 0-1 rows but got ", nrow(bridgeStatusValues)))
	latestBridgeUploadDate<-bridgeStatusValues[1,1] # there's only one column in the result
	if (is(latestBridgeUploadDate, "character")) latestBridgeUploadDate<-as.Date(latestBridgeUploadDate)
	
	mPowerBatchSql<-paste0("select * from ", mPowerBatchStatusId, " where ",
			bridgeUploadDateColumnName, "='", latestBridgeUploadDate, "'")
	mPowerBatchStatusQueryResult<-synTableQuery(mPowerBatchSql)
	mPowerBatchStatusValues<-mPowerBatchStatusQueryResult@values
	if (nrow(mPowerBatchStatusValues)==0) {
		# if no row then we can 'lock it'.
		mPowerBatchStatusValues<-data.frame(
				latestBridgeUploadDate,
				now, 
				hostname,
				inProgressStatusValue,
				stringsAsFactors=F
				)
		names(mPowerBatchStatusValues)<-
				c(bridgeUploadDateColumnName, 
						mPowerBatchStartColumnName,
					hostNameColumnName, 
					batchStatusColumnName)
		statusTable<-Table(mPowerBatchStatusQueryResult@schema, mPowerBatchStatusValues)
		mPowerBatchStatusQueryResult<-synStore(statusTable, retrieveData=TRUE)
	} else if (nrow(mPowerBatchStatusValues)==1) {
		# if there IS a row, we can only process it if leaseTimeOut is specified AND
		# processing=InProgress AND the start time is too old
		if (!missing(leaseTimeOut) && mPowerBatchStatusValues[1,batchStatusColumnName]==inProgressStatusValue &&
				now-mPowerBatchStatusValues[1,mPowerBatchStartColumnName]>leaseTimeOut) {
			mPowerBatchStatusQueryResult@values[1,mPowerBatchStartColumnName]<-now
			mPowerBatchStatusQueryResult@values[1,hostNameColumnName]<-hostname
			mPowerBatchStatusQueryResult<-synStore(mPowerBatchStatusQueryResult, retrieveData=TRUE)
		} else {
			mPowerBatchStatusQueryResult<-NULL
		}
	} else {
		stop(paste0("Expected 0-1 rows but got ", nrow(mPowerBatchStatusValues)))
	}
	mPowerBatchStatusQueryResult
}

# This allows a unique host/worker name to be specified.  If just one
# copy of the job is running, then this isn't important and we
# can let it default to "UNKNOWN".
getHostname<-function() {
	result<-Sys.getenv("HOSTNAME")
	if (nchar(result)==0) result<-"UNKNOWN"
	result
}

markProcesingComplete<-function(batchStatusQueryResult, status) {
	if (nrow(batchStatusQueryResult@values)!=1) stop(paste0("Expected one row but found ", nrow(batchStatusQueryResult@values)))
	batchStatusQueryResult@values[1, batchStatusColumnName]<-status
	synStore(batchStatusQueryResult, retrieveData=TRUE)
}

getLastProcessedVersion<-function(df) {
  lastProcessedVersion<-df[["LAST_VERSION"]]
  names(lastProcessedVersion)<-df[["TABLE_ID"]]
  lastProcessedVersion
}

lastProcessVersionToDF<-function(lastProcessedVersion) {
	result<-data.frame(TABLE_ID=names(lastProcessedVersion), LAST_VERSION=lastProcessedVersion, stringsAsFactors=F)
	row.names(result)<-NULL
	result
}

# given a data frame having row values named according to the Synapse table convention
# find the maximum value
getMaxRowVersion<-function(df) {
  # TODO make this private method public
  max(synapseClient:::parseRowAndVersion(row.names(df))[2,])
}

process_mpower_data<-function(eId, uId, pId, mId, tId, vId1, vId2, wId, outputProjectId, 
		bridgeStatusId, mPowerBatchStatusId, lastProcessedVersionTableId) {
	# check if Bridge is done.  If not, exit
	hostname<-getHostname()
	leaseTimeout<-as.difftime("06:00:00") # not used at this time
	bridgeExportQueryResult<-checkForAndLockBridgeExportBatch(bridgeStatusId, mPowerBatchStatusId, hostname, Sys.time()) # no lease timeout given
	if (is.null(bridgeExportQueryResult) || nrow(bridgeExportQueryResult@values)==0) return(NULL)
	
	tryCatch({
				process_mpower_data_bare(eId, uId, pId, mId, tId, vId1, vId2, wId, outputProjectId, 
						bridgeStatusId, mPowerBatchStatusId, lastProcessedVersionTableId)
				markProcesingComplete(bridgeExportQueryResult, "complete")
			}, 
			error=function(e) {
				message(e)
				markProcesingComplete(bridgeExportQueryResult, "failed")
			})
}			
				
process_mpower_data_bare<-function(eId, uId, pId, mId, tId, vId1, vId2, wId, outputProjectId, 
		bridgeStatusId, mPowerBatchStatusId, lastProcessedVersionTableId) {
	lastProcessedQueryResult<-synTableQuery(paste0("SELECT * FROM ", lastProcessedVersionTableId))
	lastProcessedVersion<-getLastProcessedVersion(lastProcessedQueryResult@values)
	
	######
	# Data Cleaning
	######
	cat("Processing survey v1...\n")
	eDatResult<-process_survey_v1(eId, lastProcessedVersion[eId])
	eDat<-eDatResult$eDat
	lastProcessedVersion[eId]<-eDatResult$maxRowVersion
	cat("... done.  # rows: ", nrow(eDat), ", max row version: ", eDatResult$maxRowVersion, "\n")
	
	cat("Processing survey v2...\n")
	uDatResult<-process_survey_v2(uId, lastProcessedVersion[uId])
	uDat<-uDatResult$uDat
	lastProcessedVersion[uId]<-uDatResult$maxRowVersion
	cat("... done.  # rows: ", nrow(uDat), ", max row version: ", uDatResult$maxRowVersion, "\n")
	
	cat("Processing survey v3...\n")
	pDatResult<-process_survey_v3(pId, lastProcessedVersion[pId])
	pDat<-pDatResult$pDat
	lastProcessedVersion[pId]<-pDatResult$maxRowVersion
	cat("... done.  # rows: ", nrow(pDat), ", max row version: ", pDatResult$maxRowVersion, "\n")
	
	cat("Processing memory activity...\n")
	mResults<-process_memory_activity(mId, lastProcessedVersion[mId])
	mDat<-mResults$mDat
	mFilehandleCols<-mResults$mFilehandleCols
	lastProcessedVersion[mId]<-mResults$maxRowVersion
	cat("... done.  # rows: ", nrow(mDat), ", max row version: ", mResults$maxRowVersion, "\n")
	
	cat("Processing tapping activity...\n")
	tResults<-process_tapping_activity(tId, lastProcessedVersion[tId])
	tDat<-tResults$tDat
	tFilehandleCols<-tResults$tFilehandleCols
	cat("... done.  # rows: ", nrow(tDat))
	for (id in names(tResults$maxRowProcessed)) {
		lastProcessedVersion[id]<-tResults$maxRowProcessed[[id]]
		cat(", max row version for ", id, ": ", tResults$maxRowProcessed[[id]])
	}
	cat("\n")
	
	cat("Processing voice activity...\n")
	vResults<-process_voice_activity(vId1, vId2, lastProcessedVersion[vId1], lastProcessedVersion[vId2])
	vDat<-vResults$vDat
	vFilehandleCols<-vResults$vFilehandleCols
	cat("... done.  # rows: ", nrow(vDat))
	for (id in names(vResults$maxRowProcessed)) {
		lastProcessedVersion[id]<-vResults$maxRowProcessed[[id]]
		cat(", max row version for ", id, ": ", vResults$maxRowProcessed[[id]])
	}
	cat("\n")
	
	cat("Processing walking activity...\n")
	wResults<-process_walking_activity(wId, lastProcessedVersion[wId])
	wDat<-wResults$wDat
	cat("... done.  # rows: ", nrow(wDat))
	for (id in names(wResults$maxRowProcessed)) {
		lastProcessedVersion[id]<-wResults$maxRowProcessed[[id]]
		cat(", max row version for ", id, ": ", wResults$maxRowProcessed[[id]])
	}
	cat("\n")
	
	cat("Cleaning up missing med data...\n")
	clean_up_result<-cleanup_missing_med_data(mDat, tDat, vDat, wDat)
	mDat<-clean_up_result$mDat
	tDat<-clean_up_result$tDat
	vDat<-clean_up_result$vDat
	wDat<-clean_up_result$wDat
	cat("... done.\n")
	
	cat("Storing cleaned data...\n")
	store_cleaned_data(outputProjectId, eDat, uDat, pDat, mDat, tDat, vDat, wDat, mFilehandleCols, tFilehandleCols, vFilehandleCols)
	cat("... done.\n")
	
	# **** other steps go here ****
	
	# Now call the Visualization Data API 
	#https://sagebionetworks.jira.com/wiki/display/BRIDGE/mPower+Visualization#mPowerVisualization-WritemPowerVisualizationData
	cat("Invoking visualization API...\n")
	# place holder
	content<-list(
		"healthCode"="test-d9c31718-481f-4d75-b7d8-49154653504a",
		"date"="2016-03-04",
		"visualization"=list(
			"standingPreMedication"=0.8,
			"standingPostMedication"=0.9,
			"tappingPreMedication"=0.4,
			"tappingPostMedication"=0.6,
			"voicePreMedication"=0.7,
			"voicePostMedication"=0.8,
			"walkingPreMedication"=0.5,
			"walkingPostMedication"=0.8
		)
	)			
	url <- bridger:::uriToUrl("/parkinson/visualization", bridger:::.getBridgeCache("bridgeEndpoint"))
	response<-getURL(url, postfields=toJSON(content), customrequest="POST", 
			.opts=bridger:::.getBridgeCache("opts"), httpheader=bridger:::.getBridgeCache("httpheader"))
	# response is "Visualization created."
	cat("... done.\n")

	cat("Wrapping up...\n")
	# update the last processed version
	lastProcessedQueryResult@values<-lastProcessVersionToDF(lastProcessedVersion)
	synStore(lastProcessedQueryResult)
	
	cat("... ALL DONE!!!\n")
}
