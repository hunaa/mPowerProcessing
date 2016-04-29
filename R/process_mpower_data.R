
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
		synStore(statusTable)
		mPowerBatchStatusQueryResult<-synTableQuery(sprintf("select * from %s where %s='%s'",
						mPowerBatchStatusId, bridgeUploadDateColumnName, latestBridgeUploadDate))
	} else if (nrow(mPowerBatchStatusValues)==1) {
		# if there IS a row, we can only process it if leaseTimeOut is specified AND
		# processing=InProgress AND the start time is too old
		if (!missing(leaseTimeOut) && mPowerBatchStatusValues[1,batchStatusColumnName]==inProgressStatusValue &&
				now-mPowerBatchStatusValues[1,mPowerBatchStartColumnName]>leaseTimeOut) {
			mPowerBatchStatusQueryResult@values[1,mPowerBatchStartColumnName]<-now
			mPowerBatchStatusQueryResult@values[1,hostNameColumnName]<-hostname
			synStore(mPowerBatchStatusQueryResult)
			mPowerBatchStatusQueryResult<-synTableQuery(sprintf("select * from %s where %s='%s'",
							mPowerBatchStatusId, bridgeUploadDateColumnName, latestBridgeUploadDate))
		} else {
			mPowerBatchStatusQueryResult<-NULL
		}
	} else {
		stop(paste0("Expected 0-1 rows but got ", nrow(mPowerBatchStatusValues)))
	}
	if (!is.null(mPowerBatchStatusQueryResult) && nrow(mPowerBatchStatusQueryResult@values)!=1)
		stop("Problem 'locking' the mPower batch for processing.  Expected one row for the batch in the status table but found ", nrow(mPowerBatchStatusQueryResult@values), ".")
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

mergeLastProcessVersionIntoToDF<-function(lastProcessedVersion, df) {
	mergeDataFrames(df, lastProcessVersionToDF(lastProcessedVersion), "TABLE_ID")
}

# the the last row version of the given cleaned data table for which the given feature was computed
lastProcessedFeatureVersion<-function(lastProcessedFeatureVersionTableId, cleanedTableId, featureName) {
	queryResult<-synTableQuery(paste0("SELECT * FROM ", 
					lastProcessedFeatureVersionTableId, " WHERE TABLE_ID='", 
					cleanedTableId, "' AND FEATURE='", featureName, "'"))
	if (nrow(queryResult@values)==0) {
		queryResult@values[1,"TABLE_ID"]<-cleanedTableId
		queryResult@values[1,"FEATURE"]<-featureName
		queryResult@values[1,"LAST_VERSION"]<-NA
	}
	queryResult
}

# given a data frame having row values named according to the Synapse table convention
# find the maximum value
getMaxRowVersion<-function(df) {
  # TODO make this private method public
  max(synapseClient:::parseRowAndVersion(row.names(df))[2,])
}

process_mpower_data<-function(eId, uId, pId, mId, tId, vId1, vId2, wId, outputProjectId, 
		tappingFeatureTableId, voiceFeatureTableId, balanceFeatureTableId, gaitFeatureTableId,
	bridgeStatusId, mPowerBatchStatusId, lastProcessedVersionTableId, lastProcessedFeatureVersionTableId) {
	# check if Bridge is done.  If not, exit
	hostname<-getHostname()
	leaseTimeout<-as.difftime("06:00:00") # not used at this time
	bridgeExportQueryResult<-checkForAndLockBridgeExportBatch(bridgeStatusId, mPowerBatchStatusId, hostname, Sys.time()) # no lease timeout given
	if (is.null(bridgeExportQueryResult) || nrow(bridgeExportQueryResult@values)==0) return(NULL)
	
	tryCatch({
				process_mpower_data_bare(eId, uId, pId, mId, tId, vId1, vId2, wId, outputProjectId, 
						tappingFeatureTableId, voiceFeatureTableId, balanceFeatureTableId, gaitFeatureTableId,
						bridgeStatusId, mPowerBatchStatusId, lastProcessedVersionTableId, lastProcessedFeatureVersionTableId)
				markProcesingComplete(bridgeExportQueryResult, "complete")
			}, 
			error=function(e) {
				message(e)
				markProcesingComplete(bridgeExportQueryResult, "failed")
			})
}			

# this entry point, which lacks the 'try-catch', is exposed for testing purposes
process_mpower_data_bare<-function(eId, uId, pId, mId, tId, vId1, vId2, wId, outputProjectId, 
		tappingFeatureTableId, voiceFeatureTableId, balanceFeatureTableId, gaitFeatureTableId,
		bridgeStatusId, mPowerBatchStatusId, lastProcessedVersionTableId, lastProcessedFeatureVersionTableId) {
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
	nameToTableIdMap<-store_cleaned_data(outputProjectId, eDat, uDat, pDat, mDat, tDat, vDat, wDat, mFilehandleCols, tFilehandleCols, vFilehandleCols)
	cat("... done.\n")
	
	# update the last processed version for the cleaned data tables
	cat("Updating 'last processed' table for cleaned data tables...\n")
	lastProcessedQueryResult@values<-mergeLastProcessVersionIntoToDF(
			lastProcessedVersion, lastProcessedQueryResult@values)
	synStore(lastProcessedQueryResult)
	cat("... done.\n")
	
	# **** compute features ****
	tappingCleanedDataId<-nameToTableIdMap[["Tapping Activity"]]
	if (is.null(tappingCleanedDataId)) stop("No cleaned Tapping Activity data")
	lp<-lastProcessedFeatureVersion(lastProcessedFeatureVersionTableId, tappingCleanedDataId, "tap_count")
	newLastProcessedVersion<-computeTappingFeatures(tappingCleanedDataId, lp@values[1, "LAST_VERSION"], tappingFeatureTableId)
	if (!is.na(newLastProcessedVersion)) {
		lp@values[1, "LAST_VERSION"]<-newLastProcessedVersion
		synStore(lp)
	}

	walkingCleanedDataId<-nameToTableIdMap[["Walking Activity"]]
	if (is.null(walkingCleanedDataId)) stop("No cleaned Walking Activity data")
	# compute gait and balance features
	lp<-lastProcessedFeatureVersion(lastProcessedFeatureVersionTableId, walkingCleanedDataId, "F0XY")
	lastProcessedGaitVersion<-computeGaitFeatures(walkingCleanedDataId, 
			lp@values[1, "LAST_VERSION"], gaitFeatureTableId)
	if (!is.na(lastProcessedGaitVersion)) {
		lp@values[1, "LAST_VERSION"]<-lastProcessedGaitVersion
		synStore(lp)
	}

	lp<-lastProcessedFeatureVersion(lastProcessedFeatureVersionTableId, walkingCleanedDataId, "zcrAA")
	lastProcessedBalanceVersion<-computeBalanceFeatures(walkingCleanedDataId, 
			lp@values[1, "LAST_VERSION"], balanceFeatureTableId)
	if (!is.na(lastProcessedBalanceVersion)) {
		lp@values[1, "LAST_VERSION"]<-lastProcessedBalanceVersion
		synStore(lp)
	}
	
	voiceCleanedDataId<-nameToTableIdMap[["Voice Activity"]]
	if (is.null(voiceCleanedDataId)) stop("No cleaned Voice Activity data")
	
	# TODO compute voice features
	
	# Now compute the normalized features
	demographicsCleanedDataId<-nameToTableIdMap[["Demographics Survey"]]
	if (is.null(demographicsCleanedDataId)) stop("No cleaned Demographics Survey data")

	tables<-list(demographics=demographicsCleanedDataId, tapping=tappingCleanedDataId, 
			voice=voiceCleanedDataId, walking=walkingCleanedDataId)
	features<-list(tapping=tappingFeatureTableId, gait=gaitFeatureTableId, 
			balance=balanceFeatureTableId, voice=voiceFeatureTableId)
	thirtyDayWindow <- list(start=Sys.Date()-as.difftime(30, units="days"), end=Sys.Date())
	
	# TODO add voice feature featureNames <- list(balance='zcrAA', gait='F0XY', tap='tap_count', voice='???')
	featureNames <- list(balance='zcrAA', gait='F0XY', tap='tap_count')
	normalizedFeatures<-runNormalization(tables, features, featureNames, thirtyDayWindow)
	
	for (healthCode in names(normalizedFeatures)) {
		normdata <- normalizedFeatures[[healthCode]]
		if (inherits(normdata, "try-error")) {
			message(sprintf('skipping %s due to error in processing', healthCode))
		} else {
			jsonString <- visDataToJSON(healthCode, normalizedFeatures[[healthCode]])
			print(jsonString)
			# TODO call Bridge Visualization API here
		}
	}
	
	# Now call the Visualization Data API 
	#https://sagebionetworks.jira.com/wiki/display/BRIDGE/mPower+Visualization#mPowerVisualization-WritemPowerVisualizationData
	cat("Invoking visualization API...\n")
	# place holder
	content<-list(
		"healthCode"="test-d9c31718-481f-4d75-b7d8-49154653504a",
		"date"="2016-03-04",
		"visualization"=list(
				"tap"=list(pre=0.8, post=0.9, controlMin=0.5, controlMax=0.99),
				"gait"=list(pre=0.8, post=0.9, controlMin=0.5, controlMax=0.99),
				"balance"= list(pre=0.8, post=0.9, controlMin=0.5, controlMax=0.99),
				"voice"=list(pre=0.8, post=0.9, controlMin=0.5, controlMax=0.99)
		)
	)			
	url <- bridger:::uriToUrl("/parkinson/visualization", bridger:::.getBridgeCache("bridgeEndpoint"))
	response<-getURL(url, postfields=toJSON(content), customrequest="POST", 
			.opts=bridger:::.getBridgeCache("opts"), httpheader=bridger:::.getBridgeCache("httpheader"))
	# response is "Visualization created."
	cat("... done.\n")

	cat("... ALL DONE!!!\n")
}
