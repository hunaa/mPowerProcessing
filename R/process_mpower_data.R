
getLastProcessedVersion<-function(df) {
	lastProcessedVersion<-df[["LAST_VERSION"]]
	names(lastProcessedVersion)<-df[["TABLE_ID"]]
	lastProcessedVersion
}

# given a data frame having row values named according to the Synapse table convention
# find the maximum value
getMaxRowVersion<-function(df) {
	# TODO make this private method public
	max(synapseClient:::parseRowAndVersion(row.names(df))[2,])
}

process_mpower_data<-function(eId, uId, pId, mId, tId, vId1, vId2, wId, newParent, lastProcessedVersionTableId) {
	# TODO check if Bridge is done.  If not, exit
	
	lastProcessedQueryResult<-synTableQuery(paste0("SELECT * FROM ", lastProcessedVersionTableId))
	lastProcessedVersion<-getLastProcessedVersion(lastProcessedQueryResult@values)
	
	######
	# Data Cleaning
	######
	eDat<-process_survey_v1(eId, lastProcessedVersion[eId])
	lastProcessedVersion[eId]<-getMaxRowVersion(eDat)
	
	uDat<-process_survey_v2(uId, lastProcessedVersion[uId])
	lastProcessedVersion[uId]<-getMaxRowVersion(uDat)
	
	pDat<-process_survey_v3(pId, lastProcessedVersion[pId])
	lastProcessedVersion[pId]<-getMaxRowVersion(pDat)
	
	mResults<-process_memory_activity(mId, lastProcessedVersion[mId])
	mDat<-mResults$mDat
	mFilehandleCols<-mResults$mFilehandleCols
	lastProcessedVersion[mId]<-getMaxRowVersion(mDat)
	
	tResults<-process_tapping_activity(tId, lastProcessedVersion[tId])
	tDat<-tResults$tDat
	tFilehandleCols<-tResults$tFilehandleCols
	for (id in names(tResults$maxRowProcessed)) {
		lastProcessedVersion[id]<-tResults$maxRowProcessed[[id]]
	}
	
	vResults<-process_voice_activity(vId1, vId2, lastProcessedVersion[vId1], lastProcessedVersion[vId2])
	vDat<-vResults$vDat
	vFilehandleCols<-vResults$vFilehandleCols
	for (id in names(vResults$maxRowProcessed)) {
		lastProcessedVersion[id]<-vResults$maxRowProcessed[[id]]
	}
	
	wResults<-process_walking_activity(wId, lastProcessedVersion[wId])
	wDat<-wResults$wDat
	for (id in names(wResults$maxRowProcessed)) {
		lastProcessedVersion[id]<-wResults$maxRowProcessed[[id]]
	}
	
	clean_up_result<-cleanup_missing_med_data(mDat, tDat, vDat, wDat)
	mDat<-clean_up_result$mDat
	tDat<-clean_up_result$tDat
	vDat<-clean_up_result$vDat
	wDat<-clean_up_result$wDat
	
	store_cleaned_data(newParent, eDat, uDat, pDat, mDat, tDat, vDat, wDat, mFilehandleCols, tFilehandleCols, vFilehandleCols)
	
	# update the last processed version
	lastProcessedQueryResult@values<-lastProcessedVersion
	synStore(lastProcessedQueryResult)
	
	# Note, we skip feature_selection for now
	
	
	# this returns the json request body to be passed to the API
	"feature_normalization/normExecute.R"
	
	# Now call the Visualization Data API 
	#https://sagebionetworks.jira.com/wiki/display/BRIDGE/mPower+Visualization#mPowerVisualization-WritemPowerVisualizationData
}
