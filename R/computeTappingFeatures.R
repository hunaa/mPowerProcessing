# This computes tapping features on cleaned data
#
# returns new last-processed version
# 
# Author: bhoff
###############################################################################

computeTappingFeatures<-function(cleanDataTableId, lastProcessedVersion, featureTableId, hand=NA) {
  
  ## PICK THE RIGHT COLUMN NAME DEPENDING ON 'HAND'
  if(is.na(hand)){
    cat("Computing tapping features...\n")
    jsonColName<-"tapping_results.json.TappingSamples"
  } else if(hand=="left"){
    cat("Computing tapping features - left hand...\n")
    jsonColName<-"tapping_left.json.TappingSamples"
  } else if(hand=="right"){
    cat("Computing tapping features - right hand...\n")
    jsonColName<-"tapping_right.json.TappingSamples"
  } else{
    stop(paste0("Unexpected hand specified for tapping feature extraction: ", hand))
  }
  
	# retrieve
	if (is.na(lastProcessedVersion)) {
		queryString<-paste0('SELECT "recordId", "', jsonColName, '" FROM ', cleanDataTableId)
	} else {
		queryString<-paste0('SELECT "recordId", "', jsonColName, '" FROM ', cleanDataTableId, ' WHERE ROW_VERSION > ', lastProcessedVersion)
	}
	
	queryResults<-synTableQuery(queryString)
	
	# if no results, just return
	if (nrow(queryResults@values)==0) {
		cat("...done.\n")
		return(lastProcessedVersion)
	}
	
	recordIds<-queryResults@values$recordId
	n<-length(recordIds)
	
	# create a new data frame to hold the computed feature(s)
	featureDataFrame<-data.frame(
			recordId=recordIds, 
			"is_computed"=rep(FALSE, n), 
			"tap_count"=rep(NA, n),
			stringsAsFactors=FALSE)

	if (!all(is.na(queryResults@values[[jsonColName]]))) {
		# now compute the features
		tappingFiles<-synDownloadTableColumns(queryResults, jsonColName)
		for (i in 1:n) {
			fileHandleId<-queryResults@values[i,jsonColName]
			if (is.na(fileHandleId) || is.null(fileHandleId)) next
			tapCount<-try({
						tappingFile<-tappingFiles[[fileHandleId]]
						tappingData<-fromJSON(tappingFile)
						tappingCountStatistic(tappingData)
					}, silent=T)
			if (is(tapCount, "try-error")) {
				cat("computeTappingFeatures:  tappingCountStatistic failed for i=", i, ", fileHandleId=", fileHandleId, "\n")
			} else {
				featureDataFrame[i,"tap_count"] <- tapCount
				featureDataFrame[i,"is_computed"] <- TRUE
			}
		}
	}
	
	# store the results
	featureTable<-synTableQuery(paste0('SELECT * FROM ',featureTableId))
	featureTable@values<-mergeDataFrames(featureTable@values, featureDataFrame, "recordId", delta=TRUE)
	synStore(featureTable)
	
	cat("...done.\n")
	
	# return the last processed version
	getMaxRowVersion(queryResults@values)
}


