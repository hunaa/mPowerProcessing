# This computes tapping features on cleaned data
#
# returns new last-processed version
# 
# Author: bhoff
###############################################################################

computeTappingFeatures<-function(cleanDataTableId, lastProcessedVersion, featureTableId) {
	# retrieve
	if (is.na(lastProcessedVersion)) {
		queryString<-paste0("SELECT * FROM ", cleanDataTableId)
	} else {
		queryString<-paste0("SELECT * FROM ", cleanDataTableId, " WHERE ROW_VERSION > ", lastProcessedVersion)
	}
	
	queryResults<-synTableQuery(queryString)
	
	# if no results, just return
	if (nrow(queryResults@values)==0) return(lastProcessedVersion)
	
	recordIds<-queryResults@values$recordId
	n<-length(recordIds)
	
	# create a new data frame to hold the computed feature(s)
	featureDataFrame<-data.frame(
			recordId=recordIds, 
			"is_computed"=rep(FALSE, n), 
			"tap_count"=rep(NA, n),
			stringsAsFactors=FALSE)

	# now compute the features
	tappingFiles<-synDownloadTableColumns(queryResults, "tapping_results.json.TappingSamples")
	for (i in 1:n) {
		fileHandleId<-queryResults@values[i,"tapping_results.json.TappingSamples"]
		if (is.na(fileHandleId) || is.null(fileHandleId)) next
		tappingFile<-tappingFiles[[fileHandleId]]
		tappingData<-fromJSON(tappingFile)
		featureDataFrame[i,"tap_count"] <- tappingCountStatistic(tappingData)
		featureDataFrame[i,"is_computed"] <- TRUE
	}
	
	# store the results
	featureTable<-Table(featureTableId, featureDataFrame)
	synStore(featureTable)
	
	# return the last processed version
	getMaxRowVersion(queryResults@values)
}


