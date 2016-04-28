# This computes gait features on cleaned data
#
# returns new last-processed version
# 
# Author: bhoff
###############################################################################

computeGaitFeatures<-function(cleanDataTableId, lastProcessedVersion, featureTableId) {
	jsonColName<-"deviceMotion_walking_outbound.json.items"
	# retrieve
	if (is.na(lastProcessedVersion)) {
		queryString<-paste0('SELECT "recordId", "', jsonColName, '" FROM ', cleanDataTableId)
	} else {
		queryString<-paste0('SELECT "recordId", "', jsonColName, '" FROM ', cleanDataTableId, ' WHERE ROW_VERSION > ', lastProcessedVersion)
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
			"F0XY"=rep(NA, n),
			stringsAsFactors=FALSE)

	# now compute the features
	jsonFiles<-synDownloadTableColumns(queryResults, jsonColName)
	for (i in 1:n) {
		fileHandleId<-queryResults@values[i,jsonColName]
		if (is.na(fileHandleId) || is.null(fileHandleId)) next
		file<-jsonFiles[[fileHandleId]]
		data<-fromJSON(file)
		featureDataFrame[i,"F0XY"] <- gait_F0XY(data)
		featureDataFrame[i,"is_computed"] <- TRUE
	}
	
	# store the results
	featureTable<-Table(featureTableId, featureDataFrame)
	synStore(featureTable)
	
	# return the last processed version
	getMaxRowVersion(queryResults@values)
}


