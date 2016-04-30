# This computes balance features on cleaned data
#
# returns new last-processed version
# 
# Author: bhoff
###############################################################################

computeBalanceFeatures<-function(cleanDataTableId, lastProcessedVersion, featureTableId) {
	cat("Computing balance features...\n")
	jsonColName<-"deviceMotion_walking_rest.json.items"
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
			"zcrAA"=rep(NA, n),
			stringsAsFactors=FALSE)

	# now compute the features
	jsonFiles<-synDownloadTableColumns(queryResults, jsonColName)
	for (i in 1:n) {
		fileHandleId<-queryResults@values[i,jsonColName]
		if (is.na(fileHandleId) || is.null(fileHandleId)) next
		file<-jsonFiles[[fileHandleId]]
		data<-fromJSON(file)		
		zcrAA<-try(balance_zcrAA(data), silent=T)
		if (is(zcrAA, "try-error")) {
			cat("computeBalanceFeatures:  balance_zcrAA failed for i=", i, ", fileHandleId=", fileHandleId, "\n")
		} else {
			featureDataFrame[i,"zcrAA"] <- zcrAA
			featureDataFrame[i,"is_computed"] <- TRUE
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


