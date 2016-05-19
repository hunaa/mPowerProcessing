# This computes voice features on cleaned data
#
# returns new last-processed version
# 
###############################################################################

computeVoiceFeatures <- function(cleanDataTableId, lastProcessedVersion, featureTableId) {
  cat("Computing voice features...\n")

  ## retrieve voice files from Synapse
  dataColumnName <- "audio_audio.m4a"
  if (is.na(lastProcessedVersion)) {
    queryString <- paste0('SELECT "recordId", "', dataColumnName, '" FROM ', cleanDataTableId)
  } else {
    queryString <- paste0('SELECT "recordId", "', dataColumnName, '" FROM ', cleanDataTableId, ' WHERE ROW_VERSION > ', lastProcessedVersion)
  }
  queryResults <- synTableQuery(queryString)

  # if no results, just return
  if (nrow(queryResults@values)==0) {
    cat("...done.\n")
    return(lastProcessedVersion)
  }

  recordIds <- queryResults@values$recordId
  n <- length(recordIds)

  # create a new data frame to hold the computed feature(s)
  featureDataFrame <- data.frame(
      recordId=recordIds,
      "is_computed"=rep(FALSE, n),
      "medianF0"=rep(NA, n),
      stringsAsFactors=FALSE)

  # now compute the features
  fileMap <- synDownloadTableColumns(queryResults, dataColumnName)
  for (i in seq(along=recordIds)) {
    fileHandleId <- queryResults@values[i, dataColumnName]
    recordId <- queryResults@values[i, "recordId"]
    if (is.na(fileHandleId) || is.null(fileHandleId)) next
    medianF0 <- try({
      filepath <- fileMap[[fileHandleId]]
      medianF0(convert_to_wav(filepath))
    })
    if (is(medianF0, "try-error")) {
      cat("computeVoiceFeatures:  medianF0 failed for recordId=", recordId, ", fileHandleId=", fileHandleId, "\n")
    } else {
      featureDataFrame[i, "medianF0"] <- medianF0
      featureDataFrame[i, "is_computed"] <- TRUE
    }
  }

  # store the results
  featureTable <- synTableQuery(paste0('SELECT * FROM ',featureTableId))
  featureTable@values <- mergeDataFrames(featureTable@values, featureDataFrame, "recordId", delta=TRUE)
  synStore(featureTable)

  cat("...done.\n")

  # return the last processed version
  getMaxRowVersion(queryResults@values)
}
