
standardMetadataColumns <- c(
  "recordId", "healthCode", "createdOn",
  "appVersion", "phoneInfo", "calculatedMeds")

createTappingFeatureTable <- function(sourceTable='syn5511439', features='syn5612449', metadataColumns=standardMetadataColumns) {
  table <- synTableQuery(sprintf('SELECT * FROM %s WHERE "tapping_results.json.TappingSamples" is not null', sourceTable))
  df <- table@values
  colnames(df)[colnames(df) == 'medTimepoint'] <- "calculatedMeds"
  metadata <- df[,metadataColumns]
  rownames(metadata) <- metadata$recordId

  ## GET TAPPING FEATURES
  tapFile <- synGet(features)
  tapFeat <- read.delim(getFileLocation(tapFile), sep="\t", stringsAsFactors=FALSE, row.names="filehandle")
  tapFeat <- tapFeat[df$tapping_results.json.TappingSamples, ]
  cbind(metadata, tapFeat)
}

createVoiceFeatureTable <- function(sourceTable='syn5511444', features='syn5647604', metadataColumns=standardMetadataColumns) {
  ## Read metadata
  table <- synTableQuery(sprintf('SELECT * FROM %s', sourceTable))
  df <- table@values
  colnames(df)[colnames(df) == 'medTimepoint'] <- "calculatedMeds"
  metadata <- df[,metadataColumns]
  rownames(metadata) <- metadata$recordId

  featuresFile <- synGet(features)
  features <- read.csv(getFileLocation(featuresFile), stringsAsFactors=FALSE, row.names="recordId")
  colnames(features)[colnames(features) == 'medTimepoint'] <- "calculatedMeds"
  cols <- c( na.omit(match(metadataColumns, colnames(features))), 10:ncol(features) )
  features[,cols]
}

createFeatureTable <- function(sourceTable, features, metadataColumns=standardMetadataColumns) {
  ## Read metadata
  table <- synTableQuery(sprintf('SELECT * FROM %s', sourceTable))
  df <- table@values
  colnames(df)[colnames(df) == 'medTimepoint'] <- "calculatedMeds"
  metadata <- df[,metadataColumns]
  rownames(metadata) <- metadata$recordId

  ## Read features
  featuresFile <- synGet(features)
  features <- read.delim(getFileLocation(featuresFile), sep="\t", stringsAsFactors=FALSE, row.names="recordId")
  features <- features[metadata$recordId,]

  ## return combined table
  cbind(metadata, features)
}

selectMostSignificantFeatures <- function(dat, metadataColumns=standardMetadataColumns, detrend=FALSE) {
  ## count number of tasks performed before, after, etc
  counts <- CountBeforeAfterMedicationPerParticipant(dat)

  ## select participants which performed at least 30 tasks before and 30 tasks after medication
  sel <- GetParticipants(counts, beforeThr = 30, afterThr = 30)

  ## get before/after data
  casesBefore <- dat[which(dat$calculatedMeds == "Immediately before Parkinson medication"),]
  dim(casesBefore)
  casesAfter <- dat[which(dat$calculatedMeds == "Just after Parkinson medication (at your best)"),]
  dim(casesAfter)
  cases <- rbind(casesBefore, casesAfter)

  if (class(metadataColumns)=="character") {
    featureColumns <- which(!(colnames(cases) %in% metadataColumns))
  } else if (class(metadataColumns)=="numeric") {
    featureColumns <- 1:ncol(cases)[-metadataColumns]
  } else {
    stop("Unexpected value for metadataColumns")
  }

  ## check for duplicates
  dupli <- duplicated(cases[featureColumns])
  if (sum(dupli) > 0) {
    warning("duplicate observations detected while selecting most significant features")
  }

  ## include individuals which have sufficent observations before and after
  cases <- cases[cases$healthCode %in% sel,]

  ## detrend the feature data (only for tapping, you can skip this step for the other streams)
  if (detrend) {
    cat('detrending...\n')
    cases <- LoessDetrendedFeatures(cases, colnames(cases)[featureColumns])
  }

  ## run UI-tests without adjusting for time of the day
  ui <- RunUITests(cases, sel, featNames = names(cases)[featureColumns], adjustByTime = FALSE, sorted = TRUE)
  pvals <- data.frame(ui[[1]])
  pvals$corrected <- p.adjust(pvals[, 1], method = "BH")

  ## run UI-tests adjusting for time of the day
  aui <- RunUITests(cases, rownames(ui[[1]]), featNames = names(cases)[featureColumns], adjustByTime = TRUE, sorted = FALSE)
  apvals <- data.frame(aui[[1]])
  apvals$corrected <- p.adjust(apvals[, 1], method = "BH")

  ## select participants for which the multiple testing corrected UI-test p-value is less or equal to 0.05
  participantIds <- rownames(apvals)[apvals[, 2] <= 0.05]

  ## get the top personalized feature for each selected participant
  GetTopFeatures(x = aui$participantOutputs, participantIds, top = 1)
}


# voice <- createVoiceFeatureTable()
# msf <- selectMostSignificantFeatures(voice)

# tap <- createTappingFeatureTable()
# msf <- selectMostSignificantFeatures(voice)

# bal <- createFeatureTable(sourceTable='syn5511449', features='syn5698502')
# msf <- selectMostSignificantFeatures(bal)

# gait <- createFeatureTable(sourceTable='syn5511449', features='syn5698497')
# msf <- selectMostSignificantFeatures(gait)



# balance <- createFeatureTable('syn5511449', '')
# msf <- selectMostSignificantFeatures(balance)

# gait <- createFeatureTable('syn5511449', '')
# msf <- selectMostSignificantFeatures(gait)
