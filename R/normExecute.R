fetchActivityFeatureTables<-function(tables, features) {
  demoTb <- synTableQuery(sprintf("SELECT * FROM %s", tables$demographics))
  demo <- demoTb@values
  message("Got demographics table")

  ## GET TAPPING FEATURES
  tapTable <- synTableQuery(sprintf('SELECT * FROM %s WHERE "tapping_results.json.TappingSamples" is not null', tables$tapping))
  tap <- tapTable@values
  tapFile <- synGet(features$tapping)
  tapFeat <- read.delim(getFileLocation(tapFile), sep="\t", stringsAsFactors=FALSE, row.names = "filehandle")
  tapFeat <- tapFeat[tap$tapping_results.json.TappingSamples, ]
  tap <- cbind(tap, tapFeat)
  message("Got tapping table")

  ## GET VOICE FEATURES
  # voiceTable <- synTableQuery('SELECT * FROM syn5511444 WHERE "audio_audio.m4a" is not null')
  # voice <- voiceTable@values
  voiceFile <- synGet(features$voice)
  voice <- read.delim(getFileLocation(voiceFile), sep=",", stringsAsFactors=FALSE)
  message("Got voice features")

  ## GET WALKING TEST METADATA FOR BOTH GAIT AND BALANCE
  walkTable <- synTableQuery(sprintf('SELECT * FROM %s', tables$walking))
  walk <- walkTable@values
  rownames(walk) <- walk$recordId
  message("Got walking table")

  ## GET GAIT FEATURES
  gaitFile <- synGet(features$gait)
  gaitFeat <- read.delim(getFileLocation(gaitFile), sep=",", stringsAsFactors=FALSE, row.names = 1)
  gait <- walk[rownames(gaitFeat), ]
  gait <- cbind(gait, gaitFeat)
  message("Got gait features")

  ## GET BALANCE FEATURES
  balanceFile <- synGet(features$balance)
  balanceFeat <- read.delim(getFileLocation(balanceFile), sep=",", stringsAsFactors=FALSE, row.names = 1)
  balance <- walk[rownames(balanceFeat), ]
  balance <- cbind(balance, balanceFeat)
  message("Got balance features")

  ## Return a feature table for each type of activity
  return(list(balance=balance, gait=gait, tap=tap, voice=voice))
}


getVisData <- function(participantId, featureNames, featureTables, windowStart, windowEnd) {
  activityTypes <- c('balance', 'gait', 'tap', 'voice')
  norms <- lapply(activityTypes, function(activity) {
    NormalizeFeature(featureTables[[activity]], participantId, featureNames[[activity]], demo, ageInterval=5)
  })
  names(norms) <- activityTypes

  # Return a list of data.frames, one per activity, with columns "data",
  # "pre" and "post"
  norms <- lapply(norms, function(x) {
    tmp <- x$fdat
    tmp$createdOn <- as.Date(tmp$createdOn)
    tmp <- tmp[ tmp$createdOn >= windowStart & tmp$createdOn <= windowEnd, ]
    tmp <- tmp[!duplicated(tmp[, c("createdOn", "medTimepoint")]), ]
    pre <- tmp[tmp$medTimepoint == "Immediately before Parkinson medication", ]
    pre$medTimepoint <- NULL
    names(pre) <- c("date", "pre")
    post <- tmp[tmp$medTimepoint == "Just after Parkinson medication (at your best)", ]
    post$medTimepoint <- NULL
    names(post) <- c("date", "post")
    df <- data.frame(date=seq(windowStart, windowEnd, by="day"), 
                     stringsAsFactors=FALSE)
    df <- merge(df, post, all=TRUE)
    df <- merge(df, pre, all=TRUE)
    rownames(df) <- df$date
    x$fdat <- df
    return(x)
  })

  towardJSON <- lapply(as.list(seq(windowStart, windowEnd, by="day")), function(thisDate){
    res <- list(list(
      tap=list(
        pre=norms$tap$fdat[as.character(thisDate), "pre"],
        post=norms$tap$fdat[as.character(thisDate), "post"],
        controlMin=norms$tap$controlLower,
        controlMax=norms$tap$controlUpper
      ),
      voice=list(
        pre=norms$voice$fdat[as.character(thisDate), "pre"],
        post=norms$voice$fdat[as.character(thisDate), "post"],
        controlMin=norms$voice$controlLower,
        controlMax=norms$voice$controlUpper
      ),
      gait=list(
        pre=norms$gait$fdat[as.character(thisDate), "pre"],
        post=norms$gait$fdat[as.character(thisDate), "post"],
        controlMin=norms$gait$controlLower,
        controlMax=norms$gait$controlUpper
      ),
      balance=list(
        pre=norms$balance$fdat[as.character(thisDate), "pre"],
        post=norms$balance$fdat[as.character(thisDate), "post"],
        controlMin=norms$balance$controlLower,
        controlMax=norms$balance$controlUpper
      )))
    names(res) <- thisDate
    return(res)
  })

  ## INSERT CODE TO CALL BRIDGE APIS WHERE APPROPRIATE
  return(toJSON(towardJSON))
}


## LET US TRY FOR THIS INDIVIDUAL
# participantId <- "6c130d05-41af-49e9-9212-aaae738d3ec1"
# tapName <- "numberTaps"
# voiceName <- "shimmerLocaldB_sma3nz_amean"
# gaitName <- "medianZ"
# balanceName <- "rangeAA"
# windowEnd <- as.Date("2015-05-01")
# windowStart <- windowEnd-29
testNormalization <- function() {
  ## public table IDs
  tables   <- list(demographics='syn5511429',
                   tapping='syn5511439',
                   voice='syn5511444',
                   walking='syn5511449')

  ## features derived from public tables
  features <- list(balance='syn5678820',
                   gait='syn5679280',
                   tapping='syn5612449',
                   voice='syn5653006')

  featureTables <- fetchActivityFeatureTables(tables, features)
  featureNames <- list(balance='rangeAA', gait='medianZ', tap='numberTaps', voice='shimmerLocaldB_sma3nz_amean')
  participantId <- "6c130d05-41af-49e9-9212-aaae738d3ec1"
  windowEnd <- as.Date("2015-05-01")
  windowStart <- windowEnd-29

  visData <- getVisData(participantId, featureNames, featureTables, windowStart, windowEnd)
  return(visData)
}


cases <- na.omit(demo$healthCode[demo$`professional-diagnosis`])
normalizedFeatures <- lapply(cases, function(participantId) {
  cat(participantId, "\n")
  try(getVisData(participantId, featureNames, featureTables, windowStart, windowEnd))
})



