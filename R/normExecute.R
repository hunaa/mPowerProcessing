fetchActivityFeatureTables<-function(tables, features) {
  ## GET TAPPING FEATURES
  tapTable <- synTableQuery(sprintf('SELECT * FROM %s WHERE "tapping_results.json.TappingSamples" is not null', tables$tapping))
  tap <- tapTable@values
  tapFile <- synGet(features$tapping)
  tapFeat <- read.delim(getFileLocation(tapFile), sep="\t", stringsAsFactors=FALSE, row.names = "filehandle")
  tapFeat <- tapFeat[tap$tapping_results.json.TappingSamples, ]
  tap <- cbind(tap, tapFeat)
  tap$date <- as.Date(tap$createdOn)
  message("Got tapping table")

  ## GET VOICE FEATURES
  # voiceTable <- synTableQuery('SELECT * FROM syn5511444 WHERE "audio_audio.m4a" is not null')
  # voice <- voiceTable@values
  voiceFile <- synGet(features$voice)
  voice <- read.delim(getFileLocation(voiceFile), sep=",", stringsAsFactors=FALSE)
  voice$date <- as.Date(voice$createdOn)
  message("Got voice features")

  ## GET WALKING TEST METADATA FOR BOTH GAIT AND BALANCE
  walkTable <- synTableQuery(sprintf('SELECT * FROM %s', tables$walking))
  walk <- walkTable@values
  walk$date <- as.Date(walk$createdOn)
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

# return a list, indexed by activity type, of normalized pre and
# post medication values for a selected feature
#
# $gait
# $gait$fdat
#                  date     post       pre
# 2015-04-02 2015-04-02 0.502782 0.4981415
# $gait$controlMean
# [1] 0.5883402
# $gait$controlUpper
# [1] 0.7726447
# $gait$controlLower
# [1] 0.4040358
getVisData <- function(healthCode, featureNames, featureTables, window) {
  activityTypes <- names(featureTables)
  norms <- lapply(activityTypes, function(activity) {
    NormalizeFeature(featureTables[[activity]], healthCode, featureNames[[activity]], demo, ageInterval=5)
  })
  names(norms) <- activityTypes

  # Return a list of data.frames, one per activity, with columns "date",
  # "pre" and "post"
  norms <- lapply(norms, function(x) {
    tmp <- x$fdat
    tmp <- tmp[ tmp$date >= window$start & tmp$date <= window$end, ]
    tmp <- tmp[!duplicated(tmp[, c("date", "medTimepoint")]), ]
    pre <- tmp[tmp$medTimepoint == "Immediately before Parkinson medication", ]
    pre$medTimepoint <- NULL
    names(pre) <- c("date", "pre")
    post <- tmp[tmp$medTimepoint == "Just after Parkinson medication (at your best)", ]
    post$medTimepoint <- NULL
    names(post) <- c("date", "post")
    df <- data.frame(date=seq(window$start, window$end, by="day"), 
                     stringsAsFactors=FALSE)
    df <- merge(df, post)
    df <- merge(df, pre)
    rownames(df) <- df$date
    x$fdat <- df
    return(x)
  })

  ## remove activities with no data
  Filter(function(x) { nrow(x$fdat) > 0}, norms)
}


visDataToJSON <- function(norms, window) {
  towardJSON <- lapply(as.list(seq(window$start, window$end, by="day")), function(thisDate) {
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
  window <- list(start=as.Date("2015-05-01")-29, end=as.Date("2015-05-01"))

  visData <- getVisData(participantId, featureNames, featureTables, window)
  return(visData)
}


runNormalization <- function(tables, features, window) {
  # demographics table
  demoTb <- synTableQuery(sprintf("SELECT * FROM %s", tables$demographics))
  demo <- demoTb@values

  featureTables <- fetchActivityFeatureTables(tables, features)
  featureNames <- list(balance='rangeAA', gait='medianZ', tap='numberTaps', voice='shimmerLocaldB_sma3nz_amean')

  casesWithPrepostActivity <- findCasesWithPrepostActivity(demo, featureTables, window)

  normalizedFeatures <- lapply(casesWithPrepostActivity, function(healthCode) {
    message(healthCode)
    try(getVisData(healthCode, featureNames, featureTables, window))
  })

  # convert to JSON

  # call Bridge Visualization API
}
