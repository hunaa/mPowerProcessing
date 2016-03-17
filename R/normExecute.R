#' Retreive feature data from Synapse and load
#'
#' @param tables a named list of Synapse IDs of feature metadata tables
#' @param features a named list of Synapse IDs of feature files
#'
#' @return A named list of feature tables
#'
#' All lists are indexed by activity name ('balance', 'gait', 'tap', 'voice').
#'
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

#' Return a list, indexed by activity type, of normalized pre and
#' post medication values for a selected feature to push to the
#' Bridge mPower Visualization API
#' \url{https://sagebionetworks.jira.com/wiki/display/BRIDGE/mPower+Visualization}
#'
#' \code{
#'    $gait
#'    $gait$fdat
#'                     date     post       pre
#'    2015-04-02 2015-04-02 0.502782 0.4981415
#'    $gait$controlMean
#'    [1] 0.5883402
#'    $gait$controlUpper
#'    [1] 0.7726447
#'    $gait$controlLower
#'    [1] 0.4040358
#' }
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
    df <- merge(pre, post, all=TRUE)
    rownames(df) <- df$date
    x$fdat <- df
    return(x)
  })
}


#' Convert getVisData output into JSON.
#'
#' @return a JSON string
#'
#' \code{
#'  [
#'    {
#'      "healthCode": "8beab5c6-6067-4c47-b655-999999999999",
#'      "date": "2015-04-03",
#'      "visualization": {
#'        "tap": {
#'          "pre": 0.4885,
#'          "post": "NA",
#'          "controlMin": 0.3636,
#'          "controlMax": 0.8649
#'        },
#'        "voice": {
#'          "pre": 0.5223,
#'          "post": "NA",
#'          "controlMin": 0,
#'          "controlMax": 0.5738
#'        },
#'        "gait": {
#'          "pre": "NA",
#'          "post": "NA",
#'          "controlMin": 0.1285,
#'          "controlMax": 0.3539
#'        },
#'        "balance": {
#'          "pre": "NA",
#'          "post": "NA",
#'          "controlMin": 0,
#'          "controlMax": 0.1049
#'        }
#'      }
#'    },
#'    ...
#'  ]
#' }
visDataToJSON <- function(healthCode, normdata) {
  towardJSON <- lapply(collectDates(normdata), function(thisDate) {
    list(
      healthCode=healthCode,
      date=as.character(thisDate),
      visualization=list(
        tap=list(
          pre=normdata$tap$fdat[as.character(thisDate), "pre"],
          post=normdata$tap$fdat[as.character(thisDate), "post"],
          controlMin=normdata$tap$controlLower,
          controlMax=normdata$tap$controlUpper
        ),
        voice=list(
          pre=normdata$voice$fdat[as.character(thisDate), "pre"],
          post=normdata$voice$fdat[as.character(thisDate), "post"],
          controlMin=normdata$voice$controlLower,
          controlMax=normdata$voice$controlUpper
        ),
        gait=list(
          pre=normdata$gait$fdat[as.character(thisDate), "pre"],
          post=normdata$gait$fdat[as.character(thisDate), "post"],
          controlMin=normdata$gait$controlLower,
          controlMax=normdata$gait$controlUpper
        ),
        balance=list(
          pre=normdata$balance$fdat[as.character(thisDate), "pre"],
          post=normdata$balance$fdat[as.character(thisDate), "post"],
          controlMin=normdata$balance$controlLower,
          controlMax=normdata$balance$controlUpper
        )))
  })
  return(toJSON(towardJSON))
}


#' Compute normalized features for one participant as a test
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


#' Normalize feature data and export to Bridge Visualization API
#'
#' @param tables named list of Synapse tables
#' @param features named list of Synapse IDs of features
#' @param window date window
#'
#' @return a list of normalized features indexed by health code for
#'         PD patients with activity within the date window
#'
#' The date window is a list of Date objects with two elements labeled
#' start and end.
#'
#' @examples
#'  tables   <- list(demographics='syn5511429',
#'                   tapping='syn5511439',
#'                   voice='syn5511444',
#'                   walking='syn5511449')
#'  features <- list(balance='syn5678820',
#'                   gait='syn5679280',
#'                   tapping='syn5612449',
#'                   voice='syn5653006')
#'  window = list(start=as.Date("2015-05-01")-29, end=as.Date("2015-05-01")
#'
#'  normalizedFeatures <- runNormalization(tables, features, window)
#'
#'  for (healthCode in names(normalizedFeatures)) {
#'    jsonString <- visDataToJSON(healthCode, normalizedFeatures[[healthCode]])
#'    # call Bridge Visualization API here
#'  }
#'
runNormalization <- function(tables, features, window) {
  # demographics table
  demoTb <- synTableQuery(sprintf("SELECT * FROM %s", tables$demographics))
  demo <- demoTb@values

  featureTables <- fetchActivityFeatureTables(tables, features)
  featureNames <- list(balance='rangeAA', gait='medianZ', tap='numberTaps', voice='shimmerLocaldB_sma3nz_amean')

  casesWithPrepostActivity <- findCasesWithPrepostActivity(demo, featureTables, window)

  lapply(casesWithPrepostActivity, function(healthCode) {
    message(healthCode, " - ", demo$age[demo$healthCode==healthCode])
    try(getVisData(healthCode, featureNames, featureTables, window))
  })
}
