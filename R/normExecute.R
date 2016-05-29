#' Retrieve feature data from Synapse and load
#'
#' @param tables a named list of Synapse IDs of feature metadata tables
#' @param features a named list of Synapse IDs of feature files
#'
#' @return A named list of feature tables
#'
#' All lists are indexed by activity name ('balance', 'gait', 'tap', 'voice').
#'
fetchActivityFeatureTables<-function(tables, features) {
  ## TODO: consider time zone in the use of as.Date here ignores time zone
  ## GET TAPPING FEATURES
  tapTable <- synTableQuery(sprintf('SELECT * FROM %s WHERE "tapping_results.json.TappingSamples" is not null', tables$tapping))
  tap <- tapTable@values
	tapFeat <- synTableQuery(sprintf('SELECT * FROM %s', features$tapping))@values
	tap<-merge(tap, tapFeat, by="recordId", all=FALSE) # inner join
  tap$date <- as.Date(tap$createdOn)
  message("Got tapping table")
  
  tapLeftRightTable <- synTableQuery(sprintf('SELECT * FROM %s', tables$tappingLeftright))
  tapLeftright <- tapLeftRightTable@values
  tapLeftright$date <- as.Date(tapLeftright$createdOn)
  ## LEFT HANDED TAPPING
  tapLeftFeat <- synTableQuery(sprintf('SELECT * FROM %s', features$tappingLeft))@values
  tapLeft<-merge(tapLeftright, tapLeftFeat, by="recordId", all=FALSE) # inner join
  message("Got tapping table - left hand")
  
  ## RIGHT HANDED TAPPING
  tapRightFeat <- synTableQuery(sprintf('SELECT * FROM %s', features$tappingRight))@values
  tapRight<-merge(tapLeftright, tapRightFeat, by="recordId", all=FALSE) # inner join
  message("Got tapping table - right hand")
  
  ## GET VOICE FEATURES
  voiceTable <- synTableQuery((sprintf('SELECT * FROM %s WHERE "audio_audio.m4a" is not null', tables$voice)))
  voice <- voiceTable@values
	voiceFeat <- synTableQuery(sprintf('SELECT * FROM %s', features$voice))@values
	voice<-merge(voice, voiceFeat, by="recordId", all=FALSE) # inner join
	voice$date <- as.Date(voice$createdOn)
  message("Got voice features")

  ## GET WALKING TEST METADATA FOR BOTH GAIT AND BALANCE
  walkTable <- synTableQuery(sprintf('SELECT * FROM %s', tables$walking))
  walk <- walkTable@values
  walk$date <- as.Date(walk$createdOn)
  rownames(walk) <- walk$recordId
  message("Got walking table")

  ## GET GAIT FEATURES
	gaitFeat <- synTableQuery(sprintf('SELECT * FROM %s', features$gait))@values
	gait<-merge(walk, gaitFeat, by="recordId", all=FALSE) # inner join
  message("Got gait features")

  ## GET BALANCE FEATURES
	balanceFeat <- synTableQuery(sprintf('SELECT * FROM %s', features$balance))@values
	balance<-merge(walk, balanceFeat, by="recordId", all=FALSE) # inner join
	message("Got balance features")

  ## Return a feature table for each type of activity
  return(list(balance=balance, gait=gait, tap=tap, tapLeft=tapLeft, tapRight=tapRight, voice=voice))
}


#' Transforms the output of NormalizeFeature.
#'
#' Given the output of NormalizeFeature - a list one entry per activity of
#' a list of fdat, controlMean, controlUpper and controlLower - transform the
#' data.frame fdat into one containing columns "date", "pre" and "post".
#' Columns pre and post hold normalized data for a selected feature.
transformNormalizedData <- function(norms, window) {
  lapply(norms, function(x) {
    tmp <- x$fdat
    tmp <- tmp[!is.na(tmp$date) & tmp$date >= window$start & tmp$date <= window$end, ]
    tmp <- tmp[!duplicated(tmp[, c("date", "medTimepoint")]), ]
    pre <- tmp[!is.na(tmp$medTimepoint) & tmp$medTimepoint == "Immediately before Parkinson medication", ]
    pre$medTimepoint <- NULL
    names(pre) <- c("date", "pre")
    post <- tmp[!is.na(tmp$medTimepoint) & tmp$medTimepoint == "Just after Parkinson medication (at your best)", ]
    post$medTimepoint <- NULL
    names(post) <- c("date", "post")
    df <- merge(pre, post, all=TRUE)
    rownames(df) <- df$date
    x$fdat <- df
    return(x)
  })
}


#' Return a list, indexed by activity type, of normalized pre and
#' post medication values for a selected feature to push to the
#' Bridge mPower Visualization API
#' \url{https://sagebionetworks.jira.com/wiki/display/BRIDGE/mPower+Visualization}
#'
#' @param healthCode
#' @param featureNames
#' @param featureTables
#' @param window Date window in which to find activities
#' @param demo data.frame demographics table
#' @param ageInterval integer defaults to 5 meaning the age matched controls
#'        are to be plus or minus 5 years from the age of the patient
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
getVisData <- function(healthCode, featureNames, featureTables, window, demo, ageInterval=5) {
  activityTypes <- names(featureNames)
  norms <- lapply(activityTypes, function(activity) {
    ## FOR HANDED TAPPING - USE THE ORIGINAL TAPPING FOR NORMALIZATION
    if(activity %in% c("tapLeft", "tapRight")){
      NormalizeFeature(featureTables[[activity]], featureTables[["tap"]], healthCode, featureNames[[activity]], demo, ageInterval=ageInterval)
    } else{
      NormalizeFeature(featureTables[[activity]], featureTables[[activity]], healthCode, featureNames[[activity]], demo, ageInterval=ageInterval)
    }
  })
  names(norms) <- activityTypes

  transformNormalizedData(norms, window)
}


#' Convert getVisData output into JSON.
#'
#' @return a list of JSON strings
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
  lapply(collectDates(normdata), function(thisDate) {
	rjson::toJSON(list(
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
        ))))
  })
}


#' Compute normalized features for one participant as a test
testNormalization <- function(participantId, featureNames, featureTables, window, demo) {
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

  if (missing(demo)) {
    demoTb <- synTableQuery(sprintf("SELECT * FROM %s", tables$demographics))
    demo <- demoTb@values
  }

  if (missing(featureTables)) {
    featureTables <- fetchActivityFeatureTables(tables, features)
  }

  if (missing(featureNames)) {
    featureNames <- list(balance='rangeAA', gait='medianZ', tap='numberTaps', voice='shimmerLocaldB_sma3nz_amean')
  }

  if (missing(participantId)) {
    participantId <- "6c130d05-41af-49e9-9212-aaae738d3ec1"
  }

  if (missing(window)) {
    window <- list(start=as.Date("2015-05-01")-29, end=as.Date("2015-05-01"))
  }

  getVisData(participantId, featureNames, featureTables, window, demo)
}


#' Normalize feature data for export to Bridge mPower Visualization API
#'
#' @param tables named list of Synapse tables
#' @param features named list of Synapse IDs of features
#' @param window date window
#'
#' @return a list of normalized feature data.
#'
#' This function calls getVisData for each PD patient who has any
#' activity during the date window and returns the results in a list
#' indexed by health code.
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
#'  window <- list(start=as.Date("2015-05-01"), end=as.Date("2015-05-31"))
#'
#'  normalizedFeatures <- runNormalization(tables, features, window)
#'
#'
runNormalization <- function(tables, features, featureNames, window) {
  # demographics table
  demoTb <- synTableQuery(sprintf("SELECT * FROM %s", tables$demographics))
  demo <- demoTb@values

  featureTables <- fetchActivityFeatureTables(tables, features)

  casesWithPrepostActivity <- findCasesWithPrepostActivity(demo, featureTables, window)

  lapply(casesWithPrepostActivity, function(healthCode) {
    message(healthCode)
    try(getVisData(healthCode, featureNames, featureTables, window, demo))
  })
}
