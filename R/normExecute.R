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


getVisData <- function(healthCode, featureNames, featureTables, windowStart, windowEnd) {
  activityTypes <- names(featureTables)
  norms <- lapply(activityTypes, function(activity) {
    NormalizeFeature(featureTables[[activity]], healthCode, featureNames[[activity]], demo, ageInterval=5)
  })
  names(norms) <- activityTypes

  # Return a list of data.frames, one per activity, with columns "date",
  # "pre" and "post"
  norms <- lapply(norms, function(x) {
    tmp <- x$fdat
    tmp <- tmp[ tmp$date >= windowStart & tmp$date <= windowEnd, ]
    tmp <- tmp[!duplicated(tmp[, c("date", "medTimepoint")]), ]
    pre <- tmp[tmp$medTimepoint == "Immediately before Parkinson medication", ]
    pre$medTimepoint <- NULL
    names(pre) <- c("date", "pre")
    post <- tmp[tmp$medTimepoint == "Just after Parkinson medication (at your best)", ]
    post$medTimepoint <- NULL
    names(post) <- c("date", "post")
    df <- data.frame(date=seq(windowStart, windowEnd, by="day"), 
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

prepost <- c("Immediately before Parkinson medication", "Just after Parkinson medication (at your best)")

# demographics table
demoTb <- synTableQuery(sprintf("SELECT * FROM %s", tables$demographics))
demo <- demoTb@values

# find participants with a PD diagnosis
cases <- na.omit(demo$healthCode[demo$`professional-diagnosis`])
names(cases) <- cases

# date window
window <- list(start=as.Date("2015-05-01")-29, end=as.Date("2015-05-01"))


categorizeTimepoints <- function(timepoints) {
  result <- vector(mode="character", length=length(timepoints))
  result[timepoints=='Immediately before Parkinson medication'] <- 'pre'
  result[timepoints=='Just after Parkinson medication (at your best)'] <- 'post'
  result[result==""] <- 'other'
  return(result)
}

# count the activities performed by the given participant within the
# date window at the given pre/post medication timepoint
countActivities <- function(activity, dat, healthCode, window) {
  dat1 <- dat[ dat$healthCode==healthCode & dat$date >= window$start & dat$date <= window$end, ]
  pre <- sum(na.omit(dat1$medTimepoint=='Immediately before Parkinson medication'))
  post <- sum(na.omit(dat1$medTimepoint=='Just after Parkinson medication (at your best)'))
  other <- nrow(dat1) - pre - post
  data.frame(activity=activity,
             medTimepoint=c('pre','post','other'),
             count=c(pre, post, other))
}

# for all PD patients, return a data.frame with counts of each activity
# performed within the date window
activityCounts <- lapply(cases, function(healthCode) {
  do.call(rbind,
          mapply(countActivities, names(featureTables), featureTables,
                 MoreArgs=list(healthCode=healthCode, window=window),
                 SIMPLIFY=FALSE))
})

nonzeroActivityCounts <- Filter(function(df) {
  sum(df$count[df$medTimepoint %in% c('pre','post')]) > 0
  }, activityCounts)

cases_with_activity <- names(nonzero_activity_counts)
names(cases_with_activity) <- names(cases_with_activity)

normalizedFeatures <- lapply(cases_with_activity[1:10], function(healthCode) {
  cat(healthCode, "\n")
  try(getVisData(healthCode, featureNames, featureTables, windowStart, windowEnd))
})



