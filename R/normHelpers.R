GetAgeMatchedControlIds <- function(patientAge, ageInterval = 5, demo){
  controlAges <- demo[which(!demo$`professional-diagnosis`), c("healthCode", "age")]
  controlAges[which(controlAges$age <= 17), "age"] <- NA
  controlAges[which(controlAges$age >= 115), "age"] <- NA
  idx1 <- which(controlAges$age >= (patientAge - ageInterval))
  idx2 <- which(controlAges$age <= (patientAge + ageInterval))
  idx <- intersect(idx1, idx2)
  return(controlAges$healthCode[idx])
}

GetControlFeatureSummaryStats <- function(dat, controlIds, featName){
  idx <- dat$healthCode %in% controlIds
  pdat <- dat[idx, featName]
  res <- list(median = median(pdat, na.rm = TRUE),
              mean = mean(pdat, na.rm = TRUE),
              sd = sd(pdat, na.rm = TRUE),
              nControls = length(controlIds),
              nTasks = length(pdat))
  return(res)
}

#' Normalize feature data relative to an age matched control.
#'
#' @param dat data.frame of feature data for an activity
#' @param patientId patient's healthCode
#' @param featName name of feature column
#' @param demo data.frame holding demographic data
#' @param ageInterval age interval of controls
#' @param floorCeilingRange
#' @param standardDeviations
#' @param reverse
#'
#' @return a list of fdat, controlMean, controlUpper and controlLower where
#'         fdat is a data.frame with columns "medTimepoint", "date",
#'         and featName, with the feature column normalized to fall between
#'         0 and 1.
NormalizeFeature <- function(dat,
                             patientId,
                             featName,
                             demo,
                             ageInterval,
                             floorCeilingRange = 1,
                             standardDeviations = 1,
                             reverse = FALSE){
  patientAge <- demo$age[match(patientId, demo$healthCode)]
  controlIds <- GetAgeMatchedControlIds(patientAge, ageInterval, demo)
  controlStats <- GetControlFeatureSummaryStats(dat, controlIds, featName)
  fdat <- dat[dat$healthCode %in% patientId, c("medTimepoint", "date", featName)]
  z <- (fdat[, featName] - controlStats$mean)/controlStats$sd
  alpha <- (1 - floorCeilingRange)/2
  q <- quantile(c(z, -standardDeviations, standardDeviations), c(alpha, 1 - alpha), na.rm=TRUE)
  ql <- as.numeric(q[1])
  qu <- as.numeric(q[2])
  zstar <- pmax(pmin(z, qu), ql)
  y <- (zstar - ql)/(qu - ql)
  controlMean <- (0 - ql)/(qu - ql)
  controlUpper <- (standardDeviations - ql)/(qu - ql)
  controlLower <- (-standardDeviations - ql)/(qu - ql)
  if (reverse) {
    y <- 1 - y
    controlMean <- 1 - controlMean
    controlUpper <- 1 - controlUpper
    controlLower <- 1 - controlLower
  }
  fdat[, featName] <- y
  res <- list(fdat = fdat, 
              controlMean = controlMean, 
              controlUpper = controlUpper, 
              controlLower = controlLower)
  return(res)
}


# recategorize medTimepoint as 'pre', 'post' or 'other'
categorizeTimepoints <- function(timepoints) {
  result <- vector(mode="character", length=length(timepoints))
  result[timepoints=='Immediately before Parkinson medication'] <- 'pre'
  result[timepoints=='Just after Parkinson medication (at your best)'] <- 'post'
  result[result==""] <- 'other'
  return(result)
}

# count the activities performed by the given participant within the
# date window at the given pre/post medication timepoint
countParticipantActivity <- function(activity, dat, healthCode, window) {
  dat1 <- dat[ dat$healthCode==healthCode & dat$date >= window$start & dat$date <= window$end, ]
  pre <- sum(na.omit(dat1$medTimepoint=='Immediately before Parkinson medication'))
  post <- sum(na.omit(dat1$medTimepoint=='Just after Parkinson medication (at your best)'))
  other <- nrow(dat1) - pre - post
  data.frame(activity=activity,
             medTimepoint=c('pre','post','other'),
             count=c(pre, post, other))
}


## Return a list of data frames, one per PD patient, which counts activities
## performed within the date window. For example:
##
##     $`43294479-32e0-4589-92ee-370b47a81e57`
##               activity medTimepoint count
##     balance.1  balance          pre    17
##     balance.2  balance         post     9
##     balance.3  balance        other     1
##     gait.1        gait          pre    17
##     gait.2        gait         post     9
##     gait.3        gait        other     1
##     tap.1          tap          pre    19
##     tap.2          tap         post    12
##     tap.3          tap        other     1
##     voice.1      voice          pre    19
##     voice.2      voice         post    11
##     voice.3      voice        other     1
##
countActivity <- function(demo, featureTables, window) {
  # find participants with a PD diagnosis
  cases <- na.omit(demo$healthCode[demo$`professional-diagnosis`])
  names(cases) <- cases

  # for each PD patient, return a data.frame with counts of activities
  # performed within the date window
  lapply(cases, function(healthCode) {
    message(healthCode)
    do.call(rbind,
            mapply(countParticipantActivity, names(featureTables), featureTables,
                   MoreArgs=list(healthCode=healthCode, window=window),
                   SIMPLIFY=FALSE))
  })
}

## filter activity count to those with activities performed at
## pre or post medication time points.
countPrepostActivity <- function(demo, featureTables, window) {
  activityCounts <- countActivity(demo, featureTables, window)
  Filter(function(df) {
           sum(df$count[df$medTimepoint %in% c('pre','post')]) > 0
         }, activityCounts)
}

## return a list of health codes of cases with activities performed
## at pre or post medication time points.
reallySlowFindCasesWithPrepostActivity <- function(demo, featureTables, window) {
  activityCounts <- countPrepostActivity(demo, featureTables, window)
  healthCodes <- names(activityCounts)
  names(healthCodes) <- healthCodes
  return(healthCodes)
}

#' Find cases who have performed activities withing the date
#' window and at pre or post medication time points
#'
#' @param lists a list of lists
#' @return a list of health codes
findCasesWithPrepostActivity <- function(demo, featureTables, window) {
  # find participants with a PD diagnosis
  cases <- na.omit(demo$healthCode[demo$`professional-diagnosis` & !is.na(demo$age)])
  names(cases) <- cases

  prepost <- c('Immediately before Parkinson medication',
               'Just after Parkinson medication (at your best)')

  unique(unlist(
    lapply(featureTables, function(dat) {
      ## subset the data frame keeping activities performed by PD
      ## patients, within the date window, pre or post medication
      dat$healthCode[ dat$date >= window$start &
                      dat$date <= window$end &
                      dat$healthCode %in% cases &
                      dat$medTimepoint %in% prepost ]
    })
  ))
}
