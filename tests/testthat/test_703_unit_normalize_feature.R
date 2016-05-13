# Test normalization - test normalizeFeature()
# 
# Author: J. Christopher Bare
###############################################################################

require(testthat)

context("test_unit_normalization")


#-----------------------------------------------------------
# Helper functions
#-----------------------------------------------------------

# count the activities performed by the given participant within the
# date window at the given pre/post medication timepoint
countParticipantActivityDays <- function(activity, dat, healthCode, window) {
  dat1 <- dat[ dat$healthCode==healthCode & dat$date >= window$start & dat$date <= window$end, ]
  pre_days  <- unique(dat1$date[!is.na(dat1$medTimepoint) & dat1$medTimepoint=='Immediately before Parkinson medication'])
  post_days <- unique(dat1$date[!is.na(dat1$medTimepoint) & dat1$medTimepoint=='Just after Parkinson medication (at your best)'])
  other <- length( setdiff(setdiff(unique(dat1$date), pre_days), post_days) )
  data.frame(activity=activity,
             medTimepoint=c('pre','post','other'),
             days=c(length(pre_days), length(post_days), other))
}

## Return a list of data frames, one per PD patient, which counts activities
## performed within the date window. For example:
##
##     $`43294479-32e0-4589-92ee-370b47a81e57`
##               activity medTimepoint  days
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
  cases <- na.omit(demo$healthCode[demo$`professional-diagnosis` & !is.na(demo$age)])
  names(cases) <- cases

  # for each PD patient, return a data.frame with counts of activities
  # performed within the date window
  lapply(cases, function(healthCode) {
    message(healthCode)
    do.call(rbind,
            mapply(countParticipantActivityDays, names(featureTables), featureTables,
                   MoreArgs=list(healthCode=healthCode, window=window),
                   SIMPLIFY=FALSE))
  })
}

## filter activity count to those with activities performed at
## pre or post medication time points.
countPrepostActivity <- function(demo, featureTables, window) {
  activityCounts <- countActivity(demo, featureTables, window)
  Filter(function(df) {
           sum(df$days[df$medTimepoint %in% c('pre','post')]) > 0
         }, activityCounts)
}


#-----------------------------------------------------------
# Test set-up and tests
#-----------------------------------------------------------


timepoints <- c('Immediately before Parkinson medication',
                'Just after Parkinson medication (at your best)',
                'Another time',
                NA)
dates <- as.Date(sapply(1:30, function(d) sprintf('2015-04-%02d',d)))
cases <- sprintf('test-%d', c(90050, 90060, 90080))
controls <- sprintf('test-%d', 10020:10080)

## create bogus demographics table with cases and controls
demo <- data.frame(
  healthCode=c(cases, controls),
  `professional-diagnosis`=c(rep(TRUE, length(cases)), rep(FALSE, length(controls))),
  age=c(50,60,80,20:80),
  check.names=FALSE,
  stringsAsFactors=FALSE)


## test that age matched controls are in the expected range
expect_equal(GetAgeMatchedControlIds(50, ageInterval=5, demo=demo), sprintf('test-100%02d', 45:55))
expect_equal(GetAgeMatchedControlIds(79, ageInterval=5, demo=demo), sprintf('test-100%02d', 74:80))


## controls
n <- 10000
i <- sample(1:61, n, replace=TRUE)
dat_controls <- data.frame(
  healthCode=controls[i],
  medTimepoint=sample(timepoints, n, replace=TRUE),
  date=sample(dates, n, replace=TRUE),
  featureX=(rbeta(n, 3, 3) - 0.5) * 9 + 90 - 0.5*(i-1))

## cases pre meds
n <- 100
dat_cases_pre <- data.frame(
  healthCode=sample(cases, n, replace=TRUE),
  medTimepoint='Immediately before Parkinson medication',
  date=sample(dates, n, replace=TRUE),
  featureX=(rbeta(n, 3, 3) - 0.5) * 9 + 20)

## cases post meds
n <- 100
dat_cases_post <- data.frame(
  healthCode=sample(cases, n, replace=TRUE),
  medTimepoint='Just after Parkinson medication (at your best)',
  date=sample(dates, n, replace=TRUE),
  featureX=(rbeta(n, 3, 3) - 0.5) * 9 + 50)

## cases other time points
n <- 100
dat_cases_other <- data.frame(
  healthCode=sample(cases, n, replace=TRUE),
  medTimepoint=sample(timepoints[3:4], n, replace=TRUE),
  date=sample(dates, n, replace=TRUE),
  featureX=(rbeta(n, 3, 3) - 0.5) * 9 + 40)

dat <- rbind(dat_controls, dat_cases_pre, dat_cases_post, dat_cases_other)
dat <- dat[order(dat$date),]
normDat <- dat
normDat$featureX <- normDat$featureX + 7

## test age matched controls
## We know the mean value of the controls will fall in specific
## ranges relative to age because the data is built that way in
## the definition of dat_controls.
controlIds <- GetAgeMatchedControlIds(patientAge=20, ageInterval=5, demo=demo)
controlStats <- GetControlFeatureSummaryStats(dat, controlIds, 'featureX')
expect_true(abs(controlStats$mean - 90) < 5)

controlIds <- GetAgeMatchedControlIds(patientAge=40, ageInterval=5, demo=demo)
controlStats <- GetControlFeatureSummaryStats(dat, controlIds, 'featureX')
expect_true(abs(controlStats$mean - 80) < 5)

controlIds <- GetAgeMatchedControlIds(patientAge=60, ageInterval=5, demo=demo)
controlStats <- GetControlFeatureSummaryStats(dat, controlIds, 'featureX')
expect_true(abs(controlStats$mean - 70) < 5)

controlIds <- GetAgeMatchedControlIds(patientAge=80, ageInterval=5, demo=demo)
controlStats <- GetControlFeatureSummaryStats(dat, controlIds, 'featureX')
## use 61.25 here because 80 is the max age for the controls so
## we're getting controls between 75 and 80.
expect_true(abs(controlStats$mean - 61.25) < 5)


## Count days on which pre and post activity occurs
featureTables <- list(foo=dat)
window <- list(start=as.Date('2015-04-01'), end=as.Date('2015-04-30'))
cppa <- countPrepostActivity(demo, featureTables, window)


## test normalization
##   * test that the pre values are less than post values as they are
##     contrived to be in the bogus data created above.
##   * test that the counts of days with pre and post data are as expected
for (healthCode in cases) {
  message('testing ', healthCode)
  norm <- NormalizeFeature(dat,
                           dat,
                           patientId=healthCode,
                           featName='featureX',
                           demo=demo,
                           ageInterval=5)
  mpre <- mean(norm$fdat$featureX[norm$fdat$medTimepoint==timepoints[1]], na.rm=TRUE)
  mpost <- mean(norm$fdat$featureX[norm$fdat$medTimepoint==timepoints[2]], na.rm=TRUE)
  expect_true(mpre < mpost)

  norms <- list(foo=norm)
  tnorms <- transformNormalizedData(norms, window)
  expect_equal(sum(!is.na(tnorms$foo$fdat$pre)),
               cppa[[healthCode]]$days[ cppa[[healthCode]]$medTimepoint=='pre' ])
  expect_equal(sum(!is.na(tnorms$foo$fdat$post)),
               cppa[[healthCode]]$days[ cppa[[healthCode]]$medTimepoint=='post' ])
  
  ## TEST WITH A DIFFERENT NORMALIZATION DATASET (SAME FEATURE)
  ## NORM DATA IS CONSISTENTLY LARGER - SO NORMALIZED DATA SHOULD BE SMALLER
  normLR <- NormalizeFeature(dat,
                             normDat,
                             patientId=healthCode,
                             featName='featureX',
                             demo=demo,
                             ageInterval=5)
  mpreLR <- mean(normLR$fdat$featureX[norm$fdat$medTimepoint==timepoints[1]], na.rm=TRUE)
  mpostLR <- mean(normLR$fdat$featureX[norm$fdat$medTimepoint==timepoints[2]], na.rm=TRUE)
  expect_true(mpre > mpreLR)
  expect_true(mpost > mpostLR)
}

