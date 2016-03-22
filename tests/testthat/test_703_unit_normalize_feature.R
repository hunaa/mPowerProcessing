# Test normalization - test normalizeFeature()
# 
# Author: J. Christopher Bare
###############################################################################

require(testthat)
require(synapseClient)

context("test_unit_normalization")


timepoints <- c('Immediately before Parkinson medication',
                'Just after Parkinson medication (at your best)',
                'Another time',
                NA)
dates <- as.Date(sapply(1:30, function(d) sprintf('2015-04-%02d',d)))
cases <- sprintf('test-%d', c(90050, 90060, 90080))
controls <- sprintf('test-%d', 10020:10080)

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


## test normalization
## TODO the tests here are a little weak, just that the pre values are less
##      than post values as they are contrived to be in the bogus data created
##      above. Ideally, we'd have tests resulting in specific expected values.
norm <- NormalizeFeature(dat,
                 patientId='test-90050',
                 featName='featureX',
                 demo=demo,
                 ageInterval=5)
mpre <- mean(norm$fdat$featureX[norm$fdat$medTimepoint==timepoints[1]], na.rm=TRUE)
mpost <- mean(norm$fdat$featureX[norm$fdat$medTimepoint==timepoints[2]], na.rm=TRUE)
expect_true(mpre < mpost)

norm <- NormalizeFeature(dat,
                 patientId='test-90060',
                 featName='featureX',
                 demo=demo,
                 ageInterval=5)
mpre <- mean(norm$fdat$featureX[norm$fdat$medTimepoint==timepoints[1]], na.rm=TRUE)
mpost <- mean(norm$fdat$featureX[norm$fdat$medTimepoint==timepoints[2]], na.rm=TRUE)
expect_true(mpre < mpost)

norm <- NormalizeFeature(dat,
                 patientId='test-90080',
                 featName='featureX',
                 demo=demo,
                 ageInterval=5)
mpre <- mean(norm$fdat$featureX[norm$fdat$medTimepoint==timepoints[1]], na.rm=TRUE)
mpost <- mean(norm$fdat$featureX[norm$fdat$medTimepoint==timepoints[2]], na.rm=TRUE)
expect_true(mpre < mpost)

