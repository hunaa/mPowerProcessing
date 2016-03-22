# Test normalization - transform normalized feature data 
# 
# Author: J. Christopher Bare
###############################################################################

require(testthat)
require(synapseClient)

context("test_unit_normalization")

pre <- 'Immediately before Parkinson medication'
post <- 'Just after Parkinson medication (at your best)'

## test that transformNormalizedData correctly handles merging of dates,
## missing dates, and missing time point
norms <- list(
  'balance'=list(
    fdat=data.frame(
      date=as.Date(c("2015-04-1", "2015-04-24", "2015-04-24", "2015-04-28")),
      medTimepoint=c(pre, pre, post, post),
      featureX=c(0.1,0.2,0.3,0.4)),
    controlMean = 0.25,
    controlUpper = 0.9960938,
    controlLower = 0.00390625),
  'gait'=list(
    fdat=data.frame(
      date=as.Date(c(NA, "2015-04-24", "2015-04-24", NA)),
      medTimepoint=c(pre, pre, post, post),
      featureX=c(0.1,0.2,0.3,0.4)),
    controlMean = 0.5,
    controlUpper = 0.8,
    controlLower = 0.2),
  'tap'=list(
    fdat=data.frame(
      date=as.Date(c("2015-04-1", "2015-04-24", "2015-04-24", "2015-04-28")),
      medTimepoint=c(pre, pre, post, NA),
      featureX=c(0.1,0.2,0.3,0.4)),
    controlMean = 0.5,
    controlUpper = 0.7,
    controlLower = 0.3),
  'voice'=list(
    fdat=data.frame(
      date=as.Date(c("2015-04-24", "2015-04-24", "2015-04-24", "2015-04-24")),
      medTimepoint=c(pre, pre, post, post),
      featureX=c(0.1,0.2,0.3,0.4)),
    controlMean = 0.5,
    controlUpper = 0.6,
    controlLower = 0.4))

window <- list(start=as.Date('2015-04-01'), end=as.Date('2015-04-30'))
tnorms <- transformNormalizedData(norms, window)

expect_equal(tnorms$balance$controlMean, 0.25)
expect_equal(tnorms$balance$controlUpper, 0.9960938)
expect_equal(tnorms$balance$controlLower, 0.00390625)

expect_equal(tnorms$balance$fdat$date, as.Date(c("2015-04-1", "2015-04-24", "2015-04-28")))
expect_equal(tnorms$balance$fdat$pre,  c(0.1, 0.2, NA))
expect_equal(tnorms$balance$fdat$post, c(NA, 0.3, 0.4))

expect_equal(nrow(tnorms$gait$fdat), 1)
expect_equal(as.list(tnorms$gait$fdat[1,]), list(date=as.Date("2015-04-24"), pre=0.2, post=0.3))

expect_equal(nrow(tnorms$tap$fdat), 2)

## TODO: how multiple activities on the same date are handled is not yet specified
