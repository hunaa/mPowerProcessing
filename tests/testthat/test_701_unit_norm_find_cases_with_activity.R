# Test normalization - find Cases With Pre/post Activity 
# 
# Author: J. Christopher Bare
###############################################################################

require(testthat)

context("test_unit_normalization")

## fake demographics table
demo <- data.frame(
  healthCode=c(
    'test-10001',
    'test-10002',
    'test-10003',
    'test-10004',
    'test-10005',
    'test-10006',
    'test-10007',
    'test-10008'),
  `professional-diagnosis`=c(
    FALSE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE),
  age=c(
    47, NA, 55, 66, 77, 81, 88, 89),
  check.names=FALSE,
  stringsAsFactors=FALSE)

## fake feature data
featureTables <- list(
  tap=data.frame(
    date=c(
      as.Date('2015-04-24'),
      as.Date('2015-04-24'),
      as.Date('2015-04-28'),
      as.Date('2015-05-15'),
      as.Date('2015-05-28'),
      as.Date('2015-04-28')),
    healthCode=c(
      'test-10001',
      'test-10002',
      'test-10003',
      'test-10004',
      'test-10005',
      'test-10006'),
    medTimepoint=c(
      'Immediately before Parkinson medication',
      'Just after Parkinson medication (at your best)',
      'Another time',
      'Just after Parkinson medication (at your best)',
      'Just after Parkinson medication (at your best)',
      'Just after Parkinson medication (at your best)'),
    featureX=rnorm(6),
    stringsAsFactors=FALSE),
  voice=data.frame(
    date=c(
      as.Date('2015-04-14'),
      as.Date('2015-04-24'),
      as.Date('2015-04-28'),
      as.Date('2015-04-29'),
      as.Date('2015-05-13'),
      as.Date('2015-04-24'),
      as.Date('2015-04-28'),
      as.Date('2015-04-28'),
      as.Date('2015-04-28')),
    healthCode=c(
      'test-10001',
      'test-10002',
      'test-10003',
      'test-10004',
      'test-10005',
      'test-10006',
      'test-10007',
      'test-10007',
      'test-10007'),
    medTimepoint=c(
      'Another time',
      'Immediately before Parkinson medication',
      'Another time',
      NA,
      NA,
      'Another time',
      NA,
      'Another time',
      'Immediately before Parkinson medication'),
    featureY=rnorm(9),
    stringsAsFactors=FALSE),
  balance=data.frame(
    date=c(
      as.Date('2015-04-01'),
      as.Date('2015-04-02'),
      as.Date('2015-04-03'),
      as.Date('2015-04-04'),
      as.Date('2015-04-17'),
      as.Date('2015-04-28'),
      as.Date('2015-04-28')),
    healthCode=c(
      'test-10001',
      'test-10002',
      'test-10003',
      'test-10004',
      'test-10005',
      'test-10006',
      'test-10007'),
    medTimepoint=c(
      'Just after Parkinson medication (at your best)',
      'Immediately before Parkinson medication',
      NA,
      'Another time',
      'Another time',
      'Immediately before Parkinson medication',
      'Another time'),
    featureZ=rnorm(7),
    stringsAsFactors=FALSE))

## define date window
window <- list(start=as.Date("2015-04-01"), end=as.Date("2015-04-30"))

cases <- findCasesWithPrepostActivity(demo, featureTables, window)

cat("\nfound:", cases,"\n")

## we expect to get back these two
expected_cases = c('test-10006', 'test-10007')
names(expected_cases) <- expected_cases

expect_equal(cases, expected_cases)

## The other health codes shouldn't be returned for these reasons
## test-10001 Not a PD patient
## test-10002 age is NA
## test-10003 no pre or post medication
## test-10004 no pre or post inside window
## test-10005 no pre or post inside window
## test-10008 no activity

