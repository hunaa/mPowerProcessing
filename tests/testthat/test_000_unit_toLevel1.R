# Test for process_survey_v1
# 
# Author: bhoff
###############################################################################

require(testthat)
require(synapseClient)
require(uuid)

context("test_unit_toLevel1")

# Test mergeDataFrames
current<-data.frame(foo=c(4,3,2,1), bar=c("a", "b", "c", "d"), stringsAsFactors=FALSE)
rownames(current)<-c("A", "B", "C", "D")
new<-data.frame(foo=c(5,1,3), bar=c("x", "y", "z"), stringsAsFactors=FALSE)
rownames(new)<-c("X", "Y", "Z")
result<-mergeDataFrames(current, new, "foo")
expected<-data.frame(foo=c(4,3,2,1,5), bar=c("a", "z", "c", "y", "x"), stringsAsFactors=FALSE)
rownames(expected)<-c("A", "B", "C", "D", "X")
expect_equal(result, expected)

# test disjoint case (no merge)
current<-data.frame(foo=c(4,3,2,1), bar=c("a", "b", "c", "d"), stringsAsFactors=FALSE)
rownames(current)<-c("A", "B", "C", "D")
new<-data.frame(foo=c(5,6,7), bar=c("x", "y", "z"), stringsAsFactors=FALSE)
rownames(new)<-c("X", "Y", "Z")
result<-mergeDataFrames(current, new, "foo")
expected<-data.frame(foo=c(4,3,2,1,5,6,7), bar=c("a", "b", "c", "d", "x", "y", "z"), stringsAsFactors=FALSE)
rownames(expected)<-c("A", "B", "C", "D", "X", "Y", "Z")
expect_equal(result, expected)

current<-data.frame(foo=c(4,3,2,1), bar=c("a", "b", "c", "d"), stringsAsFactors=FALSE)
rownames(current)<-c("A", "B", "C", "D")
# can't have multiple rows in 'new' that match some row in 'current'
new<-data.frame(foo=c(5,1,3,1), bar=c("x", "y", "z", "w"), stringsAsFactors=FALSE)
rownames(new)<-c("X", "Y", "Z", "W")
# this is an error
expect_error(mergeDataFrames(current, new, "foo"))

# can't have multiple rows in 'current' that match some row in 'new'
current<-data.frame(foo=c(4,3,2,1,1), bar=c("a", "b", "c", "d", "e"), stringsAsFactors=FALSE)
rownames(current)<-c("A", "B", "C", "D", "E")
new<-data.frame(foo=c(5,1,3), bar=c("x", "y", "z"), stringsAsFactors=FALSE)
rownames(new)<-c("X", "Y", "Z")
# this is an error
expect_error(mergeDataFrames(current, new, "foo"))

# another case of unallowed duplicates
current<-data.frame(foo=c(4,3,2,1,1), bar=c("a", "b", "c", "d", "e"), stringsAsFactors=FALSE)
rownames(current)<-c("A", "B", "C", "D", "E")
new<-data.frame(foo=c(5,1,3,1), bar=c("x", "y", "z", "w"), stringsAsFactors=FALSE)
rownames(new)<-c("X", "Y", "Z", "W")
expect_error(mergeDataFrames(current, new, "foo"))

# test what happens when one or the other data frame is empty
current<-data.frame(foo=c(4,3,2,1), bar=c("a", "b", "c", "d"), stringsAsFactors=FALSE)
rownames(current)<-c("A", "B", "C", "D")
new<-data.frame()
result<-mergeDataFrames(current, new, "foo")
expected<-current
expect_equal(result, expected)

current<-data.frame()
new<-data.frame(foo=c(5,1,3), bar=c("x", "y", "z"), stringsAsFactors=FALSE)
rownames(new)<-c("X", "Y", "Z")
result<-mergeDataFrames(current, new, "foo")
expected<-new
expect_equal(result, expected)

# what happens if the columns don't match
current<-data.frame(foo=c(4,3,2,1), bar=c("a", "b", "c", "d"), stringsAsFactors=FALSE)
rownames(current)<-c("A", "B", "C", "D")
new<-data.frame(foo=c(5,6,7), baz=c("x", "y", "z"), stringsAsFactors=FALSE)
rownames(new)<-c("X", "Y", "Z")
expect_error(mergeDataFrames(current, new, "foo"))

# test incompatible column order
current<-data.frame(foo=c(4,3,2,1), bar=c("a", "b", "c", "d"), stringsAsFactors=FALSE)
rownames(current)<-c("A", "B", "C", "D")
new<-data.frame(bar=c("x", "y", "z"), foo=c(5,1,3), stringsAsFactors=FALSE)
rownames(new)<-c("X", "Y", "Z")
result<-mergeDataFrames(current, new, "foo")
expected<-data.frame(foo=c(4,3,2,1,5), bar=c("a", "z", "c", "y", "x"), stringsAsFactors=FALSE)
rownames(expected)<-c("A", "B", "C", "D", "X")
expect_equal(result, expected)

# OK if 'new' has an extra column:
current<-data.frame(foo=c(4,3,2,1), bar=c("a", "b", "c", "d"), stringsAsFactors=FALSE)
rownames(current)<-c("A", "B", "C", "D")
new<-data.frame(bar=c("x", "y", "z"), foo=c(5,1,3), bar=c(9,7,8), stringsAsFactors=FALSE)
rownames(new)<-c("X", "Y", "Z")
result<-mergeDataFrames(current, new, "foo")
expected<-data.frame(foo=c(4,3,2,1,5), bar=c("a", "z", "c", "y", "x"), stringsAsFactors=FALSE)
rownames(expected)<-c("A", "B", "C", "D", "X")
expect_equal(result, expected)

# NOT OK if 'new' is missing a column that 'current' has
current<-data.frame(foo=c(4,3,2,1), bar=c("a", "b", "c", "d"), stringsAsFactors=FALSE)
rownames(current)<-c("A", "B", "C", "D")
new<-data.frame(foo=c(5,1,3), stringsAsFactors=FALSE)
rownames(new)<-c("X", "Y", "Z")
expect_error(mergeDataFrames(current, new, "foo"))

#####
## Test the subsetThis function
#####
coreNames <- c("recordId", "healthCode", "createdOn", "appVersion", "phoneInfo")
releaseVersions <- c("version 1.0, build 7", "version 1.0.5, build 12", "version 1.1, build 22", "version 1.2, build 31", "version 1.3, build 42")

## CONSTRUCT A 7 ROW DATA FRAME TO START EACH TEST WITH - with all 'valid' data
current <- data.frame(recordId=generateUuids(7),
                      healthCode=generateUuids(7),
                      createdOn=c(rep(as.Date("2016-01-03"), 7)),
                      appVersion=c(releaseVersions, releaseVersions[1:2]),
                      phoneInfo=rep("iPhone awesome", 7),
                      foo=c('A', 'B', 'C', 'D', 'E', 'F', 'G'), 
                      bar=c(1:7), 
                      stringsAsFactors = FALSE)
expect_equal(subsetThis(current), current)

## Test all NAs in a non-core columns
preDf <- current
preDf[6, c("foo", "bar")] <- NA
expect_equal(subsetThis(preDf), preDf[-6, ])

## Test removal of old date
preDf <- current
preDf$createdOn[3] <- as.Date("2014-01-04")
expect_equal(subsetThis(preDf), preDf[-3, ])

## Test removal of app version
preDf <- current
preDf$appVersion[5:6] <- "this awesome"
expect_equal(subsetThis(preDf), preDf[-c(5:6), ])

## Test removal of healthCodes ('theseOnes')
preDf <- current
expect_equal(subsetThis(preDf, preDf$healthCode[2]), preDf[-2, ])

## Test removal of duplicates (remove the second one, keep first)
preDf <- current
preDf[2, c("healthCode", "createdOn")] <- preDf[1, c("healthCode", "createdOn")]
expect_equal(subsetThis(preDf), preDf[-2, ])

## Test the ordering of createdOn
preDf <- current
preDf$createdOn[4] <- as.Date("2016-01-02")
expect_equal(subsetThis(preDf), preDf[c(4, 1:3, 5:7), ])
  