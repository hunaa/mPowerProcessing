# Test for process_survey_v1
# 
# Author: bhoff
###############################################################################

require(testthat)
require(synapseClient)

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



