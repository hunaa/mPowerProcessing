# Test normalization - test normalizeFeature()
# 
# Author: Hoff
###############################################################################

library(testthat)
library(mPowerStatistics)

context("test_unit_tapping_feature")



lastProcessedVersion<-c("syn123"="1")
cleanDataTableId<-"syn000"
featureTableId<-"syn999"
cleanedTappingData<-data.frame(
		recordId=c("AA", "BB", "CC"),
		"tapping_results.json.TappingSamples"=c("111", "222", "333"),
		"tapping_left.json.TappingSamples"=c("444", "555", "666"),
		"tapping_right.json.TappingSamples"=c("777", "888", "999"),
		stringsAsFactors=F
)
rownames(cleanedTappingData)<-c("1_1", "2_1", "3_8")
queryResults<-Table("syn000", cleanedTappingData)

with_mock(
		synTableQuery=function(sql) {
			tableId<-getIdFromSql(sql)
			if (tableId=="syn000") {
				queryResults
			} else if (tableId=="syn999") {
				Table("syn999", data.frame())
			} else {
				stop(paste0("Unexpected table id <", tableId, ">"))
			}
		},
		synDownloadTableColumns=function(table, columns){c("111"="file1", "222"="file2", "333"="file3")},
		fromJSON=function(file){"[]"},
		tappingCountStatistic=function(data){as.integer(7)},
		synStore=function(table) {
			expect_equal(table@schema, "syn999")
			expect_equal(table@values$recordId, c("AA", "BB", "CC"))
			expect_equal(table@values[["is_computed"]], c(T,T,T))
			expect_equal(table@values[["tap_count"]], c(as.integer(7),as.integer(7),as.integer(7)))
		},
		{
			newLastProcessedVersion<-computeTappingFeatures(cleanDataTableId, lastProcessedVersion, featureTableId)
			expect_equal(newLastProcessedVersion, 8)
		}
)

## FOR LEFT
with_mock(
  synTableQuery=function(sql) {
    tableId<-getIdFromSql(sql)
    if (tableId=="syn000") {
      queryResults
    } else if (tableId=="syn999") {
      Table("syn999", data.frame())
    } else {
      stop(paste0("Unexpected table id <", tableId, ">"))
    }
  },
  synDownloadTableColumns=function(table, columns){c("444"="file1", "555"="file2", "666"="file3")},
  fromJSON=function(file){"[]"},
  tappingCountStatistic=function(data){as.integer(7)},
  synStore=function(table) {
    expect_equal(table@schema, "syn999")
    expect_equal(table@values$recordId, c("AA", "BB", "CC"))
    expect_equal(table@values[["is_computed"]], c(T,T,T))
    expect_equal(table@values[["tap_count"]], c(as.integer(7),as.integer(7),as.integer(7)))
  },
  {
    newLastProcessedVersion<-computeTappingFeatures(cleanDataTableId, lastProcessedVersion, featureTableId, hand="left")
    expect_equal(newLastProcessedVersion, 8)
  }
)


## FOR RIGHT
with_mock(
  synTableQuery=function(sql) {
    tableId<-getIdFromSql(sql)
    if (tableId=="syn000") {
      queryResults
    } else if (tableId=="syn999") {
      Table("syn999", data.frame())
    } else {
      stop(paste0("Unexpected table id <", tableId, ">"))
    }
  },
  synDownloadTableColumns=function(table, columns){c("777"="file1", "888"="file2", "999"="file3")},
  fromJSON=function(file){"[]"},
  tappingCountStatistic=function(data){as.integer(7)},
  synStore=function(table) {
    expect_equal(table@schema, "syn999")
    expect_equal(table@values$recordId, c("AA", "BB", "CC"))
    expect_equal(table@values[["is_computed"]], c(T,T,T))
    expect_equal(table@values[["tap_count"]], c(as.integer(7),as.integer(7),as.integer(7)))
  },
  {
    newLastProcessedVersion<-computeTappingFeatures(cleanDataTableId, lastProcessedVersion, featureTableId, hand="right")
    expect_equal(newLastProcessedVersion, 8)
  }
)

 ## All NAs
 
 cleanedTappingData<-data.frame(
		 recordId=c("AA", "BB", "CC"),
		 "tapping_results.json.TappingSamples"=c(NA, NA, NA),
		 "tapping_left.json.TappingSamples"=c(NA, NA, NA),
		 "tapping_right.json.TappingSamples"=c(NA, NA, NA),
		 stringsAsFactors=F
 )
 rownames(cleanedTappingData)<-c("1_1", "2_1", "3_8")
 queryResults<-Table("syn000", cleanedTappingData)
 
 with_mock(
		 synTableQuery=function(sql) {
			 tableId<-getIdFromSql(sql)
			 if (tableId=="syn000") {
				 queryResults
			 } else if (tableId=="syn999") {
				 Table("syn999", data.frame())
			 } else {
				 stop(paste0("Unexpected table id <", tableId, ">"))
			 }
		 },
		 synDownloadTableColumns=function(table, columns){c("111"="file1", "222"="file2", "333"="file3")},
		 fromJSON=function(file){"[]"},
		 tappingCountStatistic=function(data){as.integer(7)},
		 synStore=function(table) {
			 expect_equal(table@schema, "syn999")
			 expect_equal(table@values$recordId, c("AA", "BB", "CC"))
			 expect_equal(table@values[["is_computed"]], c(F,F,F))
			 expect_equal(table@values[["tap_count"]], c(NA, NA, NA))
		 },
		 {
			 newLastProcessedVersion<-computeTappingFeatures(cleanDataTableId, lastProcessedVersion, featureTableId)
			 expect_equal(newLastProcessedVersion, 8)
		 }
 )
 
 
