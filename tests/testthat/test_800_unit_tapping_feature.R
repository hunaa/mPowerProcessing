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
