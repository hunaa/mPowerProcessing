# Test normalization - test normalizeFeature()
# 
# Author: Hoff
###############################################################################

library(testthat)
library(mPowerStatistics)

context("test_unit_gait_feature")



lastProcessedVersion<-c("syn123"="1")
cleanDataTableId<-"syn000"
featureTableId<-"syn999"
cleanedWalkingData<-data.frame(
		recordId=c("AA", "BB", "CC"),
		"deviceMotion_walking_outbound.json.items"=c("111", "222", "333"),
		stringsAsFactors=F
)
rownames(cleanedWalkingData)<-c("1_1", "2_1", "3_8")
queryResults<-Table("syn000", cleanedWalkingData)
		
with_mock(
		synTableQuery=function(sql) {queryResults},
		synDownloadTableColumns=function(table, columns){c("111"="file1", "222"="file2", "333"="file3")},
		fromJSON=function(file){"[]"},
		gait_F0XY=function(data){as.integer(7)},
		synStore=function(table) {
			expect_equal(table@schema, "syn999")
			expect_equal(table@values$recordId, c("AA", "BB", "CC"))
			expect_equal(table@values[["is_computed"]], c(T,T,T))
			expect_equal(table@values[["F0XY"]], c(as.integer(7),as.integer(7),as.integer(7)))
		},
		{
			newLastProcessedVersion<-computeGaitFeatures(cleanDataTableId, lastProcessedVersion, featureTableId)
			expect_equal(newLastProcessedVersion, 8)
		}
)
