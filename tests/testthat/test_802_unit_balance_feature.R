# Test normalization - test normalizeFeature()
# 
# Author: Hoff
###############################################################################

library(synapseClient)
library(RJSONIO)
library(testthat)
library(mPowerStatistics)

context("test_unit_balance_feature")



lastProcessedVersion<-c("syn123"="1")
cleanDataTableId<-"syn000"
featureTableId<-"syn999"
cleanedWalkingData<-data.frame(
		recordId=c("AA", "BB", "CC"),
		"deviceMotion_walking_rest.json.items"=c("111", "222", "333"),
		stringsAsFactors=F
)
rownames(cleanedWalkingData)<-c("1_1", "2_1", "3_8")
queryResults<-Table("syn000", cleanedWalkingData)
		
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
		balance_zcrAA=function(data){as.integer(7)},
		synStore=function(table) {
			expect_equal(table@schema, "syn999")
			expect_equal(table@values$recordId, c("AA", "BB", "CC"))
			expect_equal(table@values[["is_computed"]], c(T,T,T))
			expect_equal(table@values[["zcrAA"]], c(as.integer(7),as.integer(7),as.integer(7)))
		},
		{
			newLastProcessedVersion<-computeBalanceFeatures(cleanDataTableId, lastProcessedVersion, featureTableId)
			expect_equal(newLastProcessedVersion, 8)
		}
)

cleanedWalkingData<-data.frame(
		recordId=c("AA", "BB", "CC"),
		"deviceMotion_walking_rest.json.items"=c(NA, NA, NA),
		stringsAsFactors=F
)
rownames(cleanedWalkingData)<-c("1_1", "2_1", "3_8")
queryResults<-Table("syn000", cleanedWalkingData)

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
		balance_zcrAA=function(data){as.integer(7)},
		synStore=function(table) {
			expect_equal(table@schema, "syn999")
			expect_equal(table@values$recordId, c("AA", "BB", "CC"))
			expect_equal(table@values[["is_computed"]], c(F,F,F))
			expect_equal(table@values[["zcrAA"]], c(NA, NA, NA))
		},
		{
			newLastProcessedVersion<-computeBalanceFeatures(cleanDataTableId, lastProcessedVersion, featureTableId)
			expect_equal(newLastProcessedVersion, 8)
		}
)
