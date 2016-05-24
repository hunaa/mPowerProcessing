# Test voice feature extraction
###############################################################################

library(testthat)
library(mPowerStatistics)

context("test_unit_voice_feature")


lastProcessedVersion<-c("syn123"="1")
cleanDataTableId<-"syn000"
featureTableId<-"syn999"
cleanedData<-data.frame(
		recordId=c("AA", "BB", "CC"),
		"audio_audio.m4a"=c("111", "222", "333"),
		stringsAsFactors=F)

rownames(cleanedData)<-c("1_1", "2_1", "3_8")
queryResults<-Table("syn000", cleanedData)

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
		medianF0=function(filename){pi},
		convert_to_wav=function(filename){"foo.wav"},
		synStore=function(table) {
			expect_equal(table@schema, "syn999")
			expect_equal(table@values$recordId, c("AA", "BB", "CC"))
			expect_equal(table@values[["is_computed"]], c(T,T,T))
			expect_equal(table@values[["medianF0"]], rep(pi, 3))
		},
		{
			newLastProcessedVersion <- computeVoiceFeatures(cleanDataTableId, lastProcessedVersion, featureTableId)
			expect_equal(newLastProcessedVersion, 8)
		}
)
