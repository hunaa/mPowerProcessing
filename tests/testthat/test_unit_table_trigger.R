# Unit tests for table trigger
# 
# Author: bhoff
###############################################################################

library(testthat)
library(synapseClient)

context("test_unit_table_trigger_1")

# Happy path: a new batch is available for processing, so we lock it
savedTable<<-NULL
with_mock(
		synTableQuery=function(sql) {
			latestBridgeUploadDate<-"2016-03-21"
			if (sql=="select uploadDate from syn101 ORDER BY uploadDate DESC LIMIT 1 OFFSET 0") {
				df<-data.frame(uploadDate=latestBridgeUploadDate, stringsAsFactors=FALSE)
				Table("syn101", df)
			} else if (sql==paste0("select * from syn202 where bridgeUploadDate='", latestBridgeUploadDate, "'")) {
				if (is.null(savedTable)) {
					df<-data.frame() # empty
					Table("syn202", df)
				} else {
					savedTable
				}
			} else {
				stop(paste0("Unexpected query: <", sql, ">"))
			}
		},
		synStore=function(table, retrieveData=F) {
			savedTable<<-table
			table
		},
		{
			bridgeStatusId<-"syn101"
			mPowerBatchStatusId<-"syn202"
			hostname<-"hostname"
			now<-Sys.time()
			result<-checkForAndLockBridgeExportBatch(bridgeStatusId, mPowerBatchStatusId, hostname, now)
			expect_true(is(result, "TableDataFrame"))
			expect_equal(result@schema, "syn202")
			expect_equal(result@values, data.frame(
							bridgeUploadDate=as.Date("2016-03-21"), 
							mPowerBatchStart=now, 
							hostName="hostname",
							batchStatus="inProgress", stringsAsFactors=FALSE))
		}
)

context("test_unit_table_trigger_2")


# There's no batch to process
with_mock(
		synTableQuery=function(sql) {
			if (sql=="select uploadDate from syn101 ORDER BY uploadDate DESC LIMIT 1 OFFSET 0") {
				df<-data.frame() # empty
				Table("syn101", df)
			} else if (sql=="select * from syn202 where bridgeUploadDate='2016-03-21'") {
				df<-data.frame() # empty
				Table("syn202", df)
			} else {
				stop(paste0("Unexpected query: <", sql, ">"))
			}
		},
		synStore=function(table, retrieveData=F) {table},
		{
			bridgeStatusId<-"syn101"
			mPowerBatchStatusId<-"syn202"
			hostname<-"hostname"
			now<-Sys.time()
			result<-checkForAndLockBridgeExportBatch(bridgeStatusId, mPowerBatchStatusId, hostname, now)
			expect_true(is.null(result))
		}
)

context("test_unit_table_trigger_3")

# There's a batch, but it's being processed (we don't specify a timeout)
with_mock(
		synTableQuery=function(sql) {
			latestBridgeUploadDate<-"2016-03-21"
			if (sql=="select uploadDate from syn101 ORDER BY uploadDate DESC LIMIT 1 OFFSET 0") {
				df<-data.frame(uploadDate=latestBridgeUploadDate, stringsAsFactors=FALSE)
				Table("syn101", df)
			} else if (sql==paste0("select * from syn202 where bridgeUploadDate='", latestBridgeUploadDate, "'")) {
				df<-data.frame(
						bridgeUploadDate="2016-03-21", 
						mPowerBatchStart=now, 
						hostName="someotherhost",
						batchStatus="inProgress", stringsAsFactors=FALSE)
				Table("syn202", df)
			} else {
				stop(paste0("Unexpected query: ", sql))
			}
		},
		synStore=function(table, retrieveData=F) {table},
		{
			bridgeStatusId<-"syn101"
			mPowerBatchStatusId<-"syn202"
			hostname<-"hostname"
			now<-Sys.time()
			result<-checkForAndLockBridgeExportBatch(bridgeStatusId, mPowerBatchStatusId, hostname, now)
			expect_true(is.null(result))
		}
)

context("test_unit_table_trigger_4")


# There's a batch, but it's done
with_mock(
		synTableQuery=function(sql) {
			latestBridgeUploadDate<-"2016-03-21"
			if (sql=="select uploadDate from syn101 ORDER BY uploadDate DESC LIMIT 1 OFFSET 0") {
				df<-data.frame(uploadDate=latestBridgeUploadDate, stringsAsFactors=FALSE)
				Table("syn101", df)
			} else if (sql==paste0("select * from syn202 where bridgeUploadDate='", latestBridgeUploadDate, "'")) {
				df<-data.frame(
						bridgeUploadDate="2016-03-21", 
						mPowerBatchStart=now, 
						hostName="someotherhost",
						batchStatus="complete", stringsAsFactors=FALSE)
				Table("syn202", df)
			} else {
				stop(paste0("Unexpected query: ", sql))
			}
		},
		synStore=function(table, retrieveData=F) {table},
		{
			bridgeStatusId<-"syn101"
			mPowerBatchStatusId<-"syn202"
			hostname<-"hostname"
			now<-Sys.time()
			result<-checkForAndLockBridgeExportBatch(bridgeStatusId, mPowerBatchStatusId, hostname, now)
			expect_true(is.null(result))
		}
)

context("test_unit_table_trigger_5")


# There's a batch, but it's being processed and hasn't surpassed its timeout
with_mock(
		synTableQuery=function(sql) {
			latestBridgeUploadDate<-"2016-03-21"
			if (sql=="select uploadDate from syn101 ORDER BY uploadDate DESC LIMIT 1 OFFSET 0") {
				df<-data.frame(uploadDate=latestBridgeUploadDate, stringsAsFactors=FALSE)
				Table("syn101", df)
			} else if (sql==paste0("select * from syn202 where bridgeUploadDate='", latestBridgeUploadDate, "'")) {
				df<-data.frame(
						bridgeUploadDate="2016-03-21", 
						mPowerBatchStart=now-10, # stated 10 seconds ago 
						hostName="someotherhost",
						batchStatus="inProgress", stringsAsFactors=FALSE)
				Table("syn202", df)
			} else {
				stop(paste0("Unexpected query: ", sql))
			}
		},
		synStore=function(table, retrieveData=F) {table},
		{
			bridgeStatusId<-"syn101"
			mPowerBatchStatusId<-"syn202"
			hostname<-"hostname"
			now<-Sys.time()
			leaseTimeout<-as.difftime("06:00:00")
			result<-checkForAndLockBridgeExportBatch(bridgeStatusId, mPowerBatchStatusId, hostname, now, leaseTimeout)
			expect_true(is.null(result))
		}
)

context("test_unit_table_trigger_6")


# There's a batch 'InProgress' but it's timed out
savedTable<<-NULL
with_mock(
		synTableQuery=function(sql) {
			latestBridgeUploadDate<-"2016-03-21"
			if (sql=="select uploadDate from syn101 ORDER BY uploadDate DESC LIMIT 1 OFFSET 0") {
				df<-data.frame(uploadDate=latestBridgeUploadDate, stringsAsFactors=FALSE)
				Table("syn101", df)
			} else if (sql==paste0("select * from syn202 where bridgeUploadDate='", latestBridgeUploadDate, "'")) {
				if (is.null(savedTable)) {
					df<-data.frame(
							bridgeUploadDate="2016-03-21", 
							mPowerBatchStart=now-as.difftime("07:00:00"), # stated 10 seconds ago 
							hostName="someotherhost",
							batchStatus="inProgress",
							stringsAsFactors=F)
					Table("syn202", df)
				} else {
					savedTable
				}
			} else {
				stop(paste0("Unexpected query: ", sql))
			}
		},
		synStore=function(table, retrieveData=F) {
			savedTable<<-table
			table
		},
		{
			bridgeStatusId<-"syn101"
			mPowerBatchStatusId<-"syn202"
			hostname<-"hostname"
			now<-Sys.time()
			leaseTimeout<-as.difftime("06:00:00")
			result<-checkForAndLockBridgeExportBatch(bridgeStatusId, mPowerBatchStatusId, hostname, now, leaseTimeout)
			expect_true(is(result, "TableDataFrame"))
			expect_equal(result@schema, "syn202")
			expect_equal(result@values, data.frame(
							bridgeUploadDate="2016-03-21", 
							mPowerBatchStart=now, 
							hostName="hostname",
							batchStatus="inProgress", stringsAsFactors=FALSE))
		}
)

context("test_unit_table_trigger_7")


# test markProcesingComplete
with_mock(
		synStore=function(table, retrieveData=F) {table},
		{
			mPowerBatchStatusId<-"syn202"
			now<-Sys.time()
			
			queryResult<-Table(mPowerBatchStatusId, data.frame(
							bridgeUploadDate="2016-03-21", 
							mPowerBatchStart=now, 
							hostName="hostname",
							batchStatus="inProgress", stringsAsFactors=FALSE))
			result<-markProcesingComplete(queryResult, "complete")
			
			expect_true(is(result, "TableDataFrame"))
			expect_equal(result@schema, mPowerBatchStatusId)
			expect_equal(result@values, data.frame(
							bridgeUploadDate="2016-03-21", 
							mPowerBatchStart=now, 
							hostName="hostname",
							batchStatus="complete", stringsAsFactors=FALSE))
		}
)
