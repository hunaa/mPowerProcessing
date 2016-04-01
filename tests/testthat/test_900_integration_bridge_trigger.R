#
# Test of the table trigger mechanism
# 
# To run the environment variables SYNAPSE_USERNAME, SYNAPSE_APIKEY must be set.
# 
# Author: bhoff
###############################################################################
library(testthat)
library(synapseClient)

context("test_integration_bridge_trigger")

library(synapseClient)
library(bridger)

canExecute<-TRUE

username<-Sys.getenv("SYNAPSE_USERNAME")
if (nchar(username)==0) {
	message("WARNING: Cannot run integration test.  Environment variable SYNAPSE_USERNAME is missing.")
	canExecute<-FALSE
}
apiKey<-Sys.getenv("SYNAPSE_APIKEY")
if (nchar(apiKey)==0) {
	message("WARNING: Cannot run integration test.  Environment variable SYNAPSE_APIKEY is missing.")
	canExecute<-FALSE
}

if (canExecute) {
	# log in to Synapse
	synapseLogin(username=username, apiKey=apiKey, rememberMe=F)
	
	message("\nCreating project ...")
	project<-Project()
	project<-synStore(project)
	outputProjectId<-propertyValue(project, "id")
	message("...done.  Project ID is ", outputProjectId)
	
	# set up bridgeStatusId
	column<-TableColumn(name="uploadDate", columnType="STRING")
	bridgeStatusSchema<-TableSchema("Bridge Status Schema", project, list(column))
	bridgeStatusSchema<-synStore(bridgeStatusSchema)
	bridgeStatusId<-propertyValue(bridgeStatusSchema, "id")
	
	# set up  mPowerBatchStatusId
	c1<-TableColumn(name="bridgeUploadDate", columnType="STRING")
	c2<-TableColumn(name="mPowerBatchStart", columnType="DATE")
	c3<-TableColumn(name="hostName", columnType="STRING")
	c4<-TableColumn(name="batchStatus", columnType="STRING")
	mPowerBatchStatusSchema<-TableSchema("mPower Batch Status Schema", project, list(c1,c2,c3,c4))
	mPowerBatchStatusSchema<-synStore(mPowerBatchStatusSchema)
	mPowerBatchStatusId<-propertyValue(mPowerBatchStatusSchema, "id")
	
	# write a row into the bridgeStatusId table to kick off the job
	trigger<-Table( bridgeStatusId, data.frame(uploadDate=as.character(Sys.Date())) )
	trigger<-synStore(trigger)
	
	bridgeExportQueryResult<-checkForAndLockBridgeExportBatch(bridgeStatusId, mPowerBatchStatusId, "hostname", Sys.time())
	expect_equal(nrow(bridgeExportQueryResult@values), 1)
	
	# check that the batch has been marked 'inProgress'
	jobStatus<-synTableQuery(paste0("select * from ", mPowerBatchStatusId))
	expect_equal(nrow(jobStatus@values), 1)
	expect_equal(jobStatus@values[1,"batchStatus"], "inProgress")
	
	markProcesingComplete(bridgeExportQueryResult, "complete")
	
	# check that the batch has been marked 'complete'
	jobStatus<-synTableQuery(paste0("select * from ", mPowerBatchStatusId,
					" where bridgeUploadDate='", as.character(Sys.Date()), "'"))
	expect_equal(nrow(jobStatus@values), 1)
	expect_equal(jobStatus@values[1,"batchStatus"], "complete")
	
	
	synDelete(project)
	message("Deleted project ", outputProjectId)
}
