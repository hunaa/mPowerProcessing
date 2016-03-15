# This is the main entry point for the mPower daily processing
# 
# Author: bhoff
###############################################################################

# make sure to be 'logged in' to Synapse as Bridge exporter
#
main<-function() {
	eId <- c("syn3474927")
	uId <- c("syn3420500")
	pId <- c("syn3420263")
	mId <- c("syn4216473")
	tId <- c("syn3420272", "syn3809916", "syn3809917")
	vId1 <- c("syn3420254") ## ACTUALLY ONLY ONE HERE INSTEAD OF THREE
	vId2 <- c("syn4601589")
	wId <- c("syn3809915", "syn3809914", "syn3420243")
	outputProjectId <- "syn5761747" ## TEST PROJECT
	lastProcessedVersionTableId <- "syn5706434" ## MOVED INTO TEST PROJECT
	bridgeStatusId <- "syn5720756"
	mPowerBatchStatusId <- "TBD"

	synapseClient::synapseLogin()
	
	process_mpower_data(eId, uId, pId, mId, tId, vId1, vId2, wId, outputProjectId, 
			bridgeStatusId, mPowerBatchStatusId, lastProcessedVersionTableId)
}
