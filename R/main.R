# This is the main entry point for the mPower daily processing
# 
# Author: bhoff
###############################################################################

# TODO we should probably remove this file altogether and 
# TODO instead put production IDs, log-in info, etc. on the 
# TODO server which runs the script
# 
# TODO make sure to be 'logged in' to Synapse as Bridge exporter
#
main<-function() {
	eId <- c("syn4961453")
	uId <- c("syn4961480")
	pId <- c("syn4961472")
	mId <- c("syn4961459")
	tId <- c("syn4961463", "syn4961465", "syn4961484")
	vId1 <- c("syn4961455", "syn4961457", "syn4961464")
	vId2 <- c("syn4961456")
	wId <- c("syn4961452", "syn4961466", "syn4961469")
	outputProjectId <- "syn4993293"
	lastProcessedVersionTableId <- "syn5706434"
	
	synapseClient::synapseLogin()
	
	process_mpower_data(eId, uId, pId, mId, tId, vId1, vId2, wId, outputProjectId, 
			bridgeStatusId, mPowerBatchStatusId, lastProcessedVersionTableId)
}
