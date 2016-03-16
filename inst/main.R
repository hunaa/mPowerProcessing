# This is the main entry point for the mPower daily processing
# 
# Author: bhoff
###############################################################################

library(mPowerProcessing)

# make sure to be 'logged in' to Synapse as Bridge exporter
#
main<-function() {
	eId <- c("syn3474927")
	uId <- c("syn3420500")
	pId <- c("syn3420263")
	mId <- c("syn4216473")
	tId <- c("syn3420272", "syn3809916", "syn3809917")
	vId1 <- c("syn3420254") ## NOTE: ONLY ONE HERE INSTEAD OF THREE
	vId2 <- c("syn4601589")
	wId <- c("syn3809915", "syn3809914", "syn3420243")
	outputProjectId <- "syn5761747" ## NOTE: THIS IS A TEST PROJECT
	lastProcessedVersionTableId <- "syn5706434" ## NOTE: THIS WAS MOVED INTO TEST PROJECT
	bridgeStatusId <- "syn5720756"
	mPowerBatchStatusId <- "syn5762675" # NOTE: THIS IS IN THE TEST PROJECT

	synapseClient::synapseLogin()
	
	process_mpower_data(eId, uId, pId, mId, tId, vId1, vId2, wId, outputProjectId, 
			bridgeStatusId, mPowerBatchStatusId, lastProcessedVersionTableId)
}

username<-Sys.getenv("SYNAPSE_USERNAME")
if (nchar(username)==0) {
	message("WARNING: Cannot run integration test.  Environment variable SYNAPSE_USERNAME is missing.")
	q("no")
}
apiKey<-Sys.getenv("SYNAPSE_APIKEY")
if (nchar(apiKey)==0) {
	message("WARNING: Cannot run integration test.  Environment variable SYNAPSE_APIKEY is missing.")
	q("no")
}
bridgeUsername<-Sys.getenv("BRIDGE_USERNAME")
if (nchar(bridgeUsername)==0) {
	message("WARNING: Cannot run integration test.  Environment variable BRIDGE_USERNAME is missing.")
	q("no")
}
bridgePassword<-Sys.getenv("BRIDGE_PASSWORD")
if (nchar(bridgePassword)==0) {
	message("WARNING: Cannot run integration test.  Environment variable BRIDGE_PASSWORD is missing.")
	q("no")
}

# log in to Synapse
synapseLogin(username=username, apiKey=apiKey, rememberMe=F)

# log in to Bridge
bridgeLogin(email=bridgeUsername, password=bridgePassword, study='parkinson')


main()
