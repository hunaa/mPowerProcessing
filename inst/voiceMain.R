# process a batch of voice data
# 
# Author: bhoff
###############################################################################

library(mPowerProcessing)
library(mPowerStatistics)
library(synapseClient)

# make sure to be 'logged in' to Synapse as Bridge exporter
#
voiceMain<-function() {
	voiceInputTableId <- "syn5762681"
	voiceFeatureTableId <- "syn5987316"
	batchTableId <- "syn6038352"
	batchSize<-100
	
	mPowerProcessing::batchVoiceProcess(
			voiceInputTableId, 
			voiceFeatureTableId, 
			batchTableId, 
			batchSize, 
			hostName=Sys.getenv("HOSTNAME")
	)
}

username<-Sys.getenv("SYNAPSE_USERNAME")
if (nchar(username)==0) {
	cat("ERROR: Environment variable SYNAPSE_USERNAME is missing.\n")
	q("no")
}
apiKey<-Sys.getenv("SYNAPSE_APIKEY")
if (nchar(apiKey)==0) {
	cat("ERROR: Environment variable SYNAPSE_APIKEY is missing.\n")
	q("no")
}

cacheDir<-Sys.getenv("CACHE_DIR")
if (nchar(cacheDir)>0) {
	synapseCacheDir(cacheDir)
}

if (nchar(Sys.getenv("STAGING")>0)) {
	synSetEndpoints(
			"https://repo-staging.prod.sagebase.org/repo/v1", 
			"https://repo-staging.prod.sagebase.org/auth/v1", 
			"https://repo-staging.prod.sagebase.org/file/v1")
}

# log in to Synapse
synapseLogin(username=username, apiKey=apiKey, rememberMe=F)

voiceMain()
