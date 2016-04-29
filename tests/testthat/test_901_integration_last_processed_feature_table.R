#
# Test of the table trigger mechanism
# 
# To run the environment variables SYNAPSE_USERNAME, SYNAPSE_APIKEY must be set.
# 
# Author: bhoff
###############################################################################
library(testthat)
library(synapseClient)

context("test_integration_last_processed_feature")

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
	
	lastProcessedFeatureVersionTableId <- createLastProcessedFeatureVersionTable(propertyValue(project, "id"))
	
	cleanedTableId<-"syn101"
	featureName<-"some feature"
	queryResult<-lastProcessedFeatureVersion(lastProcessedFeatureVersionTableId, cleanedTableId, featureName)
	df<-queryResult@values
	expected<-data.frame(TABLE_ID=cleanedTableId, FEATURE=featureName, LAST_VERSION=as.integer(0), stringsAsFactors=F)
	expected[1,"LAST_VERSION"]<-NA
	rownames(expected)<-"1"
	expect_equal(expected, df)
	
	queryResult@values[1, "LAST_VERSION"]<-10
	synStore(queryResult)
	
	# now get it again, the stored value should be there
	queryResult<-lastProcessedFeatureVersion(lastProcessedFeatureVersionTableId, cleanedTableId, featureName)
	df<-queryResult@values
	expected<-data.frame(TABLE_ID=cleanedTableId, FEATURE=featureName, LAST_VERSION=as.integer(10), stringsAsFactors=F)
	rownames(expected)<-"0_0"
	expect_equal(expected, df)
	

	
	synDelete(project)
	message("Deleted project ", outputProjectId)
}
