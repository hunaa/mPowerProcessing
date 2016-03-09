# End-to-end integration test
# To run:
# library(synapseClient)
# synapseLogin()
# source(system.file("integration_tests/process_mpower_data_integration_test.R", package="mPowerProcessing"))
# 
# Author: bhoff
###############################################################################

library(synapseClient)
synapseLogin()
message("Creating project ...")
project<-Project()
project<-synStore(project)
newParent<-propertyValue(project, "id")
message("...done.")
## create and populate the source tables
testDataFolder<-system.file("testdata", package="mPowerProcessing")
v1SurveyInputFile<-file.path(testDataFolder, "v1SurveyInput.RData")

message("Creating Survey V1 table ...")
load(v1SurveyInputFile) # brings into namespace: schema, query, eComFiles, eComContent
# create the file handles
tableData<-query@values
#	for each in eComContent:  write to disk, upload to file handle, replace fh-id in tableData
for (n in names(eComContent)) {
	fileContent<-eComContent[n]
	origFileHandleId<-names(eComFiles[which(eComFiles==n)])
	# upload a file and receive the file handle
	filePath<-tempfile()
	connection<-file(filePath)
	writeChar(fileContent, connection, eos=NULL)
	close(connection)  
	fileHandle<-synapseClient:::chunkedUploadFile(filePath)
	newFileHandleId<-fileHandle$id
	tableData[["health-history"]][which(tableData[["health-history"]]==origFileHandleId)]<-newFileHandleId
}
# create the schema
columns<-list()
for (column in schema@columns@content) {
	column@id<-character(0)
	columns<-append(columns, column)
}
v1Schema<-TableSchema("v1Survey", project, columns)
v1Schema<-synStore(v1Schema)
# create the v1 survey content
v1Table<-Table(v1Schema, tableData)
synStore(v1Table)
eId<-propertyValue(v1Schema, "id")
message("...done.")


#uId <- c("syn4961480")
#pId <- c("syn4961472")
#mId <- c("syn4961459")
#tId <- c("syn4961463", "syn4961465", "syn4961484")
#vId1 <- c("syn4961455", "syn4961457", "syn4961464")
#vId2 <- c("syn4961456")
#wId <- c("syn4961452", "syn4961466", "syn4961469")

#lastProcessedVersionTableId <- "syn5706434"


#process_mpower_data(eId, uId, pId, mId, tId, vId1, vId2, wId, newParent, lastProcessedVersionTableId)

# TODO verify content

#synDelete(project)


