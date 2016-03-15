## HELPER FUNCTIONS FOR PERMUTATIONS
## generateUuids, permuteMe

generateUuids <- function(n){
  require(uuid)
  res <- rep(NA, n)
  for(i in 1:n){
    res[i] <- UUIDgenerate(FALSE)
  }
  return(res)
}

## CREATES RANDOM UUIDS FOR APPROPRIATE COLUMNS
## SHUFFLES ALL COLUMNS
permuteMe <- function(dat){
  uuidFields <- c("recordId", "healthCode")
  for(ii in uuidFields){
    dat[[ii]] <- generateUuids(nrow(dat))
  }
  for(ii in names(dat)){
    dat[[ii]] <- dat[[ii]][sample(1:nrow(dat), size=nrow(dat))]
  }
  return(dat)
}

getIdFromSql<-function(sql) {
	sql<-tolower(sql)
	start<-pracma::strfind(sql, "syn")
	end<-pracma::strfind(sql, " where")
	substr(sql, start, end-1)
}

# when set to 'true' the test suite will generate unit test data that becomes part of the code base
# Note that since data generators have to be run in a certain order, we have named the tests 
# (under test/testthat) to ensure execution in that order.
createTestData<-function() {FALSE}

# prepend the column called 'healthCode' with the given prefix
prependHealthCodes<-function(df, prefix) {
	df['healthCode'] <- sapply(df['healthCode'], function(x) {"test-" %+% x})
	df
}

# mocks attachment files using the files in the given 'folder'
# if 'readJson' is true, reads/converts json content of files
# returns two lists:
# The first maps file handles to file paths.
# The second maps file paths to file content.
mockFileAttachments<-function(folder, readJson=F) {
	# make a list of the mock attachment files, found in the given folder
	mockFiles <- list.files(folder, full.names = TRUE)
	# create fake file handle ID labels on 'mockFiles'
	names(mockFiles)<-sample(100000:999999, size=length(mockFiles), replace=F)
	# read in the content
	if (readJson) {
		fileContent <- sapply(mockFiles, read_json_from_file)
	} else {
		fileContent <- sapply(mockFiles, readLines, warn=F)
	}
	names(fileContent)<-mockFiles # maps file path to file content
	list(mockFiles=mockFiles, fileContent=fileContent)
}
