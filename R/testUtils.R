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
