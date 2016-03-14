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


