#####################################################################
## Extract Balance Features from mPower walking data
##
## Example to process first 10 rows from public walking table:
##   Rscript balance_extract_features.R syn5511449 1 10
## 
## Authors: Elias Chaibub Neto, J. Christopher Bare
## Sage Bionetworks (http://sagebase.org)
#####################################################################

balance_extract_features<-function() {
  args <- commandArgs(trailingOnly=TRUE)
  source_table <- args[1]
  limit <- as.integer(args[2])
  offset   <- as.integer(args[3])
  
  ## An RData file holding completed rows
  if (length(args)>3) {
    e <- new.env()
    load(args[4], envir=e)
    name <- ls(e)[1]
    completed_recordIds <- rownames(get(name, envir=e))
  } else {
    completed_recordIds <- c()
  }
  
  require(synapseClient)
  synapseLogin()
  
  ## syn5511449 = walking activity from public researcher portal
  ## syn4590866 = walking from mpower level 1
  walk <- synTableQuery(sprintf("SELECT * FROM %s LIMIT %s OFFSET %s", source_table, limit, offset))
  cat("dim(walk@values)=", dim(walk@values), "\n")
  
  walkToDownload <- walk
  toDownload <- !(walkToDownload@values$recordId %in% completed_records$recordId)
  walkToDownload@values <- walkToDownload@values[ toDownload ,]
  
  fileMap <- synDownloadTableColumns(walkToDownload, "deviceMotion_walking_rest.json.items")
  
  ldat <- fromJSON(fileMap[1])
  bdat <- ShapeBalanceData(ldat)
  feat1 <- GetBalanceFeatures(bdat)
  
  feat <- matrix(NA, nrow(walk@values), length(feat1))
  rownames(feat) <- walk@values$recordId
  colnames(feat) <- names(feat1)
  feat[1,] <- feat1
  
  n <- nrow(walk@values)
  cat(sprintf("Processing %d rows...\n", n))
  
  ## replace ntest by n to get all data
  for (i in 1:nrow(walk@values)) {
    cat(i, "\n")
    recordId <- walk@values[i,'recordId']
    if (recordId %in% rownames(completed_records)) {
      feat[i,] <- completed_records[recordId,]
    } else {
      try({
        fileHandleId <- walk@values[i,'deviceMotion_walking_outbound.json.items']
        filepath <- fileMap[fileHandleId]
        ldat <- fromJSON(filepath)
        bdat <- ShapeBalanceData(ldat)
        feat[i,] <- GetBalanceFeatures(bdat)
      })
    }
  }
  
  save(feat, file=sprintf("balance_features_%d_%d.RData", limit, offset), compress=TRUE)
  
}
