## CORE METADATA
# recordId - unique per task
# healthCode - unique per participant
# phoneInfo - the type of phone
coreNames <- c("recordId", "healthCode", "createdOn", "appVersion", "phoneInfo")

# Registered versions of the app (anything else is taken as test data)
releaseVersions <- c("version 1.0, build 7", "version 1.0.5, build 12", "version 1.1, build 22", "version 1.2, build 31", "version 1.3, build 42")

## x IS EXPECTED TO BE A CHARACTER VECTOR TO BE CLEANED UP
cleanString <- function(x){
  gsub('[', '', gsub(']', '', gsub('["', '', gsub('"]', '', x, fixed=T), fixed=T), fixed=T), fixed=T)
}

## x IS EXPECTED TO BE A LIST OF COLUMN MODEL OBJECTS
# returns the names of the columns which are file handle columns
whichFilehandle <- function(x){
  cc <- sapply(as.list(1:length(x)), function(y){
    if(x[[y]]@columnType=="FILEHANDLEID"){
      return(x[[y]]@name)
    } else{
      return(NULL)
    }
  })
  cc <- unlist(cc)
  return(cc)
}

createQueryString<-function(id, lastProcessedVersion) {
  if (is.na(lastProcessedVersion)) {
    queryString<-paste0("SELECT * FROM ", id, " WHERE appVersion NOT LIKE '%YML%'")
  } else {
    queryString<-paste0("SELECT * FROM ", id, " WHERE appVersion NOT LIKE '%YML%' AND ROW_VERSION > ", lastProcessedVersion)
  }
}

## x IS A DATAFRAME TO BE SUBSETTING STANDARDLY
subsetThis <- function(x, theseOnes){
  xSub <- x[, setdiff(names(x), coreNames)]
  xIdx <- rowSums(is.na(xSub)) != ncol(xSub)
  x <- x[ which(xIdx), ]
  #  anything before the firstDate is erroneous and prior to study 'launch'
  firstDate <- as.Date("2015-03-09")
  x <- x[ which(as.Date(x$createdOn) >= firstDate), ]
  x <- x[ which(x$appVersion %in% releaseVersions), ]
  if (!missing(theseOnes)) x <- x[ which(!(x$healthCode %in% theseOnes)), ]
  x <- x[ which(!duplicated(x[, c("healthCode", "createdOn")])), ]
  x[ order(x$createdOn), ]
}

# combine 'new' into 'current', replacing the rows of current with that 
# of 'new' where the values in column 'col' are the same, otherwise appending
# the row to the bottom of the data frame
mergeDataFrames<-function(current, new, col) {
	if (nrow(current)==0) return(new)
	if (nrow(new)==0) return(current)
	if (is.null(current[[col]])) stop("'current' has no column ", col)
	if (is.null(new[[col]])) stop("'new' has no column ", col)
	# these are the indices in 'current' that require merging
	colIndicesInCurrentThatMatchNew<-which(sapply(current[[col]], function(x) any(new[[col]]==x)))
	# these are the indices in 'new' to be merged into 'current'
	colIndicesInNewThatMatchCurrent<-which(sapply(new[[col]], function(x) any(current[[col]]==x)))
	# the two vectors should be the same length
	if (length(colIndicesInCurrentThatMatchNew)>length(colIndicesInNewThatMatchCurrent))
		stop("There are multiple rows in 'new' that match rows in 'current'")
	if (length(colIndicesInNewThatMatchCurrent)>length(colIndicesInCurrentThatMatchNew))
		stop("There are multiple rows in 'current' that match rows in 'new'")
	
	# we have to make sure that the column order of 'new' matches that of 'current'
	permuteOrder<-sapply(names(current), function(x){which(names(new)==x)})
	if (is(permuteOrder, "list")) stop("'current' has rows that 'new' lacks")
	new<-new[permuteOrder]
	
	# the following is only necessary if there are matching rows in the two dataframes
	if (length(colIndicesInCurrentThatMatchNew)>0) {
		# Within the space of colIndicesInNewThatMatchCurrent, what is the index in current
		# that holds the matching value?
		subIndex<-sapply(new[colIndicesInNewThatMatchCurrent,col], function(x) which(current[colIndicesInCurrentThatMatchNew, col]==x))
		if (is(subIndex, "list")) stop("There are multiple matches in the given key column between 'current' and 'new'")
		current[colIndicesInCurrentThatMatchNew,]<-new[colIndicesInNewThatMatchCurrent[subIndex],]
		# now append the values of new that were not merged in to current
		# and return the result
		rbind(current, new[-colIndicesInNewThatMatchCurrent,])
	} else {
		rbind(current, new)
	}
}


#####
## ENROLLMENT for first survey: parkinson-EnrollmentSurvey-v1
#####
process_survey_v1<-function(eId, lastProcessedVersion) {
  eSc <- synGet(eId)
  eStringCols <- sapply(as.list(1:length(eSc@columns)), function(x){
    if(eSc@columns[[x]]@columnType=="STRING"){
      return(eSc@columns[[x]]@name)
    } else{
      return(NULL)
    }
  })
  eStringCols <- unlist(eStringCols)
  eStringCols <- eStringCols[ eStringCols != "race" ]
  
  eTab <- synTableQuery(createQueryString(eId, lastProcessedVersion))
  
  eDat <- eTab@values
	
	if (nrow(eDat)==0) {
		return(list(maxRowVersion=lastProcessedVersion, eDat=eDat))
	}
	
	maxRowVersion<-getMaxRowVersion(eDat)
  for(i in eStringCols){
    eDat[[i]] <- cleanString(eDat[[i]])
  }
  eDat$race <- gsub('[', '', gsub(']', '', eDat$race, fixed=T), fixed=T)
  eDat$externalId <- NULL
  eDat$uploadDate <- NULL
  eDat$Enter_State <- NULL
  eDat$`last-smoked` <- as.numeric(format(eDat$`last-smoked`, "%Y"))
  eDat$employment[ which(eDat$employment=="Military") ] <- "Employment for wages"
  eDat$employment[ which(eDat$employment=="Out of work but not currently looking for work") ] <- "Out of work"
  eDat$employment[ which(eDat$employment=="Out of work and looking for work") ] <- "Out of work"
  eDat$`packs-per-day` <- as.integer(eDat$`packs-per-day`)
  eDat$age[ which(eDat$age>90 & eDat$age<101) ] <- 90
  
  ## PULL IN THE COMORBIDITIES
  eComFiles <- synDownloadTableColumns(eTab, "health-history")
  eCom <- sapply(eComFiles, readLines)
  for(rn in rownames(eDat)){
    if(!is.na(eDat[rn, "health-history"])){
      eDat[rn, "health-history"] <- eCom[[eDat[rn, "health-history"]]]
    }
  }
  eDat$`health-history` <- gsub(' (TIA)', '', gsub(' (COPD)', '', gsub('[', '', gsub(']', '', eDat$`health-history`, fixed=T), fixed=T), fixed=T), fixed=T)
  
  ## KEEP THE FIRST INSTANCE OF ENROLLMENT SURVEY
  eDat <- eDat[ !duplicated(eDat$healthCode), ]
  rownames(eDat) <- eDat$recordId
  
  ## THESE ENTERED INVALID AGES - EVEN THOUGH TWICE IN REGISTRATION CERTIFIED THAT OVER 18
  theseOnes <- eDat$healthCode[ which(eDat$age < 18  | eDat$age > 100) ]
  
  eDat <- subsetThis(eDat, theseOnes)
  
  list(maxRowVersion=maxRowVersion, eDat=eDat)
}
  
#####
## UPDRS - the SECOND survey
#####
process_survey_v2<-function(uId, lastProcessedVersion) {
  uSc <- synGet(uId)
  uStringCols <- sapply(as.list(1:length(uSc@columns)), function(x){
    if(uSc@columns[[x]]@columnType=="STRING"){
      return(uSc@columns[[x]]@name)
    } else{
      return(NULL)
    }
  })
  uStringCols <- unlist(uStringCols)
  
  uTab <- synTableQuery(createQueryString(uId, lastProcessedVersion))
  
  uDat <- uTab@values
	
	if (nrow(uDat)==0) {
		return(list(maxRowVersion=lastProcessedVersion, uDat=uDat))
	}
	
	maxRowVersion<-getMaxRowVersion(uDat)
	for(i in uStringCols){
    uDat[[i]] <- cleanString(uDat[[i]])
  }
  uDat$externalId <- NULL
  uDat$uploadDate <- NULL
  uDat$`MDS-UPDRS1.1` <- uDat$`MDS-UPRDRS1.1`
  uDat$`MDS-UPRDRS1.1` <- NULL
  uDat <- subsetThis(uDat)
  rownames(uDat) <- uDat$recordId
	list(maxRowVersion=maxRowVersion, uDat=uDat)
}

#####
## PDQ8
#####
process_survey_v3<-function(pId, lastProcessedVersion) {
  pSc <- synGet(pId)
  pStringCols <- sapply(as.list(1:length(pSc@columns)), function(x){
    if(pSc@columns[[x]]@columnType=="STRING"){
      return(pSc@columns[[x]]@name)
    } else{
      return(NULL)
    }
  })
  pStringCols <- unlist(pStringCols)
  
  pTab <- synTableQuery(createQueryString(pId, lastProcessedVersion))
  
  pDat <- pTab@values
	
	if (nrow(pDat)==0) {
		return(list(maxRowVersion=lastProcessedVersion, pDat=pDat))
	}
	
	maxRowVersion<-getMaxRowVersion(pDat)
	
  for(i in pStringCols){
    pDat[[i]] <- cleanString(pDat[[i]])
  }
  pDat$externalId <- NULL
  pDat$uploadDate <- NULL
  pDat$`PDQ8-4` <- pDat$`PQD8-4`
  pDat$`PQD8-4` <- NULL
  pDat <- pDat[, c(names(pDat)[-grep("PDQ", names(pDat))], paste('PDQ8', 1:8, sep="-"))]
  
  pDat <- subsetThis(pDat)
  rownames(pDat) <- pDat$recordId
	list(maxRowVersion=maxRowVersion, pDat=pDat)
}

#####
## MEMORY
#####
process_memory_activity<-function(mId, lastProcessedVersion) {
  mSc <- synGet(mId)
  mFilehandleCols <- whichFilehandle(mSc@columns)
  
  mTab <- synTableQuery(createQueryString(mId, lastProcessedVersion))
  mDat <- mTab@values
	
	if (nrow(mDat)==0) {
		return(list(mDat=mDat, mFilehandleCols=mFilehandleCols, maxRowVersion=lastProcessedVersion))
	}
	
	maxRowVersion<-getMaxRowVersion(mDat)
	
  mDat$externalId <- NULL
  mDat$uploadDate <- NULL
  mDat$momentInDayFormat.json.choiceAnswers <- cleanString(mDat$momentInDayFormat.json.choiceAnswers)
  
  mDat <- subsetThis(mDat)
  rownames(mDat) <- mDat$recordId
  list(mDat=mDat, mFilehandleCols=mFilehandleCols, maxRowVersion=maxRowVersion)
}

#####
## TAPPING
#####
process_tapping_activity<-function(tId, lastProcessedVersion) {
  tSc <- synGet(tId[length(tId)])
  tFilehandleCols <- whichFilehandle(tSc@columns)
  
  maxRowProcessed<-NULL
  
  tAll <- lapply(as.list(tId), function(x){
    vals <- synTableQuery(createQueryString(x, lastProcessedVersion[x]))@values
		if (nrow(vals)==0) {
			maxRowProcessed[x]<<-lastProcessedVersion[x]
		} else {
			maxRowProcessed[x]<<-getMaxRowVersion(vals)
		}
    return(vals)
  })
  tAllNames <- unique(unlist(sapply(tAll, names)))
  tAll <- lapply(tAll, function(x){
				if (nrow(x)==0) {
					x
				} else {
					these <- setdiff(tAllNames, names(x))
					x[, these] <- NA
					x[, tAllNames]
				}
  })
  tDat <- do.call(rbind, tAll)
	
	if (nrow(tDat)==0) {
		return(list(tDat=tDat, tFilehandleCols=tFilehandleCols, maxRowProcessed=maxRowProcessed))
	}
  
  tDat$externalId <- NULL
  tDat$uploadDate <- NULL
  tDat$tapping_results.json.item <- NULL
  tDat$momentInDayFormat.json.saveable <- NULL
  tDat$momentInDayFormat.json.answer <- NULL
  tDat$momentInDayFormat.json.userInfo <- NULL
  tDat$momentInDayFormat.json.questionTypeName <- NULL
  tDat$momentInDayFormat.json.questionType <- NULL
  tDat$momentInDayFormat.json.item <- NULL
  tDat$momentInDayFormat.json.endDate <- NULL
  tDat$momentInDayFormat.json.startDate <- NULL
  tDat$accelerometer_tapping.items <- NULL
  tDat$momentInDayFormat.json.choiceAnswers <- cleanString(tDat$momentInDayFormat.json.choiceAnswers)
  
  tDat <- subsetThis(tDat)
  rownames(tDat) <- tDat$recordId
  list(tDat=tDat, tFilehandleCols=tFilehandleCols, maxRowProcessed=maxRowProcessed)
}

# we introduce a 'mockable' function
read_json_from_file<-function(file) {
  if (is.na(file)) return(NA)
  con = file(file, "r")
  tryCatch({
        content<-paste(readLines(con, warn=F), collapse="\n")
        fromJSON(content) 
      },
      finally=close(con)
  )
}

#####
## VOICE
#####
process_voice_activity<-function(vId1, vId2, lastProcessedVersion1, lastProcessedVersion2) {
  maxRowProcessed<-c()
  
  ## FIRST SET OF IDS HAVE TO PARSE INTO momentInDayFormat.json FILES TO EXTRACT MED INFO
  vFirst <- lapply(as.list(vId1), function(x) {
    vTab <- synTableQuery(createQueryString(x, lastProcessedVersion1[x]))
    vals <- vTab@values
		if (nrow(vals)==0) {
			maxRowProcessed[x]<<-lastProcessedVersion1[x]
		} else {
   		maxRowProcessed[x]<<-getMaxRowVersion(vals)
    
	    vMap <- synDownloadTableColumns(vTab, "momentInDayFormat.json")
	    vMID <- sapply(as.list(rownames(vals)), function(rn){
	      if( is.na(vals[rn, "momentInDayFormat.json"]) ){
	        return(c(choiceAnswers=NA))
	      } else{
	        loc <- vMap[[vals[rn, "momentInDayFormat.json"]]]
	        dat <- try(read_json_from_file(loc))
	        if( class(dat) == "try-error" ){
	          return(c(choiceAnswers=NA))
	        } else{
	          return(unlist(dat))
	        }
	      }
	    })
	    vAllNames <- unique(unlist(sapply(vMID, names)))
	    vMID <- lapply(vMID, function(y){
	      these <- setdiff(vAllNames, names(y))
	      y[ these ] <- NA
	      return(y[ vAllNames ])
	    })
	    vMID <- do.call(rbind, vMID)
	    vMID <- as.data.frame(vMID, stringsAsFactors=FALSE)
	    names(vMID) <- paste("momentInDayFormat.json", names(vMID), sep=".")
	    vals$momentInDayFormat.json <- NULL
	    vals$momentInDayFormat.json.choiceAnswers <- vMID$momentInDayFormat.json.choiceAnswers
		}
    return(vals)
  })
  vFirst <- do.call(rbind, vFirst)
	if (nrow(vFirst)>0) {
		vFirst<- vFirst[!duplicated(vFirst$recordId, fromLast=TRUE),]
	  rownames(vFirst) <- vFirst$recordId
	}
  
  ## SECOND SET (1) IS AS WE WOULD EXPECT
  vSc <- synGet(vId2)
  vFilehandleCols <- whichFilehandle(vSc@columns)
  
  vSecond <- synTableQuery(createQueryString(vId2, lastProcessedVersion2))@values
	if (nrow(vSecond)==0) {
		maxRowProcessed[vId2]<-lastProcessedVersion2
	} else {
		maxRowProcessed[vId2]<-getMaxRowVersion(vSecond)
		vSecond<- vSecond[!duplicated(vSecond$recordId, fromLast=TRUE),]
		rownames(vSecond) <- vSecond$recordId
	}
  
  vDat <- rbind(vFirst, vSecond)
	if (nrow(vDat)>0) {
		vDat$externalId <- NULL
		vDat$uploadDate <- NULL
		vDat <- subsetThis(vDat)
	}
	
  list(vDat=vDat, vFilehandleCols=vFilehandleCols, maxRowProcessed=maxRowProcessed)
}
  
#####
## WALKING
#####
process_walking_activity<-function(wId, lastProcessedVersion) {
  maxRowProcessed<-c()
  wAll <- lapply(as.list(wId), function(x){
    vals <- synTableQuery(createQueryString(x, lastProcessedVersion[x]))@values
		if (nrow(vals)==0) {
			maxRowProcessed[x]<<-lastProcessedVersion[x]
		} else {
   		maxRowProcessed[x]<<-getMaxRowVersion(vals)
		}
    return(vals)
  })
  wAllNames <- unique(unlist(sapply(wAll, names)))
  wAll2 <- lapply(wAll, function(x){
				if (nrow(x)==0) {
					x
				} else {
					these <- setdiff(wAllNames, names(x))
					x[, these] <- NA
					x[, wAllNames]
				}
  })
  wDat <- do.call(rbind, wAll2)
  
  wDat$externalId <- NULL
  wDat$uploadDate <- NULL
  wDat$momentInDayFormat.json.answers <- NULL
  wDat$momentInDayFormat.json.item <- NULL
  wDat$momentInDayFormat.json.endDate <- NULL
  wDat$momentInDayFormat.json.questionType <- NULL
  wDat$momentInDayFormat.json.questionTypeName <- NULL
  wDat$momentInDayFormat.json.saveable <- NULL
  wDat$momentInDayFormat.json.startDate <- NULL
  wDat$momentInDayFormat.json.userInfo <- NULL
  wDat$pedometer_walking.outbound.items <- NULL
  wDat$accelerometer_walking.rest.items <- NULL
  wDat$deviceMotion_walking.rest.items <- NULL
  
  wDat <- subsetThis(wDat)
  rownames(wDat) <- wDat$recordId
  list(wDat=wDat, maxRowProcessed=maxRowProcessed)
}

################################################
################################################
## NOW DO CLEANUP OF MISSING MED DATA FOR ACTIVITIES
cleanup_missing_med_data<-function(mDat, tDat, vDat, wDat) {
  theseColumns <- c("recordId", "healthCode", "createdOn", "momentInDayFormat.json.choiceAnswers")
  allActs <- rbind(mDat[, theseColumns], tDat[, theseColumns], vDat[, theseColumns], wDat[, theseColumns])
  allActs <- allActs[ order(allActs$healthCode, allActs$createdOn), ]
  allActs$momentInDayFormat.json.choiceAnswers <- sub('"]', '', sub('["', '', allActs$momentInDayFormat.json.choiceAnswers, fixed=T))
  reDo <- lapply(as.list(unique(allActs$healthCode)), function(pt){
    this <- allActs[ allActs$healthCode==pt, ]
    if( nrow(this) > 1 ){
      for( rec in 2:nrow(this) ){
        if( this$createdOn[rec]-this$createdOn[rec-1] < (60*20) ){
          if( is.na(this$momentInDayFormat.json.choiceAnswers[rec]) ){
            this$momentInDayFormat.json.choiceAnswers[rec] <- this$momentInDayFormat.json.choiceAnswers[rec-1]
          } else if( this$momentInDayFormat.json.choiceAnswers[rec] %in% c("", "[]") ){
            this$momentInDayFormat.json.choiceAnswers[rec] <- this$momentInDayFormat.json.choiceAnswers[rec-1]
          }
        }
      }
    }
    return(this)
  })
  newAllActs <- do.call(rbind, reDo)
  
  ## MERGE BACK INTO EACH TABLE
  mDat$medTimepoint <- newAllActs[ rownames(mDat), "momentInDayFormat.json.choiceAnswers" ]
  mDat$momentInDayFormat.json.choiceAnswers <- NULL
  tDat$medTimepoint <- newAllActs[ rownames(tDat), "momentInDayFormat.json.choiceAnswers" ]
  tDat$momentInDayFormat.json.choiceAnswers <- NULL
  vDat$medTimepoint <- newAllActs[ rownames(vDat), "momentInDayFormat.json.choiceAnswers" ]
  vDat$momentInDayFormat.json.choiceAnswers <- NULL
  wDat$medTimepoint <- newAllActs[ rownames(wDat), "momentInDayFormat.json.choiceAnswers" ]
  wDat$momentInDayFormat.json.choiceAnswers <- NULL
  
  ## ADDITIONAL SUBSETTING FOR MEMORY
  mSub <- mDat[, grep("MemoryGameResults.json", names(mDat), fixed=T)]
  mIdx <- rowSums(is.na(mSub)) != ncol(mSub)
  mDat <- mDat[ mIdx, ]
  list(mDat=mDat, tDat=tDat, vDat=vDat, wDat=wDat)
}

################################################
################################################
## STORE BACK TO SYNAPSE
## LOG IN AS BRIDGE EXPORTER TO STORE BACK
# synapseLogout()
store_cleaned_data<-function(outputProjectId, eDat, uDat, pDat, mDat, tDat, vDat, wDat, 
		mFilehandleCols, tFilehandleCols, vFilehandleCols) {
  storeThese <- list('Demographics Survey' = list(vals=eDat, fhCols=NULL),
                     'UPDRS Survey' = list(vals=uDat, fhCols=NULL),
                     'PDQ8 Survey' = list(vals=pDat, fhCols=NULL),
                     'Memory Activity' = list(vals=mDat, fhCols=intersect(names(mDat), mFilehandleCols)),
                     'Tapping Activity' = list(vals=tDat, fhCols=intersect(names(tDat), tFilehandleCols)),
                     'Voice Activity' = list(vals=vDat, fhCols=intersect(names(vDat), vFilehandleCols)),
                     'Walking Activity' = list(vals=wDat, fhCols=grep("json.items", names(wDat), value = TRUE)))
  
  ## SCHEMAS ALREADY STORED - FIND THEM
  qq <- synQuery(paste0('SELECT id, name FROM table WHERE parentId=="', outputProjectId, '"'))
  
  ## NOW LETS DO SOMETHING WITH ALL OF THIS DATA
  ## FINALLY, STORE THE OUTPUT
  for (i in 1:length(storeThese)) {
    thisId <- qq$table.id[qq$table.name == names(storeThese)[i]]
		cat("\tStoring results in ", thisId, "...\n")
		# if there's no data there's nothing to do
		if (nrow(storeThese[[i]]$vals)>0) {
			# Appending the new data is not sufficient since there may be
			# rows in the new data that _replace_ rows in the current data.
			# Instead we have to _merge_, based on the 'recordId' column.
			rownames(storeThese[[i]]$vals)<-NULL
			tableContent<-synTableQuery(sprintf("select * from %s", thisId))
			tableContent@values<-mergeDataFrames(tableContent@values, storeThese[[i]]$vals, "recordId")
			tableContent@values<-formatDF(tableContent@values, synGet(thisId))
			synStore(tableContent)
		}
		cat("\t...done.\n")
  }
}

# given a dataframe and a schema, format the data frame
# columns to be compatible with the schema
formatDF<-function(dataframe, schema) {
	schemaColumns<-schema@columns
	schemaColumnMap<-list()
	for (column in schemaColumns@content) schemaColumnMap[[column@name]]<-column
	for (dfColumnName in names(dataframe)) {
		schemaColumn<-schemaColumnMap[[dfColumnName]]
		if (is.null(schemaColumn)) stop(sprintf("Data frame has column %s but %s has no such column.", 
							dfColumnName, propertyValue(schema, "name")))
		dfColumnType<-class(dataframe[[dfColumnName]])[1]
		expectedTableColumnTypes<-synapseClient:::getTableColumnTypeForDataFrameColumnType(dfColumnType)
		tableColumnType<-schemaColumn@columnType
		if (tableColumnType=="BOOLEAN") {
			if (!is.logical(dataframe[[dfColumnName]])) {
				dataframe[[dfColumnName]]<-as.logical(dataframe[[dfColumnName]])
			}
		} else if (tableColumnType=="INTEGER") {
			if (!is.integer(dataframe[[dfColumnName]])) {
				dataframe[[dfColumnName]]<-as.integer(dataframe[[dfColumnName]])
			}
		}
	}
	dataframe
}

