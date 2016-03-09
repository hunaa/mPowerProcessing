normExecute<-function() {
	
	procRep <- getRepo('Sage-Bionetworks/mPower-processing')
	sourceRepoFile(procRepo, 'feature_normalization/normHelpers.R')
	helpCode <- getPermlink(procRepo, 'feature_normalization/normHelpers.R')
	execCode <- getPermlink(procRepo, 'feature_normalization/normExecute.R')
	
	demoTb <- synTableQuery("SELECT * FROM syn5511429")
	dim(demoTb@values)
	demo <- demoTb@values
	
	## GET TAPPING FEATURES
	tapTable <- synTableQuery('SELECT * FROM syn5511439 WHERE "tapping_results.json.TappingSamples" is not null')
	tap <- tapTable@values
	tapFile <- synGet("syn5612449")
	tapFeat <- read.delim(getFileLocation(tapFile), sep="\t", stringsAsFactors=FALSE, row.names = "filehandle")
	tapFeat <- tapFeat[tap$tapping_results.json.TappingSamples, ]
	tap <- cbind(tap, tapFeat)
	tap$task <- "tap"
	
	## GET VOICE FEATURES
	# voiceTable <- synTableQuery('SELECT * FROM syn5511444 WHERE "audio_audio.m4a" is not null')
	# voice <- voiceTable@values
	voiceFile <- synGet("syn5653006")
	voiceFeat <- read.delim(getFileLocation(voiceFile), sep=",", stringsAsFactors=FALSE)
	voice <- voiceFeat
	voice$task <- "voice"
	
	## GET WALKING TEST METADATA FOR BOTH GAIT AND BALANCE
	walkTable <- synTableQuery('SELECT * FROM syn5511449')
	walk <- walkTable@values
	rownames(walk) <- walk$recordId
	walk$task <- "walk"
	
	## GET GAIT FEATURES
	gaitFile <- synGet("syn5679280")
	gaitFeat <- read.delim(getFileLocation(gaitFile), sep=",", stringsAsFactors=FALSE, row.names = 1)
	gait <- walk[rownames(gaitFeat), ]
	gait <- cbind(gait, gaitFeat)
	gait$task <- "gait"
	
	## GET BALANCE FEAURES
	balanceFile <- synGet("syn5678820")
	balanceFeat <- read.delim(getFileLocation(balanceFile), sep=",", stringsAsFactors=FALSE, row.names = 1)
	balance <- walk[rownames(balanceFeat), ]
	balance <- cbind(balance, balanceFeat)
	balance$task <- "balance"
	
	
	getVisData <- function(participantId, tapName, voiceName, gaitName, balanceName, windowStart, windowEnd){
	  norms <- list(tapNorm = NormalizeFeature(tap, participantId, tapName, demo, ageInterval = 5),
	                voiceNorm = NormalizeFeature(voice, participantId, voiceName, demo, ageInterval = 5),
	                gaitNorm = NormalizeFeature(gait, participantId, gaitName, demo, ageInterval = 5),
	                balanceNorm = NormalizeFeature(balance, participantId, balanceName, demo, ageInterval = 5))
	  
	  norms <- lapply(norms, function(x){
	    tmp <- x$fdat
	    tmp$createdOn <- as.Date(tmp$createdOn)
	    tmp <- tmp[ tmp$createdOn >= windowStart & tmp$createdOn <= windowEnd, ]
	    tmp <- tmp[!duplicated(tmp[, c("createdOn", "medTimepoint")]), ]
	    pre <- tmp[tmp$medTimepoint == "Immediately before Parkinson medication", ]
	    pre$medTimepoint <- NULL
	    names(pre) <- c("date", "pre")
	    post <- tmp[tmp$medTimepoint == "Just after Parkinson medication (at your best)", ]
	    post$medTimepoint <- NULL
	    names(post) <- c("date", "post")
	    df <- data.frame(date=seq(windowStart, windowEnd, by="day"), 
	                     stringsAsFactors = FALSE)
	    df <- merge(df, post, all=TRUE)
	    df <- merge(df, pre, all=TRUE)
	    rownames(df) <- df$date
	    x$fdat <- df
	    return(x)
	  })
	  
	  towardJSON <- lapply(as.list(seq(windowStart, windowEnd, by="day")), function(thisDate){
	    res <- list(list(
	      tap=list(
	        pre=norms$tapNorm$fdat[as.character(thisDate), "pre"],
	        post=norms$tapNorm$fdat[as.character(thisDate), "post"],
	        controlMin=norms$tapNorm$controlLower,
	        controlMax=norms$tapNorm$controlUpper
	      ),
	      voice=list(
	        pre=norms$voiceNorm$fdat[as.character(thisDate), "pre"],
	        post=norms$voiceNorm$fdat[as.character(thisDate), "post"],
	        controlMin=norms$voiceNorm$controlLower,
	        controlMax=norms$voiceNorm$controlUpper
	      ),
	      gait=list(
	        pre=norms$gaitNorm$fdat[as.character(thisDate), "pre"],
	        post=norms$gaitNorm$fdat[as.character(thisDate), "post"],
	        controlMin=norms$gaitNorm$controlLower,
	        controlMax=norms$gaitNorm$controlUpper
	      ),
	      balance=list(
	        pre=norms$balanceNorm$fdat[as.character(thisDate), "pre"],
	        post=norms$balanceNorm$fdat[as.character(thisDate), "post"],
	        controlMin=norms$balanceNorm$controlLower,
	        controlMax=norms$balanceNorm$controlUpper
	      )))
	    names(res) <- thisDate
	    return(res)
	  })
	  
	  ## INSERT CODE TO CALL BRIDGE APIS WHERE APPROPRIATE
	  return(toJSON(towardJSON))
	}
	
	
	
	## LET US TRY FOR THIS INDIVIDUAL
	# participantId <- "6c130d05-41af-49e9-9212-aaae738d3ec1"
	# tapName <- "numberTaps"
	# voiceName <- "shimmerLocaldB_sma3nz_amean"
	# gaitName <- "medianZ"
	# balanceName <- "rangeAA"
	# windowEnd <- as.Date("2015-05-01")
	# windowStart <- windowEnd-29
	
}
