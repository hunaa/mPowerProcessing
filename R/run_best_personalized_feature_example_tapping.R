run_best_personalized_feature_example_tapping<-function() {
  tapTable <- synTableQuery('SELECT * FROM syn5511439 WHERE "tapping_results.json.TappingSamples" is not null')
  tap <- tapTable@values
  
  ## GET TAPPING FEATURES
  tapFile <- synGet("syn5612449")
  tapFeat <- read.delim(getFileLocation(tapFile), sep="\t", stringsAsFactors=FALSE, row.names = "filehandle")
  tapFeat <- tapFeat[tap$tapping_results.json.TappingSamples, ]
  tap <- cbind(tap, tapFeat)
  dim(tap)
  
  ## change name to previous name used by Brian
  names(tap)[13] <- "calculatedMeds"
  
  ## count number of tasks performed before, after, etc
  counts <- CountBeforeAfterMedicationPerParticipant(tap)
  
  ## select participants which performed at least 30 tasks before and 30 tasks after medication
  tapSel <- GetParticipants(counts, beforeThr = 30, afterThr = 30)
  
  dat <- tap
  sel <- tapSel
  
  ## get before/after data
  casesBefore <- dat[which(dat$calculatedMeds == "Immediately before Parkinson medication"),]
  dim(casesBefore)
  casesAfter <- dat[which(dat$calculatedMeds == "Just after Parkinson medication (at your best)"),]
  dim(casesAfter)
  cases <- rbind(casesBefore, casesAfter)
  
  ## check for duplicates
  dupli <- duplicated(cases[,-c(1:13)])
  sum(dupli)
  
  ## include individuals which have sufficent observations before and after
  cases <- cases[cases$healthCode %in% sel,]
  
  ## detrend the feature data (only for tapping, you can skip this step for the other streams) 
  dcases <- LoessDetrendedFeatures(cases, colnames(cases)[14:56])
  cases <- dcases
  
  ## run UI-tests without adjusting for time of the day
  ui <- RunUITests(cases, sel, featNames = names(cases)[-c(1:13)], adjustByTime = FALSE, sorted = TRUE)
  pvals <- data.frame(ui[[1]])
  pvals$corrected <- p.adjust(pvals[, 1], method = "BH")
  
  ## run UI-tests adjusting for time of the day
  aui <- RunUITests(cases, rownames(ui[[1]]), featNames = names(cases)[-c(1:13)], adjustByTime = TRUE, sorted = FALSE)
  apvals <- data.frame(aui[[1]])
  apvals$corrected <- p.adjust(apvals[, 1], method = "BH")
  
  
  plot(-log(pvals[, 2], 10), -log(apvals[, 2], 10), xlab = "-log10 p-value", 
       ylab = "-log10 p-value (adjusted by time of day)", main = "tapping")
  abline(a = 0, b = 1)
  abline(h = -log(0.05, 10), col = "red")
  abline(v = -log(0.05, 10), col = "red")
  
  ## select participants for which the multiple testing corrected UI-test p-value is less or equal to 0.05
  participantIds <- rownames(apvals)[apvals[, 2] <= 0.05]
  
  ## get the top personalized feature for each selected participant
  GetTopFeatures(x = aui$participantOutputs, participantIds, top = 1)
}

