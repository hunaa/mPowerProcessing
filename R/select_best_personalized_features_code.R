
## return time of the day in hour
##
GetTimeOfDay <- function(x) {
  n <- length(x)
  x <- as.character(x)
  aux <- unlist(strsplit(x, " "))
  aux <- aux[seq(2, 2*n, by = 2)]
  aux <- as.numeric(unlist(strsplit(aux, ":")))
  ih <- seq(1, 3*n, by = 3)
  im <- seq(2, 3*n, by = 3)
  is <- seq(2, 3*n, by = 3)
  secs <- 3600 * aux[ih] + 60 * aux[im] + aux[is]
  secs/3600
}

## response must be on the first column
##
UITtests <- function(dat, 
                     mtmethod = "BH", 
                     alternative = "two.sided") {
  nfeat <- ncol(dat) - 1
  featnms <- colnames(dat)[-1]
  pvals <- rep(NA, nfeat)
  names(pvals) <- featnms
  respLevels <- levels(dat[, 1])
  for (i in seq(nfeat)) {
    xa <- dat[, i + 1][which(dat[, 1] == respLevels[1])]
    xb <- dat[, i + 1][which(dat[, 1] == respLevels[2])] 
    xa <- xa[which(!is.na(xa))]
    xb <- xb[which(!is.na(xb))]
    pvals[i] <- t.test(xa, xb, var.equal = TRUE, alternative = alternative)$p.value
  }
  pval <- min(p.adjust(pvals, method = mtmethod), na.rm = TRUE)
  list(pval = pval, pvals = pvals)
}

## response must be on the first column
##
UITimeAdjustedTests <- function(resdat, mtmethod = "BH") {
  nfeat <- ncol(resdat) - 1
  featnms <- colnames(resdat)[-1]
  pvals <- rep(NA, nfeat)
  names(pvals) <- featnms
  for (i in seq(nfeat)) {
    y <- resdat[, 1]
    x <- resdat[, i + 1]
    fit <- summary(lm(y ~ x))
    if ('x' %in% rownames(fit$coefficients)) {
      pvals[i] <- fit$coefficients['x', 4]
    } else {
      pvals[i] <- NA
    }
  }
  pval <- min(p.adjust(pvals, method = mtmethod), na.rm = TRUE)
  list(pval = pval, pvals = pvals)
}


GetResiduals <- function(dat, respName, featNames, timeOfDay) {
  dat$timeOfDay <- timeOfDay
  nfeat <- length(featNames)
  idx <- match(respName, colnames(dat))
  myform <- as.formula(paste(respName, " ~ timeOfDay", sep = ""))
  aux <- lm(myform, dat)
  dat[match(names(aux$resid), rownames(dat)), idx] <- aux$resid
  for (i in seq(along.with=featNames)) {
    idx <- match(featNames[i], colnames(dat))
    myform <- as.formula(paste(featNames[i], " ~ timeOfDay", sep = ""))
    aux <- lm(myform, dat)
    dat[match(names(aux$resid), rownames(dat)), idx] <- aux$resid
  }
  dat[, -ncol(dat)]
}

RunUITests <- function(dat, 
                       selParticipants, 
                       featNames, 
                       adjustByTime = TRUE, 
                       sorted = TRUE) {
  nParticipants <- length(selParticipants)
  participantOutputs <- vector(mode = "list", length = nParticipants)
  names(participantOutputs) <- selParticipants
  UIpvals <- matrix(NA, nParticipants, 1)
  colnames(UIpvals) <- "pval"
  rownames(UIpvals) <- selParticipants
  respName <- "calculatedMeds"
  respColumn <- which(colnames(dat) == respName)
  featColumns <- match(featNames, colnames(dat))
  for (i in seq(along.with=selParticipants)) {
    cat("running participant ", i, "\n") 
    pdat <- GetParticipantBeforeAfterData(dat, selParticipants[i])
    if (adjustByTime) {
      timeOfDay <- GetTimeOfDay(pdat$createdOn)
      pdat[, respColumn] <- as.numeric(pdat[, respColumn])
      pdat[which(pdat[, respColumn] == 2), respColumn] <- 0 ## case == 1, control == 0
      resdat <- GetResiduals(pdat[, c(respColumn, featColumns)], respName, featNames, timeOfDay)
      aux <- UITimeAdjustedTests(resdat, mtmethod = "BH")
      participantOutputs[[i]] <- aux
      UIpvals[i, 1] <- aux$pval
    }
    else {
      aux <- UITtests(pdat[, c(respColumn, featColumns)], mtmethod = "BH", alternative = "two.sided")
      participantOutputs[[i]] <- aux
      UIpvals[i, 1] <- aux$pval
    }
  }
  if (sorted) {
    UIpvals <- UIpvals[order(UIpvals[, 1], decreasing = FALSE),, drop = FALSE] 
  }
  
  list(UIpvals = UIpvals, participantOutputs = participantOutputs)
}

LoessDetrendedFeatures <- function(dat, featNames) {
  participantIds <- unique(dat$healthCode)
  nParticipants <- length(participantIds)
  featColumns <- match(featNames, colnames(dat))
  for (i in seq(along.with=participantIds)) {
    cat(i, "\n")
    idx1 <- which(dat$healthCode == participantIds[i])
    pdat <- dat[idx1,]
    o <- order(pdat$createdOn)
    pdat <- pdat[o,]
    xaxis <- seq(nrow(pdat))
    for (j in featColumns) {
      aux <- loess(pdat[, j] ~ xaxis)
      pdat[as.numeric(names(aux$residuals)), j] <- aux$residuals
    }    
    dat[idx1[o],] <- pdat
  }
  dat
}

GetParticipantBeforeAfterData <- function(x, participantId) {
  pdat <- x[which(x$healthCode == participantId),]
  aux <- as.character(pdat$calculatedMeds)
  iBefore <- which(aux == "Immediately before Parkinson medication")
  iAfter <- which(aux == "Just after Parkinson medication (at your best)")
  ii <- sort(c(iBefore, iAfter))
  pdat <- pdat[ii,]
  aux <- as.character(pdat$calculatedMeds)
  iBefore <- which(aux == "Immediately before Parkinson medication")
  iAfter <- which(aux == "Just after Parkinson medication (at your best)")
  aux[iBefore] <- "before"
  aux[iAfter] <- "after"  
  pdat$calculatedMeds <- factor(aux)
  pdat <- pdat[order(pdat$createdOn),]
  pdat
}


CountBeforeAfterMedicationPerParticipant <- function(x) {
  participantIds <- unique(x$healthCode)
  nParticipants <- length(participantIds)
  out <- data.frame(matrix(NA, nParticipants, 7))
  colnames(out) <- c("healthCode", "nBefore", "nAfter", "nOther", "nDon't", "nNAs", "n")
  out[, 1] <- participantIds
  for (i in seq(along.with=participantIds)) {
    cat(i, "\n")
    pdat <- x[which(x$healthCode == participantIds[i]),]
    aux <- as.character(pdat$calculatedMeds)
    out[i, "n"] <- nrow(pdat)
    out[i, "nBefore"] <- sum(aux == "Immediately before Parkinson medication", na.rm = TRUE)
    out[i, "nAfter"] <- sum(aux == "Just after Parkinson medication (at your best)", na.rm = TRUE)
    out[i, "nOther"] <- sum(aux == "Another time", na.rm = TRUE)
    out[i, "nDon't"] <- sum(aux == "I don't take Parkinson medications", na.rm = TRUE)
    out[i, "nNAs"] <- out[i, 7] - sum(out[i, 2:5])
  }
  out[order(out[, "nBefore"] + out[, "nAfter"], decreasing = TRUE),]
}

GetParticipants <- function(x, beforeThr, afterThr) {
  idx <- which(x$nBefore >= beforeThr & x$nAfter >= afterThr)
  x$healthCode[idx]
}

GetTopFeatures <- function(x, participantIds, top = 3) {
  nParticipants <- length(participantIds)
  cat("nParticipants=",nParticipants,"\n")
  out <- vector(mode = "list", length = nParticipants)
  names(out) <- participantIds
  for (i in seq(along.with=participantIds)) {
    cat("i=",i,"\n\n")
    pvals <- x[[match(participantIds[i], names(x))]]$pvals
    pvals <- sort(pvals)
    out[[i]] <- names(pvals)[1:top]
  }
  out
}


