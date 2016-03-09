GetAgeMatchedControlIds <- function(patientAge, ageInterval = 5, demo){
  controlAges <- demo[which(!demo$`professional-diagnosis`), c("healthCode", "age")]
  controlAges[which(controlAges$age <= 17), "age"] <- NA
  controlAges[which(controlAges$age >= 115), "age"] <- NA
  idx1 <- which(controlAges$age >= (patientAge - ageInterval))
  idx2 <- which(controlAges$age <= (patientAge + ageInterval))
  idx <- intersect(idx1, idx2)
  return(controlAges$healthCode[idx])
}

GetControlFeatureSummaryStats <- function(dat, controlIds, featName){
  idx <- dat$healthCode %in% controlIds
  pdat <- dat[idx, featName]
  res <- list(median = median(pdat, na.rm = TRUE), 
              mean = mean(pdat, na.rm = TRUE),
              sd = sd(pdat, na.rm = TRUE),
              nControls = length(controlIds),
              nTasks = length(pdat))
  return(res)
}

NormalizeFeature <- function(dat, 
                             patientId, 
                             featName, 
                             demo, 
                             ageInterval,
                             floorCeilingRange = 1,
                             standardDeviations = 1,
                             reverse = FALSE){
  patientAge <- demo$age[match(patientId, demo$healthCode)]
  controlIds <- GetAgeMatchedControlIds(patientAge, ageInterval, demo)  
  controlStats <- GetControlFeatureSummaryStats(dat, controlIds, featName)
  fdat <- dat[dat$healthCode %in% patientId, c("medTimepoint", "createdOn", featName)]
  z <- (fdat[, featName] - controlStats$mean)/controlStats$sd 
  alpha <- (1 - floorCeilingRange)/2
  q <- quantile(c(z, -standardDeviations, standardDeviations), c(alpha, 1 - alpha))  
  ql <- as.numeric(q[1])
  qu <- as.numeric(q[2])
  zstar <- pmax(pmin(z, qu), ql)
  y <- (zstar - ql)/(qu - ql)
  controlMean <- (0 - ql)/(qu - ql)
  controlUpper <- (standardDeviations - ql)/(qu - ql)
  controlLower <- (-standardDeviations - ql)/(qu - ql)
  if (reverse) {
    y <- 1 - y
    controlMean <- 1 - controlMean
    controlUpper <- 1 - controlUpper
    controlLower <- 1 - controlLower
  }
  fdat[, featName] <- y
  res <- list(fdat = fdat, 
              controlMean = controlMean, 
              controlUpper = controlUpper, 
              controlLower = controlLower)
  return(res)
}

