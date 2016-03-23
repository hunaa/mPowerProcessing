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

#' Normalize feature data relative to an age matched control.
#'
#' @param dat data.frame of feature data for an activity
#' @param patientId patient's healthCode
#' @param featName name of feature column
#' @param demo data.frame holding demographic data
#' @param ageInterval age interval of controls
#' @param floorCeilingRange
#' @param standardDeviations
#' @param reverse
#'
#' @return a list of fdat, controlMean, controlUpper and controlLower where
#'         fdat is a data.frame with columns "medTimepoint", "date",
#'         and featName, with the feature column normalized to fall between
#'         0 and 1.
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
  fdat <- dat[dat$healthCode %in% patientId, c("medTimepoint", "date", featName)]
  z <- (fdat[, featName] - controlStats$mean)/controlStats$sd
  alpha <- (1 - floorCeilingRange)/2
  q <- quantile(c(z, -standardDeviations, standardDeviations), c(alpha, 1 - alpha), na.rm=TRUE)
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


#' Find cases who have performed activities withing the date
#' window and at pre or post medication time points
#'
#' @param lists a list of lists
#' @return a list of health codes
findCasesWithPrepostActivity <- function(demo, featureTables, window) {
  # find participants with a PD diagnosis
  cases <- na.omit(demo$healthCode[demo$`professional-diagnosis` & !is.na(demo$age)])
  names(cases) <- cases

  prepost <- c('Immediately before Parkinson medication',
               'Just after Parkinson medication (at your best)')

  healthCodes <- unique(unlist(
    lapply(featureTables, function(dat) {
      ## subset the data frame keeping activities performed by PD
      ## patients, within the date window, pre or post medication
      dat$healthCode[ dat$date >= window$start &
                      dat$date <= window$end &
                      dat$healthCode %in% cases &
                      dat$medTimepoint %in% prepost ]
    })
  ))
  names(healthCodes) <- healthCodes
  return(healthCodes)
}

#' collect dates on which a participant performed some activity
#' from the output of getVisData
collectDates <- function(x) {
  unique(do.call(c,
    lapply(x, function(xa) {
      xa$fdat$date
    })))
}
