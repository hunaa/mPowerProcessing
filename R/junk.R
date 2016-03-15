
participation <- lapply(featureTables, function(fdat) {
  cfdat <- fdat[ fdat$healthCode %in% cases, ]
  cfdat$date <- as.Date(cfdat$createdOn)
  tbl <- table(cfdat$date)
  data.frame(date=as.Date(names(tbl)), freq=as.vector(tbl))
})

windowStart <- as.Date("2015-03-09")
windowEnd   <- as.Date("2015-09-09")
df <- data.frame(date=seq(windowStart, windowEnd, by="day"), stringsAsFactors=FALSE)

for (activity in names(participation)) {
  colnames(participation[[activity]]) <- c("date", activity)
  df <- merge(df, participation[[activity]], all=TRUE)
}

ggplot(df, aes(date)) + 
  ggtitle(expression(atop(bold("Participation in mPower activities"), atop(italic("2015 public data"), "")))) +
  geom_line(aes(y=balance, colour="balance")) + 
  geom_line(aes(y=gait, colour="gait")) +
  geom_line(aes(y=tap, colour="tap")) + 
  geom_line(aes(y=voice, colour="voice"))



close_enough <- function(df1, df2, epsilon=1e-9) {
  if (all(colnames(df1)!=colnames(df2))) { return(FALSE) }
  float_cols = (sapply(df1, class)=="numeric")

  return(all(df1[,!float_cols] == df2[,!float_cols]) && all(df1[,float_cols] - df2[,float_cols] < epsilon))
}


compare_frames <- function(df1, df2, df, epsilon=1e-9) {
  if (all(colnames(df1)!=colnames(df2))) { return(FALSE) }

  df1 <- df1[df$recordId,]
  df2 <- df2[df$recordId,]

  na1 <- which(is.na(df1$recordId))
  na2 <- which(is.na(df2$recordId))

  message(sprintf("%d records missing in df1. Of those, %d are present in df2", length(na1), sum(!(na1 %in% na2))))
  message(sprintf("%d records missing in df2. Of those, %d are present in df1", length(na2), sum(!(na2 %in% na1))))
  message(sprintf("%d records missing in both", length(intersect(na1,na2))))

  float_cols = (sapply(df1, class)=="numeric")

  vals_eq <- df1[,float_cols] - df2[,float_cols] < epsilon
  row_vals_eq <- apply(vals_eq, 1, all)
  md_eq <- df1[,!float_cols] == df2[,!float_cols]
  row_md_eq <- apply(md_eq, 1, all)

  row_eq <- row_vals_eq & row_md_eq
  names(row_eq) <- df$recordId

  return(row_eq)
}

cases <- na.omit(demo$healthCode[demo$`professional-diagnosis`])
names(cases) <- cases

window <- list(start=windowStart, end=windowEnd)

count_activities <- function(dat, healthCode, window) {
  sum( dat$healthCode==healthCode & dat$date >= window$start & dat$date <= window$end )
}

activity_counts <- lapply(cases, function(healthCode) {
  data.frame(activity=names(featureTables),
             count=sapply(featureTables, count_activities, 
                                         healthCode=healthCode,
                                         window=window))
})

nonzero_activity_counts <- Filter(function(df) { sum(df$count) > 0 }, activity_counts)


