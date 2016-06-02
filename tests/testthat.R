# Run all tests
# run this from the base folder (i.e. the folder above 'src' and 'test')
# 
# Author: bhoff
###############################################################################
library(testthat)
library("mPowerProcessing")

filter<-Sys.getenv("TEST_THAT_FILTER")
if (nchar(filter)>0) {
	test_check("mPowerProcessing", filter=filter)
} else {
	test_check("mPowerProcessing")
}
