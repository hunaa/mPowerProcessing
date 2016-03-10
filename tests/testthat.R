# Run all tests
# run this from the base folder (i.e. the folder above 'src' and 'test'
# 
# Author: bhoff
###############################################################################
library(testthat)
library("mPowerProcessing")

#TODO reenable tests by removing filter
test_check("mPowerProcessing", filter="process_survey_v1")
