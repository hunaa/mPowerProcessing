# Run all tests
# run this from the base folder (i.e. the folder above 'src' and 'test')
# 
# Author: bhoff
###############################################################################
library(testthat)
library("mPowerProcessing")

# https://github.com/travis-ci/travis-ci/issues/3849
test_check("mPowerProcessing", filter="_0")
test_check("mPowerProcessing", filter="_1")
test_check("mPowerProcessing", filter="_2")
test_check("mPowerProcessing", filter="_3")
test_check("mPowerProcessing", filter="_7")
test_check("mPowerProcessing", filter="_8")
test_check("mPowerProcessing", filter="_unit")
