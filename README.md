# HLTMonitoring

The code in this repository is a sample of code that I wrote to automate the review of updates to trigger algorithms in a comparison of recorded data and reprocessed data using the new algorithms. It is important that this review takes place before updates are implemented in the trigger and thereby influence the run-time operation of the detector and data-taking. This code is written to interface with the Data Quality Monitoring Framework and update the Data Quality servers. 

* BinContentDiff.cxx - bin-by-bin comparison of two histograms 
* hltmon.py - main program for generating the comparison of the histograms in two files and publishing to the data quality server