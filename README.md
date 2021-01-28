# SpiderRock
Utilities for downloading and analysing fills from the SpiderRock system.  All the code was written in Python 3.9.1.  Required packages are detailed in requirements.txt.

*There are currently three python scripts in this project.*

## QuerySRTables.py
This script uses MySQL to connect to SpiderRock's SRSE Trade database and store the results into the FillData folder in this repo.  This needs to be run each day since the SRSE tables do not persist reliably.

## ProcessExecutions.py
This generates a table of TCA information from a file from FillData.  It stores this as a .csv file to the TCA folder in this repo.

## FillVizualizer.py
This produces an graphic showing the progress of an execution over time from a file from FillData. It stores this as a .html file to the TCA folder in this repo.
