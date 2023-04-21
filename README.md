# MUMH quarterly RAP
## What is the MUMH RAP?
This repository is for the Reproducible Analytical Pipeline (RAP) used to produce the outputs for the [Medicines Used in Mental Health quarterly publication](https://www.nhsbsa.nhs.uk/statistical-collections/medicines-used-mental-health-england). The annual publication is produced using a separate RAP. 
The RAP is designed using guidance from the [Reproducible Analytical Pipelines](https://gss.civilservice.gov.uk/reproducible-analytical-pipelines/) working practices and principles from the [AQUA Book guidelines](https://www.gov.uk/government/publications/the-aqua-book-guidance-on-producing-quality-analysis-for-government).

This RAP currently uses the MUMH quarterly R package, which will be replaced in future by a new set of packages for NHS Business Services Authority Official Statistics. It is a work in progress.

This repository is owned and maintained by the NHSBSA Official Statistics team. Responsible statistician: 

## Links to Publication Website
* [ Most recent MUMH publication](https://www.nhsbsa.nhs.uk/statistical-collections/medicines-used-mental-health-england/medicines-used-mental-health-england-quarterly-summary-statistics-october-december-2022)
* [Previous MUMH publications](https://www.nhsbsa.nhs.uk/statistical-collections/medicines-used-mental-health-england)

# Getting Started 
## Initial package set up
The MUMH RAP package is set up to run only in Windows, using Rstudio software. 

This code is being published as part of our commitment to transparency, and to provide an example of RAPs. The pipeline runs without errors but access credentials to internal data have been removed. Although the pipeline is not designed to be run by external users, you are welcome to create test data to apply the code on.

To start, clone this repository and run the `pipeline.R` file in Rstudio from start to finish. The start of the code will load the required R packages, with optional code for installing the `nhsbsaR` and `mumhquarterly` packages from git, if not already installed.

The file `pipeline.R` contains the full RAP with all the code needed to produce the outputs for the publication. It connects to a database, pulls the required data into tables, then analyses the data and runs a linear model on a selected time period. The results are formatted to meet accessibility requirements, then saved into excel tables for quality review and publishing. The narrative and background documents are rendered and saved into the outputs folder as html files. The pipeline also renders the narrative and background as Microsoft Word documents for ease of quality review. 

The pipeline states which sections require any manual code adjustment for new publications, mostly for updating dates in text.

# Contributing
Contributions are not currently being accepted for the MUMH RAP repository. If this changes, a contributing guide will be made available.

# Licence
The MUMH RAP is released under the MIT License. Details can be found in the `LICENSE` file.

The documentation, including `background.rmd`, is © NHSBSA and available under the terms of the [Open Government License (OGL) 3.0 ](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).
