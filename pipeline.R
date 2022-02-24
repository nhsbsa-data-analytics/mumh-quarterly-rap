
# pipeline.R --------------------------------------------------------------
# This script is used to run the RAP for the MUMH quarterly publication


# 1. install required packages --------------------------------------------
# TODO: investigate using renv package for dependency management
req_pkgs <- c("dplyr", "stringr", "data.table", "yaml", "openxlsx","rmarkdown",
              "logr")

utils::install.packages(req_pkgs, dependencies = TRUE)

devtools::install_github(
  "nhsbsa-data-analytics/mumhquarterly",
  auth_token = readLines("C:/Users/MAWIL/credentials/github-pat.txt")
)

invisible(lapply(c(req_pkgs, "mumhquarterly"), library, character.only = TRUE))

# 2. setup logging --------------------------------------------------------

lf <- logr::log_open(autolog = TRUE)

# send code to log
logr::log_code()

# 3. set options ----------------------------------------------------------

mumhquarterly::mumh_options()

# 4. extract data from NHSBSA DWH -----------------------------------------
# build connection to database
con <- con_nhsbsa(
  dsn = "FBS_8192k",
  driver = "Oracle in OraClient19Home1",
  "DWCP",
  username = rstudioapi::showPrompt(title = "Username", message = "Username"),
  password = rstudioapi::askForPassword()
)

#### build time dimension table in schema ####

# drop time dimension if exists
exists <- con %>%
  DBI::dbExistsTable(name = "MUMH_MONTH_TDIM")
# Drop any existing table beforehand
if (exists) {
  con %>%
    DBI::dbRemoveTable(name = "MUMH_MONTH_TDIM")
}

#build table
create_tdim(con, start = 201504) %>%
  compute("MUMH_MONTH_TDIM", analyze = FALSE, temporary = FALSE)

#### build org dimension table in schema ####

# drop org dimension if exists
exists <- con %>%
  DBI::dbExistsTable(name = "MUMH_MONTH_PORG_DIM")
# Drop any existing table beforehand
if (exists) {
  con %>%
    DBI::dbRemoveTable(name = "MUMH_MONTH_PORG_DIM")
}

#build table
create_org_dim(con, country = 1) %>%
  compute("MUMH_MONTH_PORG_DIM", analyze = FALSE, temporary = FALSE)


#### build drug dimension table in schema ####

# drop drug dimension if exists
exists <- con %>%
  DBI::dbExistsTable(name = "MUMH_MONTH_DRUG_DIM")
# Drop any existing table beforehand
if (exists) {
  con %>%
    DBI::dbRemoveTable(name = "MUMH_MONTH_DRUG_DIM")
}

# build table
create_drug_dim(con, bnf_codes = c("0401", "0402", "0403", "0404", "0411"))  %>%
  compute("MUMH_MONTH_DRUG_DIM", analyze = FALSE, temporary = FALSE)

#create fact table
fact <- create_fact(con)

# drop raw data if exists
exists <- con %>%
  DBI::dbExistsTable(name = "MUMH_RAW_DATA")
# Drop any existing table beforehand
if (exists) {
  con %>%
    DBI::dbRemoveTable(name = "MUMH_RAW_DATA")
}

## create raw data in schema
fact %>%
  inner_join(tbl(con, "MUMH_MONTH_TDIM") , by = "YEAR_MONTH") %>%
  inner_join(tbl(con, "MUMH_MONTH_PORG_DIM") , by = c("PRESC_TYPE_PRNT" = "LVL_5_OUPDT",
                                                      "PRESC_ID_PRNT" = "LVL_5_OU")) %>%
  inner_join(tbl(con, "MUMH_MONTH_DRUG_DIM"), by = c("CALC_PREC_DRUG_RECORD_ID" = "RECORD_ID",
                                                     "YEAR_MONTH" = "YEAR_MONTH")) %>%
  compute("MUMH_RAW_DATA", analyze = FALSE, temporary = FALSE)

## build and collect quarterly data
mumh_quarterly <- tbl(con, "MUMH_RAW_DATA") %>%
  group_by(FINANCIAL_YEAR, FINANCIAL_QUARTER, IDENTIFIED_PATIENT_ID, SECTION_DESCR, BNF_SECTION, IDENTIFIED_FLAG) %>%
  summarise(ITEM_COUNT = sum(ITEM_COUNT),
            ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC)/100) %>%
  mutate(PATIENT_COUNT = case_when(
    IDENTIFIED_FLAG == "Y" ~ 1,
    IDENTIFIED_FLAG == "N" ~ 0
  )) %>%
  ungroup() %>%
  group_by(FINANCIAL_YEAR, FINANCIAL_QUARTER, SECTION_DESCR, BNF_SECTION, IDENTIFIED_FLAG) %>%
  summarise(ITEM_COUNT = sum(ITEM_COUNT),
            ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC),
            PATIENT_COUNT = sum(PATIENT_COUNT)) %>%
  rename(SECTION_NAME = SECTION_DESCR,
         SECTION_CODE = BNF_SECTION) %>%
  arrange(FINANCIAL_YEAR, FINANCIAL_QUARTER, SECTION_CODE, desc(IDENTIFIED_FLAG)) %>%
  collect

## build and collect monthly data
mumh_monthly <- tbl(con, "MUMH_RAW_DATA") %>%
  group_by(FINANCIAL_YEAR, FINANCIAL_QUARTER, YEAR_MONTH, IDENTIFIED_PATIENT_ID, SECTION_DESCR, BNF_SECTION, IDENTIFIED_FLAG) %>%
  summarise(ITEM_COUNT = sum(ITEM_COUNT),
            ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC)/100) %>%
  mutate(PATIENT_COUNT = case_when(
    IDENTIFIED_FLAG == "Y" ~ 1,
    IDENTIFIED_FLAG == "N" ~ 0
  )) %>%
  ungroup() %>%
  group_by(FINANCIAL_YEAR, FINANCIAL_QUARTER, YEAR_MONTH, SECTION_DESCR, BNF_SECTION, IDENTIFIED_FLAG) %>%
  summarise(ITEM_COUNT = sum(ITEM_COUNT),
            ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC),
            PATIENT_COUNT = sum(PATIENT_COUNT)) %>%
  rename(SECTION_NAME = SECTION_DESCR,
         SECTION_CODE = BNF_SECTION) %>%
  arrange(YEAR_MONTH, SECTION_CODE, desc(IDENTIFIED_FLAG)) %>%
  collect

#write data
write.csv(mumh_quarterly,
          "data/mumh_quarterly_data.csv",
          row.names = FALSE)

write.csv(mumh_monthly,
          "data/mumh_monthly_data.csv",
          row.names = FALSE)

DBI::dbDisconnect(con)

# 5. save data to folder  -------------------------------------------------

# mumhquarterly::save_data(raw_data, filename = "mumh-quarterly-2122q4")


# 6. import data ----------------------------------------------------------
# import data from data folder to perform aggregations etc without having to
# maintain connection to DWH

logr::sep("read data")

raw_data <- list()

raw_data$monthly <- data.table::fread("data/mumh_monthly_data.csv",
                                      keepLeadingZeros = TRUE)

raw_data$quarterly <- data.table::fread("data/mumh_quarterly_data.csv",
                                        keepLeadingZeros = TRUE)

logr::put(raw_data)

dispensing_days <- mumhquarterly::get_dispensing_days(2022)

logr::put(dispensing_days)


# 7. data manipulation ----------------------------------------------------

logr::sep("data manipulations")

# patient identification rates for most recent data
period <- raw_data$quarterly %>%
  dplyr::select(FINANCIAL_QUARTER) %>%
  dplyr::distinct() %>%
  dplyr::slice_max(
    FINANCIAL_QUARTER,
    n = 4
  ) %>%
  dplyr::pull()

logr::put(period)

# create dataframe
patient_identification <- raw_data$quarterly %>%
  dplyr::filter(FINANCIAL_QUARTER %in% period) %>%
  tidyr::pivot_wider(
    names_from = IDENTIFIED_FLAG,
    values_from = c(ITEM_COUNT, ITEM_PAY_DR_NIC, PATIENT_COUNT)
  ) %>%
  dplyr::mutate(
    RATE = round(ITEM_COUNT_Y/(ITEM_COUNT_Y + ITEM_COUNT_N) * 100, 10)
  ) %>%
  select(
    FINANCIAL_QUARTER,
    `BNF Section Name` = SECTION_NAME,
    `BNF Section Code` = SECTION_CODE,
    RATE
  ) %>%
  tidyr::pivot_wider(
    names_from = FINANCIAL_QUARTER,
    values_from = RATE
  ) %>%
  dplyr::arrange(`BNF Section Code`)

logr::put(patient_identification)

# join monthly data to dispensing days to allow modelling
model_data <- raw_data$monthly %>%
  dplyr::group_by(
    YEAR_MONTH,
    SECTION_NAME,
    SECTION_CODE
  ) %>%
  dplyr::summarise(
    ITEM_COUNT = sum(ITEM_COUNT),
    ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC)
  ) %>%
  dplyr::group_by(SECTION_NAME, SECTION_CODE) %>%
  dplyr::mutate(
    MONTH_INDEX = dplyr::row_number(),
    MONTH_START = as.Date(paste0(YEAR_MONTH,"01"), format = "%Y%m%d"),
    MONTH_NUM = lubridate::month(MONTH_START)
  ) %>%
  dplyr::left_join(
    dispensing_days,
    by = "YEAR_MONTH"
  ) %>%
  dplyr::ungroup()

logr::put(model_data)


# 8. write data to .xlsx --------------------------------------------------

# create dataframe for full patient identification
patient_identification_excel <- raw_data$quarterly %>%
  tidyr::pivot_wider(
    names_from = IDENTIFIED_FLAG,
    values_from = c(ITEM_COUNT, ITEM_PAY_DR_NIC, PATIENT_COUNT)
  ) %>%
  dplyr::mutate(
    RATE = round(ITEM_COUNT_Y/(ITEM_COUNT_Y + ITEM_COUNT_N) * 100, 10)
  ) %>%
  select(
    FINANCIAL_QUARTER,
    `BNF Section Name` = SECTION_NAME,
    `BNF Section Code` = SECTION_CODE,
    RATE
  ) %>%
  tidyr::pivot_wider(
    names_from = FINANCIAL_QUARTER,
    values_from = RATE
  ) %>%
  dplyr::arrange(`BNF Section Code`)

logr::put(patient_identification_excel)

# create wb object
# create list of sheetnames needed (overview and metadata created automatically)
sheetNames <- c("Patient_Identification",
                "Monthly_Data",
                "Quarterly_Data")

wb <- create_wb(sheetNames)

#create metadata tab (will need to open file and auto row heights once ran)
meta_fields <- c("BNF Section Code",
                 "BNF Section Name",
                 "Financial Year",
                 "Year Month",
                 "Financial Quarter",
                 "Identified Patient",
                 "Total Items",
                 "Total Net Ingredient Cost (GBP)",
                 "Total Patients"
)
meta_descs <- c("The unique code used to refer to the British National Formulary (BNF) section.",
                "The name given to a British National Formulary (BNF) section. This is the next broadest grouping of the BNF therapeutical classification system after chapter.",
                "The financial year to which the data belongs.",
                "The year and month to which the data belongs, denoted in YYYYMM format.",
                "The financial quarter to which the data belongs.",
                "This shows where an item has been attributed to an NHS number that has been verified by the Personal Demographics Service (PDS).",
                "The number of prescription items dispensed. 'Items' is the number of times a product appears on a prescription form. Prescription forms include both paper prescriptions and electronic messages.",
                "Total Net Ingredient Cost is the amount that would be paid using the basic price of the prescribed drug or appliance and the quantity prescribed. Sometimes called the 'Net Ingredient Cost' (NIC). The basic price is given either in the Drug Tariff or is determined from prices published by manufacturers, wholesalers or suppliers. Basic price is set out in Parts 8 and 9 of the Drug Tariff. For any drugs or appliances not in Part 8, the price is usually taken from the manufacturer, wholesaler or supplier of the product. This is given in GBP (£).",
                "Where patients are identified via the flag, the number of patients that the data corresponds to. This will always be 0 where 'Identified Patient' = N."
)
create_metadata(wb,
                meta_fields,
                meta_descs
)

#### Patient identification
# write data to sheet
write_sheet(wb,
            "Patient_Identification",
            "Medicines Used in Mental Health - Quarterly Summary Statistics April 2015 to December 2021 - Proportion of items for which an NHS number was recorded (%)",
            c("The below proportions reflect the percentage of prescription items where a PDS verified NHS number was recorded."),
            patient_identification_excel,
            42)

#left align columns A and B
format_data(wb,
            "Patient_Identification",
            c("A", "B"),
            "left",
            "")

#right align columns and round to 2 DP - C to AC (!!NEED TO UPDATE AS DATA EXPANDS!!)
format_data(wb,
            "Patient_Identification",
            c("C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N",
              "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
              "AA", "AB", "AC"),
            "right",
            "0.00")

#### Monthly data
# write data to sheet
write_sheet(wb,
            "Monthly_Data",
            "Medicines Used in Mental Health - Quarterly Summary Statistics April 2015 to December 2021 totals by year month",
            c("1. Field definitions can be found on the 'Metadata' tab.",
              "2. Patient counts should not be aggregated to any other level than that which is displayed to prevent multiple counting of patients."),
            raw_data$monthly %>%
              rename(`Financal Year` = FINANCIAL_YEAR,
                     `Financial Quarter` = FINANCIAL_QUARTER,
                     `Year Month` = YEAR_MONTH,
                     `BNF Section Name` = SECTION_NAME,
                     `BNF Section Code` = SECTION_CODE,
                     `Identified Patient` = IDENTIFIED_FLAG,
                     `Total Patients` = PATIENT_COUNT,
                     `Total Items` = ITEM_COUNT,
                     `Total Net Ingredient Cost (GBP)` = ITEM_PAY_DR_NIC) %>%
              select(`Financal Year`,
                     `Financial Quarter`,
                     `Year Month`,
                     `BNF Section Name`,
                     `BNF Section Code`,
                     `Identified Patient`,
                     `Total Patients`,
                     `Total Items`,
                     `Total Net Ingredient Cost (GBP)`),
            14)

#left align columns A to F
format_data(wb,
            "Monthly_Data",
            c("A", "B", "C", "D", "E", "F"),
            "left",
            "")

#right align columns G and H and round to whole numbers with thousand separator
format_data(wb,
            "Monthly_Data",
            c("G", "H"),
            "right",
            "#,##0")

#right align column I and round to 2dp with thousand separator
format_data(wb,
            "Monthly_Data",
            c("I"),
            "right",
            "#,##0.00")

#### Quarterly data
# write data to sheet
write_sheet(wb,
            "Quarterly_Data",
            "Medicines Used in Mental Health - Quarterly Summary Statistics April 2015 to December 2021 totals by quarter",
            c("1. Field definitions can be found on the 'Metadata' tab.",
              "2. Patient counts should not be aggregated to any other level than that which is displayed to prevent multiple counting of patients."),
            raw_data$quarterly %>%
              rename(`Financal Year` = FINANCIAL_YEAR,
                     `Financial Quarter` = FINANCIAL_QUARTER,
                     `BNF Section Name` = SECTION_NAME,
                     `BNF Section Code` = SECTION_CODE,
                     `Identified Patient` = IDENTIFIED_FLAG,
                     `Total Patients` = PATIENT_COUNT,
                     `Total Items` = ITEM_COUNT,
                     `Total Net Ingredient Cost (GBP)` = ITEM_PAY_DR_NIC) %>%
              select(`Financal Year`,
                     `Financial Quarter`,
                     `BNF Section Name`,
                     `BNF Section Code`,
                     `Identified Patient`,
                     `Total Patients`,
                     `Total Items`,
                     `Total Net Ingredient Cost (GBP)`),
            14)

#left align columns A to F
format_data(wb,
            "Quarterly_Data",
            c("A", "B", "C", "D", "E"),
            "left",
            "")

#right align columns G and H and round to whole numbers with thousand separator
format_data(wb,
            "Quarterly_Data",
            c("F", "G"),
            "right",
            "#,##0")

#right align column I and round to 2dp with thousand separator
format_data(wb,
            "Quarterly_Data",
            c("H"),
            "right",
            "#,##0.00")

#save file into outputs folder
openxlsx::saveWorkbook(wb,
                       "outputs/mumh_quarterly_dec21_v001.xlsx",
                       overwrite = TRUE)




# 9. render markdown ------------------------------------------------------

rmarkdown::render("mumh-quarterly-narrative.Rmd",
                  output_format = "html_document",
                  output_file = "outputs/mumh_quarterly_dec21_v001.html")

rmarkdown::render(
  "mumh-quarterly-narrative.Rmd",
  output_format = "word_document",
  output_file = paste0("outputs/mumh_quarterly_dec21_v001.docx")
)

logr::log_close()
