# pipeline.R --------------------------------------------------------------
# This script is used to run the RAP for the MUMH quarterly publication

# 1. install required packages --------------------------------------------
# TODO: investigate using renv package for dependency management
req_pkgs <- c("dplyr", "stringr", "data.table", "yaml", "openxlsx","rmarkdown",
              "logr", "highcharter", "lubridate", "dbplyr")

# utils::install.packages(req_pkgs, dependencies = TRUE)
#
# devtools::install_github(
#   "nhsbsa-data-analytics/mumhquarterly",
#   auth_token = Sys.getenv("GITHUB_PAT")
#   )

#devtools::install_github("nhsbsa-data-analytics/nhsbsaR")

invisible(lapply(c(req_pkgs, "mumhquarterly", "nhsbsaR"), library, character.only = TRUE))

# 2. setup logging --------------------------------------------------------

#lf <- logr::log_open(autolog = TRUE)

# send code to log
#logr::log_code()

# 3. set options ----------------------------------------------------------

mumhquarterly::mumh_options()

# 4. load most recent data and add 3 months to max date -------------------

#get most recent monthly file
recent_file_monthly <- rownames(file.info(
  list.files(
    "Y:/Official Stats/MUMH/data",
    full.names = T,
    pattern = "monthly"
  )
))[which.max(file.info(
  list.files(
    "Y:/Official Stats/MUMH/data",
    full.names = T,
    pattern = "monthly"
  )
)$mtime)]

#read data
recent_data_monthly <- data.table::fread(recent_file_monthly,
                                 keepLeadingZeros = TRUE)

#get most recent monthly file
recent_file_quarterly <- rownames(file.info(
  list.files(
    "Y:/Official Stats/MUMH/data",
    full.names = T,
    pattern = "quarterly"
  )
))[which.max(file.info(
  list.files(
    "Y:/Official Stats/MUMH/data",
    full.names = T,
    pattern = "quarterly"
  )
)$mtime)]

#read data
recent_data_quarterly <- data.table::fread(recent_file_quarterly,
                                         keepLeadingZeros = TRUE)


#get max month
max_month <- as.Date(
  paste0(
    max(
      recent_data_monthly$YEAR_MONTH
      ),
    "01"
    ),
  format = "%Y%m%d"
)

#get max month plus one
max_month_plus <- as.Date(paste0(max(recent_data_monthly$YEAR_MONTH),
                                 "01"),
                          format = "%Y%m%d") %m+% months(1)

#convert to DW format
max_month_plus_dw <- as.numeric(paste0(format(max_month_plus,
                                              "%Y"),
                                       format(max_month_plus,
                                              "%m")))

# 5. extract data from NHSBSA DWH -----------------------------------------
# build connection to database
con <- con_nhsbsa(
  dsn = "FBS_8192k",
  driver = "Oracle in OraClient19Home1",
  "DWCP",
  username = rstudioapi::showPrompt(title = "Username", message = "Username"),
  password = rstudioapi::askForPassword()
)

#get max month available in DWH
ym_dim <- dplyr::tbl(con,
                     from = dbplyr::in_schema("DIM", "YEAR_MONTH_DIM")) %>%

#shrink table to remove unnecessary data
dplyr::filter(
    YEAR_MONTH >= 201401L,
    YEAR_MONTH <= dplyr::sql(
      "MGMT.PKG_PUBLIC_DWH_FUNCTIONS.f_get_latest_period('EPACT2')"
    )
  ) %>%
  dplyr::select(YEAR_MONTH,
                FINANCIAL_YEAR,
                FINANCIAL_QUARTER,
                FINANCIAL_QUARTER_EXT)  %>%
  # add month counts for financial quarters and financial years to latest
  # complete periods
  dplyr::mutate(
    # create our own financial quarter column that max/min and sort operations
    # will work on
    FINANCIAL_QUARTER_NM = dplyr::sql("FINANCIAL_YEAR||' Q'||FINANCIAL_QUARTER"),
    # window function to perform counts within groups
    Q_COUNT = dbplyr::win_over(
      expr = dplyr::sql("count(distinct YEAR_MONTH)"),
      partition = "FINANCIAL_QUARTER_EXT",
      con = con
    ),
    FY_COUNT = dbplyr::win_over(
      expr = dplyr::sql("count(distinct YEAR_MONTH)"),
      partition = "FINANCIAL_YEAR",
      con = con
    )
  )

# extract latest available full financial quarter
ltst_month <- ym_dim %>%
  dplyr::filter(Q_COUNT == 3) %>%
  dplyr::select(YEAR_MONTH, FINANCIAL_QUARTER_EXT, FINANCIAL_QUARTER_NM) %>%
  dplyr::filter(YEAR_MONTH == max(YEAR_MONTH, na.rm = TRUE)) %>%
  dplyr::distinct() %>%
  dplyr::pull(YEAR_MONTH)

#get max month in dwh
max_month_dw <- as.Date(paste0(ltst_month,
                               "01"),
                        format = "%Y%m%d")

# only get new data if max month in dwh is greater than that in most recent data
if(max_month_dw <= max_month) {
  print("No new quarterly data available in the Data Warehouse, will use most recent saved data.")

  DBI::dbDisconnect(con)
} else {

# build time dimension table in schema

# drop time dimension if exists
exists <- con %>%
  DBI::dbExistsTable(name = "MUMH_MONTH_TDIM")
# Drop any existing table beforehand
if (exists) {
  con %>%
    DBI::dbRemoveTable(name = "MUMH_MONTH_TDIM")
}

#build table
create_tdim(con, start = max_month_plus_dw) %>%
  compute("MUMH_MONTH_TDIM", analyze = FALSE, temporary = FALSE)

# build org dimension table in schema

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

# build drug dimension table in schema

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
  group_by(
    FINANCIAL_YEAR,
    FINANCIAL_QUARTER,
    IDENTIFIED_PATIENT_ID,
    SECTION_DESCR,
    BNF_SECTION,
    IDENTIFIED_FLAG
  ) %>%
  summarise(
    ITEM_COUNT = sum(ITEM_COUNT),
    ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC) / 100
  ) %>%
  mutate(PATIENT_COUNT = case_when(IDENTIFIED_FLAG == "Y" ~ 1,
                                   IDENTIFIED_FLAG == "N" ~ 0)) %>%
  ungroup() %>%
  group_by(FINANCIAL_YEAR,
           FINANCIAL_QUARTER,
           SECTION_DESCR,
           BNF_SECTION,
           IDENTIFIED_FLAG) %>%
  summarise(
    ITEM_COUNT = sum(ITEM_COUNT),
    ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC),
    PATIENT_COUNT = sum(PATIENT_COUNT)
  ) %>%
  rename(SECTION_NAME = SECTION_DESCR,
         SECTION_CODE = BNF_SECTION) %>%
  arrange(FINANCIAL_YEAR,
          FINANCIAL_QUARTER,
          SECTION_CODE,
          desc(IDENTIFIED_FLAG)) %>%
  collect

## build and collect monthly data
mumh_monthly <- tbl(con, "MUMH_RAW_DATA") %>%
  group_by(
    FINANCIAL_YEAR,
    FINANCIAL_QUARTER,
    YEAR_MONTH,
    IDENTIFIED_PATIENT_ID,
    SECTION_DESCR,
    BNF_SECTION,
    IDENTIFIED_FLAG
  ) %>%
  summarise(
    ITEM_COUNT = sum(ITEM_COUNT),
    ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC) / 100
  ) %>%
  mutate(PATIENT_COUNT = case_when(IDENTIFIED_FLAG == "Y" ~ 1,
                                   IDENTIFIED_FLAG == "N" ~ 0)) %>%
  ungroup() %>%
  group_by(
    FINANCIAL_YEAR,
    FINANCIAL_QUARTER,
    YEAR_MONTH,
    SECTION_DESCR,
    BNF_SECTION,
    IDENTIFIED_FLAG
  ) %>%
  summarise(
    ITEM_COUNT = sum(ITEM_COUNT),
    ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC),
    PATIENT_COUNT = sum(PATIENT_COUNT)
  ) %>%
  rename(SECTION_NAME = SECTION_DESCR,
         SECTION_CODE = BNF_SECTION) %>%
  arrange(YEAR_MONTH, SECTION_CODE, desc(IDENTIFIED_FLAG)) %>%
  collect

DBI::dbDisconnect(con)

##join any new data to most recent saved data
mumh_monthly <- recent_data_monthly %>%
  bind_rows(mumh_monthly)

mumh_quarterly <- recent_data_quarterly %>%
  bind_rows(mumh_quarterly)

save_data(mumh_quarterly, dir = "Y:/Official Stats/MUMH", filename = "mumh_quarterly")

save_data(mumh_monthly, dir = "Y:/Official Stats/MUMH", filename = "mumh_monthly")
}

# 6. import data ----------------------------------------------------------
# import data from data folder to perform aggregations etc without having to
# maintain connection to DWH
#logr::sep("read data")

raw_data <- list()

#read most recent monthly file
raw_data$monthly <- data.table::fread(rownames(file.info(
  list.files(
    "Y:/Official Stats/MUMH/data",
    full.names = T,
    pattern = "monthly"
  )
))[which.max(file.info(
  list.files(
    "Y:/Official Stats/MUMH/data",
    full.names = T,
    pattern = "monthly"
  )
)$mtime)],
keepLeadingZeros = TRUE)

#read most recent quarterly file
raw_data$quarterly <- data.table::fread(rownames(file.info(
  list.files(
    "Y:/Official Stats/MUMH/data",
    full.names = T,
    pattern = "quarterly"
  )
))[which.max(file.info(
  list.files(
    "Y:/Official Stats/MUMH/data",
    full.names = T,
    pattern = "quarterly"
  )
)$mtime)],
keepLeadingZeros = TRUE)

#logr::put(raw_data)

dispensing_days <- mumhquarterly::get_dispensing_days(2022)

#logr::put(dispensing_days)

# 7. data manipulation ----------------------------------------------------

#logr::sep("data manipulations")

# patient identification rates for most recent data
period <- raw_data$quarterly %>%
  dplyr::select(FINANCIAL_QUARTER) %>%
  dplyr::distinct() %>%
  dplyr::slice_max(FINANCIAL_QUARTER,
                   n = 4) %>%
  dplyr::pull()

#logr::put(period)

# create dataframe
patient_identification <- raw_data$quarterly %>%
  dplyr::filter(FINANCIAL_QUARTER %in% period) %>%
  tidyr::pivot_wider(
    names_from = IDENTIFIED_FLAG,
    values_from = c(ITEM_COUNT, ITEM_PAY_DR_NIC, PATIENT_COUNT)
  ) %>%
  dplyr::mutate(RATE = paste0(format(round(
    ITEM_COUNT_Y / (ITEM_COUNT_Y + ITEM_COUNT_N) * 100, 1
  ),
  nsmall = 1), "%")) %>%
  select(FINANCIAL_QUARTER,
         `BNF Section Name` = SECTION_NAME,
         `BNF Section Code` = SECTION_CODE,
         RATE) %>%
  tidyr::pivot_wider(names_from = FINANCIAL_QUARTER,
                     values_from = RATE) %>%
  dplyr::arrange(`BNF Section Code`)

#logr::put(patient_identification)

# chart data for use in markdown

chart_data<- list()

chart_data$monthly <- raw_data$monthly %>%
  dplyr::group_by(YEAR_MONTH,
                  SECTION_NAME,
                  SECTION_CODE) %>%
  dplyr::summarise(
    ITEM_COUNT = sum(ITEM_COUNT),
    ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC),
    PATIENT_COUNT = sum(PATIENT_COUNT),
    .groups = "drop"
  ) %>%
  dplyr::group_by(SECTION_NAME, SECTION_CODE) %>%
  dplyr::mutate(
    MONTH_INDEX = dplyr::row_number(),
    MONTH_START = as.Date(paste0(YEAR_MONTH, "01"), format = "%Y%m%d"),
    MONTH_NUM = lubridate::month(MONTH_START)
  ) %>%
  dplyr::left_join(dispensing_days,
                   by = "YEAR_MONTH") %>%
  dplyr::ungroup()

#logr::put(chart_data$monthly)

# join monthly data to dispensing days to allow modelling
model_data <- raw_data$monthly %>%
  dplyr::group_by(YEAR_MONTH,
                  SECTION_NAME,
                  SECTION_CODE) %>%
  dplyr::summarise(ITEM_COUNT = sum(ITEM_COUNT),
                   ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC)) %>%
  dplyr::group_by(SECTION_NAME, SECTION_CODE) %>%
  dplyr::mutate(
    MONTH_INDEX = dplyr::row_number(),
    MONTH_START = as.Date(paste0(YEAR_MONTH, "01"), format = "%Y%m%d"),
    MONTH_NUM = lubridate::month(MONTH_START)
  ) %>%
  dplyr::left_join(dispensing_days,
                   by = "YEAR_MONTH") %>%
  dplyr::ungroup()

#logr::put(model_data)

# 8. write data to .xlsx --------------------------------------------------

# create dataframe for full patient identification
patient_identification_excel <- raw_data$quarterly %>%
  tidyr::pivot_wider(
    names_from = IDENTIFIED_FLAG,
    values_from = c(ITEM_COUNT, ITEM_PAY_DR_NIC, PATIENT_COUNT)
  ) %>%
  dplyr::mutate(RATE = round(ITEM_COUNT_Y / (ITEM_COUNT_Y + ITEM_COUNT_N) * 100, 10)) %>%
  select(FINANCIAL_QUARTER,
         `BNF Section Name` = SECTION_NAME,
         `BNF Section Code` = SECTION_CODE,
         RATE) %>%
  tidyr::pivot_wider(names_from = FINANCIAL_QUARTER,
                     values_from = RATE) %>%
  dplyr::arrange(`BNF Section Code`)

#logr::put(patient_identification_excel)

# create wb object
# create list of sheetnames needed (overview and metadata created automatically)
sheetNames <- c("Patient_Identification",
                "Monthly_Data",
                "Quarterly_Data")

wb <- create_wb(sheetNames)

#create metadata tab (will need to open file and auto row heights once ran)
meta_fields <- c(
  "BNF Section Code",
  "BNF Section Name",
  "Financial Year",
  "Year Month",
  "Financial Quarter",
  "Identified Patient",
  "Total Items",
  "Total Net Ingredient Cost (GBP)",
  "Total Patients"
)

meta_descs <-
  c(
    "The unique code used to refer to the British National Formulary (BNF) section.",
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
write_sheet(
  wb,
  "Patient_Identification",
  "Medicines Used in Mental Health - Quarterly Summary Statistics April 2015 to March 2022 - Proportion of items for which an NHS number was recorded (%)",
  c(
    "The below proportions reflect the percentage of prescription items where a PDS verified NHS number was recorded."
  ),
  patient_identification_excel,
  42
)

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
              "AA", "AB", "AC", "AD"),
            "right",
            "0.00")

#### Monthly data
# write data to sheet
write_sheet(
  wb,
  "Monthly_Data",
  "Medicines Used in Mental Health - Quarterly Summary Statistics April 2015 to March 2022 totals by year month",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Patient counts should not be aggregated to any other level than that which is displayed to prevent multiple counting of patients."
  ),
  raw_data$monthly %>%
    rename(
      `Financal Year` = FINANCIAL_YEAR,
      `Financial Quarter` = FINANCIAL_QUARTER,
      `Year Month` = YEAR_MONTH,
      `BNF Section Name` = SECTION_NAME,
      `BNF Section Code` = SECTION_CODE,
      `Identified Patient` = IDENTIFIED_FLAG,
      `Total Patients` = PATIENT_COUNT,
      `Total Items` = ITEM_COUNT,
      `Total Net Ingredient Cost (GBP)` = ITEM_PAY_DR_NIC
    ) %>%
    select(
      `Financal Year`,
      `Financial Quarter`,
      `Year Month`,
      `BNF Section Name`,
      `BNF Section Code`,
      `Identified Patient`,
      `Total Patients`,
      `Total Items`,
      `Total Net Ingredient Cost (GBP)`
    ),
  14
)

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
write_sheet(
  wb,
  "Quarterly_Data",
  "Medicines Used in Mental Health - Quarterly Summary Statistics April 2015 to March 2022 totals by quarter",
  c(
    "1. Field definitions can be found on the 'Metadata' tab.",
    "2. Patient counts should not be aggregated to any other level than that which is displayed to prevent multiple counting of patients."
  ),
  raw_data$quarterly %>%
    rename(
      `Financal Year` = FINANCIAL_YEAR,
      `Financial Quarter` = FINANCIAL_QUARTER,
      `BNF Section Name` = SECTION_NAME,
      `BNF Section Code` = SECTION_CODE,
      `Identified Patient` = IDENTIFIED_FLAG,
      `Total Patients` = PATIENT_COUNT,
      `Total Items` = ITEM_COUNT,
      `Total Net Ingredient Cost (GBP)` = ITEM_PAY_DR_NIC
    ) %>%
    select(
      `Financal Year`,
      `Financial Quarter`,
      `BNF Section Name`,
      `BNF Section Code`,
      `Identified Patient`,
      `Total Patients`,
      `Total Items`,
      `Total Net Ingredient Cost (GBP)`
    ),
  14
)

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
                       "outputs/mumh_quarterly_mar22_v001.xlsx",
                       overwrite = TRUE)

#Write covid model data to table for QR

bnf_list <- c("0403", "0401", "0402", "0404", "0411")

for (i in 1:length(bnf_list)) {
  bnf_data <- model_data %>%
    covid_model() %>%
    filter(SECTION_CODE == bnf_list[i], YEAR_MONTH > 202002) %>%
    select(SECTION_NAME,
           SECTION_CODE,
           YEAR_MONTH,
           ITEM_COUNT,
           PRED_ITEMS_95_FIT)

  fwrite(
    bnf_data,
    paste0(
      "Y:/Official Stats/MUMH/Covid model tables/",
      as.character(unlist(bnf_data[1, 1])),
      ".csv"
    )
  )
}

# 9. output figures needed for QR purposes --------------------------------
### BUILD FILTERS
#first get max month and quarter
max_month <- max(raw_data$monthly$YEAR_MONTH)

quarter <- raw_data$monthly %>%
  dplyr::filter(YEAR_MONTH == max_month) %>%
  tail(1) %>%
  pull(FINANCIAL_QUARTER)

#get previous quarter for filtering
prev_quarter <- quarter(
  #uses max_month minus 3 months
  as.Date(paste0(max_month, "01"), format = "%Y%m%d") %m-% months(3),
  type = "quarter",
  #set for fiscal year starting in April
  fiscal_start = 4
)

#get financial year of previous quarter
prev_quarter_fy <- quarter(
  #uses max_month minus 3 months
  as.Date(paste0(max_month, "01"), format = "%Y%m%d") %m-% months(3),
  type = "year.quarter",
  #set for fiscal year starting in April
  fiscal_start = 4
) %>%
  substr(1, 4) %>%
  as.numeric()

#build filter for previous quarter
prev_quarter_filter <- paste0(prev_quarter_fy-1, "/", prev_quarter_fy,
                              " Q", prev_quarter)

#get previous year for filtering
prev_year <- quarter(
  #uses max_month minus 12 months
  as.Date(paste0(max_month, "01"), format = "%Y%m%d") %m-% months(12),
  type = "quarter",
  #set for fiscal year starting in April
  fiscal_start = 4
)

#get financial year of previous year
prev_year_fy <- quarter(
  #uses max_month minus 12 months
  as.Date(paste0(max_month, "01"), format = "%Y%m%d") %m-% months(12),
  type = "year.quarter",
  #set for fiscal year starting in April
  fiscal_start = 4
) %>%
  substr(1, 4) %>%
  as.numeric()

#build filter for previous year
prev_year_filter <- paste0(prev_year_fy - 1, "/", prev_year_fy,
                           " Q", prev_year)


prev_5_year <- quarter(
  #uses max_month minus 12 months
  as.Date(paste0(max_month, "01"), format = "%Y%m%d") %m-% months(60),
  type = "quarter",
  #set for fiscal year starting in April
  fiscal_start = 4
)

#get financial year of previous quarter
prev_5_year_quarter_fy <- quarter(
  #uses max_month minus 3 months
  as.Date(paste0(max_month, "01"), format = "%Y%m%d") %m-% months(60),
  type = "year.quarter",
  #set for fiscal year starting in April
  fiscal_start = 4
) %>%
  substr(1, 4) %>%
  as.numeric()

#build filter for previous quarter
filter_5_years <-
  paste0(prev_5_year_quarter_fy - 1,
         "/",
         prev_5_year_quarter_fy,
         " Q",
         prev_5_year)

#build filters for previous 12 month period and 12 month period prior to that
filter_12_months <-
  as.numeric(format(as.Date(paste0(max_month, "01"),
                            format = "%Y%m%d") -
                      months(0:11),
                    format = "%Y%m"))

filter_prev_12_months <-
  as.numeric(format(as.Date(paste0(max_month, "01"),
                            format = "%Y%m%d") -
                      months(12:23),
                    format = "%Y%m"))

#get min and max dates of each quarter formatted as nice text
min_filter_12_months <-
  format(as.Date(paste0(min(filter_12_months), "01"),
                 format = "%Y%m%d"),
         format = "%B %Y")

max_filter_12_months <-
  format(as.Date(paste0(max(filter_12_months), "01"),
                 format = "%Y%m%d"),
         format = "%B %Y")

min_filter_prev_12_months <-
  format(as.Date(paste0(min(
    filter_prev_12_months
  ), "01"),
  format = "%Y%m%d"),
  format = "%B %Y")

max_filter_prev_12_months <-
  format(as.Date(paste0(max(
    filter_prev_12_months
  ), "01"),
  format = "%Y%m%d"),
  format = "%B %Y")

#create blank workbook for saving
qrwb <- openxlsx::createWorkbook()

openxlsx::modifyBaseFont(qrwb, fontName = "Arial", fontSize = 10)

#loop through bnf_list
for(j in 1:length(bnf_list)){

  # create data
  code <- bnf_list[j]
  name <- raw_data$quarterly %>%
    filter(SECTION_CODE == code) %>%
    select(SECTION_NAME) %>%
    unique() %>%
    pull()

  #add sheet to wb with bnf_code
  openxlsx::addWorksheet(qrwb,
                         sheetName = code,
                         gridLines = FALSE)

  #current quarter volume
  cur_quart_volume <- raw_data$quarterly %>%
    filter(SECTION_CODE == code,
           FINANCIAL_QUARTER == quarter) %>%
    select(ITEM_COUNT) %>%
    colSums(.) %>%
    as.numeric()

  #previous quarter volume
  prev_quart_volume <- raw_data$quarterly %>%
    filter(SECTION_CODE == code,
           FINANCIAL_QUARTER == prev_quarter_filter) %>%
    select(ITEM_COUNT) %>%
    colSums(.) %>%
    as.numeric()

  #previous year quarter volume
  prev_year_quart_volume <- raw_data$quarterly %>%
    filter(SECTION_CODE == code,
           FINANCIAL_QUARTER == prev_year_filter) %>%
    select(ITEM_COUNT) %>%
    colSums(.) %>%
    as.numeric()

  #get volume from same quarter 5 years ago
  prev_5_year_quart_volume <- raw_data$quarterly %>%
    filter(SECTION_CODE == code,
           FINANCIAL_QUARTER == filter_5_years) %>%
    select(ITEM_COUNT) %>%
    colSums(.) %>%
    as.numeric()

  annual_per_change <-
    ((cur_quart_volume - prev_year_quart_volume) / prev_year_quart_volume) *
    100
  quarterly_per_change <-
    ((cur_quart_volume - prev_quart_volume) / prev_quart_volume) * 100
  annual_5_per_change <-
    ((cur_quart_volume - prev_5_year_quart_volume) / prev_5_year_quart_volume) *
    100

  #get patient count of current quarter
  cur_quart_patients <- raw_data$quarterly %>%
    filter(SECTION_CODE == code,
           FINANCIAL_QUARTER == quarter) %>%
    select(PATIENT_COUNT) %>%
    colSums(.) %>%
    as.numeric()

  #previous quarter patient count
  prev_quart_patients <- raw_data$quarterly %>%
    filter(SECTION_CODE == code,
           FINANCIAL_QUARTER == prev_quarter_filter) %>%
    select(PATIENT_COUNT) %>%
    colSums(.) %>%
    as.numeric()

  #previous year quarter patient count
  prev_year_quart_patients <- raw_data$quarterly %>%
    filter(SECTION_CODE == code,
           FINANCIAL_QUARTER == prev_year_filter) %>%
    select(PATIENT_COUNT) %>%
    colSums(.) %>%
    as.numeric()

  #get patient count from same quarter 5 years ago
  prev_5_year_quart_patients <- raw_data$quarterly %>%
    filter(SECTION_CODE == code,
           FINANCIAL_QUARTER == filter_5_years) %>%
    select(PATIENT_COUNT) %>%
    colSums(.) %>%
    as.numeric()

  annual_per_change_patients <-
    ((cur_quart_patients - prev_year_quart_patients) / prev_year_quart_patients) *
    100
  quarterly_per_change_patients <-
    ((cur_quart_patients - prev_quart_patients) / prev_quart_patients) * 100
  annual_5_per_change_patients <-
    ((cur_quart_patients - prev_5_year_quart_patients) / prev_5_year_quart_patients) *
    100

  #get identified patient rates
  current_quart_identified <- patient_identification_excel %>%
    filter(`BNF Section Code` == code) %>%
    select(quarter) %>%
    colSums(.) %>%
    as.numeric()

  prev_5_year_quart_identified <- patient_identification_excel %>%
    filter(`BNF Section Code` == code) %>%
    select(filter_5_years) %>%
    colSums(.) %>%
    as.numeric()

  #average monthly patients
  ave_12_month_patients <- raw_data$monthly %>%
    filter(SECTION_CODE == code,
           YEAR_MONTH %in% filter_12_months,
           IDENTIFIED_FLAG == "Y") %>%
    select(PATIENT_COUNT) %>%
    colMeans(.) %>%
    as.numeric()

  ave_12_month_patients_prev <- raw_data$monthly %>%
    filter(SECTION_CODE == code,
           YEAR_MONTH %in% filter_prev_12_months,
           IDENTIFIED_FLAG == "Y") %>%
    select(PATIENT_COUNT) %>%
    colMeans(.) %>%
    as.numeric()

  average_patient_change <-
    ((ave_12_month_patients - ave_12_month_patients_prev) / ave_12_month_patients_prev) *
    100

  #monthly volumes
  `12_month_volume` <- raw_data$monthly %>%
    filter(SECTION_CODE == code,
           YEAR_MONTH %in% filter_12_months) %>%
    select(ITEM_COUNT) %>%
    colSums(.) %>%
    as.numeric()

  `12_month_volume_prev` <- raw_data$monthly %>%
    filter(SECTION_CODE == code,
           YEAR_MONTH %in% filter_prev_12_months) %>%
    select(ITEM_COUNT) %>%
    colSums(.) %>%
    as.numeric()

  volume_change <-
    ((`12_month_volume` - `12_month_volume_prev`) / `12_month_volume_prev`) *
    100

  #covid model volumes
  covid_data <- model_data %>%
    covid_model() %>%
    filter(SECTION_CODE == code, YEAR_MONTH > 202002) %>%
    select(SECTION_NAME,
           SECTION_CODE,
           YEAR_MONTH,
           ITEM_COUNT,
           PRED_ITEMS_95_FIT) %>%
    ungroup()

  covid_data_min <- format(as.Date(paste0(202003, "01"),
                                   format = "%Y%m%d"),
                           format = "%B %Y")

  covid_data_month_count <- covid_data %>%
    select(YEAR_MONTH) %>%
    unique() %>%
    nrow()

  covid_volume_actual <- covid_data %>%
    select(ITEM_COUNT) %>%
    colSums(.) %>%
    as.numeric()

  covid_volume_predicted <- covid_data %>%
    select(PRED_ITEMS_95_FIT) %>%
    colSums(.) %>%
    as.numeric()

  covid_volume_dif <- covid_volume_actual - covid_volume_predicted
  covid_volume_dif_per <-
    ((covid_volume_actual - covid_volume_predicted) / covid_volume_predicted) *
    100

  #build names for use in narratives
  name_formatted <- ""
  if(name == "Hypnotics and anxiolytics") {
    name <- "hypnotics and anxiolytics"
    name_formatted <- "hypnotic and anxiolytic item"
  } else if(name == "Drugs used in psychoses and related disorders") {
    name <- "antipsychotic items"
    name_formatted <- "antipsychotic drug item"
  } else if(name == "Antidepressant drugs") {
    name <- "antidepressant drug items"
    name_formatted <- "antidepressant drug item"
  } else if(name == "CNS stimulants and drugs used for ADHD") {
    name <- "CNS stimulants and drugs used for ADHD items"
    name_formatted <- "CNS stimulants and drugs used for ADHD item"
  } else if(name == "Drugs for dementia") {
    name <- "drugs for dementia"
    name_formatted <- "drug for dementia item"
  }

  #build data
  qr_data <- data.frame(
    Section_Name = rep(name, 29),
    Section_Formatted = rep(name_formatted, 29),
    Period = c(
      quarter,
      prev_year_filter,
      paste0(prev_year_filter, " to ", quarter),
      prev_quarter_filter,
      paste0(prev_quarter_filter, " to ", quarter),
      filter_5_years,
      paste0(filter_5_years, " to ", quarter),
      paste0(filter_5_years, " to ", quarter),
      quarter,
      prev_year_filter,
      paste0(prev_year_filter, " to ", quarter),
      prev_quarter_filter,
      paste0(prev_quarter_filter, " to ", quarter),
      filter_5_years,
      paste0(filter_5_years, " to ", quarter),
      paste0(filter_5_years, " to ", quarter),
      filter_5_years,
      quarter,
      paste0(filter_5_years, " to ", quarter),
      paste0(min_filter_12_months, " to ", max_filter_12_months),
      paste0(min_filter_prev_12_months, " to ", max_filter_prev_12_months),
      paste0(min_filter_prev_12_months, " to ", max_filter_12_months),
      paste0(min_filter_12_months, " to ", max_filter_12_months),
      paste0(min_filter_prev_12_months, " to ", max_filter_prev_12_months),
      paste0(min_filter_prev_12_months, " to ", max_filter_12_months),
      paste0(covid_data_min, " to ", max_filter_12_months),
      paste0(covid_data_min, " to ", max_filter_12_months),
      paste0(covid_data_min, " to ", max_filter_12_months),
      paste0(covid_data_min, " to ", max_filter_12_months)
    ),
    Measure = c(
      "Items",
      "Items",
      "% change items",
      "Items",
      "% change items",
      "Items",
      "Items change",
      "% change volume",
      "Patients",
      "Patients",
      "% change patients",
      "Patients",
      "% change patients",
      "Patients",
      "Patients change",
      "% change patients",
      "% identified patients",
      "% identified patients",
      "Pecentage points change",
      "Mean patients",
      "Mean patients",
      "% change patients",
      "Items",
      "Items",
      "% change items",
      "Items",
      "Items predicted",
      "Items diff (pred to act)",
      "% items diff (pred to act)"
    ),
    Value = c(
      cur_quart_volume,
      prev_year_quart_volume,
      annual_per_change,
      prev_quart_volume,
      quarterly_per_change,
      prev_5_year_quart_volume,
      (cur_quart_volume - prev_5_year_quart_volume),
      annual_5_per_change,
      cur_quart_patients,
      prev_year_quart_patients,
      annual_per_change_patients,
      prev_quart_patients,
      quarterly_per_change_patients,
      prev_5_year_quart_patients,
      (cur_quart_patients - prev_5_year_quart_patients),
      annual_5_per_change_patients,
      prev_5_year_quart_identified,
      current_quart_identified,
      current_quart_identified - prev_5_year_quart_identified,
      ave_12_month_patients,
      ave_12_month_patients_prev,
      average_patient_change,
      `12_month_volume`,
      `12_month_volume_prev`,
      volume_change,
      covid_volume_actual,
      covid_volume_predicted,
      covid_volume_dif,
      covid_volume_dif_per
    ),
    Rounded = c(
      format_number(cur_quart_volume),
      format_number(prev_year_quart_volume),
      format_number(annual_per_change, percentage = T),
      format_number(prev_quart_volume),
      format_number(quarterly_per_change, percentage = T),
      format_number(prev_5_year_quart_volume),
      format_number((
        cur_quart_volume - prev_5_year_quart_volume
      )),
      format_number(annual_5_per_change, percentage = T),
      format_number(cur_quart_patients),
      format_number(prev_year_quart_patients),
      format_number(annual_per_change_patients, percentage = T),
      format_number(prev_quart_patients),
      format_number(quarterly_per_change_patients, percentage = T),
      format_number(prev_5_year_quart_patients),
      format_number((
        cur_quart_patients - prev_5_year_quart_patients
      )),
      format_number(annual_5_per_change_patients, percentage = T),
      format_number(prev_5_year_quart_identified, percentage = T),
      format_number(current_quart_identified, percentage = T),
      format_number(current_quart_identified - prev_5_year_quart_identified),
      format_number(ave_12_month_patients),
      format_number(ave_12_month_patients_prev),
      format_number(average_patient_change, percentage = T),
      format_number(`12_month_volume`),
      format_number(`12_month_volume_prev`),
      format_number(volume_change, percentage = T),
      format_number(covid_volume_actual),
      format_number(covid_volume_predicted),
      format_number(covid_volume_dif),
      format_number(covid_volume_dif_per, percentage = T)
    )
  )

  #output tables to global enviroment for narrative automation
  assign(paste0("table_", code), qr_data, envir = globalenv())

  #write data to sheet
  openxlsx::writeDataTable(qrwb,
                      sheet = code,
                      startRow = 1,
                      x = qr_data,
                      tableName = paste0("table_", code),
                      tableStyle = "none")

  #auto width columns
  setColWidths(qrwb,
               sheet = code,
               cols = 1:5,
               widths = "auto")
}

#save workbook in shared QR folder
openxlsx::saveWorkbook(
  qrwb,
  paste0(
    "Y:\\Official Stats\\MUMH\\QR Data\\QR ",
    max_filter_12_months,
    ".xlsx"
  ),
  overwrite = T
)

# 10. automate narratives --------------------------------------------------

#ordinal number function
ordinal_number <- function(x) {
  data <- data.frame(
    Number = c(1,2,3,4),
    Word = c("first", "second", "third", "fourth")
  ) %>%
    filter(Number == x)

  return(data[1,2])
}

ordinal_quarter <- ordinal_number(
  as.numeric(str_sub(quarter,-1,-1))
)

ordinal_quarter_prev <- ordinal_number(
  as.numeric(str_sub(prev_quarter_filter,-1,-1))
)

fy_formatted <- paste0(
  str_sub(quarter, 1,5),
  str_sub(quarter, 8,9)
)

fy_formatted_prev <- paste0(
  str_sub(prev_year_filter, 1,5),
  str_sub(prev_year_filter, 8,9)
)

fy_formatted_prev_5<- paste0(
  str_sub(filter_5_years, 1,5),
  str_sub(filter_5_years, 8,9)
)

#build loop to output narrative for each bnf section
tables <- list()
tables$`0401` <- table_0401
tables$`0402` <- table_0402
tables$`0403` <- table_0403
tables$`0404` <- table_0404
tables$`0411` <- table_0411

for(k in 1:length(tables)) {
narrative_data <- tables[[k]]

#paragraph 1 sentence 1
par1a <- paste0(
  "There were ",
  narrative_data$Rounded[1],
  " ",
  narrative_data$Section_Name[1],
  " prescribed in the ",
  ordinal_quarter,
  " quarter of financial year ",
  fy_formatted,
  "."
)

#paragraph 1 sentence 2
#check for annual increase/decrease in items
year_change_items <- ""
if (narrative_data$Value[1] > narrative_data$Value[2]) {
  year_change_items <- " increase"
} else {
  year_change_items <- " decrease"
}

#check for quarterly increase/decrease in items
quarter_change_items <- ""
if (narrative_data$Value[1] > narrative_data$Value[4]) {
  quarter_change_items <- " increase"
} else {
  quarter_change_items <- " decrease"
}

par1b <- paste0(
  "This was a ",
  narrative_data$Rounded[3],
  year_change_items,
  " from ",
  narrative_data$Rounded[2],
  " items compared with the same quarter a year ago, and a ",
  narrative_data$Rounded[5],
  quarter_change_items,
  " from ",
  narrative_data$Rounded[4],
  " items in the previous quarter."
)

#paragraph 1 sentence 3
#check for 5 year increase / decrease
year_5_change_items <- ""
year_5_change_items2 <- ""
year_5_up_down <- ""
if (narrative_data$Value[1] > narrative_data$Value[6]) {
  year_5_change_items <- "increasing"
  year_5_change_items2 <- "an increase"
  year_5_up_down <- " more "
} else {
  year_5_change_items <- "decreasing"
  year_5_change_items2 <- "a decrease"
  year_5_up_down <- " fewer "
}

par1c <-   paste0(
  "Prescribing of ",
  narrative_data$Section_Name[1],
  " has been ",
  year_5_change_items,
  " since ",
  fy_formatted_prev_5,
  ", with ",
  narrative_data$Rounded[7],
  year_5_up_down,
  "items prescribed in ",
  paste0(str_sub(quarter, -2, -1), " ", fy_formatted),
  " when compared to ",
  paste0(str_sub(quarter, -2, -1), " ", fy_formatted_prev_5),
  ", ",
  year_5_change_items2,
  " of ",
  narrative_data$Rounded[8],
  " over the period."
)

#build 1st paragraph
assign(
  paste0("par1_", names(tables)[k]),
  paste0(
    par1a,
    " ",
    par1b,
    " ",
    par1c
  )
)

#remove sentences from environment
rm(par1a)
rm(par1b)
rm(par1c)

#paragraph 2 sentence 1
par2a <- paste0(
  "There were an estimated ",
  narrative_data$Rounded[9],
  " identified patients who were prescribed at least one ",
  narrative_data$Section_Formatted[1],
  " in quarter ",
  str_sub(quarter,-1,-1),
  " of ",
  fy_formatted,
  "."
)

#paragraph 2 sentence 2
#check for annual increase/decrease in patients
year_change_patients <- ""
if(narrative_data$Value[9] > narrative_data$Value[10]) {
  year_change_patients <- " increase"
} else {
  year_change_patients <- " decrease"
}

#check for quarterly increase/decrease in patients
quarter_change_patients <- ""
if(narrative_data$Value[9] > narrative_data$Value[12]) {
  quarter_change_patients <- " increase"
} else {
  quarter_change_patients <- " decrease"
}

par2b <- paste0(
  "This was a ",
  narrative_data$Rounded[11],
  year_change_patients,
  " from ",
  narrative_data$Rounded[10],
  " identified patients when compared with the same quarter in ",
  fy_formatted_prev,
  ", and a ",
  narrative_data$Rounded[13],
  quarter_change_patients,
  " from ",
  narrative_data$Rounded[12],
  " identified patients in the previous quarter."
)

#paragraph 2 sentence 3
par2c <- paste0(
  "The long-term trends for patients receiving ",
  narrative_data$Section_Name[1],
  " are similar to the overall prescribing of items."
)

#paragraph 2 sentence 4
#check for 5 year increase/decrease in patients
year_5_change_patients <- ""
year_5_up_down_patients <- ""
year_5_estimate <- ""
year_5_change_patients2 <- ""
if(narrative_data$Value[9] > narrative_data$Value[14]) {
  year_5_change_patients <- " an increase "
  year_5_up_down_patients <- " more "
  year_5_estimate <- " overestimate "
  year_5_change_patients2 <- " increase "
} else {
  year_5_change_patients <- " a decrease "
  year_5_up_down_patients<- " fewer "
  year_5_estimate <- " underestimate "
  year_5_change_patients2 <- " decrease "
}

#check for vowel at beginning of section name
vowel_check <- tolower(substr(narrative_data$Section_Formatted[1], 1, 1))
and_a <- "a"
if(vowel_check %in% c("a", "e", "i", "o", "u")) {
  and_a <- "an "
}

par2d <- paste0(
  "There were ",
  narrative_data$Rounded[15],
  year_5_up_down_patients,
  "identified patients who received ",
  and_a,
  narrative_data$Section_Formatted[1],
  " in quarter ",
  str_sub(quarter,-1,-1),
  " ",
  fy_formatted,
  " compared to quarter ",
  str_sub(quarter,-1,-1),
  " ",
  fy_formatted_prev_5,
  ", ",
  year_5_change_patients,
  "of ",
  narrative_data$Rounded[16],
  "."
)

#paragraph 2 sentence 5
par2e <- paste0(
  "However, it should be noted that this is likely to be an",
  year_5_estimate,
  "of the actual",
  year_5_change_patients2,
  "in patient numbers, as the proportion of patients who could be identified increased."
)

#paragraph 2 sentence 6
par2f <- paste0(
  "In ",
  paste0(str_sub(quarter,-2,-1), " ", fy_formatted_prev_5),
  ", ",
  narrative_data$Rounded[17],
  " of items were prescribed to identified patients, this increased by ",
  narrative_data$Rounded[19],
  " percentage points in ",
  paste0(str_sub(quarter,-2,-1), " ", fy_formatted),
  " to ",
  narrative_data$Rounded[18],
  " of items."
)

#build paragraph 2
assign(
  paste0("par2_", names(tables)[k]),
  paste0(
    par2a,
    " ",
    par2b,
    " ",
    par2c,
    " ",
    par2d,
    " ",
    par2e,
    " ",
    par2f
  )
)

#remove sentences from environment
rm(par2a)
rm(par2b)
rm(par2c)
rm(par2d)
rm(par2e)
rm(par2f)

#paragraph 3
assign(
  paste0("par3_", names(tables)[k]),
  paste0(
    "The monthly time series for ",
    narrative_data$Section_Name[1],
    " show some regular patterns as well as month-to-month variation. Some of the regular changes may reflect seasonal patterns; most notably there tends to be less prescribing in months with fewer dispensing days, such as February."
  )
)

#paragraph 4
par4 <- paste0(
  "As patients can appear in more than one month of data, adding the patients for different months together would result in an inaccurate estimate of the number of unique patients that have received prescribing in this period. Therefore, we have calculated the average number of patients for comparison purposes. This average is a mean, calculated by summing the number of patients for the periods in question and dividing by the number of months in the period."
)

#paragraph 5
#paragraph 5 sentence 1``
par5a <- paste0(
  "The monthly average number of identified patients receiving at least one ",
  narrative_data$Section_Formatted[1],
  " in the 12 months ",
  min_filter_12_months,
  " to ",
  max_filter_12_months,
  " was ",
  narrative_data$Rounded[20],
  "."
)

#paragraph 5 sentence 2
#check for 12 month increase/decrease in average patients
year_change_ave_patients <- ""
year_up_down_ave_patients <- ""
if(narrative_data$Value[20] > narrative_data$Value[21]) {
  year_change_ave_patients <- " an increase "
  year_up_down_ave_patients <- " more "
} else {
  year_change_ave_patients <- " a decrease "
  year_up_down_ave_patients<- " fewer "
}

par5b <- paste0(
  "This was",
  year_change_ave_patients,
  "of ",
  narrative_data$Rounded[22],
  " from a monthly average of ",
  narrative_data$Rounded[21],
  " identified patients in the period ",
  min_filter_prev_12_months,
  " to ",
  max_filter_prev_12_months,
  "."
)

#build paragraph 5
assign(
  paste0("par5_", names(tables)[k]),
  paste0(
    par5a,
    " ",
    par5b
  )
)

#remove sentences from environment
rm(par5a)
rm(par5b)

#paragraph 6
#paragraph 6 sentence 1
par6a <- paste0(
  "There were ",
  narrative_data$Rounded[23],
  " ",
  narrative_data$Section_Name[1],
  " prescribed in the 12 months ",
  min_filter_12_months,
  " to ",
  max_filter_12_months,
  "."
)

#check for 12 month increase/decrease in items
year_change_12_month_items <- ""
year_up_down_12_month_items <- ""
if(narrative_data$Value[23] > narrative_data$Value[24]) {
  year_change_12_month_items <- " increase "
  year_up_down_12_month_items <- " more "
} else {
  year_change_12_month_items <- " decrease "
  year_up_down_12_month_items<- " fewer "
}

#paragraph 6 sentence 2
par6b <- paste0(
  "This was a ",
  narrative_data$Rounded[25],
  year_change_12_month_items,
  "from ",
  narrative_data$Rounded[24],
  " items when compared with ",
  min_filter_prev_12_months,
  " to ",
  max_filter_prev_12_months,
  "."
)

#build paragraph 6
assign(
  paste0("par6_", names(tables)[k]),
  paste0(
    par6a,
    " ",
    par6b
  )
)

#remove sentences from environment
rm(par6a)
rm(par6b)

#paragraph 7
par7 <- paste0(
  "It should be noted, national lockdowns were implemented between 23 March to 4 July 2020, 5 November to 2 December 2020, and from 6 January 2021 with relaxation of lockdown restrictions commencing from March 2021 onwards. Further measures without a lockdown were implemented on 30 November 2021 due to the emergence of the Omicron variant of COVID-19."
  )

#covid paragraph
#covid paragraph sentence 1
covpar1a <- paste0(
  "In the ",
  covid_data_month_count,
  "-month period from ",
  covid_data_min,
  " to ",
  max_filter_12_months,
  ", ",
  narrative_data$Rounded[26],
  " ",
  narrative_data$Section_Name[1],
  " were prescribed in England."
)

#covid paragraph sentence 2
#check if covid volumes are more/less than predicied
covid_up_down <- ""
if(narrative_data$Value[26] > narrative_data$Value[27]) {
  covid_up_down <- " more "
} else {
  covid_up_down<- " fewer "
}
covpar1b <- paste0(
  "This was ",
  narrative_data$Rounded[28],
  " or ",
  narrative_data$Rounded[29],
  covid_up_down,
  "than the ",
  narrative_data$Rounded[27],
  " items expected based on historical trends."
  )

#build covid paragraph
assign(
  paste0("covpar_", names(tables)[k]),
  paste0(
    covpar1a,
    " ",
    covpar1b
  )
)

#remove sentences from environment
rm(covpar1a)
rm(covpar1b)
}

# 11. render markdown ------------------------------------------------------

rmarkdown::render("mumh-quarterly-narrative.Rmd",
                  output_format = "html_document",
                  output_file = "outputs/mumh_quarterly_mar22_v001.html")

rmarkdown::render("mumh-quarterly-narrative.Rmd",
                  output_format = "word_document",
                  output_file = "outputs/mumh_quarterly_mar22_v001.docx")

#logr::log_close()

