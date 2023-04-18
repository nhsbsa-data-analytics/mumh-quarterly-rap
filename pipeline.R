# pipeline.R -------------------------------------------------------------------
# This script is used to run the RAP for the MUMH quarterly publication

# 1. install required packages -------------------------------------------------
# TODO: investigate using renv package for dependency management
req_pkgs <- c("broom", "data.table", "dbplyr", "dplyr", "DT" , "highcharter", "lubridate",
              "logr", "openxlsx", "rmarkdown", "rsample", "stringr", "tidyr", "yaml")

#  utils::install.packages(req_pkgs, dependencies = TRUE)
# # #
#   devtools::install_github(
#     "nhsbsa-data-analytics/mumhquarterly",
#     auth_token = Sys.getenv("GITHUB_PAT")
#     )
# #
#  devtools::install_github("nhsbsa-data-analytics/nhsbsaR")

invisible(lapply(c(req_pkgs, "mumhquarterly", "nhsbsaR"), library, character.only = TRUE))

# 2. set options ---------------------------------------------------------------

mumhquarterly::mumh_options()

# 3. load most recent data and add 3 months to max date ------------------------

# get most recent monthly file
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

# read data
recent_data_monthly <- data.table::fread(recent_file_monthly,
                                         keepLeadingZeros = TRUE)

# get most recent quarterly file
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

# read data
recent_data_quarterly <- data.table::fread(recent_file_quarterly,
                                           keepLeadingZeros = TRUE)

# get most recent monthly model data file
recent_file_model <- rownames(file.info(
  list.files(
    "Y:/Official Stats/MUMH/data",
    full.names = T,
    pattern = "model"
  )
))[which.max(file.info(
  list.files(
    "Y:/Official Stats/MUMH/data",
    full.names = T,
    pattern = "model"
  )
)$mtime)]

# read data
recent_data_model <- data.table::fread(recent_file_model,
                                       keepLeadingZeros = TRUE)

# get max month
max_month <- as.Date(
  paste0(
    max(
      recent_data_monthly$YEAR_MONTH
    ),
    "01"
  ),
  format = "%Y%m%d"
)

# get max month plus one
max_month_plus <- as.Date(paste0(max(recent_data_monthly$YEAR_MONTH),
                                 "01"),
                          format = "%Y%m%d") %m+% months(1)

# convert to DW format
max_month_plus_dw <- as.numeric(paste0(format(max_month_plus,
                                              "%Y"),
                                       format(max_month_plus,
                                              "%m")))

# 4. extract data from NHSBSA Data Warehouse -----------------------------------
# build connection to database
con <- con_nhsbsa(
  dsn = "FBS_8192k",
  driver = "Oracle in OraClient19Home1",
  "DWCP",
  username = rstudioapi::showPrompt(title = "Username", message = "Username"),
  password = rstudioapi::askForPassword()
)

# get max month available in DWH
ym_dim <- dplyr::tbl(con,
                     from = dbplyr::in_schema("DIM", "YEAR_MONTH_DIM")) %>%

  # shrink table to remove unnecessary data
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

# get max month in dwh
max_month_dw <- as.Date(paste0(ltst_month,
                               "01"),
                        format = "%Y%m%d")

# create table names

time_table <- paste0("MUMH_MONTH_TDIM_", as.character(ltst_month))

porg_table <- paste0("MUMH_MONTH_PORG_DIM_", as.character(ltst_month))

drug_table <- paste0("MUMH_MONTH_DRUG_DIM_", as.character(ltst_month))

# only get new data if max month in dwh is greater than that in most recent data
 if(max_month_dw <= max_month) {
 print("No new quarterly data available in the Data Warehouse, will use most recent saved data.")
   DBI::dbDisconnect(con)
   } else {

  # build time dimension table in schema

  # drop time dimension if exists

  exists <- con %>%
    DBI::dbExistsTable(name = time_table)
  # Drop any existing table beforehand
  if (exists) {
  con %>%
      DBI::dbRemoveTable(name = time_table)
    }

  # build table
  # this will create a new table of all data, so table will be large
  create_tdim(con) %>%
    compute(time_table, analyze = FALSE, temporary = FALSE)

  # if only getting new data for each quarterly publication use commented code below
  # and append to previous data from shared area
  #create_tdim(con, start = max_month_plus_dw) %>%
  #compute(time_table, analyze = FALSE, temporary = FALSE)

  # build org dimension table in schema

  # drop org dimension if exists
  exists <- con %>%
    DBI::dbExistsTable(name = porg_table)
  # Drop any existing table beforehand
  if (exists) {
    con %>%
      DBI::dbRemoveTable(name = porg_table)
  }

  # build table
  create_org_dim(con, country = 1) %>%
    compute(porg_table, analyze = FALSE, temporary = FALSE)

  # build drug dimension table in schema

  # drop drug dimension if exists
  exists <- con %>%
    DBI::dbExistsTable(name = drug_table)
  # Drop any existing table beforehand
  if (exists) {
    con %>%
      DBI::dbRemoveTable(name = drug_table)
  }

  # build table
  create_drug_dim(con, bnf_codes = c("0401", "0402", "0403", "0404", "0411"))  %>%
    compute(drug_table, analyze = FALSE, temporary = FALSE)

  # build table
  age <- dplyr::tbl(con,
                    from = dbplyr::in_schema("DIM", "AGE_DIM")) %>%
    select(AGE,
           DALL_5YR_BAND)

  # create fact table
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
    inner_join(tbl(con, time_table) , by = "YEAR_MONTH") %>%
    inner_join(tbl(con, porg_table) , by = c("PRESC_TYPE_PRNT" = "LVL_5_OUPDT",
                                             "PRESC_ID_PRNT" = "LVL_5_OU")) %>%
    inner_join(tbl(con, drug_table), by = c("CALC_PREC_DRUG_RECORD_ID" = "RECORD_ID",
                                            "YEAR_MONTH" = "YEAR_MONTH")) %>%
    inner_join(age,
               by = c("CALC_AGE" = "AGE")) %>%
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

  ## build and collect model data
  mumh_model <- tbl(con, "MUMH_RAW_DATA") %>%
    mutate(
      PDS_GENDER = case_when(PDS_GENDER == 1 ~ "M",
                             PDS_GENDER == 2 ~ "F",
                             TRUE ~ "U"),
      DALL_5YR_BAND = case_when(is.na(DALL_5YR_BAND) ~ "Unknown",
                                TRUE ~ DALL_5YR_BAND)
    ) %>%
    group_by(
      FINANCIAL_YEAR,
      FINANCIAL_QUARTER,
      YEAR_MONTH,
      IDENTIFIED_PATIENT_ID,
      SECTION_DESCR,
      BNF_SECTION,
      IDENTIFIED_FLAG,
      PDS_GENDER,
      DALL_5YR_BAND
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
      IDENTIFIED_FLAG,
      PDS_GENDER,
      DALL_5YR_BAND,
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

  # if only getting new data for each quarterly publication
  # join any new data to most recent saved data
  #mumh_monthly <- recent_data_monthly %>%
    #bind_rows(mumh_monthly)

  #mumh_quarterly <- recent_data_quarterly %>%
    #bind_rows(mumh_quarterly)

  #mumh_model <- recent_data_model %>%
    #bind_rows(mumh_model)

  save_data(mumh_quarterly, dir = "Y:/Official Stats/MUMH", filename = "mumh_quarterly")

  save_data(mumh_monthly, dir = "Y:/Official Stats/MUMH", filename = "mumh_monthly")

  save_data(mumh_model, dir = "Y:/Official Stats/MUMH", filename = "mumh_model")
   }

# 5. import data ---------------------------------------------------------------
# import data from data folder to perform aggregations etc without having to
# maintain connection to DWH

raw_data <- list()

# read most recent monthly file
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

# read most recent quarterly file
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

# read most recent model data file
raw_data$model_data <- data.table::fread(rownames(file.info(
  list.files(
    "Y:/Official Stats/MUMH/data",
    full.names = T,
    pattern = "model"
  )
))[which.max(file.info(
  list.files(
    "Y:/Official Stats/MUMH/data",
    full.names = T,
    pattern = "model"
  )
)$mtime)],
keepLeadingZeros = TRUE)

# calculate dispensing days for use in covid model
# use latest year of current financial year, eg. 2023 for financial year 2022/23
dispensing_days <- mumhquarterly::get_dispensing_days(2023)

# 6. data manipulation ---------------------------------------------------------

# patient identification rates for most recent data
period <- raw_data$quarterly %>%
  dplyr::select(FINANCIAL_QUARTER) %>%
  dplyr::distinct() %>%
  dplyr::slice_max(FINANCIAL_QUARTER,
                   n = 4) %>%
  dplyr::pull()

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

# model data using old version of covid model for use in model testing
# filter monthly raw data to identified patients only
# join monthly raw data to dispensing days to allow modelling

model_data_old <- raw_data$monthly %>%
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

# 7. write data to .xlsx -------------------------------------------------------
# Text referencing time periods in sheet titles and notes needs to be manually updated
# for example 'April 2015 to December 2022' is the time period for current publication
# To do: automate time periods in file and sheet titles and notes

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

# create wb object
# create list of sheetnames needed (overview and metadata created automatically)
sheetNames <- c("Patient_Identification",
                "Monthly_Data",
                "Quarterly_Data")

wb <- mumhquarterly::create_wb(sheetNames)

# create metadata tab (will need to open file to auto adjust some row height once ran)
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

mumhquarterly::create_metadata(wb,
                meta_fields,
                meta_descs
)

#### Patient identification
# write data to sheet
mumhquarterly::write_sheet(
  wb,
  "Patient_Identification",
  "Medicines Used in Mental Health - Quarterly Summary Statistics April 2015 to December 2022 - Proportion of items for which an NHS number was recorded (%)",
  c(
    "The below proportions reflect the percentage of prescription items where a PDS verified NHS number was recorded."
  ),
  patient_identification_excel,
  42
)

# left align text/character columns A and B
mumhquarterly::format_data(wb,
            "Patient_Identification",
            c("A", "B"),
            "left",
            "")

# right align columns and round to 2 decimal places for numerical columns C to AG
# column letter references will need to be updated whenever more data added
mumhquarterly::format_data(wb,
            "Patient_Identification",
            c("C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N",
              "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
              "AA", "AB", "AC", "AD", "AE", "AF", "AG"),
            "right",
            "0.00")

#### Monthly data
# write data to sheet
mumhquarterly::write_sheet(
  wb,
  "Monthly_Data",
  "Medicines Used in Mental Health - Quarterly Summary Statistics April 2015 to December 2022 totals by year month",
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

# left align columns A to F
mumhquarterly::format_data(wb,
            "Monthly_Data",
            c("A", "B", "C", "D", "E", "F"),
            "left",
            "")

# right align columns G and H and round to whole numbers with thousand separator
mumhquarterly::format_data(wb,
            "Monthly_Data",
            c("G", "H"),
            "right",
            "#,##0")

# right align column I and round to 2 decimal places with thousand separator
mumhquarterly::format_data(wb,
            "Monthly_Data",
            c("I"),
            "right",
            "#,##0.00")

#### Quarterly data
# write data to sheet
mumhquarterly::write_sheet(
  wb,
  "Quarterly_Data",
  "Medicines Used in Mental Health - Quarterly Summary Statistics April 2015 to December 2022 totals by quarter",
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

# left align columns A to F
mumhquarterly::format_data(wb,
            "Quarterly_Data",
            c("A", "B", "C", "D", "E"),
            "left",
            "")

# right align columns G and H and round to whole numbers with thousand separator
mumhquarterly::format_data(wb,
            "Quarterly_Data",
            c("F", "G"),
            "right",
            "#,##0")

# right align column I and round to 2 decimal places with thousand separator
mumhquarterly::format_data(wb,
            "Quarterly_Data",
            c("H"),
            "right",
            "#,##0.00")

# save file into outputs folder
# file name will need to be updated to new time period for each new publication
openxlsx::saveWorkbook(wb,
                       "outputs/mumh_quarterly_dec22_v001.xlsx",
                       overwrite = TRUE)

# 8. Covid model figures -------------------------------------------------------
# this section builds, tests and implements the new covid model
# using patient ageband and gender data
# To do: turn new covid model back into function to reduce size of code

# join dispensing days to raw data, add columns for position of month in year,
# position of month in full dataset, and month start date
# keep original 5 year agebands and fill rows where ageband has no items recorded
df5 <- raw_data$model_data |>
  dplyr::group_by(YEAR_MONTH,
                  SECTION_NAME,
                  SECTION_CODE,
                  IDENTIFIED_FLAG,
                  PDS_GENDER,
                  DALL_5YR_BAND) |>
  dplyr::summarise(ITEM_COUNT = sum(ITEM_COUNT),
                   ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC),
                   .groups = "drop") |>
  tidyr::complete(DALL_5YR_BAND,
                  nesting(YEAR_MONTH,
                          SECTION_NAME,
                          SECTION_CODE,
                          IDENTIFIED_FLAG,
                          PDS_GENDER),
                  fill = list(ITEM_COUNT = 0,
                              ITEM_PAY_DR_NIC = 0,
                              PATIENT_COUNT = 0)) |>
  tidyr::complete(IDENTIFIED_FLAG,
                  nesting(YEAR_MONTH,
                          SECTION_NAME,
                          SECTION_CODE,
                          DALL_5YR_BAND,
                          PDS_GENDER),
                  fill = list(ITEM_COUNT = 0,
                              ITEM_PAY_DR_NIC = 0,
                              PATIENT_COUNT = 0)) |>
  tidyr::complete(PDS_GENDER,
                  nesting(YEAR_MONTH,
                          SECTION_NAME,
                          SECTION_CODE,
                          IDENTIFIED_FLAG,
                          DALL_5YR_BAND),
                  fill = list(ITEM_COUNT = 0,
                              ITEM_PAY_DR_NIC = 0,
                              PATIENT_COUNT = 0)) |>
  dplyr::group_by(SECTION_NAME,
                  SECTION_CODE,
                  IDENTIFIED_FLAG,
                  PDS_GENDER,
                  DALL_5YR_BAND) |>
  dplyr::group_by(SECTION_NAME, SECTION_CODE, IDENTIFIED_FLAG,
                  PDS_GENDER, DALL_5YR_BAND) |>
  dplyr::mutate(
    MONTH_START = as.Date(paste0(YEAR_MONTH, "01"), format = "%Y%m%d"),
    MONTH_NUM = lubridate::month(MONTH_START),
    MONTH_INDEX = lubridate::interval(lubridate::dmy(01032015), as.Date(MONTH_START)) %/% months(1)
  ) |>
  dplyr::left_join(dispensing_days,
                   by = "YEAR_MONTH") |>
  dplyr::filter(!(IDENTIFIED_FLAG == "N" & PDS_GENDER == "F"),
                !(IDENTIFIED_FLAG == "N" & PDS_GENDER == "M"),
                !(PDS_GENDER == "U" | DALL_5YR_BAND == "Unknown")) %>%
  dplyr::ungroup()

# repeat to create dataset with 10 year agebands instead of 5 year
# only keep observations with known age and gender
# and fill rows where ageband has no items recorded
df10 <- raw_data$model |>
  dplyr::mutate(BAND_10YR = dplyr::case_when(DALL_5YR_BAND %in% c("00-04", "05-09", "10-14", "15-19") ~ "00-19",
                                             DALL_5YR_BAND %in% c("20-24", "25-29", "30-34", "35-39") ~ "20-39",
                                             DALL_5YR_BAND %in% c("40-44", "45-49", "50-54", "55-59") ~ "40-59",
                                             DALL_5YR_BAND %in% c("60-64", "65-69", "70-74", "75-79") ~ "60-79",
                                             DALL_5YR_BAND == "Unknown" ~ "Unknown",
                                             TRUE ~ "80+")) |>
  dplyr::select(!(DALL_5YR_BAND)) |>
  dplyr::group_by(YEAR_MONTH,
                  SECTION_NAME,
                  SECTION_CODE,
                  IDENTIFIED_FLAG,
                  PDS_GENDER,
                  BAND_10YR) |>
  dplyr::summarise(ITEM_COUNT = sum(ITEM_COUNT),
                   ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC),
                   .groups = "drop") |>
  tidyr::complete(BAND_10YR,
                  nesting(YEAR_MONTH,
                          SECTION_NAME,
                          SECTION_CODE,
                          IDENTIFIED_FLAG,
                          PDS_GENDER),
                  fill = list(ITEM_COUNT = 0,
                              ITEM_PAY_DR_NIC = 0,
                              PATIENT_COUNT = 0)) |>
  tidyr::complete(IDENTIFIED_FLAG,
                  nesting(YEAR_MONTH,
                          SECTION_NAME,
                          SECTION_CODE,
                          BAND_10YR,
                          PDS_GENDER),
                  fill = list(ITEM_COUNT = 0,
                              ITEM_PAY_DR_NIC = 0,
                              PATIENT_COUNT = 0)) |>
  tidyr::complete(PDS_GENDER,
                  nesting(YEAR_MONTH,
                          SECTION_NAME,
                          SECTION_CODE,
                          IDENTIFIED_FLAG,
                          BAND_10YR),
                  fill = list(ITEM_COUNT = 0,
                              ITEM_PAY_DR_NIC = 0,
                              PATIENT_COUNT = 0)) |>
  dplyr::group_by(SECTION_NAME,
                  SECTION_CODE,
                  IDENTIFIED_FLAG,
                  PDS_GENDER,
                  BAND_10YR) |>
  dplyr::group_by(SECTION_NAME, SECTION_CODE, IDENTIFIED_FLAG,
                  PDS_GENDER, BAND_10YR) |>
  dplyr::mutate(
    MONTH_START = as.Date(paste0(YEAR_MONTH, "01"), format = "%Y%m%d"),
    MONTH_NUM = lubridate::month(MONTH_START),
    MONTH_INDEX = lubridate::interval(lubridate::dmy(01032015), as.Date(MONTH_START)) %/% months(1)
  ) |>
  dplyr::left_join(dispensing_days,
                   by = "YEAR_MONTH") |>
  dplyr::filter(!(IDENTIFIED_FLAG == "N" & PDS_GENDER == "F"),
                !(IDENTIFIED_FLAG == "N" & PDS_GENDER == "M"),
                !(PDS_GENDER == "U" | BAND_10YR == "Unknown")) %>%
  dplyr::ungroup()

# create additional dataset with both agebands for use in variable selection later
df_both <- raw_data$model |>
  dplyr::mutate(BAND_10YR = dplyr::case_when(DALL_5YR_BAND %in% c("00-04", "05-09", "10-14", "15-19") ~ "00-19",
                                             DALL_5YR_BAND %in% c("20-24", "25-29", "30-34", "35-39") ~ "20-39",
                                             DALL_5YR_BAND %in% c("40-44", "45-49", "50-54", "55-59") ~ "40-59",
                                             DALL_5YR_BAND %in% c("60-64", "65-69", "70-74", "75-79") ~ "60-79",
                                             DALL_5YR_BAND == "Unknown" ~ "Unknown",
                                             TRUE ~ "80+")) |>
  dplyr::group_by(YEAR_MONTH,
                  SECTION_NAME,
                  SECTION_CODE,
                  IDENTIFIED_FLAG,
                  PDS_GENDER,
                  DALL_5YR_BAND,
                  BAND_10YR) |>
  dplyr::summarise(ITEM_COUNT = sum(ITEM_COUNT),
                   ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC)) |>
  dplyr::group_by(SECTION_NAME, SECTION_CODE, IDENTIFIED_FLAG,
                  PDS_GENDER, BAND_10YR, DALL_5YR_BAND) |>
  dplyr::mutate(
    MONTH_START = as.Date(paste0(YEAR_MONTH, "01"), format = "%Y%m%d"),
    MONTH_NUM = lubridate::month(MONTH_START),
    MONTH_INDEX = lubridate::interval(lubridate::dmy(01032015), as.Date(MONTH_START)) %/% months(1)
  ) |>
  dplyr::left_join(dispensing_days,
                   by = "YEAR_MONTH") |>
  dplyr::filter(!(IDENTIFIED_FLAG == "N" & PDS_GENDER == "F"),
                !(IDENTIFIED_FLAG == "N" & PDS_GENDER == "M"),
                !(PDS_GENDER == "U" | DALL_5YR_BAND == "Unknown" | BAND_10YR == "Unknown")) |>
  dplyr::ungroup()

# add columns to separate out months into individual factor variables
# for use in later predictions
df5 <- df5 %>%
  dplyr::mutate(
    m_01 = 1*(MONTH_NUM == 1),
    m_02 = 1*(MONTH_NUM == 2),
    m_03 = 1*(MONTH_NUM == 3),
    m_04 = 1*(MONTH_NUM == 4),
    m_05 = 1*(MONTH_NUM == 5),
    m_06 = 1*(MONTH_NUM == 6),
    m_07 = 1*(MONTH_NUM == 7),
    m_08 = 1*(MONTH_NUM == 8),
    m_09 = 1*(MONTH_NUM == 9),
    m_10 = 1*(MONTH_NUM == 10),
    m_11 = 1*(MONTH_NUM == 11),
    m_12 = 1*(MONTH_NUM == 12)
  )

# repeat for 10 year ageband data
df10 <- df10 %>%
  dplyr::mutate(
    m_01 = 1*(MONTH_NUM == 1),
    m_02 = 1*(MONTH_NUM == 2),
    m_03 = 1*(MONTH_NUM == 3),
    m_04 = 1*(MONTH_NUM == 4),
    m_05 = 1*(MONTH_NUM == 5),
    m_06 = 1*(MONTH_NUM == 6),
    m_07 = 1*(MONTH_NUM == 7),
    m_08 = 1*(MONTH_NUM == 8),
    m_09 = 1*(MONTH_NUM == 9),
    m_10 = 1*(MONTH_NUM == 10),
    m_11 = 1*(MONTH_NUM == 11),
    m_12 = 1*(MONTH_NUM == 12)
  )

## Model exploration and fitting

# explore variable selection for use in model
# check if smaller or larger agebands makes a difference
# test and train data for df_both dataset, split by pre-covd trend data and covid data
both_time <- df_both %>%
  ungroup() %>%
  dplyr::mutate(time_period = case_when(YEAR_MONTH <= 202002 ~ "pre_covid",
                                        TRUE ~ "covid"))

both_split <- rsample::group_initial_split(both_time, time_period)
both_train <- rsample::training(both_split)
both_test <- rsample::testing(both_split)

# build 5yr vs 10yr agebands model for each BNF section using full set of variables
# linear model using lm() function
mod_0401_5 <- lm(ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + as.factor(MONTH_NUM)
                 + PDS_GENDER*as.factor(DALL_5YR_BAND),
                 data = filter(both_train, SECTION_CODE == "0401"))
mod_0401_10 <- lm(ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + as.factor(MONTH_NUM)
                  + PDS_GENDER*as.factor(BAND_10YR),
                  data = filter(both_train, SECTION_CODE == "0401"))

mod_0402_5 <- lm(ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + as.factor(MONTH_NUM)
                 + PDS_GENDER*as.factor(DALL_5YR_BAND),
                 data = filter(both_train, SECTION_CODE == "0402"))
mod_0402_10 <- lm(ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + as.factor(MONTH_NUM)
                  + PDS_GENDER*as.factor(BAND_10YR),
                  data = filter(both_train, SECTION_CODE == "0402"))

mod_0403_5 <- lm(ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + as.factor(MONTH_NUM)
                 + PDS_GENDER*as.factor(DALL_5YR_BAND),
                 data = filter(both_train, SECTION_CODE == "0403"))
mod_0403_10 <- lm(ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + as.factor(MONTH_NUM)
                  + PDS_GENDER*as.factor(BAND_10YR),
                  data = filter(both_train, SECTION_CODE == "0403"))

mod_0404_5 <- lm(ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + as.factor(MONTH_NUM)
                 + PDS_GENDER*as.factor(DALL_5YR_BAND),
                 data = filter(both_train, SECTION_CODE == "0404"))
mod_0404_10 <- lm(ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + as.factor(MONTH_NUM)
                  + PDS_GENDER*as.factor(BAND_10YR),
                  data = filter(both_train, SECTION_CODE == "0404"))

mod_0411_5 <- lm(ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + as.factor(MONTH_NUM)
                 + PDS_GENDER*as.factor(DALL_5YR_BAND),
                 data = filter(both_train, SECTION_CODE == "0411"))
mod_0411_10 <- lm(ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + as.factor(MONTH_NUM)
                  + PDS_GENDER*as.factor(BAND_10YR),
                  data = filter(both_train, SECTION_CODE == "0411"))

# compare model fits using Akaike Information Criteria (AIC)
# lower AIC is generally considered preferable depending on other considerations

# 5 year band vs 10 year band, 5 year band consistently has lower AIC
broom::glance(mod_0401_5)
broom::glance(mod_0401_10)

broom::glance(mod_0402_5)
broom::glance(mod_0402_10)

broom::glance(mod_0403_5)
broom::glance(mod_0403_10)

broom::glance(mod_0404_5)
broom::glance(mod_0404_10)

broom::glance(mod_0411_5)
broom::glance(mod_0411_10)

# however evidence of overfitting of model, suggests 10 year ageband should be used

# fit further models using 10 year agebands (with known age and gender)
# split 10yr ageband dataset
# BNF section 0404 used as main example to reduce code repetition, other sections
# used in MUMH publication gave similar overall results

df10_time <- df10 %>%
  ungroup() %>%
  dplyr::mutate(time_period = case_when(YEAR_MONTH <= 202002 ~ "pre_covid",
                                        TRUE ~ "covid"))

df10_item <- rsample::group_initial_split(df10_time, time_period)
df10_train <- rsample::training(df10_item)
df10_test <- rsample::testing(df10_item)

# most basic linear model with only dispensing days
mod_0404.0 <- lm(ITEM_COUNT ~ DISPENSING_DAYS,
                 data = filter(df10_train, SECTION_CODE == "0404"))
# add month position in year
mod_0404.1 <- lm(ITEM_COUNT ~ DISPENSING_DAYS + as.factor(MONTH_NUM),
                 data = filter(df10_train, SECTION_CODE == "0404"))
# add month position in full time series
mod_0404.2 <- lm(ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + as.factor(MONTH_NUM),
                 data = filter(df10_train, SECTION_CODE == "0404"))
# add ageband
mod_0404.3 <- lm(ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + as.factor(MONTH_NUM)
                 + as.factor(BAND_10YR),
                 data = filter(df10_train, SECTION_CODE == "0404"))
# add gender
mod_0404.4 <- lm(ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + as.factor(MONTH_NUM)
                 + PDS_GENDER,
                 data = filter(df10_train, SECTION_CODE == "0404"))
# add age and gender separately
mod_0404.5 <- lm(ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + as.factor(MONTH_NUM)
                 + PDS_GENDER + as.factor(BAND_10YR),
                 data = filter(df10_train, SECTION_CODE == "0404"))
# add age and gender as an interaction
mod_0404.6 <- lm(ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + as.factor(MONTH_NUM)
                 + PDS_GENDER*as.factor(BAND_10YR),
                 data = filter(df10_train, SECTION_CODE == "0404"))

#change plot window to two by two
par(mfrow = c(2,2))

# plot residuals to assess whether model assumptions are met
# compare previous model fitted on new data and new model on new data
# evidence of heteroscedasticity, to keep in mind when building future model
# as another type of model (such as forecasting) may be better suited to the data
# and/or possible exploration to see if non-linear relationship
# poisson glm explored but did not offer improvement on the linear model
# log transformation of data also applied but did not look suitable for data or interpretation
plot(mod_0404.2)
plot(mod_0404.6)

# compare model fits using AIC
broom::glance(mod_0404.0)
broom::glance(mod_0404.1)
broom::glance(mod_0404.2)
broom::glance(mod_0404.3)
broom::glance(mod_0404.4)
broom::glance(mod_0404.5)
broom::glance(mod_0404.6)

# mod_0404.6 gives lower AIC
# resulting model is large but AIC tends to balance number of variables with
# whether adding variables improves model fit
summary(mod_0404.6)

#testing of model variables for other BNF sections gave similar overall results

# model needs to use the separate individual month number variables instead of MONTH_NUM
# fit every level of MONTH_NUM as its own variable instead of a level
# first month (January) is excluded from model as all other months compared against it
# AIC is the same as if MONTH_NUM was used instead

mod_0401 <- lm(ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + m_02 + m_03
               + m_04 + m_05 + m_06 + m_07 + m_08 + m_09 + m_10 + m_11 + m_12
               + PDS_GENDER*as.factor(BAND_10YR),
               data = filter(df10_train, SECTION_CODE == "0401"))

mod_0402 <- lm(ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + m_02 + m_03
               + m_04 + m_05 + m_06 + m_07 + m_08 + m_09 + m_10 + m_11 + m_12
               + PDS_GENDER*as.factor(BAND_10YR),
               data = filter(df10_train, SECTION_CODE == "0402"))

mod_0403 <- lm(ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + m_02 + m_03
               + m_04 + m_05 + m_06 + m_07 + m_08 + m_09 + m_10 + m_11 + m_12
               + PDS_GENDER*as.factor(BAND_10YR),
               data = filter(df10_train, SECTION_CODE == "0403"))

mod_0404 <- lm(ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + m_02 + m_03
               + m_04 + m_05 + m_06 + m_07 + m_08 + m_09 + m_10 + m_11 + m_12
               + PDS_GENDER*as.factor(BAND_10YR),
               data = filter(df10_train, SECTION_CODE == "0404"))

mod_0411 <- lm(ITEM_COUNT ~ MONTH_INDEX + DISPENSING_DAYS + m_02 + m_03
               + m_04 + m_05 + m_06 + m_07 + m_08 + m_09 + m_10 + m_11 + m_12
               + PDS_GENDER*as.factor(BAND_10YR),
               data = filter(df10_train, SECTION_CODE == "0411"))

## Model predictions

# fit final model onto full time series of known data, extrapolate expected values
# based on pre-covid trends

# select dataset based on one used in final model selection and assign as 'df'
df <- df10

# add functions for use in making predictions and prediction intervals
# calculate numbers manually rather than use predict() since predictions
# are summed from predictions for each combination of ageband and gender to get
# total items per month
# Therefore prediction intervals must be manually calculated using `fast_agg_pred`
# as prediction interval is for summed total value not each original predicted value

# Function obtained from external open source code, and adjusted for use in MUMH
fast_agg_pred <- function (w, lmObject, newdata, alpha = 0.95) {
  ## input checking
  if (!inherits(lmObject, "lm")) stop("'lmObject' is not a valid 'lm' object!")
  if (!is.data.frame(newdata)) newdata <- as.data.frame(newdata)
  if (length(w) != nrow(newdata)) stop("length(w) does not match nrow(newdata)")
  ## extract "terms" object from the fitted model, but delete response variable
  tm <- delete.response(terms(lmObject))
  ## linear predictor matrix
  Xp <- model.matrix(tm, newdata)
  ## predicted values by direct matrix-vector multiplication
  pred <- c(Xp %*% coef(lmObject))
  ## mean of the aggregation
  agg_mean <- c(crossprod(pred, w))
  ## residual variance
  sig2 <- c(crossprod(residuals(lmObject))) / df.residual(lmObject)
  ## efficiently compute variance of the aggregation without matrix-matrix computations
  QR <- lmObject$qr   ## qr object of fitted model
  piv <- QR$pivot     ## pivoting index
  r <- QR$rank        ## model rank / numeric rank
  u <- forwardsolve(t(QR$qr), c(crossprod(Xp, w))[piv], r)
  agg_variance <- c(crossprod(u)) * sig2
  ## adjusted variance of the aggregation
  agg_variance_adj <- agg_variance + c(crossprod(w)) * sig2
  ## t-distribution quantiles
  Qt <- c(-1, 1) * qt((1 - alpha) / 2, lmObject$df.residual, lower.tail = FALSE)
  ## names of CI and PI
  NAME <- c("lower", "upper")
  ## CI
  CI <- setNames(agg_mean + Qt * sqrt(agg_variance), NAME)
  ## PI
  PI <- setNames(agg_mean + Qt * sqrt(agg_variance_adj), NAME)
  ## return
  list(mean = agg_mean, var = agg_variance, CI = CI, PI = PI)
}

# create function to make aggregated predictions by month
# default 95% prediction interval and 99% prediction interval
month_pred_fun <- function(month, data, model, alpha = 0.95){
  data <- data %>%
    dplyr::filter(YEAR_MONTH == month)

  pred <- fast_agg_pred(rep.int(1, nrow(data)), data, lmObject = model, alpha = alpha)
  pred99 <- fast_agg_pred(rep.int(1, nrow(data)), data, lmObject = model, alpha = 0.99)
  output <- data.frame(unit = 1)

  output$YEAR_MONTH <- month
  output$mean_fit <- pred[["mean"]]
  output$var <- pred[["var"]]
  output$PIlwr <- pred[["PI"]][["lower"]]
  output$PIupr <- pred[["PI"]][["upper"]]
  output$PIlwr99 <- pred99[["PI"]][["lower"]]
  output$PIupr99 <- pred99[["PI"]][["upper"]]
  output$unit <- NULL

  return(output)

}

# select month list for predictions - use from March 2020 onwards
# final month will need to be changed in each future release as time series expands
## To do: wrap code into a function to run on a list of each section
pred_month_list <- df %>%
  dplyr::filter(YEAR_MONTH >= 202003 & YEAR_MONTH <= 202212) %>%
  pull(YEAR_MONTH) %>%
  unique()

## 0401 Hypnotics and Anxiolytics item prediction
df_0401 <- df %>%
  dplyr::filter(SECTION_CODE == "0401")

#default PI of 95%
pred_0401 <- lapply(pred_month_list, month_pred_fun, data = df_0401, model = mod_0401)

unlist(pred_0401)

rbindlist(pred_0401)

df_0401_sum <- df_0401 %>%
  dplyr::group_by(YEAR_MONTH, SECTION_CODE) %>%
  dplyr::summarise(total_items = sum(ITEM_COUNT)) %>%
  left_join(rbindlist(pred_0401)) %>%
  dplyr::mutate(YEAR_MONTH_string = as.character(YEAR_MONTH)) %>%
  ungroup()

## 0402 Antipsychotics item prediction
df_0402 <- df %>%
  dplyr::filter(SECTION_CODE == "0402")

#default PI of 95%
pred_0402 <- lapply(pred_month_list, month_pred_fun, data = df_0402, model = mod_0402)

unlist(pred_0402)

rbindlist(pred_0402)

df_0402_sum <- df_0402 %>%
  dplyr::group_by(YEAR_MONTH, SECTION_CODE) %>%
  dplyr::summarise(total_items = sum(ITEM_COUNT)) %>%
  left_join(rbindlist(pred_0402)) %>%
  dplyr::mutate(YEAR_MONTH_string = as.character(YEAR_MONTH)) %>%
  ungroup()

## 0403 Antidepressants items prediction
df_0403 <- df %>%
  dplyr::filter(SECTION_CODE == "0403")

#default PI of 95%
pred_0403 <- lapply(pred_month_list, month_pred_fun, data = df_0403, model = mod_0403)

unlist(pred_0403)

rbindlist(pred_0403)

df_0403_sum <- df_0403 %>%
  dplyr::group_by(YEAR_MONTH, SECTION_CODE) %>%
  dplyr::summarise(total_items = sum(ITEM_COUNT)) %>%
  left_join(rbindlist(pred_0403)) %>%
  dplyr::mutate(YEAR_MONTH_string = as.character(YEAR_MONTH)) %>%
  ungroup()

## 0404 CNS stimulants and drugs for ADHD prediction
df_0404 <- df %>%
  dplyr::filter(SECTION_CODE == "0404")

#default PI of 95%
pred_0404 <- lapply(pred_month_list, month_pred_fun, data = df_0404, model = mod_0404)

unlist(pred_0404)

rbindlist(pred_0404)

df_0404_sum <- df_0404 %>%
  dplyr::group_by(YEAR_MONTH, SECTION_CODE) %>%
  dplyr::summarise(total_items = sum(ITEM_COUNT)) %>%
  left_join(rbindlist(pred_0404)) %>%
  dplyr::mutate(YEAR_MONTH_string = as.character(YEAR_MONTH)) %>%
  ungroup()

## 0411 Drugs for dementia items prediction
df_0411 <- df %>%
  dplyr::filter(SECTION_CODE == "0411")

#default PI of 95%
pred_0411 <- lapply(pred_month_list, month_pred_fun, data = df_0411, model = mod_0411)

unlist(pred_0411)

rbindlist(pred_0411)

df_0411_sum <- df_0411 %>%
  dplyr::group_by(YEAR_MONTH, SECTION_CODE) %>%
  dplyr::summarise(total_items = sum(ITEM_COUNT)) %>%
  left_join(rbindlist(pred_0411)) %>%
  dplyr::mutate(YEAR_MONTH_string = as.character(YEAR_MONTH)) %>%
  ungroup()

# write data for all sections to csv for QR purposes

covid_model_predictions <- rbind(df_0401_sum,
                                 df_0402_sum,
                                 df_0403_sum,
                                 df_0404_sum,
                                 df_0411_sum)

# update month in file name for new publications
fwrite(covid_model_predictions, "Y:/Official Stats/MUMH/Covid model tables/dec22.csv")

# get table of sum of actual items across all months from Feb 2020 to Dec 2022, sum of
# total expected items, absolute difference between them and percentage change between them
actual_items <- covid_model_predictions %>%
  dplyr::filter(YEAR_MONTH >= 202003 & YEAR_MONTH <= 202212) %>%
  dplyr::group_by(SECTION_CODE) %>%
  dplyr::summarise(total_actual_items = sum(total_items),
                   total_expected_items = sum(mean_fit),
                   difference = (total_actual_items - total_expected_items),
                   percent_diff = ((total_actual_items - total_expected_items) / total_expected_items * 100))

# save as excel file for use in QR
# update month in file name for new publications
fwrite(actual_items, "Y:/Official Stats/MUMH/QR Data/QR_dec22_model.csv")

# 9. output figures needed for QR purposes --------------------------------
# first get max month and quarter
max_month <- max(raw_data$monthly$YEAR_MONTH)

quarter <- raw_data$monthly %>%
  dplyr::filter(YEAR_MONTH == max_month) %>%
  tail(1) %>%
  pull(FINANCIAL_QUARTER)

# get previous quarter for filtering
prev_quarter <- quarter(
  # uses max_month minus 3 months
  as.Date(paste0(max_month, "01"), format = "%Y%m%d") %m-% months(3),
  type = "quarter",
  # set for fiscal year starting in April
  fiscal_start = 4
)

# get financial year of previous quarter
prev_quarter_fy <- quarter(
  # uses max_month minus 3 months
  as.Date(paste0(max_month, "01"), format = "%Y%m%d") %m-% months(3),
  type = "year.quarter",
  # set for fiscal year starting in April
  fiscal_start = 4
) %>%
  substr(1, 4) %>%
  as.numeric()

# build filter for previous quarter
prev_quarter_filter <- paste0(prev_quarter_fy-1, "/", prev_quarter_fy,
                              " Q", prev_quarter)

# get previous year for filtering
prev_year <- quarter(
  # uses max_month minus 12 months
  as.Date(paste0(max_month, "01"), format = "%Y%m%d") %m-% months(12),
  type = "quarter",
  # set for fiscal year starting in April
  fiscal_start = 4
)

# get financial year of previous year
prev_year_fy <- quarter(
  # uses max_month minus 12 months
  as.Date(paste0(max_month, "01"), format = "%Y%m%d") %m-% months(12),
  type = "year.quarter",
  # set for fiscal year starting in April
  fiscal_start = 4
) %>%
  substr(1, 4) %>%
  as.numeric()

# build filter for previous year
prev_year_filter <- paste0(prev_year_fy - 1, "/", prev_year_fy,
                           " Q", prev_year)

prev_5_year <- quarter(
  # uses max_month minus 12 months
  as.Date(paste0(max_month, "01"), format = "%Y%m%d") %m-% months(60),
  type = "quarter",
  # set for fiscal year starting in April
  fiscal_start = 4
)

# get financial year of previous quarter
prev_5_year_quarter_fy <- quarter(
  # uses max_month minus 3 months
  as.Date(paste0(max_month, "01"), format = "%Y%m%d") %m-% months(60),
  type = "year.quarter",
  # set for fiscal year starting in April
  fiscal_start = 4
) %>%
  substr(1, 4) %>%
  as.numeric()

# build filter for previous quarter
filter_5_years <-
  paste0(prev_5_year_quarter_fy - 1,
         "/",
         prev_5_year_quarter_fy,
         " Q",
         prev_5_year)

# build filters for previous 12 month period and 12 month period prior to that
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

# get min and max dates of each quarter formatted as nice text
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

# create blank workbook for saving
qrwb <- openxlsx::createWorkbook()

openxlsx::modifyBaseFont(qrwb, fontName = "Arial", fontSize = 10)

# loop through bnf_list
bnf_list <- c("0403", "0401", "0402", "0404", "0411")

for(j in 1:length(bnf_list)){

  # create data
  code <- bnf_list[j]
  name <- raw_data$quarterly %>%
    filter(SECTION_CODE == code) %>%
    select(SECTION_NAME) %>%
    unique() %>%
    pull()

  # add sheet to wb with bnf_code
  openxlsx::addWorksheet(qrwb,
                         sheetName = code,
                         gridLines = FALSE)

  # current quarter volume
  cur_quart_volume <- raw_data$quarterly %>%
    filter(SECTION_CODE == code,
           FINANCIAL_QUARTER == quarter) %>%
    select(ITEM_COUNT) %>%
    colSums(.) %>%
    as.numeric()

  # previous quarter volume
  prev_quart_volume <- raw_data$quarterly %>%
    filter(SECTION_CODE == code,
           FINANCIAL_QUARTER == prev_quarter_filter) %>%
    select(ITEM_COUNT) %>%
    colSums(.) %>%
    as.numeric()

  # previous year quarter volume
  prev_year_quart_volume <- raw_data$quarterly %>%
    filter(SECTION_CODE == code,
           FINANCIAL_QUARTER == prev_year_filter) %>%
    select(ITEM_COUNT) %>%
    colSums(.) %>%
    as.numeric()

  # get volume from same quarter 5 years ago
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

  # get patient count of current quarter
  cur_quart_patients <- raw_data$quarterly %>%
    filter(SECTION_CODE == code,
           FINANCIAL_QUARTER == quarter) %>%
    select(PATIENT_COUNT) %>%
    colSums(.) %>%
    as.numeric()

  # previous quarter patient count
  prev_quart_patients <- raw_data$quarterly %>%
    filter(SECTION_CODE == code,
           FINANCIAL_QUARTER == prev_quarter_filter) %>%
    select(PATIENT_COUNT) %>%
    colSums(.) %>%
    as.numeric()

  # previous year quarter patient count
  prev_year_quart_patients <- raw_data$quarterly %>%
    filter(SECTION_CODE == code,
           FINANCIAL_QUARTER == prev_year_filter) %>%
    select(PATIENT_COUNT) %>%
    colSums(.) %>%
    as.numeric()

  # get patient count from same quarter 5 years ago
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

  # get identified patient rates
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

  # average monthly patients
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

  # monthly volumes
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

  # build names for use in narratives
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

  # build data
  qr_data <- data.frame(
    Section_Name = rep(name, 25),
    Section_Formatted = rep(name_formatted, 25),
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
      paste0(min_filter_prev_12_months, " to ", max_filter_12_months)
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
      "% change items"
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
      volume_change
    ),
    Rounded = c(
      mumhquarterly::format_number(cur_quart_volume),
      mumhquarterly::format_number(prev_year_quart_volume),
      mumhquarterly::format_number(annual_per_change, percentage = T),
      mumhquarterly::format_number(prev_quart_volume),
      mumhquarterly::format_number(quarterly_per_change, percentage = T),
      mumhquarterly::format_number(prev_5_year_quart_volume),
      mumhquarterly::format_number((
        cur_quart_volume - prev_5_year_quart_volume
      )),
      mumhquarterly::format_number(annual_5_per_change, percentage = T),
      mumhquarterly::format_number(cur_quart_patients),
      mumhquarterly::format_number(prev_year_quart_patients),
      mumhquarterly::format_number(annual_per_change_patients, percentage = T),
      mumhquarterly::format_number(prev_quart_patients),
      mumhquarterly::format_number(quarterly_per_change_patients, percentage = T),
      mumhquarterly::format_number(prev_5_year_quart_patients),
      mumhquarterly::format_number((
        cur_quart_patients - prev_5_year_quart_patients
      )),
      mumhquarterly::format_number(annual_5_per_change_patients, percentage = T),
      mumhquarterly::format_number(prev_5_year_quart_identified, percentage = T),
      mumhquarterly::format_number(current_quart_identified, percentage = T),
      mumhquarterly::format_number(current_quart_identified - prev_5_year_quart_identified),
      mumhquarterly::format_number(ave_12_month_patients),
      mumhquarterly::format_number(ave_12_month_patients_prev),
      mumhquarterly::format_number(average_patient_change, percentage = T),
      mumhquarterly::format_number(`12_month_volume`),
      mumhquarterly::format_number(`12_month_volume_prev`),
      mumhquarterly::format_number(volume_change, percentage = T)
    )
  )

  # output tables to global enviroment for narrative automation
  assign(paste0("table_", code), qr_data, envir = globalenv())

  # write data to sheet
  openxlsx::writeDataTable(qrwb,
                           sheet = code,
                           startRow = 1,
                           x = qr_data,
                           tableName = paste0("table_", code),
                           tableStyle = "none")

  # auto width columns
  setColWidths(qrwb,
               sheet = code,
               cols = 1:5,
               widths = "auto")
}

# save workbook in shared QR folder
openxlsx::saveWorkbook(
  qrwb,
  paste0(
    "Y:\\Official Stats\\MUMH\\QR Data\\QR ",
    max_filter_12_months,
    ".xlsx"
  ),
  overwrite = FALSE
)

# 10. render markdown narrative and background ---------------------------------
# change file names for new publications

rmarkdown::render("mumh-quarterly-narrative.Rmd",
                  output_format = "html_document",
                  output_file = "outputs/mumh_quarterly_dec22_v001.html")

rmarkdown::render("mumh-quarterly-narrative.Rmd",
                  output_format = "word_document",
                  output_file = "outputs/mumh_quarterly_dec22_v001.docx")

rmarkdown::render("background.Rmd",
                  output_format = "html_document",
                  output_file = "outputs/mumh_quarterly_dec22_background.html")

rmarkdown::render("background.Rmd",
                  output_format = "word_document",
                  output_file = "outputs/mumh_quarterly_dec22_background.docx")
