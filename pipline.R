
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
# con <- nhsbsaR::con_nhsbsa(
#   database = "DWCP",
#   username = rstudioapi::showPrompt(title = "Username",
#                                     message = "Username"),
#   password = rstudioapi::askForPassword()
# )
#
# # use functions to construct lazy tables
# tdim <- mumhquarterly::create_tdim(con = con)
#
# org <- mumhquarterly::create_org_dim(con = con)
#
# drug <- mumhquarterly::create_drug_dim(con = con,
#                         bnf_codes = c("0401", "0402", "0403", "0404", "0411"))
#
# fact <- mumhquarterly::create_fact(con = con)
#
# # join tables to create data. generate query
# raw_data_query <- fact %>%
#   dplyr::inner_join(
#     tdim,
#     by = "YEAR_MONTH"
#   ) %>%
#   dplyr::inner_join(
#     drug,
#     by = c("CALC_PREC_DRUG_RECORD_ID" = "RECORD_ID", "YEAR_MONTH")
#   ) %>%
#   dplyr::inner_join(
#     org,
#     by = c("PRESC_TYPE_PRNT" = "LVL_5_OUPDT", "PRESC_ID_PRNT" = "LVL_5_OU")
#   ) %>%
#   dplyr::group_by(
#     FINANCIAL_YEAR,
#     FINANCIAL_QUARTER,
#     YEAR_MONTH,
#     SECTION_DESCR,
#     BNF_SECTION,
#     IDENTIFIED_PATIENT_ID,
#     IDENTIFIED_FLAG
#   ) %>%
#   dplyr::summarise(
#     ITEM_COUNT = sum(ITEM_COUNT, na.rm = TRUE),
#     PATIENT_COUNT = sum(IDENTIFIED_FLAG == "Y", na.rm = TRUE),
#     ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC, na.rm = TRUE)
#   )
#
# # extract data from DWH
# raw_data <- raw_data_query %>%
#   dplyr::collect()

# # disconnect from DWH
# DBI::dbDisconnect(con)

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

# TODO


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
