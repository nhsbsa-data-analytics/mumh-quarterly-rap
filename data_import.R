# script to import monthly data with age and gender for use in pre-covid trends model

# get data from shared area
raw_data_monthly <- data.table::fread(rownames(file.info(
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

# get dispensing days using functions from mumhquarterly package
# this will be replaced in future by dispensing_days() from nhsbsaUtils
dispensing_days <- mumhquarterly::get_dispensing_days(2023)

# calculate columns for month start date, position of month in the calendar year,
# and position of month within the full time series. then join dispensing days data
df <- raw_data_model%>%
  dplyr::mutate(BAND_10YR = case_when(DALL_5YR_BAND %in% c("00-04", "05-09", "10-14", "15-19") ~ "00-19",
                                      DALL_5YR_BAND %in% c("20-24", "25-29", "30-34", "35-39") ~ "20-39",
                                      DALL_5YR_BAND %in% c("40-44", "45-49", "50-54", "55-59") ~ "40-59",
                                      DALL_5YR_BAND %in% c("60-64", "65-69", "70-74", "75-79") ~ "60-79",
                                      DALL_5YR_BAND == "Unknown" ~ "Unknown",
                                      TRUE ~ "80+")) %>%
  dplyr::select(!(DALL_5YR_BAND)) %>%
  dplyr::group_by(YEAR_MONTH,
                  SECTION_NAME,
                  SECTION_CODE,
                  IDENTIFIED_FLAG,
                  PDS_GENDER,
                  BAND_10YR) %>%
  dplyr::summarise(ITEM_COUNT = sum(ITEM_COUNT),
                   ITEM_PAY_DR_NIC = sum(ITEM_PAY_DR_NIC)) %>%
  dplyr::group_by(SECTION_NAME, SECTION_CODE, IDENTIFIED_FLAG,
                  PDS_GENDER, BAND_10YR) %>%
  dplyr::mutate(
    MONTH_START = as.Date(paste0(YEAR_MONTH, "01"), format = "%Y%m%d"),
    MONTH_NUM = lubridate::month(MONTH_START),
    MONTH_INDEX = lubridate::interval(dmy(01032015), as.Date(MONTH_START)) %/% months(1)
  ) %>%
  dplyr::left_join(dispensing_days,
                   by = "YEAR_MONTH") %>%
  dplyr::ungroup()

# check total items for manipulated dataset gives same as raw data
# both show same items over the 93 month period
total_items_raw <- raw_data_model %>%
  dplyr::summarise(total_items = sum(ITEM_COUNT))
total_items <- df %>%
  dplyr::summarise(total_items = sum(ITEM_COUNT))

# Optional - remove unknown gender and age categories
# total of [] items still left in dataset
df_known <- df %>%
  dplyr::filter(!(PDS_GENDER == "U" | BAND_10YR == "Unknown"))
total_items_known <- df_known %>%
  dplyr::summarise(total_items = sum(ITEM_COUNT))
