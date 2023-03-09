#script for implementing linear model for prescribing from March 2020 onwards

#packages required
#devtools::install_github("nhsbsa-data-analytics/mumhquarterly")
library(broom)
library(data.table)
library(dbplyr)
library(devtools)
library(dplyr)
library(lubridate)
library(mumhquarterly)
library(rsample)
library(stringr)
library(tidyr)

### 1. Data import

# get data from shared area
raw_data_model <- data.table::fread(rownames(file.info(
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

# get dispensing days using functions from mumhquarterly package
# this will be replaced in future by dispensing_days() from nhsbsaUtils
dispensing_days <- mumhquarterly::get_dispensing_days(2023)

# join dispensing days to raw data, add columns for position of month in year,
# position of month in full dataset, and month start date
# keep original 5 year agebands and fill rows where ageband has no items recorded
df5 <- raw_data_model |>
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
df10 <- raw_data_model |>
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
df_both <- raw_data_model |>
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


### 2. Model exploration and fitting

## TO DO: add code from testing script into this section, inc. overfitting explanation,
## and the other BNF sections during variable selection (all gave similar results to
## 0404 included below)

# explore variable selection for use in model

# check if smaller or larger agebands makes a difference
#test and train data for df_both dataset
both_time <- df_bothk %>%
  ungroup() %>%
  dplyr::mutate(time_period = case_when(YEAR_MONTH <= 202002 ~ "pre_covid",
                                        TRUE ~ "covid"))

both_split <- rsample::group_initial_split(both_time, time_period)
both_train <- rsample::training(both_split)
both_test <- rsample::testing(both_split)

#try 5yr vs 10yr agebands model for each BNF section
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

# TO DO: add more code from model_fitting script
anova(mod_0404.2, mod_0404.6)

#change plot window to two by two
par(mfrow = c(2,2))
# plot residuals etc to check model assumptions
# compare previous model fitted on new data and new model on new data
# evidence of heteroscedasticity, to keep in mind when building future model
# as another type of model (eg. forecasting) may be better suited to the data characteristics
# and/or non-linear relationship?
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

#repeat testing for other BNF sections

# model needs to use the separate individual month number variables instead of MONTH_NUM
# fit every level of MONTH_NUM as its own variable instead of a level
# first month (January) is excluded from model as all other months compared against it.
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

### 3. Model predictions

# fit final model onto full time series of known data,
# and make predictions of expected values

# select dataset based on one used in final model selection and assign as 'df'
df <- df10

# add functions for use in making predictions and prediction intervals
# calculate numbers manually rather than use predict() since predictions
# are summed from predictions for each combination of ageband and gender to get
# total items per month.
# Therefore prediction intervals must be manually calculated using `fast_agg_pred`
# as prediction interval is for summed total value not each original predicted value
# function obtained from open source code on stack overflow, then tweaked

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

# create function to make predictions by month
# default 95% prediction interval
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

## TO DO below can be automated/wrapped into a function to run on a list of each section
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
