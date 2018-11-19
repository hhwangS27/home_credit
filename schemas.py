schemas = {'installments_payments':
types.StructType([
                     types.StructField('SK_ID_PREV', types.LongType(), False),
                     types.StructField('SK_ID_CURR', types.LongType(), False),
                     types.StructField('NUM_INSTALMENT_VERSION', types.IntegerType(), True),
                     types.StructField('NUM_INSTALMENT_NUMBER', types.IntegerType(), True),
                     types.StructField('DAYS_INSTALMENT', types.IntegerType(), True),
                     types.StructField('DAYS_ENTRY_PAYMENT', types.IntegerType(), True),
                     types.StructField('AMT_INSTALMENT', types.DoubleType(), True),
                     types.StructField('AMT_PAYMENT', types.DoubleType(), True),
                ]),
'previous_application':
types.StructType([
                     types.StructField('SK_ID_PREV', types.LongType(), False),
                     types.StructField('SK_ID_CURR', types.LongType(), False),
                     types.StructField('NAME_CONTRACT_TYPE', types.StringType(), True),
                     types.StructField('AMT_ANNUITY', types.DoubleType(), True),
                     types.StructField('AMT_APPLICATION', types.DoubleType(), True),
                     types.StructField('AMT_CREDIT', types.DoubleType(), True),
                     types.StructField('AMT_DOWN_PAYMENT', types.DoubleType(), True),
                     types.StructField('AMT_GOODS_PRICE', types.DoubleType(), True),
                     types.StructField('WEEKDAY_APPR_PROCESS_START', types.StringType(), True),
                     types.StructField('HOUR_APPR_PROCESS_START', types.ShortType(), True),
                     types.StructField('FLAG_LAST_APPL_PER_CONTRACT', types.StringType(), True),
                     types.StructField('NFLAG_LAST_APPL_IN_DAY', types.ShortType(), True),
                     types.StructField('RATE_DOWN_PAYMENT', types.DoubleType(), True),
                     types.StructField('RATE_INTEREST_PRIMARY', types.DoubleType(), True),
                     types.StructField('RATE_INTEREST_PRIVILEGED', types.DoubleType(), True),
                     types.StructField('NAME_CASH_LOAN_PURPOSE', types.StringType(), True),
                     types.StructField('NAME_CONTRACT_STATUS', types.StringType(), True),
                     types.StructField('DAYS_DECISION', types.IntegerType(), True),
                     types.StructField('NAME_PAYMENT_TYPE', types.StringType(), True),
                     types.StructField('CODE_REJECT_REASON', types.StringType(), True),
                     types.StructField('NAME_TYPE_SUITE', types.StringType(), True),
                     types.StructField('NAME_CLIENT_TYPE', types.StringType(), True),
                     types.StructField('NAME_GOODS_CATEGORY', types.StringType(), True),
                     types.StructField('NAME_PORTFOLIO', types.StringType(), True),
                     types.StructField('NAME_PRODUCT_TYPE', types.StringType(), True),
                     types.StructField('CHANNEL_TYPE', types.StringType(), True),
                     types.StructField('SELLERPLACE_AREA', types.IntegerType(), True),
                     types.StructField('NAME_SELLER_INDUSTRY', types.StringType(), True),
                     types.StructField('CNT_PAYMENT', types.IntegerType(), True),
                     types.StructField('NAME_YIELD_GROUP', types.StringType(), True),
                     types.StructField('PRODUCT_COMBINATION', types.StringType(), True),
                     types.StructField('DAYS_FIRST_DRAWING', types.IntegerType(), True),
                     types.StructField('DAYS_FIRST_DUE', types.IntegerType(), True),
                     types.StructField('DAYS_LAST_DUE_1ST_VERSION', types.IntegerType(), True),
                     types.StructField('DAYS_LAST_DUE', types.IntegerType(), True),
                     types.StructField('DAYS_TERMINATION', types.IntegerType(), True),
                     types.StructField('NFLAG_INSURED_ON_APPROVAL', types.ShortType(), True),
                ]),
'POS_CASH_balance':
types.StructType([
                     types.StructField('SK_ID_PREV', types.LongType(), False),
                     types.StructField('SK_ID_CURR', types.LongType(), False),
                     types.StructField('MONTHS_BALANCE', types.IntegerType(), True),
                     types.StructField('CNT_INSTALMENT', types.IntegerType(), True),
                     types.StructField('CNT_INSTALMENT_FUTURE', types.IntegerType(), True),
                     types.StructField('NAME_CONTRACT_STATUS', types.StringType(), True),
                     types.StructField('SK_DPD', types.ShortType(), True),
                     types.StructField('SK_DPD_DEF', types.ShortType(), True),
                ]),
'bureau':
types.StructType([
                     types.StructField('SK_ID_CURR', types.LongType(), False),
                     types.StructField('SK_ID_BUREAU', types.LongType(), False),
                     types.StructField('CREDIT_ACTIVE', types.StringType(), True),
                     types.StructField('CREDIT_CURRENCY', types.StringType(), True),
                     types.StructField('DAYS_CREDIT', types.IntegerType(), True),
                     types.StructField('CREDIT_DAY_OVERDUE', types.IntegerType(), True),
                     types.StructField('DAYS_CREDIT_ENDDATE', types.IntegerType(), True),
                     types.StructField('DAYS_ENDDATE_FACT', types.IntegerType(), True),
                     types.StructField('AMT_CREDIT_MAX_OVERDUE', types.IntegerType(), True),
                     types.StructField('CNT_CREDIT_PROLONG', types.IntegerType(), True),
                     types.StructField('AMT_CREDIT_SUM', types.IntegerType(), True),
                     types.StructField('AMT_CREDIT_SUM_DEBT', types.IntegerType(), True),
                     types.StructField('AMT_CREDIT_SUM_LIMIT', types.IntegerType(), True),
                     types.StructField('AMT_CREDIT_SUM_OVERDUE', types.IntegerType(), True),
                     types.StructField('CREDIT_TYPE', types.StringType(), True),
                     types.StructField('DAYS_CREDIT_UPDATE', types.IntegerType(), True),
                     types.StructField('AMT_ANNUITY', types.IntegerType(), True),
                ]),
'application_test':
types.StructType([
                     types.StructField('SK_ID_CURR', types.LongType(), False),
                     types.StructField('NAME_CONTRACT_TYPE', types.StringType(), True),
                     types.StructField('CODE_GENDER', types.StringType(), True),
                     types.StructField('FLAG_OWN_CAR', types.StringType(), True),
                     types.StructField('FLAG_OWN_REALTY', types.StringType(), True),
                     types.StructField('CNT_CHILDREN', types.ShortType(), True),
                     types.StructField('AMT_INCOME_TOTAL', types.DoubleType(), True),
                     types.StructField('AMT_CREDIT', types.DoubleType(), True),
                     types.StructField('AMT_ANNUITY', types.DoubleType(), True),
                     types.StructField('AMT_GOODS_PRICE', types.DoubleType(), True),
                     types.StructField('NAME_TYPE_SUITE', types.StringType(), True),
                     types.StructField('NAME_INCOME_TYPE', types.StringType(), True),
                     types.StructField('NAME_EDUCATION_TYPE', types.StringType(), True),
                     types.StructField('NAME_FAMILY_STATUS', types.StringType(), True),
                     types.StructField('NAME_HOUSING_TYPE', types.StringType(), True),
                     types.StructField('REGION_POPULATION_RELATIVE', types.DoubleType(), True),
                     types.StructField('DAYS_BIRTH', types.IntegerType(), True),
                     types.StructField('DAYS_EMPLOYED', types.IntegerType(), True),
                     types.StructField('DAYS_REGISTRATION', types.IntegerType(), True),
                     types.StructField('DAYS_ID_PUBLISH', types.IntegerType(), True),
                     types.StructField('OWN_CAR_AGE', types.IntegerType(), True),
                     types.StructField('FLAG_MOBIL', types.ShortType(), True),
                     types.StructField('FLAG_EMP_PHONE', types.ShortType(), True),
                     types.StructField('FLAG_WORK_PHONE', types.ShortType(), True),
                     types.StructField('FLAG_CONT_MOBILE', types.ShortType(), True),
                     types.StructField('FLAG_PHONE', types.ShortType(), True),
                     types.StructField('FLAG_EMAIL', types.ShortType(), True),
                     types.StructField('OCCUPATION_TYPE', types.StringType(), True),
                     types.StructField('CNT_FAM_MEMBERS', types.ShortType(), True),
                     types.StructField('REGION_RATING_CLIENT', types.ShortType(), True),
                     types.StructField('REGION_RATING_CLIENT_W_CITY', types.ShortType(), True),
                     types.StructField('WEEKDAY_APPR_PROCESS_START', types.StringType(), True),
                     types.StructField('HOUR_APPR_PROCESS_START', types.ShortType(), True),
                     types.StructField('REG_REGION_NOT_LIVE_REGION', types.ShortType(), True),
                     types.StructField('REG_REGION_NOT_WORK_REGION', types.ShortType(), True),
                     types.StructField('LIVE_REGION_NOT_WORK_REGION', types.ShortType(), True),
                     types.StructField('REG_CITY_NOT_LIVE_CITY', types.ShortType(), True),
                     types.StructField('REG_CITY_NOT_WORK_CITY', types.ShortType(), True),
                     types.StructField('LIVE_CITY_NOT_WORK_CITY', types.ShortType(), True),
                     types.StructField('ORGANIZATION_TYPE', types.StringType(), True),
                     types.StructField('EXT_SOURCE_1', types.DoubleType(), True),
                     types.StructField('EXT_SOURCE_2', types.DoubleType(), True),
                     types.StructField('EXT_SOURCE_3', types.DoubleType(), True),
                     types.StructField('APARTMENTS_AVG', types.DoubleType(), True),
                     types.StructField('BASEMENTAREA_AVG', types.DoubleType(), True),
                     types.StructField('YEARS_BEGINEXPLUATATION_AVG', types.DoubleType(), True),
                     types.StructField('YEARS_BUILD_AVG', types.DoubleType(), True),
                     types.StructField('COMMONAREA_AVG', types.DoubleType(), True),
                     types.StructField('ELEVATORS_AVG', types.DoubleType(), True),
                     types.StructField('ENTRANCES_AVG', types.DoubleType(), True),
                     types.StructField('FLOORSMAX_AVG', types.DoubleType(), True),
                     types.StructField('FLOORSMIN_AVG', types.DoubleType(), True),
                     types.StructField('LANDAREA_AVG', types.DoubleType(), True),
                     types.StructField('LIVINGAPARTMENTS_AVG', types.DoubleType(), True),
                     types.StructField('LIVINGAREA_AVG', types.DoubleType(), True),
                     types.StructField('NONLIVINGAPARTMENTS_AVG', types.DoubleType(), True),
                     types.StructField('NONLIVINGAREA_AVG', types.DoubleType(), True),
                     types.StructField('APARTMENTS_MODE', types.DoubleType(), True),
                     types.StructField('BASEMENTAREA_MODE', types.DoubleType(), True),
                     types.StructField('YEARS_BEGINEXPLUATATION_MODE', types.DoubleType(), True),
                     types.StructField('YEARS_BUILD_MODE', types.DoubleType(), True),
                     types.StructField('COMMONAREA_MODE', types.DoubleType(), True),
                     types.StructField('ELEVATORS_MODE', types.DoubleType(), True),
                     types.StructField('ENTRANCES_MODE', types.DoubleType(), True),
                     types.StructField('FLOORSMAX_MODE', types.DoubleType(), True),
                     types.StructField('FLOORSMIN_MODE', types.DoubleType(), True),
                     types.StructField('LANDAREA_MODE', types.DoubleType(), True),
                     types.StructField('LIVINGAPARTMENTS_MODE', types.DoubleType(), True),
                     types.StructField('LIVINGAREA_MODE', types.DoubleType(), True),
                     types.StructField('NONLIVINGAPARTMENTS_MODE', types.DoubleType(), True),
                     types.StructField('NONLIVINGAREA_MODE', types.DoubleType(), True),
                     types.StructField('APARTMENTS_MEDI', types.DoubleType(), True),
                     types.StructField('BASEMENTAREA_MEDI', types.DoubleType(), True),
                     types.StructField('YEARS_BEGINEXPLUATATION_MEDI', types.DoubleType(), True),
                     types.StructField('YEARS_BUILD_MEDI', types.DoubleType(), True),
                     types.StructField('COMMONAREA_MEDI', types.DoubleType(), True),
                     types.StructField('ELEVATORS_MEDI', types.DoubleType(), True),
                     types.StructField('ENTRANCES_MEDI', types.DoubleType(), True),
                     types.StructField('FLOORSMAX_MEDI', types.DoubleType(), True),
                     types.StructField('FLOORSMIN_MEDI', types.DoubleType(), True),
                     types.StructField('LANDAREA_MEDI', types.DoubleType(), True),
                     types.StructField('LIVINGAPARTMENTS_MEDI', types.DoubleType(), True),
                     types.StructField('LIVINGAREA_MEDI', types.DoubleType(), True),
                     types.StructField('NONLIVINGAPARTMENTS_MEDI', types.DoubleType(), True),
                     types.StructField('NONLIVINGAREA_MEDI', types.DoubleType(), True),
                     types.StructField('FONDKAPREMONT_MODE', types.StringType(), True),
                     types.StructField('HOUSETYPE_MODE', types.StringType(), True),
                     types.StructField('TOTALAREA_MODE', types.DoubleType(), True),
                     types.StructField('WALLSMATERIAL_MODE', types.StringType(), True),
                     types.StructField('EMERGENCYSTATE_MODE', types.StringType(), True),
                     types.StructField('OBS_30_CNT_SOCIAL_CIRCLE', types.IntegerType(), True),
                     types.StructField('DEF_30_CNT_SOCIAL_CIRCLE', types.IntegerType(), True),
                     types.StructField('OBS_60_CNT_SOCIAL_CIRCLE', types.IntegerType(), True),
                     types.StructField('DEF_60_CNT_SOCIAL_CIRCLE', types.IntegerType(), True),
                     types.StructField('DAYS_LAST_PHONE_CHANGE', types.IntegerType(), True),
                     types.StructField('FLAG_DOCUMENT_2', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_3', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_4', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_5', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_6', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_7', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_8', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_9', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_10', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_11', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_12', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_13', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_14', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_15', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_16', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_17', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_18', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_19', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_20', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_21', types.ShortType(), True),
                     types.StructField('AMT_REQ_CREDIT_BUREAU_HOUR', types.IntegerType(), True),
                     types.StructField('AMT_REQ_CREDIT_BUREAU_DAY', types.IntegerType(), True),
                     types.StructField('AMT_REQ_CREDIT_BUREAU_WEEK', types.IntegerType(), True),
                     types.StructField('AMT_REQ_CREDIT_BUREAU_MON', types.IntegerType(), True),
                     types.StructField('AMT_REQ_CREDIT_BUREAU_QRT', types.IntegerType(), True),
                     types.StructField('AMT_REQ_CREDIT_BUREAU_YEAR', types.IntegerType(), True),
                ]),
'application_train':
types.StructType([
                     types.StructField('SK_ID_CURR', types.LongType(), False),
                     types.StructField('TARGET', types.ShortType(), True),
                     types.StructField('NAME_CONTRACT_TYPE', types.StringType(), True),
                     types.StructField('CODE_GENDER', types.StringType(), True),
                     types.StructField('FLAG_OWN_CAR', types.StringType(), True),
                     types.StructField('FLAG_OWN_REALTY', types.StringType(), True),
                     types.StructField('CNT_CHILDREN', types.ShortType(), True),
                     types.StructField('AMT_INCOME_TOTAL', types.DoubleType(), True),
                     types.StructField('AMT_CREDIT', types.DoubleType(), True),
                     types.StructField('AMT_ANNUITY', types.DoubleType(), True),
                     types.StructField('AMT_GOODS_PRICE', types.DoubleType(), True),
                     types.StructField('NAME_TYPE_SUITE', types.StringType(), True),
                     types.StructField('NAME_INCOME_TYPE', types.StringType(), True),
                     types.StructField('NAME_EDUCATION_TYPE', types.StringType(), True),
                     types.StructField('NAME_FAMILY_STATUS', types.StringType(), True),
                     types.StructField('NAME_HOUSING_TYPE', types.StringType(), True),
                     types.StructField('REGION_POPULATION_RELATIVE', types.DoubleType(), True),
                     types.StructField('DAYS_BIRTH', types.IntegerType(), True),
                     types.StructField('DAYS_EMPLOYED', types.IntegerType(), True),
                     types.StructField('DAYS_REGISTRATION', types.IntegerType(), True),
                     types.StructField('DAYS_ID_PUBLISH', types.IntegerType(), True),
                     types.StructField('OWN_CAR_AGE', types.IntegerType(), True),
                     types.StructField('FLAG_MOBIL', types.ShortType(), True),
                     types.StructField('FLAG_EMP_PHONE', types.ShortType(), True),
                     types.StructField('FLAG_WORK_PHONE', types.ShortType(), True),
                     types.StructField('FLAG_CONT_MOBILE', types.ShortType(), True),
                     types.StructField('FLAG_PHONE', types.ShortType(), True),
                     types.StructField('FLAG_EMAIL', types.ShortType(), True),
                     types.StructField('OCCUPATION_TYPE', types.StringType(), True),
                     types.StructField('CNT_FAM_MEMBERS', types.ShortType(), True),
                     types.StructField('REGION_RATING_CLIENT', types.ShortType(), True),
                     types.StructField('REGION_RATING_CLIENT_W_CITY', types.ShortType(), True),
                     types.StructField('WEEKDAY_APPR_PROCESS_START', types.StringType(), True),
                     types.StructField('HOUR_APPR_PROCESS_START', types.ShortType(), True),
                     types.StructField('REG_REGION_NOT_LIVE_REGION', types.ShortType(), True),
                     types.StructField('REG_REGION_NOT_WORK_REGION', types.ShortType(), True),
                     types.StructField('LIVE_REGION_NOT_WORK_REGION', types.ShortType(), True),
                     types.StructField('REG_CITY_NOT_LIVE_CITY', types.ShortType(), True),
                     types.StructField('REG_CITY_NOT_WORK_CITY', types.ShortType(), True),
                     types.StructField('LIVE_CITY_NOT_WORK_CITY', types.ShortType(), True),
                     types.StructField('ORGANIZATION_TYPE', types.StringType(), True),
                     types.StructField('EXT_SOURCE_1', types.DoubleType(), True),
                     types.StructField('EXT_SOURCE_2', types.DoubleType(), True),
                     types.StructField('EXT_SOURCE_3', types.DoubleType(), True),
                     types.StructField('APARTMENTS_AVG', types.DoubleType(), True),
                     types.StructField('BASEMENTAREA_AVG', types.DoubleType(), True),
                     types.StructField('YEARS_BEGINEXPLUATATION_AVG', types.DoubleType(), True),
                     types.StructField('YEARS_BUILD_AVG', types.DoubleType(), True),
                     types.StructField('COMMONAREA_AVG', types.DoubleType(), True),
                     types.StructField('ELEVATORS_AVG', types.DoubleType(), True),
                     types.StructField('ENTRANCES_AVG', types.DoubleType(), True),
                     types.StructField('FLOORSMAX_AVG', types.DoubleType(), True),
                     types.StructField('FLOORSMIN_AVG', types.DoubleType(), True),
                     types.StructField('LANDAREA_AVG', types.DoubleType(), True),
                     types.StructField('LIVINGAPARTMENTS_AVG', types.DoubleType(), True),
                     types.StructField('LIVINGAREA_AVG', types.DoubleType(), True),
                     types.StructField('NONLIVINGAPARTMENTS_AVG', types.DoubleType(), True),
                     types.StructField('NONLIVINGAREA_AVG', types.DoubleType(), True),
                     types.StructField('APARTMENTS_MODE', types.DoubleType(), True),
                     types.StructField('BASEMENTAREA_MODE', types.DoubleType(), True),
                     types.StructField('YEARS_BEGINEXPLUATATION_MODE', types.DoubleType(), True),
                     types.StructField('YEARS_BUILD_MODE', types.DoubleType(), True),
                     types.StructField('COMMONAREA_MODE', types.DoubleType(), True),
                     types.StructField('ELEVATORS_MODE', types.DoubleType(), True),
                     types.StructField('ENTRANCES_MODE', types.DoubleType(), True),
                     types.StructField('FLOORSMAX_MODE', types.DoubleType(), True),
                     types.StructField('FLOORSMIN_MODE', types.DoubleType(), True),
                     types.StructField('LANDAREA_MODE', types.DoubleType(), True),
                     types.StructField('LIVINGAPARTMENTS_MODE', types.DoubleType(), True),
                     types.StructField('LIVINGAREA_MODE', types.DoubleType(), True),
                     types.StructField('NONLIVINGAPARTMENTS_MODE', types.DoubleType(), True),
                     types.StructField('NONLIVINGAREA_MODE', types.DoubleType(), True),
                     types.StructField('APARTMENTS_MEDI', types.DoubleType(), True),
                     types.StructField('BASEMENTAREA_MEDI', types.DoubleType(), True),
                     types.StructField('YEARS_BEGINEXPLUATATION_MEDI', types.DoubleType(), True),
                     types.StructField('YEARS_BUILD_MEDI', types.DoubleType(), True),
                     types.StructField('COMMONAREA_MEDI', types.DoubleType(), True),
                     types.StructField('ELEVATORS_MEDI', types.DoubleType(), True),
                     types.StructField('ENTRANCES_MEDI', types.DoubleType(), True),
                     types.StructField('FLOORSMAX_MEDI', types.DoubleType(), True),
                     types.StructField('FLOORSMIN_MEDI', types.DoubleType(), True),
                     types.StructField('LANDAREA_MEDI', types.DoubleType(), True),
                     types.StructField('LIVINGAPARTMENTS_MEDI', types.DoubleType(), True),
                     types.StructField('LIVINGAREA_MEDI', types.DoubleType(), True),
                     types.StructField('NONLIVINGAPARTMENTS_MEDI', types.DoubleType(), True),
                     types.StructField('NONLIVINGAREA_MEDI', types.DoubleType(), True),
                     types.StructField('FONDKAPREMONT_MODE', types.StringType(), True),
                     types.StructField('HOUSETYPE_MODE', types.StringType(), True),
                     types.StructField('TOTALAREA_MODE', types.DoubleType(), True),
                     types.StructField('WALLSMATERIAL_MODE', types.StringType(), True),
                     types.StructField('EMERGENCYSTATE_MODE', types.StringType(), True),
                     types.StructField('OBS_30_CNT_SOCIAL_CIRCLE', types.IntegerType(), True),
                     types.StructField('DEF_30_CNT_SOCIAL_CIRCLE', types.IntegerType(), True),
                     types.StructField('OBS_60_CNT_SOCIAL_CIRCLE', types.IntegerType(), True),
                     types.StructField('DEF_60_CNT_SOCIAL_CIRCLE', types.IntegerType(), True),
                     types.StructField('DAYS_LAST_PHONE_CHANGE', types.IntegerType(), True),
                     types.StructField('FLAG_DOCUMENT_2', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_3', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_4', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_5', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_6', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_7', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_8', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_9', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_10', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_11', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_12', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_13', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_14', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_15', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_16', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_17', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_18', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_19', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_20', types.ShortType(), True),
                     types.StructField('FLAG_DOCUMENT_21', types.ShortType(), True),
                     types.StructField('AMT_REQ_CREDIT_BUREAU_HOUR', types.IntegerType(), True),
                     types.StructField('AMT_REQ_CREDIT_BUREAU_DAY', types.IntegerType(), True),
                     types.StructField('AMT_REQ_CREDIT_BUREAU_WEEK', types.IntegerType(), True),
                     types.StructField('AMT_REQ_CREDIT_BUREAU_MON', types.IntegerType(), True),
                     types.StructField('AMT_REQ_CREDIT_BUREAU_QRT', types.IntegerType(), True),
                     types.StructField('AMT_REQ_CREDIT_BUREAU_YEAR', types.IntegerType(), True),
                ]),
'credit_card_balance':
types.StructType([
                     types.StructField('SK_ID_PREV', types.LongType(), False),
                     types.StructField('SK_ID_CURR', types.LongType(), False),
                     types.StructField('MONTHS_BALANCE', types.ShortType(), True),
                     types.StructField('AMT_BALANCE', types.DoubleType(), True),
                     types.StructField('AMT_CREDIT_LIMIT_ACTUAL', types.DoubleType(), True),
                     types.StructField('AMT_DRAWINGS_ATM_CURRENT', types.DoubleType(), True),
                     types.StructField('AMT_DRAWINGS_CURRENT', types.DoubleType(), True),
                     types.StructField('AMT_DRAWINGS_OTHER_CURRENT', types.DoubleType(), True),
                     types.StructField('AMT_DRAWINGS_POS_CURRENT', types.DoubleType(), True),
                     types.StructField('AMT_INST_MIN_REGULARITY', types.DoubleType(), True),
                     types.StructField('AMT_PAYMENT_CURRENT', types.DoubleType(), True),
                     types.StructField('AMT_PAYMENT_TOTAL_CURRENT', types.DoubleType(), True),
                     types.StructField('AMT_RECEIVABLE_PRINCIPAL', types.DoubleType(), True),
                     types.StructField('AMT_RECIVABLE', types.DoubleType(), True),
                     types.StructField('AMT_TOTAL_RECEIVABLE', types.DoubleType(), True),
                     types.StructField('CNT_DRAWINGS_ATM_CURRENT', types.IntegerType(), True),
                     types.StructField('CNT_DRAWINGS_CURRENT', types.IntegerType(), True),
                     types.StructField('CNT_DRAWINGS_OTHER_CURRENT', types.IntegerType(), True),
                     types.StructField('CNT_DRAWINGS_POS_CURRENT', types.IntegerType(), True),
                     types.StructField('CNT_INSTALMENT_MATURE_CUM', types.IntegerType(), True),
                     types.StructField('NAME_CONTRACT_STATUS', types.StringType(), True),
                     types.StructField('SK_DPD', types.ShortType(), True),
                     types.StructField('SK_DPD_DEF', types.ShortType(), True),
                ]),
'bureau_balance':
types.StructType([
                     types.StructField('SK_ID_BUREAU', types.LongType(), False),
                     types.StructField('MONTHS_BALANCE', types.ShortType(), True),
                     types.StructField('STATUS', types.StringType(), True),
                ]),
}