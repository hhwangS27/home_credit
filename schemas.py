#schemas = {tableName:schema, ...}

from pyspark.sql import SparkSession, functions, types, Row

schemas = {'installments_payments':
types.StructType([
                     types.StructField('sk_id_prev', types.LongType(), False),
                     types.StructField('sk_id_curr', types.LongType(), False),
                     types.StructField('num_instalment_version', types.DoubleType(), True),
                     types.StructField('num_instalment_number', types.DoubleType(), True),
                     types.StructField('days_instalment', types.DoubleType(), True),
                     types.StructField('days_entry_payment', types.DoubleType(), True),
                     types.StructField('amt_instalment', types.DoubleType(), True),
                     types.StructField('amt_payment', types.DoubleType(), True),
                ]),
'previous_application':
types.StructType([
                     types.StructField('sk_id_prev', types.LongType(), False),
                     types.StructField('sk_id_curr', types.LongType(), False),
                     types.StructField('name_contract_type', types.StringType(), True),
                     types.StructField('amt_annuity', types.DoubleType(), True),
                     types.StructField('amt_application', types.DoubleType(), True),
                     types.StructField('amt_credit', types.DoubleType(), True),
                     types.StructField('amt_down_payment', types.DoubleType(), True),
                     types.StructField('amt_goods_price', types.DoubleType(), True),
                     types.StructField('weekday_appr_process_start', types.StringType(), True),
                     types.StructField('hour_appr_process_start', types.DoubleType(), True),
                     types.StructField('flag_last_appl_per_contract', types.StringType(), True),
                     types.StructField('nflag_last_appl_in_day', types.DoubleType(), True),
                     types.StructField('rate_down_payment', types.DoubleType(), True),
                     types.StructField('rate_interest_primary', types.DoubleType(), True),
                     types.StructField('rate_interest_privileged', types.DoubleType(), True),
                     types.StructField('name_cash_loan_purpose', types.StringType(), True),
                     types.StructField('name_contract_status', types.StringType(), True),
                     types.StructField('days_decision', types.DoubleType(), True),
                     types.StructField('name_payment_type', types.StringType(), True),
                     types.StructField('code_reject_reason', types.StringType(), True),
                     types.StructField('name_type_suite', types.StringType(), True),
                     types.StructField('name_client_type', types.StringType(), True),
                     types.StructField('name_goods_category', types.StringType(), True),
                     types.StructField('name_portfolio', types.StringType(), True),
                     types.StructField('name_product_type', types.StringType(), True),
                     types.StructField('channel_type', types.StringType(), True),
                     types.StructField('sellerplace_area', types.DoubleType(), True),
                     types.StructField('name_seller_industry', types.StringType(), True),
                     types.StructField('cnt_payment', types.DoubleType(), True),
                     types.StructField('name_yield_group', types.StringType(), True),
                     types.StructField('product_combination', types.StringType(), True),
                     types.StructField('days_first_drawing', types.DoubleType(), True),
                     types.StructField('days_first_due', types.DoubleType(), True),
                     types.StructField('days_last_due_1st_version', types.DoubleType(), True),
                     types.StructField('days_last_due', types.DoubleType(), True),
                     types.StructField('days_termination', types.DoubleType(), True),
                     types.StructField('nflag_insured_on_approval', types.DoubleType(), True),
                ]),
'pos_cash_balance':
types.StructType([
                     types.StructField('sk_id_prev', types.LongType(), False),
                     types.StructField('sk_id_curr', types.LongType(), False),
                     types.StructField('months_balance', types.DoubleType(), True),
                     types.StructField('cnt_instalment', types.DoubleType(), True),
                     types.StructField('cnt_instalment_future', types.DoubleType(), True),
                     types.StructField('name_contract_status', types.StringType(), True),
                     types.StructField('sk_dpd', types.DoubleType(), True),
                     types.StructField('sk_dpd_def', types.DoubleType(), True),
                ]),
'bureau':
types.StructType([
                     types.StructField('sk_id_curr', types.LongType(), False),
                     types.StructField('sk_id_bureau', types.LongType(), False),
                     types.StructField('credit_active', types.StringType(), True),
                     types.StructField('credit_currency', types.StringType(), True),
                     types.StructField('days_credit', types.DoubleType(), True),
                     types.StructField('credit_day_overdue', types.DoubleType(), True),
                     types.StructField('days_credit_enddate', types.DoubleType(), True),
                     types.StructField('days_enddate_fact', types.DoubleType(), True),
                     types.StructField('amt_credit_max_overdue', types.DoubleType(), True),
                     types.StructField('cnt_credit_prolong', types.DoubleType(), True),
                     types.StructField('amt_credit_sum', types.DoubleType(), True),
                     types.StructField('amt_credit_sum_debt', types.DoubleType(), True),
                     types.StructField('amt_credit_sum_limit', types.DoubleType(), True),
                     types.StructField('amt_credit_sum_overdue', types.DoubleType(), True),
                     types.StructField('credit_type', types.StringType(), True),
                     types.StructField('days_credit_update', types.DoubleType(), True),
                     types.StructField('amt_annuity', types.DoubleType(), True),
                ]),
'application_test':
types.StructType([
                     types.StructField('sk_id_curr', types.LongType(), False),
                     types.StructField('name_contract_type', types.StringType(), True),
                     types.StructField('code_gender', types.StringType(), True),
                     types.StructField('flag_own_car', types.StringType(), True),
                     types.StructField('flag_own_realty', types.StringType(), True),
                     types.StructField('cnt_children', types.DoubleType(), True),
                     types.StructField('amt_income_total', types.DoubleType(), True),
                     types.StructField('amt_credit', types.DoubleType(), True),
                     types.StructField('amt_annuity', types.DoubleType(), True),
                     types.StructField('amt_goods_price', types.DoubleType(), True),
                     types.StructField('name_type_suite', types.StringType(), True),
                     types.StructField('name_income_type', types.StringType(), True),
                     types.StructField('name_education_type', types.StringType(), True),
                     types.StructField('name_family_status', types.StringType(), True),
                     types.StructField('name_housing_type', types.StringType(), True),
                     types.StructField('region_population_relative', types.DoubleType(), True),
                     types.StructField('days_birth', types.DoubleType(), True),
                     types.StructField('days_employed', types.DoubleType(), True),
                     types.StructField('days_registration', types.DoubleType(), True),
                     types.StructField('days_id_publish', types.DoubleType(), True),
                     types.StructField('own_car_age', types.DoubleType(), True),
                     types.StructField('flag_mobil', types.DoubleType(), True),
                     types.StructField('flag_emp_phone', types.DoubleType(), True),
                     types.StructField('flag_work_phone', types.DoubleType(), True),
                     types.StructField('flag_cont_mobile', types.DoubleType(), True),
                     types.StructField('flag_phone', types.DoubleType(), True),
                     types.StructField('flag_email', types.DoubleType(), True),
                     types.StructField('occupation_type', types.StringType(), True),
                     types.StructField('cnt_fam_members', types.DoubleType(), True),
                     types.StructField('region_rating_client', types.DoubleType(), True),
                     types.StructField('region_rating_client_w_city', types.DoubleType(), True),
                     types.StructField('weekday_appr_process_start', types.StringType(), True),
                     types.StructField('hour_appr_process_start', types.DoubleType(), True),
                     types.StructField('reg_region_not_live_region', types.DoubleType(), True),
                     types.StructField('reg_region_not_work_region', types.DoubleType(), True),
                     types.StructField('live_region_not_work_region', types.DoubleType(), True),
                     types.StructField('reg_city_not_live_city', types.DoubleType(), True),
                     types.StructField('reg_city_not_work_city', types.DoubleType(), True),
                     types.StructField('live_city_not_work_city', types.DoubleType(), True),
                     types.StructField('organization_type', types.StringType(), True),
                     types.StructField('ext_source_1', types.DoubleType(), True),
                     types.StructField('ext_source_2', types.DoubleType(), True),
                     types.StructField('ext_source_3', types.DoubleType(), True),
                     types.StructField('apartments_avg', types.DoubleType(), True),
                     types.StructField('basementarea_avg', types.DoubleType(), True),
                     types.StructField('years_beginexpluatation_avg', types.DoubleType(), True),
                     types.StructField('years_build_avg', types.DoubleType(), True),
                     types.StructField('commonarea_avg', types.DoubleType(), True),
                     types.StructField('elevators_avg', types.DoubleType(), True),
                     types.StructField('entrances_avg', types.DoubleType(), True),
                     types.StructField('floorsmax_avg', types.DoubleType(), True),
                     types.StructField('floorsmin_avg', types.DoubleType(), True),
                     types.StructField('landarea_avg', types.DoubleType(), True),
                     types.StructField('livingapartments_avg', types.DoubleType(), True),
                     types.StructField('livingarea_avg', types.DoubleType(), True),
                     types.StructField('nonlivingapartments_avg', types.DoubleType(), True),
                     types.StructField('nonlivingarea_avg', types.DoubleType(), True),
                     types.StructField('apartments_mode', types.DoubleType(), True),
                     types.StructField('basementarea_mode', types.DoubleType(), True),
                     types.StructField('years_beginexpluatation_mode', types.DoubleType(), True),
                     types.StructField('years_build_mode', types.DoubleType(), True),
                     types.StructField('commonarea_mode', types.DoubleType(), True),
                     types.StructField('elevators_mode', types.DoubleType(), True),
                     types.StructField('entrances_mode', types.DoubleType(), True),
                     types.StructField('floorsmax_mode', types.DoubleType(), True),
                     types.StructField('floorsmin_mode', types.DoubleType(), True),
                     types.StructField('landarea_mode', types.DoubleType(), True),
                     types.StructField('livingapartments_mode', types.DoubleType(), True),
                     types.StructField('livingarea_mode', types.DoubleType(), True),
                     types.StructField('nonlivingapartments_mode', types.DoubleType(), True),
                     types.StructField('nonlivingarea_mode', types.DoubleType(), True),
                     types.StructField('apartments_medi', types.DoubleType(), True),
                     types.StructField('basementarea_medi', types.DoubleType(), True),
                     types.StructField('years_beginexpluatation_medi', types.DoubleType(), True),
                     types.StructField('years_build_medi', types.DoubleType(), True),
                     types.StructField('commonarea_medi', types.DoubleType(), True),
                     types.StructField('elevators_medi', types.DoubleType(), True),
                     types.StructField('entrances_medi', types.DoubleType(), True),
                     types.StructField('floorsmax_medi', types.DoubleType(), True),
                     types.StructField('floorsmin_medi', types.DoubleType(), True),
                     types.StructField('landarea_medi', types.DoubleType(), True),
                     types.StructField('livingapartments_medi', types.DoubleType(), True),
                     types.StructField('livingarea_medi', types.DoubleType(), True),
                     types.StructField('nonlivingapartments_medi', types.DoubleType(), True),
                     types.StructField('nonlivingarea_medi', types.DoubleType(), True),
                     types.StructField('fondkapremont_mode', types.StringType(), True),
                     types.StructField('housetype_mode', types.StringType(), True),
                     types.StructField('totalarea_mode', types.DoubleType(), True),
                     types.StructField('wallsmaterial_mode', types.StringType(), True),
                     types.StructField('emergencystate_mode', types.StringType(), True),
                     types.StructField('obs_30_cnt_social_circle', types.DoubleType(), True),
                     types.StructField('def_30_cnt_social_circle', types.DoubleType(), True),
                     types.StructField('obs_60_cnt_social_circle', types.DoubleType(), True),
                     types.StructField('def_60_cnt_social_circle', types.DoubleType(), True),
                     types.StructField('days_last_phone_change', types.DoubleType(), True),
                     types.StructField('flag_document_2', types.DoubleType(), True),
                     types.StructField('flag_document_3', types.DoubleType(), True),
                     types.StructField('flag_document_4', types.DoubleType(), True),
                     types.StructField('flag_document_5', types.DoubleType(), True),
                     types.StructField('flag_document_6', types.DoubleType(), True),
                     types.StructField('flag_document_7', types.DoubleType(), True),
                     types.StructField('flag_document_8', types.DoubleType(), True),
                     types.StructField('flag_document_9', types.DoubleType(), True),
                     types.StructField('flag_document_10', types.DoubleType(), True),
                     types.StructField('flag_document_11', types.DoubleType(), True),
                     types.StructField('flag_document_12', types.DoubleType(), True),
                     types.StructField('flag_document_13', types.DoubleType(), True),
                     types.StructField('flag_document_14', types.DoubleType(), True),
                     types.StructField('flag_document_15', types.DoubleType(), True),
                     types.StructField('flag_document_16', types.DoubleType(), True),
                     types.StructField('flag_document_17', types.DoubleType(), True),
                     types.StructField('flag_document_18', types.DoubleType(), True),
                     types.StructField('flag_document_19', types.DoubleType(), True),
                     types.StructField('flag_document_20', types.DoubleType(), True),
                     types.StructField('flag_document_21', types.DoubleType(), True),
                     types.StructField('amt_req_credit_bureau_hour', types.DoubleType(), True),
                     types.StructField('amt_req_credit_bureau_day', types.DoubleType(), True),
                     types.StructField('amt_req_credit_bureau_week', types.DoubleType(), True),
                     types.StructField('amt_req_credit_bureau_mon', types.DoubleType(), True),
                     types.StructField('amt_req_credit_bureau_qrt', types.DoubleType(), True),
                     types.StructField('amt_req_credit_bureau_year', types.DoubleType(), True),
                ]),
'application_train':
types.StructType([
                     types.StructField('sk_id_curr', types.LongType(), False),
                     types.StructField('target', types.DoubleType(), True),
                     types.StructField('name_contract_type', types.StringType(), True),
                     types.StructField('code_gender', types.StringType(), True),
                     types.StructField('flag_own_car', types.StringType(), True),
                     types.StructField('flag_own_realty', types.StringType(), True),
                     types.StructField('cnt_children', types.DoubleType(), True),
                     types.StructField('amt_income_total', types.DoubleType(), True),
                     types.StructField('amt_credit', types.DoubleType(), True),
                     types.StructField('amt_annuity', types.DoubleType(), True),
                     types.StructField('amt_goods_price', types.DoubleType(), True),
                     types.StructField('name_type_suite', types.StringType(), True),
                     types.StructField('name_income_type', types.StringType(), True),
                     types.StructField('name_education_type', types.StringType(), True),
                     types.StructField('name_family_status', types.StringType(), True),
                     types.StructField('name_housing_type', types.StringType(), True),
                     types.StructField('region_population_relative', types.DoubleType(), True),
                     types.StructField('days_birth', types.DoubleType(), True),
                     types.StructField('days_employed', types.DoubleType(), True),
                     types.StructField('days_registration', types.DoubleType(), True),
                     types.StructField('days_id_publish', types.DoubleType(), True),
                     types.StructField('own_car_age', types.DoubleType(), True),
                     types.StructField('flag_mobil', types.DoubleType(), True),
                     types.StructField('flag_emp_phone', types.DoubleType(), True),
                     types.StructField('flag_work_phone', types.DoubleType(), True),
                     types.StructField('flag_cont_mobile', types.DoubleType(), True),
                     types.StructField('flag_phone', types.DoubleType(), True),
                     types.StructField('flag_email', types.DoubleType(), True),
                     types.StructField('occupation_type', types.StringType(), True),
                     types.StructField('cnt_fam_members', types.DoubleType(), True),
                     types.StructField('region_rating_client', types.DoubleType(), True),
                     types.StructField('region_rating_client_w_city', types.DoubleType(), True),
                     types.StructField('weekday_appr_process_start', types.StringType(), True),
                     types.StructField('hour_appr_process_start', types.DoubleType(), True),
                     types.StructField('reg_region_not_live_region', types.DoubleType(), True),
                     types.StructField('reg_region_not_work_region', types.DoubleType(), True),
                     types.StructField('live_region_not_work_region', types.DoubleType(), True),
                     types.StructField('reg_city_not_live_city', types.DoubleType(), True),
                     types.StructField('reg_city_not_work_city', types.DoubleType(), True),
                     types.StructField('live_city_not_work_city', types.DoubleType(), True),
                     types.StructField('organization_type', types.StringType(), True),
                     types.StructField('ext_source_1', types.DoubleType(), True),
                     types.StructField('ext_source_2', types.DoubleType(), True),
                     types.StructField('ext_source_3', types.DoubleType(), True),
                     types.StructField('apartments_avg', types.DoubleType(), True),
                     types.StructField('basementarea_avg', types.DoubleType(), True),
                     types.StructField('years_beginexpluatation_avg', types.DoubleType(), True),
                     types.StructField('years_build_avg', types.DoubleType(), True),
                     types.StructField('commonarea_avg', types.DoubleType(), True),
                     types.StructField('elevators_avg', types.DoubleType(), True),
                     types.StructField('entrances_avg', types.DoubleType(), True),
                     types.StructField('floorsmax_avg', types.DoubleType(), True),
                     types.StructField('floorsmin_avg', types.DoubleType(), True),
                     types.StructField('landarea_avg', types.DoubleType(), True),
                     types.StructField('livingapartments_avg', types.DoubleType(), True),
                     types.StructField('livingarea_avg', types.DoubleType(), True),
                     types.StructField('nonlivingapartments_avg', types.DoubleType(), True),
                     types.StructField('nonlivingarea_avg', types.DoubleType(), True),
                     types.StructField('apartments_mode', types.DoubleType(), True),
                     types.StructField('basementarea_mode', types.DoubleType(), True),
                     types.StructField('years_beginexpluatation_mode', types.DoubleType(), True),
                     types.StructField('years_build_mode', types.DoubleType(), True),
                     types.StructField('commonarea_mode', types.DoubleType(), True),
                     types.StructField('elevators_mode', types.DoubleType(), True),
                     types.StructField('entrances_mode', types.DoubleType(), True),
                     types.StructField('floorsmax_mode', types.DoubleType(), True),
                     types.StructField('floorsmin_mode', types.DoubleType(), True),
                     types.StructField('landarea_mode', types.DoubleType(), True),
                     types.StructField('livingapartments_mode', types.DoubleType(), True),
                     types.StructField('livingarea_mode', types.DoubleType(), True),
                     types.StructField('nonlivingapartments_mode', types.DoubleType(), True),
                     types.StructField('nonlivingarea_mode', types.DoubleType(), True),
                     types.StructField('apartments_medi', types.DoubleType(), True),
                     types.StructField('basementarea_medi', types.DoubleType(), True),
                     types.StructField('years_beginexpluatation_medi', types.DoubleType(), True),
                     types.StructField('years_build_medi', types.DoubleType(), True),
                     types.StructField('commonarea_medi', types.DoubleType(), True),
                     types.StructField('elevators_medi', types.DoubleType(), True),
                     types.StructField('entrances_medi', types.DoubleType(), True),
                     types.StructField('floorsmax_medi', types.DoubleType(), True),
                     types.StructField('floorsmin_medi', types.DoubleType(), True),
                     types.StructField('landarea_medi', types.DoubleType(), True),
                     types.StructField('livingapartments_medi', types.DoubleType(), True),
                     types.StructField('livingarea_medi', types.DoubleType(), True),
                     types.StructField('nonlivingapartments_medi', types.DoubleType(), True),
                     types.StructField('nonlivingarea_medi', types.DoubleType(), True),
                     types.StructField('fondkapremont_mode', types.StringType(), True),
                     types.StructField('housetype_mode', types.StringType(), True),
                     types.StructField('totalarea_mode', types.DoubleType(), True),
                     types.StructField('wallsmaterial_mode', types.StringType(), True),
                     types.StructField('emergencystate_mode', types.StringType(), True),
                     types.StructField('obs_30_cnt_social_circle', types.DoubleType(), True),
                     types.StructField('def_30_cnt_social_circle', types.DoubleType(), True),
                     types.StructField('obs_60_cnt_social_circle', types.DoubleType(), True),
                     types.StructField('def_60_cnt_social_circle', types.DoubleType(), True),
                     types.StructField('days_last_phone_change', types.DoubleType(), True),
                     types.StructField('flag_document_2', types.DoubleType(), True),
                     types.StructField('flag_document_3', types.DoubleType(), True),
                     types.StructField('flag_document_4', types.DoubleType(), True),
                     types.StructField('flag_document_5', types.DoubleType(), True),
                     types.StructField('flag_document_6', types.DoubleType(), True),
                     types.StructField('flag_document_7', types.DoubleType(), True),
                     types.StructField('flag_document_8', types.DoubleType(), True),
                     types.StructField('flag_document_9', types.DoubleType(), True),
                     types.StructField('flag_document_10', types.DoubleType(), True),
                     types.StructField('flag_document_11', types.DoubleType(), True),
                     types.StructField('flag_document_12', types.DoubleType(), True),
                     types.StructField('flag_document_13', types.DoubleType(), True),
                     types.StructField('flag_document_14', types.DoubleType(), True),
                     types.StructField('flag_document_15', types.DoubleType(), True),
                     types.StructField('flag_document_16', types.DoubleType(), True),
                     types.StructField('flag_document_17', types.DoubleType(), True),
                     types.StructField('flag_document_18', types.DoubleType(), True),
                     types.StructField('flag_document_19', types.DoubleType(), True),
                     types.StructField('flag_document_20', types.DoubleType(), True),
                     types.StructField('flag_document_21', types.DoubleType(), True),
                     types.StructField('amt_req_credit_bureau_hour', types.DoubleType(), True),
                     types.StructField('amt_req_credit_bureau_day', types.DoubleType(), True),
                     types.StructField('amt_req_credit_bureau_week', types.DoubleType(), True),
                     types.StructField('amt_req_credit_bureau_mon', types.DoubleType(), True),
                     types.StructField('amt_req_credit_bureau_qrt', types.DoubleType(), True),
                     types.StructField('amt_req_credit_bureau_year', types.DoubleType(), True),
                ]),
'credit_card_balance':
types.StructType([
                     types.StructField('sk_id_prev', types.LongType(), False),
                     types.StructField('sk_id_curr', types.LongType(), False),
                     types.StructField('months_balance', types.DoubleType(), True),
                     types.StructField('amt_balance', types.DoubleType(), True),
                     types.StructField('amt_credit_limit_actual', types.DoubleType(), True),
                     types.StructField('amt_drawings_atm_current', types.DoubleType(), True),
                     types.StructField('amt_drawings_current', types.DoubleType(), True),
                     types.StructField('amt_drawings_other_current', types.DoubleType(), True),
                     types.StructField('amt_drawings_pos_current', types.DoubleType(), True),
                     types.StructField('amt_inst_min_regularity', types.DoubleType(), True),
                     types.StructField('amt_payment_current', types.DoubleType(), True),
                     types.StructField('amt_payment_total_current', types.DoubleType(), True),
                     types.StructField('amt_receivable_principal', types.DoubleType(), True),
                     types.StructField('amt_recivable', types.DoubleType(), True),
                     types.StructField('amt_total_receivable', types.DoubleType(), True),
                     types.StructField('cnt_drawings_atm_current', types.DoubleType(), True),
                     types.StructField('cnt_drawings_current', types.DoubleType(), True),
                     types.StructField('cnt_drawings_other_current', types.DoubleType(), True),
                     types.StructField('cnt_drawings_pos_current', types.DoubleType(), True),
                     types.StructField('cnt_instalment_mature_cum', types.DoubleType(), True),
                     types.StructField('name_contract_status', types.StringType(), True),
                     types.StructField('sk_dpd', types.DoubleType(), True),
                     types.StructField('sk_dpd_def', types.DoubleType(), True),
                ]),
'bureau_balance':
types.StructType([
                     types.StructField('sk_id_bureau', types.LongType(), False),
                     types.StructField('months_balance', types.DoubleType(), True),
                     types.StructField('status', types.StringType(), True),
                ]),
}
