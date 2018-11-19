# IOtoBool = {tableName:set(clmn1, clmn2, ...)}
# YNtoBool = {tableName:set(clmn1, clmn2, ...)}

IOtoBool = {
"application_train":(
    "flag_mobil",
    "flag_emp_phone",
    "flag_work_phone",
    "flag_cont_mobile",
    "flag_phone",
    "flag_email",
    "reg_region_not_live_region",
    "reg_region_not_work_region",
    "live_region_not_work_region",
    "reg_city_not_live_city",
    "reg_city_not_work_city",
    "live_city_not_work_city",
    "flag_document_2",
    "flag_document_3",
    "flag_document_4",
    "flag_document_5",
    "flag_document_6",
    "flag_document_7",
    "flag_document_8",
    "flag_document_9",
    "flag_document_10",
    "flag_document_11",
    "flag_document_12",
    "flag_document_13",
    "flag_document_14",
    "flag_document_15",
    "flag_document_16",
    "flag_document_17",
    "flag_document_18",
    "flag_document_19",
    "flag_document_20",
    "flag_document_21",
),

"application_test":(
    "flag_mobil",
    "flag_emp_phone",
    "flag_work_phone",
    "flag_cont_mobile",
    "flag_phone",
    "flag_email",
    "reg_region_not_live_region",
    "reg_region_not_work_region",
    "live_region_not_work_region",
    "reg_city_not_live_city",
    "reg_city_not_work_city",
    "live_city_not_work_city",
    "flag_document_2",
    "flag_document_3",
    "flag_document_4",
    "flag_document_5",
    "flag_document_6",
    "flag_document_7",
    "flag_document_8",
    "flag_document_9",
    "flag_document_10",
    "flag_document_11",
    "flag_document_12",
    "flag_document_13",
    "flag_document_14",
    "flag_document_15",
    "flag_document_16",
    "flag_document_17",
    "flag_document_18",
    "flag_document_19",
    "flag_document_20",
    "flag_document_21",
),

"previous_application":(
"nflag_last_appl_in_day",
"nflag_insured_on_approval",
),
}


YNtoBool = {
"application_train":(
"flag_own_car",
"flag_own_realty",
),
"application_test":(
"flag_own_car",
"flag_own_realty",
),
"previous_application":(
"flag_last_appl_per_contract",
),
}

if __name__ == "__main__":
    print(IOtoBool)
    print(YNtoBool)
