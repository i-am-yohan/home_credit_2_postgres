import argparse
import psycopg2
import os
import csv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description = 'The Main NLP process'
    )

    parser.add_argument(
        'db_user',
        type=str,
        help='The name of the user'
    )

    parser.add_argument(
        'in_password',
        type=str,
        help='The password for the user'
    )

    args = parser.parse_args()

    conn = psycopg2.connect(
        "dbname='hm_crdt' user='{}' password='{}'".format(args.db_user, args.in_password)
    )

    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()


    cur.execute(
        """
        create schema raw_in;    
        
        
        create table if not exists raw_in.Application_Train (
            SK_ID_CURR INT Primary KEY UNIQUE
        ,   TARGET INT
        ,   NAME_CONTRACT_TYPE CHAR(15)
        ,   CODE_GENDER CHAR(3)
        ,   FLAG_OWN_CAR CHAR(1)
        ,   FLAG_OWN_REALTY CHAR(1)
        ,   CNT_CHILDREN INT
        ,   AMT_INCOME_TOTAL float
        ,   AMT_CREDIT float
        ,   AMT_ANNUITY float
        ,   AMT_GOODS_PRICE float
        ,   NAME_TYPE_SUITE CHAR(20)
        ,   NAME_INCOME_TYPE CHAR(20)
        ,   NAME_EDUCATION_TYPE CHAR(30)
        ,   NAME_FAMILY_STATUS CHAR(20)
        ,   NAME_HOUSING_TYPE CHAR(25)
        ,   REGION_POPULATION_RELATIVE float
        ,   DAYS_BIRTH float
        ,   DAYS_EMPLOYED float
        ,   DAYS_REGISTRATION float
        ,   DAYS_ID_PUBLISH float
        ,   OWN_CAR_AGE float
        ,   FLAG_MOBIL INT
        ,   FLAG_EMP_PHONE INT
        ,   FLAG_WORK_PHONE INT
        ,   FLAG_CONT_MOBILE INT
        ,   FLAG_PHONE INT
        ,   FLAG_EMAIL INT
        ,   OCCUPATION_TYPE CHAR(25)
        ,   CNT_FAM_MEMBERS float
        ,   REGION_RATING_CLIENT INT
        ,   REGION_RATING_CLIENT_W_CITY INT
        ,   WEEKDAY_APPR_PROCESS_START CHAR(15)
        ,   HOUR_APPR_PROCESS_START INT
        ,    REG_REGION_NOT_LIVE_REGION INT
        ,    REG_REGION_NOT_WORK_REGION INT
        ,    LIVE_REGION_NOT_WORK_REGION INT
        ,    REG_CITY_NOT_LIVE_CITY INT
        ,    REG_CITY_NOT_WORK_CITY INT
        ,    LIVE_CITY_NOT_WORK_CITY INT
        ,    ORGANIZATION_TYPE CHAR(30)
        ,    EXT_SOURCE_1 float
        ,    EXT_SOURCE_2 float
        ,    EXT_SOURCE_3 float
        ,    APARTMENTS_AVG float
        ,    BASEMENTAREA_AVG float
        ,    YEARS_BEGINEXPLUATATION_AVG float
        ,    YEARS_BUILD_AVG float
        ,    COMMONAREA_AVG float
        ,    ELEVATORS_AVG float
        ,    ENTRANCES_AVG float
        ,    FLOORSMAX_AVG float
        ,    FLOORSMIN_AVG float
        ,    LANDAREA_AVG float
        ,    LIVINGAPARTMENTS_AVG float
        ,    LIVINGAREA_AVG float
        ,    NONLIVINGAPARTMENTS_AVG float
        ,    NONLIVINGAREA_AVG float
        ,    APARTMENTS_MODE float
        ,    BASEMENTAREA_MODE float
        ,    YEARS_BEGINEXPLUATATION_MODE float
        ,    YEARS_BUILD_MODE float
        ,    COMMONAREA_MODE float
        ,    ELEVATORS_MODE float
        ,    ENTRANCES_MODE float
        ,    FLOORSMAX_MODE float
        ,    FLOORSMIN_MODE float
        ,    LANDAREA_MODE float
        ,    LIVINGAPARTMENTS_MODE float
        ,    LIVINGAREA_MODE float
        ,    NONLIVINGAPARTMENTS_MODE float
        ,    NONLIVINGAREA_MODE float
        ,    APARTMENTS_MEDI float
        ,    BASEMENTAREA_MEDI float
        ,    YEARS_BEGINEXPLUATATION_MEDI float
        ,    YEARS_BUILD_MEDI float
        ,    COMMONAREA_MEDI float
        ,    ELEVATORS_MEDI float
        ,    ENTRANCES_MEDI float
        ,    FLOORSMAX_MEDI float
        ,    FLOORSMIN_MEDI float
        ,    LANDAREA_MEDI float
        ,    LIVINGAPARTMENTS_MEDI float
        ,    LIVINGAREA_MEDI float
        ,    NONLIVINGAPARTMENTS_MEDI float
        ,    NONLIVINGAREA_MEDI float
        ,    FONDKAPREMONT_MODE CHAR(30)
        ,    HOUSETYPE_MODE CHAR(20)
        ,    TOTALAREA_MODE float
        ,    WALLSMATERIAL_MODE CHAR(15)
        ,    EMERGENCYSTATE_MODE CHAR(3)
        ,    OBS_30_CNT_SOCIAL_CIRCLE float
        ,    DEF_30_CNT_SOCIAL_CIRCLE float
        ,    OBS_60_CNT_SOCIAL_CIRCLE float
        ,    DEF_60_CNT_SOCIAL_CIRCLE float
        ,    DAYS_LAST_PHONE_CHANGE float
        ,    FLAG_DOCUMENT_2 INT
        ,    FLAG_DOCUMENT_3 INT
        ,    FLAG_DOCUMENT_4 INT
        ,    FLAG_DOCUMENT_5 INT
        ,    FLAG_DOCUMENT_6 INT
        ,    FLAG_DOCUMENT_7 INT
        ,    FLAG_DOCUMENT_8 INT
        ,    FLAG_DOCUMENT_9 INT
        ,    FLAG_DOCUMENT_10 INT
        ,    FLAG_DOCUMENT_11 INT
        ,    FLAG_DOCUMENT_12 INT
        ,    FLAG_DOCUMENT_13 INT
        ,    FLAG_DOCUMENT_14 INT
        ,    FLAG_DOCUMENT_15 INT
        ,    FLAG_DOCUMENT_16 INT
        ,    FLAG_DOCUMENT_17 INT
        ,    FLAG_DOCUMENT_18 INT
        ,    FLAG_DOCUMENT_19 INT
        ,    FLAG_DOCUMENT_20 INT
        ,    FLAG_DOCUMENT_21 INT
        ,    AMT_REQ_CREDIT_BUREAU_HOUR float
        ,    AMT_REQ_CREDIT_BUREAU_DAY float
        ,    AMT_REQ_CREDIT_BUREAU_WEEK float
        ,    AMT_REQ_CREDIT_BUREAU_MON float
        ,    AMT_REQ_CREDIT_BUREAU_QRT float
        ,    AMT_REQ_CREDIT_BUREAU_YEAR float
            );
        
        
        create table raw_in.Application_Test (like raw_in.Application_Train);
        
        
        create table raw_in.Bureau (
            SK_ID_CURR BIGINT
        ,    SK_ID_BUREAU BIGINT Primary KEY
        ,    CREDIT_ACTIVE char(10)
        ,    CREDIT_CURRENCY char(10)
        ,    DAYS_CREDIT INT
        ,    CREDIT_DAY_OVERDUE	INT
        ,    DAYS_CREDIT_ENDDATE	float
        ,    DAYS_ENDDATE_FACT float
        ,    AMT_CREDIT_MAX_OVERDUE	float
        ,    CNT_CREDIT_PROLONG	INT
        ,    AMT_CREDIT_SUM	float
        ,    AMT_CREDIT_SUM_DEBT	float
        ,    AMT_CREDIT_SUM_LIMIT	float
        ,    AMT_CREDIT_SUM_OVERDUE	float
        ,    CREDIT_TYPE	char(50)
        ,    DAYS_CREDIT_UPDATE	INT
        ,    AMT_ANNUITY float
            );
    
    
        create table raw_in.Bureau_Balance (
            SK_ID_BUREAU INT
        ,    MONTHS_BALANCE INT
        ,    STATUS char(1)
        ,    PRIMARY KEY (SK_ID_BUREAU, MONTHS_BALANCE)
            );
    
    
        create table raw_in.credit_card_balance (
            SK_ID_PREV	BIGINT 
        ,    SK_ID_CURR	BIGINT 
        ,    MONTHS_BALANCE	INT
        ,    AMT_BALANCE	float
        ,    AMT_CREDIT_LIMIT_ACTUAL	float
        ,    AMT_DRAWINGS_ATM_CURRENT	float
        ,    AMT_DRAWINGS_CURRENT	float
        ,    AMT_DRAWINGS_OTHER_CURRENT	float
        ,    AMT_DRAWINGS_POS_CURRENT	float
        ,    AMT_INST_MIN_REGULARITY	float
        ,    AMT_PAYMENT_CURRENT	float
        ,    AMT_PAYMENT_TOTAL_CURRENT	float
        ,    AMT_RECEIVABLE_PRINCIPAL	float
        ,    AMT_RECIVABLE	float
        ,    AMT_TOTAL_RECEIVABLE	float
        ,    CNT_DRAWINGS_ATM_CURRENT	float
        ,    CNT_DRAWINGS_CURRENT	float
        ,    CNT_DRAWINGS_OTHER_CURRENT	float
        ,    CNT_DRAWINGS_POS_CURRENT	float
        ,    CNT_INSTALMENT_MATURE_CUM	float
        ,    NAME_CONTRACT_STATUS	char(15)
        ,    SK_DPD	INT
        ,    SK_DPD_DEF INT
        ,    PRIMARY KEY (SK_ID_PREV, SK_ID_CURR, MONTHS_BALANCE)
            );
        
        
        create table raw_in.installments_payments_temp (
            SK_ID_PREV	BIGINT
        ,    SK_ID_CURR	BIGINT
        ,    NUM_INSTALMENT_VERSION	float
        ,    NUM_INSTALMENT_NUMBER	float
        ,    DAYS_INSTALMENT	float
        ,    DAYS_ENTRY_PAYMENT	float
        ,    AMT_INSTALMENT	float
        ,    AMT_PAYMENT float
        );
        
       
        create table raw_in.POS_CASH_Balance (
            SK_ID_PREV	BIGINT
        ,    SK_ID_CURR	BIGINT
        ,    MONTHS_BALANCE	INT
        ,    CNT_INSTALMENT	float
        ,    CNT_INSTALMENT_FUTURE	float
        ,    NAME_CONTRACT_STATUS	Char(30)
        ,    SK_DPD	INT
        ,    SK_DPD_DEF INT
        ,    PRIMARY KEY (SK_ID_PREV, SK_ID_CURR, MONTHS_BALANCE)
        );
        
        
        create table raw_in.previous_application (
            SK_ID_PREV	BIGINT
        ,    SK_ID_CURR	BIGINT
        ,    NAME_CONTRACT_TYPE	CHAR(15)
        ,    AMT_ANNUITY	float
        ,    AMT_APPLICATION	float
        ,    AMT_CREDIT	float
        ,    AMT_DOWN_PAYMENT	float
        ,    AMT_GOODS_PRICE	float
        ,    WEEKDAY_APPR_PROCESS_START	CHAR(15)
        ,    HOUR_APPR_PROCESS_START	INT
        ,    FLAG_LAST_APPL_PER_CONTRACT	Char(1)
        ,    NFLAG_LAST_APPL_IN_DAY	INT
        ,    RATE_DOWN_PAYMENT	float
        ,    RATE_INTEREST_PRIMARY	float
        ,    RATE_INTEREST_PRIVILEGED	float
        ,    NAME_CASH_LOAN_PURPOSE	char(50)
        ,    NAME_CONTRACT_STATUS	char(12)
        ,    DAYS_DECISION	INT
        ,    NAME_PAYMENT_TYPE	char(50)
        ,    CODE_REJECT_REASON	char(6)
        ,    NAME_TYPE_SUITE	char(15)
        ,    NAME_CLIENT_TYPE	char(10)
        ,    NAME_GOODS_CATEGORY	char(40)
        ,    NAME_PORTFOLIO	char(5)
        ,    NAME_PRODUCT_TYPE	char(7)
        ,    CHANNEL_TYPE	char(30)
        ,    SELLERPLACE_AREA	INT
        ,    NAME_SELLER_INDUSTRY	char(30)
        ,    CNT_PAYMENT	float
        ,    NAME_YIELD_GROUP	char(10)
        ,    PRODUCT_COMBINATION	char(50)
        ,    DAYS_FIRST_DRAWING	float
        ,    DAYS_FIRST_DUE	float
        ,    DAYS_LAST_DUE_1ST_VERSION	float
        ,    DAYS_LAST_DUE	float
        ,    DAYS_TERMINATION	float
        ,    NFLAG_INSURED_ON_APPROVAL float
        ,    PRIMARY KEY (SK_ID_PREV)
        );
    
        """
    )



    # open file in read mode
    def CSV_2_SQL_IO(In_CSV, out_tbl):
        print('Loading {S1} into {S2}'.format(S1 = In_CSV, S2 = out_tbl))
        with open(In_CSV, 'r') as read_obj:
            csv_reader = csv.reader(read_obj)
            x = 1
            for row in csv_reader:
                if x == 1:
                    colnames = ','.join(row)
                    num_str = ','.join(['%s' for i in range(len(row))])
                    x = x + 1

                else:
                    cur.execute("""
                                insert into {tbl}({cols})
                                values ({s})
                                """.format(tbl = out_tbl, cols = colnames, s = num_str)
                                , [None if v is '' else v for v in row])

                    #x = x + 1
                #if x >= 1000:
                #    break

    CSV_2_SQL_IO(r'application_test.csv', 'hm_crdt.raw_in.Application_Test')
    CSV_2_SQL_IO(r'application_train.csv', 'hm_crdt.raw_in.Application_Train')
    CSV_2_SQL_IO(r'bureau.csv', 'hm_crdt.raw_in.bureau')
    CSV_2_SQL_IO(r'bureau_balance.csv', 'hm_crdt.raw_in.bureau_balance')
    CSV_2_SQL_IO(r'credit_card_balance.csv', 'hm_crdt.raw_in.credit_card_balance')
    CSV_2_SQL_IO(r'installments_payments.csv', 'hm_crdt.raw_in.installments_payments_temp')
    CSV_2_SQL_IO(r'POS_CASH_Balance.csv', 'hm_crdt.raw_in.POS_CASH_Balance')
    CSV_2_SQL_IO(r'previous_application.csv', 'hm_crdt.raw_in.previous_application')


    print('Creating installments_payments with unique Primary Key')
    cur.execute(
        """
        create table raw_in.installments_payments (like raw_in.installments_payments_temp);
        
        ALTER TABLE raw_in.installments_payments 
        ADD PRIMARY KEY (SK_ID_PREV, SK_ID_CURR, NUM_INSTALMENT_VERSION, NUM_INSTALMENT_NUMBER, DAYS_INSTALMENT, DAYS_ENTRY_PAYMENT);
        
        insert into raw_in.installments_payments (
            select SK_ID_PREV
                ,   SK_ID_CURR
                ,   NUM_INSTALMENT_VERSION
                ,   NUM_INSTALMENT_NUMBER
                ,   DAYS_INSTALMENT
                ,   DAYS_ENTRY_PAYMENT
                ,   AMT_INSTALMENT
                ,   sum(AMT_PAYMENT) as AMT_PAYMENT
            from raw_in.installments_payments_temp
            where AMT_PAYMENT is not null
            group by SK_ID_PREV
                ,   SK_ID_CURR
                ,   NUM_INSTALMENT_VERSION
                ,   NUM_INSTALMENT_NUMBER
                ,   DAYS_INSTALMENT
                ,   DAYS_ENTRY_PAYMENT
                ,   AMT_INSTALMENT
            );
        
        drop table raw_in.installments_payments_temp;
        """)


    print('Combining Train and Test Table')
    cur.execute("""
                create table raw_in.application_train_test (like raw_in.application_train);
                
                ALTER TABLE raw_in.application_train_test
                    ADD Train_Test char(5);
                    
                
                insert into raw_in.application_train_test (
                    select tr.* 
                        , 'Train' as Train_Test
                        from raw_in.application_train as tr
                    
                    union
                    
                    select te.* 
                        , 'Test' as Train_Test
                        from raw_in.application_test as te
                        );
                    
                drop table raw_in.application_train;
                drop table raw_in.application_test;
                
                ALTER TABLE raw_in.application_train_test
                add primary key (SK_ID_CURR)
                ;
                """)


    print('Deleting redundant data')
    cur.execute("""
                DELETE FROM raw_in.bureau_balance
                    WHERE NOT EXISTS (
                    SELECT *
                    FROM raw_in.bureau
                    WHERE raw_in.bureau.SK_ID_BUREAU = raw_in.bureau_balance.SK_ID_BUREAU
                    );
                
                DELETE FROM raw_in.POS_CASH_Balance
                    WHERE NOT EXISTS (
                    SELECT *
                    FROM raw_in.previous_application
                    WHERE raw_in.previous_application.SK_ID_PREV = raw_in.POS_CASH_Balance.SK_ID_PREV
                    );
                
                DELETE FROM raw_in.installments_payments
                    WHERE NOT EXISTS (
                    SELECT *
                    FROM raw_in.previous_application
                    WHERE raw_in.previous_application.SK_ID_PREV = raw_in.installments_payments.SK_ID_PREV
                    );
                    
                DELETE FROM raw_in.credit_card_balance
                    WHERE NOT EXISTS (
                    SELECT *
                    FROM raw_in.previous_application
                    WHERE raw_in.previous_application.SK_ID_PREV = raw_in.credit_card_balance.SK_ID_PREV
                    );
                """)


    print('Creating foreign keys')
    cur.execute("""
                ALTER TABLE raw_in.bureau
                    ADD CONSTRAINT bur2bse
                    FOREIGN KEY (SK_ID_CURR) 
                    REFERENCES raw_in.Application_Train_Test (SK_ID_CURR)
                    ;
    
                ALTER TABLE raw_in.bureau_balance
                    ADD CONSTRAINT bur2bal
                    FOREIGN KEY (SK_ID_BUREAU) 
                    REFERENCES raw_in.bureau (SK_ID_BUREAU)
                    ;
                    
                ALTER TABLE raw_in.previous_application
                    ADD CONSTRAINT prev2bse
                    FOREIGN KEY (SK_ID_CURR) 
                    REFERENCES raw_in.Application_Train_Test (SK_ID_CURR)
                    ;
                    
                ALTER TABLE raw_in.POS_CASH_Balance
                    ADD CONSTRAINT pos2bse
                    FOREIGN KEY (SK_ID_CURR) 
                    REFERENCES raw_in.Application_Train_Test (SK_ID_CURR)
                    ;
                    
                ALTER TABLE raw_in.POS_CASH_Balance
                    ADD CONSTRAINT pos2prev
                    FOREIGN KEY (SK_ID_PREV) 
                    REFERENCES raw_in.previous_application (SK_ID_PREV)
                    ;
                    
                ALTER TABLE raw_in.installments_payments
                    ADD CONSTRAINT ins2prev
                    FOREIGN KEY (SK_ID_PREV)
                    REFERENCES raw_in.previous_application (SK_ID_PREV)
                    ;
    
                ALTER TABLE raw_in.installments_payments
                    ADD CONSTRAINT ins2bse
                    FOREIGN KEY (SK_ID_CURR) 
                    REFERENCES raw_in.Application_Train_Test (SK_ID_CURR)
                    ;
                    
                ALTER TABLE raw_in.credit_card_balance
                    ADD CONSTRAINT cc2prev
                    FOREIGN KEY (SK_ID_PREV) 
                    REFERENCES raw_in.previous_application (SK_ID_PREV)
                    ;
                    
                ALTER TABLE raw_in.credit_card_balance
                    ADD CONSTRAINT cc2bse
                    FOREIGN KEY (SK_ID_CURR) 
                    REFERENCES raw_in.Application_Train_Test (SK_ID_CURR)
                    ;
                """)

    conn.close()