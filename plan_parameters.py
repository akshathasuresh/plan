
#IBM_DB2_Dll_Directoryr_Path= r'/TPIFX_MIGRATION\Myenv\Lib\site-packages\clidriver\bin'

FILE_TY_CD = 'P'
LAST_ACTY_OPER = 'DSPLAN'

#PLAN_WORKFILES_PATH=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\INPUT_FILES\WORKFILES\TP-PLAN-4120231117'

#Inserting row into TPODS.IF_TRANS_CYC
TPODS_IF_TRANS_CYC_LOAD="""INSERT INTO TPODS.IF_TRANS_CYC (RCDKPER_CD, CYC_DT, FILE_TY_CD, RCD_CNT, LAST_ACTY_OPER_ID, LAST_ACTY_DTM) VALUES (?,?,?,?,?,?)"""

#extracting the src_records row file  for next step which is using in next process
#


#DS_41_PLAN_DS1=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\DataSet\DS_41_PLAN_DS1.txt'


HEADER_DS= [ 'RCDKPER_CD','NEW_IND',
    'NAME',
    'COMPANY_NAME' ,
    'COMPANY_ADDR_1',
    'COMPANY_ADDR_2',
    'COMPANY_CITY ',
    'COMPANY_STATE',
    'COMPANY_ZIP ',
    'TYPE ',
    'CONTRACT_STATE',
    'YEAR_END',
    'FISCAL_YEAR_END',
    'IRS_ID',
    'IFTP_PLAN_NUM' ,
    'TP_NAME',
    'TP_PHONE',
    'TP_WEB_URL',
    'TP_WEB_NAME',
    'TP_CALL_CENTER',
    'TP_CALL_CENTER_HRS',
    'TP_PHONE_IMPAIRED',
    'TP_PHONE_NON_US','ZIP_CD_SFX']

HEADER= [ 'NEW_IND','NAME','COMPANY_NAME' ,'COMPANY_ADDR_1','COMPANY_ADDR_2', 'COMPANY_CITY ','COMPANY_STATE', 'COMPANY_ZIP ',
          'TYPE ','CONTRACT_STATE','YEAR_END', 'FISCAL_YEAR_END','IRS_ID','NUMBER' , 'TP_NAME','TP_PHONE','TP_WEB_URL',
        'TP_WEB_NAME','TP_CALL_CENTER','TP_CALL_CENTER_HRS','TP_PHONE_IMPAIRED','TP_PHONE_NON_US','ZIP_CD_SFX']

COL_DTYPE={'NEW_IND':object,
    'NAME':object,
    'COMPANY_NAME':object ,
    'COMPANY_ADDR_1':object,
    'COMPANY_ADDR_2':object,
    'COMPANY_CITY ':object,
    'COMPANY_STATE':object,
    'COMPANY_ZIP ':object,
    'TYPE ':object,
    'CONTRACT_STATE':object,
    'YEAR_END':object,
    'FISCAL_YEAR_END':object,
    'IRS_ID':object,
    'NUMBER':object ,
    'TP_NAME':object,
    'TP_PHONE':object,
    'TP_WEB_URL':object,
    'TP_WEB_NAME':object,
    'TP_CALL_CENTER':object,
    'TP_CALL_CENTER_HRS':object,
    'TP_PHONE_IMPAIRED':object,
    'TP_PHONE_NON_US':object,
    'FILLER2':object}


COL_SPECIFICATION=[(3,4),(4,64),(64,96),(96,136),(136,176),(176,196),(200,202),(203,212),(212,213),(213,215),(215,223),(223,227)
                   ,(227,236),(236,251),(251,281),(281,291),(291,366),(366,466),(466,566),(566,666),(666,676),(676,686),(686,797)]
#DATA_SET_DS_41_HeaderDtPLAN=R'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\DataSet\DS_41_HeaderDtPLAN'

STGTPODS_T_TP41_PLAN_DATA_ODS='''INSERT INTO STGTPODS.T_TP41_PLAN_DATA_ODS(NEW_IND,NAME,COMPANY_NAME ,COMPANY_ADDR_1,
                              COMPANY_ADDR_2,COMPANY_CITY ,COMPANY_STATE,COMPANY_ZIP ,TYPE ,CONTRACT_STATE,YEAR_END,FISCAL_YEAR_END,
                              IRS_ID,NUMBER  ,TP_NAME,TP_PHONE,TP_WEB_URL,TP_WEB_NAME,TP_CALL_CENTER,TP_CALL_CENTER_HRS,TP_PHONE_IMPAIRED,TP_PHONE_NON_US,
                               FILLER2 ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''






'PLAN SEQUENCE 2 PARAM'

# #Files generated for the process
# NEW_IND_N=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\OUTPUT_FILES\NEW_IND_N.txt'
# NEW_IND_Y=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\OUTPUT_FILES\NEW_IND_Y.txt'
# NEW_IND_NOT_EQL_Y_N=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\OUTPUT_FILES\NEW_IND_N_Y.txt'
#
# UNIVL_PL_IND_Y_FILE=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\OUTPUT_FILES\UNIVL_PL_IND_Y_FILE.txt'
# CDC_IF_PL_SRC=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\OUTPUT_FILES\cdc_if_pl_src.txt'
# CDC_UNIVL_PL_SRC=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\OUTPUT_FILES\df_cdc_univl_pl_src1.txt'
# universal_pl_tgt_f=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\OUTPUT_FILES\universal_pl_tgt.txt'
#
# LKPLAN_NM_DATASET=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\DataSet\LKPLAN_NM_DATASET.txt'
# DS_UNIVL_RPT_LOG41_3=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\DataSet\DS_UNIVL_RPT_LOG41_3.txt'
# DS_UNIVL_RPT_LOG41_4=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\DataSet\DS_UNIVL_RPT_LOG41_4.txt'
# DS_PLAN_RPT_LOG41_7=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\DataSet\DS_PLAN_RPT_LOG41_7.txt'
# DS_PLAN_RPT_LOG41_4=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\DataSet\DS_PLAN_RPT_LOG41_4.txt'
# DS_PLAN_RPT_LOG41_3=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\DataSet\DS_PLAN_RPT_LOG41_3.txt'
# DS_PLAN_RPT_LOG41_1=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\DataSet\DS_PLAN_RPT_LOG41_1.txt'
# TO_FUNPLAN_ERR_6_2=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\OUTPUT_FILES\DS_FUNPLAN_ERR_6_2.txt'
#
# #files generate
# if_pl_src_tgt_file=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\OUTPUT_FILES\if_pl_src_tgt.txt'
# LKPLAN_NM_DATASET_IF_PL=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\DataSet\LKPLAN_NM_DATASET_IF_PL.txt'
# DS_IF_PL_RPT_LOG41_3=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\DataSet\DS_PLAN_RPT_LOG41_3.txt'
# DS_IF_PL_RPT_LOG41_4=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\DataSet\DS_PLAN_RPT_LOG41_4.txt'
# NewPlaninsert_41_ds=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\DataSet\NewPlaninsert_41_ds.txt'
# DS_PLAN_RPT_LOG41_2=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\DataSet\DS_PLAN_RPT_LOG41_2.txt'
# NewPlaninsert_41_data=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\DataSet\NewPlaninsert_41_ds.txt'

#Parameters related to if-pl_src
if_pl_src_header_col=['RCDKPER_CD','IFTP_CO_NM','PLAN_TY_CD','ST_JURIS_CD','PLAN_YR_END_DT','TAX_IRS_NUM','IFTP_PLAN_NUM',
                     'IFTP_RCDKPER_NM','IFTP_RCDKPER_800_NUM','IFTP_WEB_URL_ADDR','IFTP_WEBSITE_NM','IFTP_CALL_CNTR_NM',
                      'IFTP_CALL_CNTR_HR_TXT','IFTP_HRING_IMPRD_TEL_NUM','IFTP_OUT_USA_TEL_NUM']

#we can use this below one for furthur use
HEADER_UPDATED_ROWS_IF_PL_SRC=['RCDKPER_CD', 'IFTP_CO_NM', 'PLAN_TY_CD', 'ST_JURIS_CD', 'PLAN_YR_END_DT',
                               'TAX_IRS_NUM', 'IFTP_PLAN_NUM','IFTP_RCDKPER_NM', 'IFTP_RCDKPER_800_NUM', 'IFTP_WEB_URL_ADDR', 'IFTP_WEBSITE_NM', 'IFTP_CALL_CNTR_NM',
                                'IFTP_CALL_CNTR_HR_TXT', 'IFTP_HRING_IMPRD_TEL_NUM', 'IFTP_OUT_USA_TEL_NUM','CHANGE_CODE']




col_dtype_if_pl_src_col={'RCDKPER_CD':object,'IFTP_CO_NM':object,'PLAN_TY_CD':object,'ST_JURIS_CD':object,'PLAN_YR_END_DT':object,'TAX_IRS_NUM':object,'IFTP_PLAN_NUM':object,
'IFTP_RCDKPER_NM':object,'IFTP_RCDKPER_800_NUM':object,'IFTP_WEB_URL_ADDR':object,'IFTP_WEBSITE_NM':object,'IFTP_CALL_CNTR_NM':object,'IFTP_CALL_CNTR_HR_TXT':object,
'IFTP_HRING_IMPRD_TEL_NUM':object,'IFTP_OUT_USA_TEL_NUM':object}

TOCDC_IF_PL_SRC={'COMPANY_NAME':'IFTP_CO_NM','TYPE': 'PLAN_TY_CD', 'CONTRACT_STATE': 'ST_JURIS_CD', \
                       'YEAR_END': 'PLAN_YR_END_DT','IRS_ID':'TAX_IRS_NUM','TP_NAME': 'IFTP_RCDKPER_NM',\
                       'TP_PHONE': 'IFTP_RCDKPER_800_NUM', 'TP_WEB_URL': 'IFTP_WEB_URL_ADDR',\
                       'TP_WEB_NAME': 'IFTP_WEBSITE_NM', 'TP_CALL_CENTER': 'IFTP_CALL_CNTR_NM',\
                       'TP_CALL_CENTER_HRS': 'IFTP_CALL_CNTR_HR_TXT',\
                       'TP_PHONE_IMPAIRED': 'IFTP_HRING_IMPRD_TEL_NUM', 'TP_PHONE_NON_US': 'IFTP_OUT_USA_TEL_NUM'}

SELECT_STATEMENT_IF_PL_SRC='''SELECT RCDKPER_CD, IFTP_CO_NM, PLAN_TY_CD, ST_JURIS_CD, PLAN_YR_END_DT, TAX_IRS_NUM, IFTP_PLAN_NUM, IFTP_RCDKPER_NM, IFTP_RCDKPER_800_NUM, IFTP_WEB_URL_ADDR, IFTP_WEBSITE_NM, IFTP_CALL_CNTR_NM, IFTP_CALL_CNTR_HR_TXT, IFTP_HRING_IMPRD_TEL_NUM, IFTP_OUT_USA_TEL_NUM FROM TPODS.IF_PLAN
                  where RCDKPER_CD='41' ORDER BY RCDKPER_CD, IFTP_PLAN_NUM'''

tgt_if_pl_src_update=''' UPDATE TPODS.IF_PLAN SET IFTP_CO_NM = ?, PLAN_TY_CD = ?, 
	 ST_JURIS_CD = ?, PLAN_YR_END_DT = ?, 
	 TAX_IRS_NUM = ?, IFTP_RCDKPER_NM = ?, 
	 IFTP_RCDKPER_800_NUM = ?, IFTP_WEB_URL_ADDR = ?, 
	 IFTP_WEBSITE_NM = ?, IFTP_CALL_CNTR_NM = ?, 
	 IFTP_CALL_CNTR_HR_TXT = ?, IFTP_HRING_IMPRD_TEL_NUM = ?, 
	 IFTP_OUT_USA_TEL_NUM = ? , 
	 LAST_ACTY_DTM= ? WHERE (RCDKPER_CD = ? AND IFTP_PLAN_NUM = ?)'''





#Parameters related to UNIVL_PL_SRC
#COMPANY_STATE[1,2]
#COMPANY_ZIP[1,5]
#COMPANY_ZIP[6,4]
SELECT_STATEMENT_UNIVL_PL_SRC='''SELECT UNIVL_PLAN_NM, FRST_ST_ADDR_LN, SEC_ST_ADDR_LN, CITY_NM, STE_CD, ZIP_CD, RCDKPER_CD, IFTP_PLAN_NUM, ZIP_CD_SFX FROM TPODS.UNIVL_PLAN where RCDKPER_CD='41' ORDER BY RCDKPER_CD, IFTP_PLAN_NUM'''

cdc_univl_pl_src={'NAME':'UNIVL_PLAN_NM','COMPANY_ADDR_1':'FRST_ST_ADDR_LN','COMPANY_ADDR_2':'SEC_ST_ADDR_LN',\
                  'COMPANY_CITY':'CITY_NM','COMPANY_STATE':'STE_CD','COMPANY_ZIP':'ZIP_CD','RCDKPER_CD':'RCDKPER_CD','IFTP_PLAN_NUM':'IFTP_PLAN_NUM'}

cdc_univl_pl_src1={'COMPANY_ZIP':'ZIP_CD_SFX'}

final_col=[['UNIVL_PLAN_NM','FRST_ST_ADDR_LN','SEC_ST_ADDR_LN','CITY_NM','STE_CD','ZIP_CD','RCDKPER_CD','IFTP_PLAN_NUM','ZIP_CD_SFX']]

col_datatype={'UNIVL_PLAN_NM':object,'FRST_ST_ADDR_LN':object,'SEC_ST_ADDR_LN':object,'CITY_NM':object,'STE_CD':object,'ZIP_CD':object,'RCDKPER_CD':object,'IFTP_PLAN_NUM':object,'ZIP_CD_SFX':object}

header_univl_pl_src=['UNIVL_PLAN_NM','FRST_ST_ADDR_LN','SEC_ST_ADDR_LN','CITY_NM','STE_CD','ZIP_CD','RCDKPER_CD','IFTP_PLAN_NUM','ZIP_CD_SFX']

col_list=('UNIVL_PLAN_NM','FRST_ST_ADDR_LN','SEC_ST_ADDR_LN','CITY_NM','STE_CD','ZIP_CD','ZIP_CD_SFX')

HEADER_UPDATED_ROWS=['UNIVL_PLAN_NM','FRST_ST_ADDR_LN','SEC_ST_ADDR_LN','CITY_NM','STE_CD','ZIP_CD','RCDKPER_CD','IFTP_PLAN_NUM','ZIP_CD_SFX','CHANGE_CODE']

tgt_unvil_pl_update='''UPDATE TPODS.UNIVL_PLAN SET UNIVL_PLAN_NM = ? , FRST_ST_ADDR_LN = ?,
	SEC_ST_ADDR_LN = ?, CITY_NM = ?, STE_CD = ?,
	ZIP_CD = ?, ZIP_CD_SFX = ?, LAST_ACTY_DTM=? WHERE (RCDKPER_CD = ?
	AND IFTP_PLAN_NUM = ?)'''
new_dataset_Col=[ 'NEW_IND',
    'NAME',
    'COMPANY_NAME' ,
    'COMPANY_ADDR_1',
    'COMPANY_ADDR_2',
    'COMPANY_CITY ',
    'COMPANY_STATE',
    'COMPANY_ZIP ',
    'TYPE ',
    'CONTRACT_STATE',
    'YEAR_END',
    'FISCAL_YEAR_END',
    'IRS_ID',
    'TP_NAME',
    'TP_PHONE',
    'TP_WEB_URL',
    'TP_WEB_NAME',
    'TP_CALL_CENTER',
    'TP_CALL_CENTER_HRS',
    'TP_PHONE_IMPAIRED',
    'TP_PHONE_NON_US', 'ZIP_CD_SFX','RCDKPER_CD',
    'IFTP_PLAN_NUM']

col_dtype_univl_pl_ind_y_col={'UNIVL_PLAN_ID':object, 'RCDKPER_CD':object, 'IFTP_PLAN_NUM':object}
df_Y_src_col=['RCDKPER_CD','NEW_IND',
    'NAME',
    'COMPANY_NAME' ,
    'COMPANY_ADDR_1',
    'COMPANY_ADDR_2',
    'COMPANY_CITY ',
    'COMPANY_STATE',
    'COMPANY_ZIP ',
    'TYPE ',
    'CONTRACT_STATE',
    'YEAR_END',
    'FISCAL_YEAR_END',
    'IRS_ID',
    'IFTP_PLAN_NUM' ,
    'TP_NAME',
    'TP_PHONE',
    'TP_WEB_URL',
    'TP_WEB_NAME',
    'TP_CALL_CENTER',
    'TP_CALL_CENTER_HRS',
    'TP_PHONE_IMPAIRED',
    'TP_PHONE_NON_US']

newplaninsert_ds_Col=['RCDKPER_CD', 'NAME', 'COMPANY_NAME' , 'COMPANY_ADDR_1',
    'COMPANY_ADDR_2','COMPANY_CITY ','COMPANY_STATE','COMPANY_ZIP ',
    'TYPE ','CONTRACT_STATE', 'YEAR_END','FISCAL_YEAR_END','IRS_ID',
    'IFTP_PLAN_NUM','TP_NAME','TP_PHONE','TP_WEB_URL', 'TP_WEB_NAME', 'TP_CALL_CENTER',
    'TP_CALL_CENTER_HRS','TP_PHONE_IMPAIRED','TP_PHONE_NON_US']



'''plan_sequence3 parameters'''

NewPlaninsert_41_data_Set=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\DataSet\NewPlaninsert_41_ds.txt'
etl_tmp_new_pl_DB_data=r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\INPUT_FILES\WORKFILES\etl_tmp_new_pl_DB_data'

ETL_TMP_NEW_PLANS_DATA='''SELECT RTRIM(LTRIM(RCDKPER_CD)) as RCDKPER_CD, RTRIM(LTRIM(IFTP_PLAN_NUM)) as IFTP_PLAN_NUM, 
                           UNIVL_PLAN_ID, LAST_ACTY_OPER_ID, LAST_ACTY_DTM FROM STGTPODS.ETL_TMP_NEW_PLANS 
                           order  by RCDKPER_CD, IFTP_PLAN_NUM'''
ETL_TMP_NEW_PLANS_DATA_COL={
'RCDKPER_CD ' :          object,
'IFTP_PLAN_NUM ':       object,
'UNIVL_PLAN_ID ' :       object,
'LAST_ACTY_OPER_ID':    object,
'LAST_ACTY_DTM  ' :      object}
NewPlaninsert_ds_col_dtype={'RCDKPER_CD':object,'NEW_IND':object,
    'NAME':object,
    'COMPANY_NAME':object ,
    'COMPANY_ADDR_1':object,
    'COMPANY_ADDR_2':object,
    'COMPANY_CITY ':object,
    'COMPANY_STATE':object,
    'COMPANY_ZIP ':object,
    'TYPE ':object,
    'CONTRACT_STATE':object,
    'YEAR_END':object,
    'FISCAL_YEAR_END':object,
    'IRS_ID':object,
    'IFTP_PLAN_NUM':object ,
    'TP_NAME':object,
    'TP_PHONE':object,
    'TP_WEB_URL':object,
    'TP_WEB_NAME':object,
    'TP_CALL_CENTER':object,
    'TP_CALL_CENTER_HRS':object,
    'TP_PHONE_IMPAIRED':object,
    'TP_PHONE_NON_US':object}
extract_data_frm_newinserplan_ds=['RCDKPER_CD', 'NAME', 'COMPANY_NAME' , 'COMPANY_ADDR_1',
    'COMPANY_ADDR_2','COMPANY_CITY ','COMPANY_STATE','COMPANY_ZIP ',
    'TYPE ','CONTRACT_STATE', 'YEAR_END','FISCAL_YEAR_END','IRS_ID',
    'IFTP_PLAN_NUM','TP_NAME','TP_PHONE','TP_WEB_URL', 'TP_WEB_NAME', 'TP_CALL_CENTER',
    'TP_CALL_CENTER_HRS','TP_PHONE_IMPAIRED','TP_PHONE_NON_US']

extract_data_from_etl_tmp_new_pl_db_data=['UNIVL_PLAN_ID', 'LAST_ACTY_OPER_ID', 'LAST_ACTY_DTM']


load_trget_col=['RCDKPER_CD', 'NAME', 'COMPANY_NAME' , 'COMPANY_ADDR_1',
    'COMPANY_ADDR_2','COMPANY_CITY ','COMPANY_STATE','COMPANY_ZIP ',
    'TYPE ','CONTRACT_STATE', 'YEAR_END','FISCAL_YEAR_END','IRS_ID',
    'IFTP_PLAN_NUM','TP_NAME','TP_PHONE','TP_WEB_URL', 'TP_WEB_NAME', 'TP_CALL_CENTER',
    'TP_CALL_CENTER_HRS','TP_PHONE_IMPAIRED','TP_PHONE_NON_US','UNIVL_PLAN_ID', 'LAST_ACTY_OPER_ID', 'LAST_ACTY_DTM']

reckpr_cd='41'
tpods_unvil_plan='''INSERT INTO TPODS.UNIVL_PLAN (UNIVL_PLAN_ID, CRETD_BY_DTM, CRETD_BY_OPER_ID, BUS_PH_AREA_CD, BUS_PH_NUM, UNIVL_ORGANZTN_ID, 
PRODT_CD, PRODT_FAM_CD, CNTRCT_NUM, UNIVL_PLAN_YR, UNIVL_PLAN_YR_EFF_DT, UNIVL_PLAN_INCEP_DT, UNIVL_PLAN_STAT_CD,
 UNIVL_PLAN_STAT_EFF_DT, UNIVL_PLAN_EFF_DT, UNIVL_PLAN_NM, LAST_ACTY_OPER_ID, LAST_ACTY_DTM, 
 DATA_SRC_SYS_CD, FRST_ST_ADDR_LN, SEC_ST_ADDR_LN, THRD_ST_ADDR_LN, FRTH_ST_ADDR_LN, CITY_NM, STE_CD, ZIP_CD, ZIP_CD_SFX, FRGN_POSTL_CD, REGN_PROVINCE_NM, CNTRY_CD, 
UNIVL_PLAN_TERMN_DT, TEAM_ID, RCDKPER_CD, IFTP_PLAN_NUM)
SELECT CAST (UNIVL_PLAN_ID as BIGINT),		
       U.CRETD_BY_DTM,		
       U.CRETD_BY_OPER_ID,		
        U.BUS_PH_AREA_CD,		
        U.BUS_PH_NUM,		
        U.UNIVL_ORGANZTN_ID,		
        U.PRODT_CD,		
        U.PRODT_FAM_CD,		
        U.CNTRCT_NUM,		
        U.UNIVL_PLAN_YR,		
        U.UNIVL_PLAN_YR_EFF_DT,		
        U.UNIVL_PLAN_INCEP_DT,		
        U.UNIVL_PLAN_STAT_CD,		
        U.UNIVL_PLAN_STAT_EFF_DT,		
        U.UNIVL_PLAN_EFF_DT,		
        CAST(UNIVL_PLAN_NM as varchar(255))	,		
       'DSPLAN',		
        current timestamp,		
       U.DATA_SRC_SYS_CD,		
        CAST(FRST_ST_ADDR_LN  as varchar(40))	,
 						CAST(SEC_ST_ADDR_LN as varchar(40))	,		
       U.THRD_ST_ADDR_LN,		
      U.FRTH_ST_ADDR_LN,		
CAST(CITY_NM as  varchar(28))	,
CAST(STE_CD as char(2))	,
CAST(ZIP_CD as char(5))	,
 CAST(ZIP_CD_SFX as char(4))	,	
       U.FRGN_POSTL_CD,		
       U.REGN_PROVINCE_NM,		
       U.CNTRY_CD,		
       U.UNIVL_PLAN_TERMN_DT,		
       U.TEAM_ID,		
 U.RCDKPER_CD,
CAST (IFTP_PLAN_NUM as CHAR(20))
  FROM TPODS.UNIVL_PLAN U	
 WHERE U.IFTP_PLAN_NUM = '00000000000000000000' and RCDKPER_CD='{}') VALUES(?,?,?,?,?,?,?,?,?,?) '''.format(reckpr_cd)


tpods_if_pl='''INSERT INTO TPODS.IF_PLAN 
SELECT
CAST (ORCHESTRATE.UNIVL_PLAN_ID as BIGINT) as UNIVL_PLAN_ID,
pl.RCDKPER_CD,
 CAST (ORCHESTRATE.IFTP_PLAN_NUM as CHAR(20)) as IFTP_PLAN_NUM,
 CAST(ORCHESTRATE.PLAN_TY_CD as char(1)), 
CAST (ORCHESTRATE.ST_JURIS_CD as char(2)) as ST_JURIS_CD,
 pl.CNTRCT_NUM, 
pl.IF_PRODT_CD, 
 pl.ASA_CD, 
CAST (ORCHESTRATE.PLAN_YR_END_DT as char(4)) ,
 CAST (ORCHESTRATE.TAX_IRS_NUM as char(9)),
 CAST (ORCHESTRATE.IFTP_CO_NM as varchar(32)),
 pl.PRU_CNTCT_NM,
 pl.WEB_DISPL_OVERVIEW_TAB_IND,
 pl.WEB_DISPL_INVST_TAB_IND,
pl. WEB_DISPL_PERSNL_INFO_TAB,
 pl.WEB_DISPL_PERFMNC_TAB_IND,
 pl.DISPL_PRU_LOGO_IND,
 pl.PRU_RIC_NUM,
 pl.CNTRCT_EFF_DT,
 pl.PLAN_TRUSTEE_NM,
 pl.PLAN_CNTCT_NM,
pl.PLAN_CNTCT_PH_NUM,
pl.PLAN_EMAIL_ADDR,
pl.RPT_5500_REQ_IND,
 pl.ICP2_TY_CD, 
CAST ( ORCHESTRATE.IFTP_RCDKPER_NM as char(30)), 
CAST (ORCHESTRATE.IFTP_RCDKPER_800_NUM as char(10)), 
CAST (ORCHESTRATE.IFTP_WEB_URL_ADDR as varchar(75)),
 pl.IFTP_OLD_PRU_PLAN_NUM, 
 CAST (ORCHESTRATE.IFTP_WEBSITE_NM as varchar(100)),
CAST ( ORCHESTRATE.IFTP_CALL_CNTR_NM as varchar(100)),
CAST ( ORCHESTRATE.IFTP_CALL_CNTR_HR_TXT as varchar(100)),
 CAST (ORCHESTRATE.IFTP_HRING_IMPRD_TEL_NUM as char(10)), 
CAST (ORCHESTRATE.IFTP_OUT_USA_TEL_NUM as char(20)),
 pl.IFTP_SETLG_NSCC_TRD_CD, 
pl.IFTP_WARM_XFER_TEL_RIC_NUM,
'DSPLAN',
current_timestamp
FROM TPODS.IF_PLAN pl	
 WHERE pl.IFTP_PLAN_NUM = '00000000000000000000' and RCDKPER_CD='{}' VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
'''.format(reckpr_cd)

tpods_prodt_xref='''INSERT INTO TPODS.IF_PLAN_PRODT_XREF (UNIVL_PLAN_ID, IF_PRODT_CD, IF_PRODT_OPEN_IND, IF_PARALLEL_EFF_DT, LAST_ACTY_OPER_ID, LAST_ACTY_DTM, RCDKPER_CD) 

select 
CAST (ORCHESTRATE.UNIVL_PLAN_ID as BIGINT), 
IF_PRODT_CD, 
'Y', 
CAST (NULL as DATE),
'DSPLAN',
	CURRENT TIMESTAMP,
'#JRCDKPER_CD#' from TPODS.IF_RCDKPER_INFO WHERE RCDKPER_CD = '{}' VALUES(?,?);'''.format(reckpr_cd)



tpods_if_reckpr_pl_asset='''INSERT INTO TPODS.IF_RCDKPER_PLAN_ASSET 
       (
            ASSET_ID, 
            RCDKPER_CD, 
            UNIVL_PLAN_ID, 
            ASSET_STAT,  
            PLAN_ASSET_BAL, 
            PLAN_ASSET_BAL_UPD_DT, 
            LAST_ACTY_OPER_ID, 
            LAST_ACTY_DTM,
            IFTP_PLAN_NUM,
            IF_PRODT_CD
       ) 
SELECT  ASSET_ID, 
        RCDKPER_CD,
        CAST (ORCHESTRATE.UNIVL_PLAN_ID as BIGINT), 
        ASSET_STAT, 
        0.0, 
        CURRENT TIMESTAMP, 
        'DSPLAN',
        CURRENT TIMESTAMP,
        CAST (ORCHESTRATE.IFTP_PLAN_NUM as CHAR(20)),
        IF_PRODT_CD
  FROM TPODS.IF_RCDKPER_PLAN_ASSET
 WHERE RCDKPER_CD='{}'
   AND IFTP_PLAN_NUM = '00000000000000000000'
   AND ASSET_STAT = '1' VALUES(?,?,?)
'''.format(reckpr_cd)


tpods_univl_pln_1='''INSERT INTO TRSODS.UNIVL_PLAN (UNIVL_PLAN_ID	,
  UNIVL_ORGANZTN_ID,
  PRODT_CD,
  PRODT_FAM_CD,
  CNTRCT_NUM,
  PLAN_NUM,
  SUBPLAN_NUM,
  DIV_NUM	,
  CNTRCT_SFX,
  UNIVL_PLAN_INCEP_DT,
  UNIVL_PLAN_YR,
  UNIVL_PLAN_YR_EFF_DT	,
  UNIVL_PLAN_STAT_CD,
  UNIVL_PLAN_STAT_EFF_DT,
  UNIVL_PLAN_EFF_DT	,
  UNIVL_PLAN_NM	,
  AC_PLAN_ID,
  RSO_PLAN_ID,
  LAST_ACTY_OPER_ID	,
  LAST_ACTY_DTM	,
  DATA_SRC_SYS_CD,
  MULTI_ER_IND	,
  UNIVL_PLAN_CONVRN_SRC_CD,
  PRMPT_NM	,
  UNIVL_PLAN_CONVRN_DT	,
  FRST_ST_ADDR_LN,
  SEC_ST_ADDR_LN ,
  THRD_ST_ADDR_LN,
  FRTH_ST_ADDR_LN,
  CITY_NM	,
  STE_CD	,
  ZIP_CD	,
  ZIP_CD_SFX	,
  FRGN_POSTL_CD	,
  REGN_PROVINCE_NM	,
  CNTRY_CD	,
  ABBR_CNTRCT_NM	,
  MAST_CNTRCT_NUM	,
  CIGNA_SELT_CD	,
  TAFT_HRTLY_IND	,
  UNIVL_PLAN_TERMN_DT	,
  DATA_SHARE_PLAN_PARTCPT_IND,
  DATA_SHARE_FUNC_CD,
  ER_TY_CD	,
  CFE_TRANSLTD_CNTRCT_NUM,
  TEAM_ID	,
  STMT_FRMT_CD	,
  UNIVL_PLAN_ALT_NM	,
  RCDKPER_CD	,
  TP_PLAN_NUM	,
  PLAN_SYMB_ID	,
  RIC_SPLASH_PG_IND
  )
  VALUES
    
  (CAST (UNIVL_PLAN_ID as BIGINT),
                                            NULL,
                                            '21',
                                            '02',
                                            NULL,
                                            NULL,
                                            NULL,
                                            NULL,
                                            NULL,
                                            '1900-01-01',
                                            '1900',
                                            '1900-01-01',
                                            'A',
                                            CURRENT DATE,
                                            CURRENT DATE,
                                             CAST(UNIVL_PLAN_NM as varchar(255))	,
                                            NULL,
                                            NULL,
                                            'DSPLAN',
                                             CURRENT TIMESTAMP,
                                            'THRDPRTY',
                                            NULL,
                                            NULL,
                                            NULL,
                                            NULL,
                                            CAST( FRST_ST_ADDR_LN  as varchar(40))	,
 						CAST(ORCHESTRATE.SEC_ST_ADDR_LN as varchar(40))	,
                                            NULL,
                                            NULL,
                                            CAST(CITY_NM as  varchar(28))	,
CAST( STE_CD as char(2))	,
CAST(  ZIP_CD as char(5))	,
 CAST(ZIP_CD_SFX as char(4))	,
                                            NULL,
                                            NULL,
                                            NULL,
                                            NULL,
                                            NULL,
                                            NULL,
                                            'N',
                                            NULL,
                                            NULL,
                                            NULL,
                                            NULL,
                                            NULL,
                                            NULL,
                                            NULL,
                                            NULL,
                                           '{}'	,
CAST (IFTP_PLAN_NUM as CHAR(20)),
NULL,
'Y'
)'''.format(reckpr_cd)


tpods_if_pln_prodt_ltr='''INSERT INTO TPODS.IF_PLAN_PRODT_LTR 
SELECT
CAST (UNIVL_PLAN_ID as BIGINT) as UNIVL_PLAN_ID,
pl.IF_PRODT_CD, 
pl.RCDKPER_CD,
 CAST (IFTP_PLAN_NUM as CHAR(20)) as IFTP_PLAN_NUM,
 pl.IFTP_PRDC_LOCKIN_LTR_IND, 
pl.IFTP_GUARD_LTR_IND, 
pl.IFTP_PRDC_SIGNUP_LTR_IND,
 pl.IFTP_IC_NEED_IND,
 pl.IFTP_RED_PROC_EXCP_IND, 
pl.IFTP_PRDC_EXCS_WTHDRL_LTR_IND,
pl.IF_ACT_LFE_IND,
'DSPLAN',
current_timestamp
FROM TPODS.IF_PLAN_PRODT_LTR pl	
 WHERE pl.IFTP_PLAN_NUM = '00000000000000000000' and RCDKPER_CD='{}';
'''.format(reckpr_cd)
