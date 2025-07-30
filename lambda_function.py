import json
import tempfile
from itertools import islice
import boto3
import os
import sys
import pandas as pd
from io import StringIO
import numpy as np
import traceback
from src.awslambda.Generic_function import s3, s3_Dataset_put_object, s3_Dataset_get_object, db2_conn_test, \
    s3_put_object, logger, \
    s3_put_incoming_object, s3_get_object,s3_seq_done_object

Conn = db2_conn_test()


def lambda_handler(event, context):
    logger.info("The starting point of plan load sequence 4")
    #logger.debug("Received event: " + json.dumps(event, indent=2))
    # if 'Records' in event:
    #     recordkeepercode = event['Records'][0]
    #     # cycledate = event['Records'][0]
    #
    #     logger.info(".....recordkeeper code....")
    #     logger.info(recordkeepercode)
    logger.info("Entered into plan sequence  4 lambda ")

    try:
        with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as tmp:
            bucket_name, file_key, s3 = s3_Dataset_get_object()
            file_key = file_key + 'DS_UNIVL_RPT_LOG41_3.txt'
            response = s3.get_object(Bucket=bucket_name, Key=file_key)
            DS_UNIVL_RPT_LOG41_3_data_Set_Res = response['Body'].read().decode('utf-8')
            print(DS_UNIVL_RPT_LOG41_3_data_Set_Res)
            with open(tmp.name, 'w', encoding='utf-8') as csv_file:
                csv_file.write(DS_UNIVL_RPT_LOG41_3_data_Set_Res)
            DS_UNIVL_RPT_LOG41_3_data_Set = pd.read_csv(tmp)
            DS_UNIVL_RPT_LOG41_3_data_Set = DS_UNIVL_RPT_LOG41_3_data_Set.copy()
    except Exception as e:
        error_msg=traceback.format_exc()
        logger.info(f"Errror occured during process of plan one sequence processing....:{e}")
        logger.info(f"Traceback:\n{error_msg}")
        sys.exit(10)

    with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as tmp1:
        bucket_name, file_key, s3 = s3_Dataset_get_object()
        file_key = file_key + 'DS_UNIVL_RPT_LOG41_4.txt'
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        DS_UNIVL_RPT_LOG41_4_data_Set_Res = response['Body'].read().decode('utf-8')
        print(DS_UNIVL_RPT_LOG41_4_data_Set_Res)
        with open(tmp1.name, 'w', encoding='utf-8') as csv_file1:
            csv_file1.write(DS_UNIVL_RPT_LOG41_4_data_Set_Res)
        DS_UNIVL_RPT_LOG41_4_data_Set = pd.read_csv(tmp1)
        DS_UNIVL_RPT_LOG41_4_data_Set = DS_UNIVL_RPT_LOG41_4_data_Set.copy()

    with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as tmp2:
        bucket_name, file_key, s3 = s3_Dataset_get_object()
        file_key = file_key + 'DS_PLAN_RPT_LOG41_3.txt'
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        DS_PLAN_RPT_LOG41_3_data_Set_res = response['Body'].read().decode('utf-8')
        print(DS_PLAN_RPT_LOG41_3_data_Set_res)
        with open(tmp2.name, 'w', encoding='utf-8') as csv_file2:
            csv_file2.write(DS_PLAN_RPT_LOG41_3_data_Set_res)
        DS_PLAN_RPT_LOG41_3_data_Set = pd.read_csv(tmp2)
        DS_PLAN_RPT_LOG41_3_data_Set = DS_PLAN_RPT_LOG41_3_data_Set.copy()

    with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as tmp3:
        bucket_name, file_key, s3 = s3_Dataset_get_object()
        file_key = file_key + 'DS_PLAN_RPT_LOG41_4.txt'
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        DS_PLAN_RPT_LOG41_4_data_Set_Res = response['Body'].read().decode('utf-8')
        print(DS_PLAN_RPT_LOG41_4_data_Set_Res)
        with open(tmp3.name, 'w', encoding='utf-8') as csv_file3:
            csv_file3.write(DS_PLAN_RPT_LOG41_4_data_Set_Res)
        DS_PLAN_RPT_LOG41_4_data_Set = pd.read_csv(tmp3)
        DS_PLAN_RPT_LOG41_4_data_Set = DS_PLAN_RPT_LOG41_4_data_Set.copy()

    read_files(DS_PLAN_RPT_LOG41_3_data_Set, DS_PLAN_RPT_LOG41_4_data_Set, DS_UNIVL_RPT_LOG41_4_data_Set,
               DS_UNIVL_RPT_LOG41_3_data_Set)


def read_files(DS_PLAN_RPT_LOG41_3_data_Set, DS_PLAN_RPT_LOG41_4_data_Set, DS_UNIVL_RPT_LOG41_4_data_Set,
               DS_UNIVL_RPT_LOG41_3_data_Set):
    try:
        ds_univl_rpt_log41_3 = DS_UNIVL_RPT_LOG41_3_data_Set.copy()
        ds_pln_rpt_log41_3 = DS_PLAN_RPT_LOG41_3_data_Set.copy()
        ds_univl_rpt_log41_4 = DS_UNIVL_RPT_LOG41_4_data_Set.copy()
        ds_plan_rpt_log41_4 = DS_PLAN_RPT_LOG41_4_data_Set.copy()

        # concatenate two dataframe
        ds_univl_pln_log41_3 = pd.concat([ds_univl_rpt_log41_3, ds_pln_rpt_log41_3], axis=0, ignore_index=True)

        # remove the duplicates
        ds_univl_pln_log41_3.drop_duplicates(inplace=True)

        # concatenate two dataframe
        ds_univl_pln_log41_4 = pd.concat([ds_univl_rpt_log41_4, ds_plan_rpt_log41_4], axis=0, ignore_index=True)

        # remove the duplicates
        ds_univl_pln_log41_4.drop_duplicates(inplace=True)
        print(ds_univl_pln_log41_4)
        print(ds_univl_pln_log41_3)

        # concatenate two dataframe after duplicate rows remove
        ds_univl_pln_log41_err_3_4 = pd.concat([ds_univl_pln_log41_3, ds_univl_pln_log41_4], axis=0, ignore_index=True)
        print(ds_univl_pln_log41_err_3_4)
        min_index = ds_univl_pln_log41_err_3_4['DESCRIPTION'].min()
        ds_univl_pln_log41_err_3_4 = ds_univl_pln_log41_err_3_4.loc[
            ds_univl_pln_log41_err_3_4['DESCRIPTION'] == min_index]
        ds_univl_pln_log41_err_3_4.rename(columns={'IFTP_PLAN_NUM': 'NUMBER'}, inplace=True)
        ds_univl_pln_log41_err_3_4 = ds_univl_pln_log41_err_3_4[
            ['RCDKPER_CD', 'TP_NAME', 'NUMBER', 'NAME', 'DESCRIPTION']]
        print(ds_univl_pln_log41_err_3_4)
        DS_FUNPLAN_ERR_6_2_buffer = StringIO()
        ds_univl_pln_log41_err_3_4.to_csv(DS_FUNPLAN_ERR_6_2_buffer, index=False)

        bucket_name, file_key, s3 = s3_Dataset_put_object()
        file_key = file_key + "DS_FUNPLAN_ERR_6_2.txt"

        result = s3.put_object(Bucket=bucket_name, Key=file_key, Body=DS_FUNPLAN_ERR_6_2_buffer.getvalue())
        res = result.get('ResponseMetadata')
        if res.get('HTTPStatusCode') == 200:

            logger.info("file DS_FUNPLAN_ERR_6_2.txt uploaded successfully")
        else:
            logger.info("file DS_FUNPLAN_ERR_6_2.txt not uploaded")

        columns = ['RCDKPER_CD', 'TP_NAME', 'NUMBER', 'NAME', 'DESCRIPTION']

        with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as tmp4:
            bucket_name, file_key, s3 = s3_Dataset_get_object()
            file_key_LOG1 = file_key + 'DS_PLAN_RPT_LOG41_1.txt'
            response = s3.get_object(Bucket=bucket_name, Key=file_key_LOG1)
            existing_data_DS_PLAN_RPT_LOG41_1_data_Set = response['Body'].read().decode('utf-8')
            print(existing_data_DS_PLAN_RPT_LOG41_1_data_Set)
            with open(tmp4.name, 'w', encoding='utf-8') as csv_file:
                csv_file.write(existing_data_DS_PLAN_RPT_LOG41_1_data_Set)
            DS_PLAN_RPT_LOG41_1_df = pd.read_csv(tmp4)
            DS_PLAN_RPT_LOG41_1_data_Set_df = DS_PLAN_RPT_LOG41_1_df.copy()

        print("log1 dataset", DS_PLAN_RPT_LOG41_1_data_Set_df)

        with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as tmp5:
            bucket_name, file_key, s3 = s3_Dataset_get_object()
            file_key_LOG7 = file_key + 'DS_PLAN_RPT_LOG41_7.txt'
            response = s3.get_object(Bucket=bucket_name, Key=file_key_LOG7)
            existing_data_DS_PLAN_RPT_LOG41_7_data_Set = response['Body'].read().decode('utf-8')
            print(existing_data_DS_PLAN_RPT_LOG41_7_data_Set)
            with open(tmp5.name, 'w', encoding='utf-8') as csv_file1:
                csv_file1.write(existing_data_DS_PLAN_RPT_LOG41_7_data_Set)
            DS_PLAN_RPT_LOG41_7_data_Set = pd.read_csv(tmp5)  # , names=columns)
            DS_PLAN_RPT_LOG41_7_data_Set = DS_PLAN_RPT_LOG41_7_data_Set.copy()

        with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as tmp6:
            bucket_name, file_key, s3 = s3_Dataset_get_object()
            file_key_log2 = file_key + 'DS_PLAN_RPT_LOG41_2.txt'
            response = s3.get_object(Bucket=bucket_name, Key=file_key_log2)
            existing_data_DS_PLAN_RPT_LOG41_2_data_Set = response['Body'].read().decode('utf-8')
            print(existing_data_DS_PLAN_RPT_LOG41_7_data_Set)
            with open(tmp6.name, 'w', encoding='utf-8') as csv_file2_PLAN_RPT_LOG41_2:
                csv_file2_PLAN_RPT_LOG41_2.write(existing_data_DS_PLAN_RPT_LOG41_2_data_Set)
            DS_PLAN_RPT_LOG41_2_data_Set = pd.read_csv(tmp6)  # , names=columns)
            DS_PLAN_RPT_LOG41_2_data_Set = DS_PLAN_RPT_LOG41_2_data_Set.copy()

        LOG41_7_LOG41_2_LOG41_1_LOG41_6_2 = pd.concat(
            [DS_PLAN_RPT_LOG41_7_data_Set, ds_univl_pln_log41_err_3_4, DS_PLAN_RPT_LOG41_1_data_Set_df,
             DS_PLAN_RPT_LOG41_2_data_Set], ignore_index=True)

        print("LOG41_7_LOG41_2_LOG41_1_LOG41_6_2", LOG41_7_LOG41_2_LOG41_1_LOG41_6_2)

        LOG41_7_LOG41_2_LOG41_1_LOG41_6_2_buffer = StringIO()
        LOG41_7_LOG41_2_LOG41_1_LOG41_6_2.to_csv(LOG41_7_LOG41_2_LOG41_1_LOG41_6_2_buffer, index=False)

        bucket_name, file_key, s3 = s3_Dataset_put_object()
        file_key = file_key + "LOG41_7_LOG41_2_LOG41_1_LOG41_6_2.txt"

        result = s3.put_object(Bucket=bucket_name, Key=file_key,
                               Body=LOG41_7_LOG41_2_LOG41_1_LOG41_6_2_buffer.getvalue())
        res = result.get('ResponseMetadata')
        if res.get('HTTPStatusCode') == 200:

            logger.info("file LOG41_7_LOG41_2_LOG41_1_LOG41_6_2.txt uploaded successfully")
        else:
            logger.info("file LOG41_7_LOG41_2_LOG41_1_LOG41_6_2.txt not uploaded")

        header = ['RCDKPER_ID', 'TP_NAME', 'NUMBER', 'NAME', 'DESCRIPTION']
        header_swap = ['TP_NAME', 'RCDKPER_ID', 'NUMBER', 'NAME', 'DESCRIPTION']
        pd.options.display.max_colwidth = 1000

        print("remove unnamed column name", LOG41_7_LOG41_2_LOG41_1_LOG41_6_2.columns)

        # LOG41_7_LOG41_2_LOG41_1_LOG41_6_2.drop('Unnamed: 0', axis=1, inplace=True)

        print(LOG41_7_LOG41_2_LOG41_1_LOG41_6_2.columns)
        df = LOG41_7_LOG41_2_LOG41_1_LOG41_6_2.copy()
        df.columns = header
        df_plan_trailer = df.copy()

        # Swap the first and second columns
        df = df.reindex(columns=header_swap)
        # Rename the columns
        df.rename(columns={'RCDKPER_ID': 'TP_NUMBER', 'NUMBER': 'TP_PLAN_NUMBER', 'NAME': 'TP_PLAN_NAME'}, inplace=True)

        # df.replace(np.nan, ' ', inplace=True)
        df.insert(5, "FILLER", '        ')
        columns = ['TP_NAME', 'TP_NUMBER', 'TP_PLAN_NUMBER', 'TP_PLAN_NAME', 'DESCRIPTION', 'FILLER']
        plan_err_detail_41_copy = df.copy()
        # Set the desired column widths (e.g., 10 characters)
        column_widths = [30, 2, 15, 64, 1, 8]

        # Custom function to concatenate columns without spaces
        def format_row(row):

            return ''.join(str(val).ljust(width) for val, width in zip(row, column_widths))

        # Apply the custom function to each row
        fwf_content = plan_err_detail_41_copy.apply(format_row, axis=1)

        if fwf_content.empty:
            My_string = " "
        else:
            My_string = fwf_content.to_string(index=False, header=False)
        fwf_content_buffer = StringIO(My_string)

        bucket_name, file_key, s3 = s3_put_incoming_object()
        file_key = file_key + "plan_err_detail_41.txt"

        result = s3.put_object(Bucket=bucket_name, Key=file_key, Body=fwf_content_buffer.read())
        res = result.get('ResponseMetadata')
        if res.get('HTTPStatusCode') == 200:

            logger.info("file plan_err_detail_41.txt uploaded successfully")
        else:
            logger.info("file plan_err_detail_41.txt not uploaded")

        # Extract the 'RCDKPER_ID' column and create a new DataFrame
        new_column_name = 'PLAN_IND'
        new_df_plan_trailer = df_plan_trailer[['RCDKPER_ID']].copy()
        new_df_plan_trailer.columns = [new_column_name]
        REC_KPR_ROW_COUNT = new_df_plan_trailer.count()
        data = [[REC_KPR_ROW_COUNT[0]]]
        plan_trailer_df = pd.DataFrame(data, columns=['PLAN_IND'])
        plan_trailer_df.rename(columns={'PLAN_IND': 'reccount1'}, inplace=True)
        reccount = plan_trailer_df['reccount1'][0] + 2
        stge_varlen = len(str(reccount))
        stgvarchecklen = 11 - stge_varlen
        record_cunt = '0' * stgvarchecklen + str(reccount)
        filler1 = ' ' * 107

        # Define your values
        plan_err_trailer_41_buffer = StringIO()
        TRAILER_REC = "99"
        RecordCount = record_cunt
        filler1 = filler1
        plan_err_trailer_41_data = [TRAILER_REC + RecordCount + filler1]
        plan_trailer_final_df = pd.DataFrame(plan_err_trailer_41_data)

        print(plan_trailer_final_df)

        plan_trailer_final_df.to_csv(plan_err_trailer_41_buffer, index=False, header=None)

        bucket_name, file_key, s3 = s3_put_incoming_object()
        file_key_plan_err_trailer_41 = file_key + 'plan_err_trailer_41.txt'
        result = s3.put_object(Bucket=bucket_name, Key=file_key_plan_err_trailer_41,
                               Body=plan_err_trailer_41_buffer.getvalue())
        res = result.get('ResponseMetadata')
        if res.get('HTTPStatusCode') == 200:

            logger.info("file plan_err_trailer_41.txt uploaded successfully")
        else:
            logger.info("file plan_err_trailer_41.txt not uploaded")

        # plan_trailer_final_df.to_string(plan_err_trailer_41, col_space=0, index=False, header=None)

        plan_err_detail_empty_df = pd.DataFrame()
        with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as tmp_pln_err_detail:
            bucket_name, file_key, s3 = s3_get_object()
            file_key_pln_err_detail = file_key + 'plan_err_detail_41.txt'
            print(file_key_pln_err_detail)
            response = s3.get_object(Bucket=bucket_name, Key=file_key_pln_err_detail)
            plan_err_detail_41_data_Set_Res = response['Body'].read().decode('utf-8')
            print(plan_err_detail_41_data_Set_Res)
            with open(tmp_pln_err_detail.name, 'w', encoding='utf-8') as csv_file_pln_err_detail:
                csv_file_pln_err_detail.write(plan_err_detail_41_data_Set_Res)
            # if data is empty this below logic works to read empty as dataframe
            try:

                plan_err_detail_41_data_Set = pd.read_csv(tmp_pln_err_detail, header=None)  # , names=columns)
            except pd.errors.EmptyDataError:

                plan_err_detail_41_data_Set = plan_err_detail_empty_df

        plan_err_detail_41_data_Set = plan_err_detail_41_data_Set.copy()

        print("plan_err_detail_41", plan_err_detail_41_data_Set)

        # plan_err_header_#JRCDKPER_CD#.txt
        '''read file from dataset s3 bucket'''
        plan_err_header_41_df_buffer = StringIO()
        with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as tmp_pln_err_header:

            bucket_name, file_key, s3 = s3_Dataset_get_object()
            file_key_HeaderDtPlan = file_key + 'DS_41_HeaderDtPLAN.txt'
            response = s3.get_object(Bucket=bucket_name, Key=file_key_HeaderDtPlan)
            existing_data_HeaderDtPlan_data_Set = response['Body'].read().decode('utf-8')

            with open(tmp_pln_err_header.name, 'w', encoding='utf-8') as csv_file_pln_err_header:
                csv_file_pln_err_header.write(existing_data_HeaderDtPlan_data_Set)
            DS_41_HeaderDtPLAN_df = pd.read_csv(tmp_pln_err_header)  # ,names=['CYC_DT' ,'dummy'])
            DS_41_HeaderDtPLAN_df = DS_41_HeaderDtPLAN_df.copy()
        print("DS_41_HeaderDtPLAN_df", DS_41_HeaderDtPLAN_df)

        CYC_DT = DS_41_HeaderDtPLAN_df.iloc[0][0]
        CYC_DT = CYC_DT[:8]

        HEADER_REC = 'AA'
        FILLER2 = ' ' * 6
        HEADER_DATE = CYC_DT
        filler = ' ' * 104
        plan_err_header_41_data = [HEADER_REC + FILLER2 + HEADER_DATE + filler]
        # plan_err_header_41 = r'C:\Users\x257716\PycharmProjects\TPIFX_MIGRATION_PLAN\INPUT_FILES\WORKFILES\plan_err_header_41.txt'
        plan_err_header_41_df = pd.DataFrame(plan_err_header_41_data)
        plan_err_header_41_df.to_csv(plan_err_header_41_df_buffer, index=False, header=None)

        bucket_name, file_key, s3 = s3_put_incoming_object()
        file_key_plan_err_header = file_key + 'plan_err_header_41.txt'
        result = s3.put_object(Bucket=bucket_name, Key=file_key_plan_err_header,
                               Body=plan_err_header_41_df_buffer.getvalue())
        res = result.get('ResponseMetadata')
        if res.get('HTTPStatusCode') == 200:

            logger.info("file plan_err_header.txt uploaded successfully")
        else:
            logger.info("file plan_err_header.txt not uploaded")

        # plan_err_trailer_template_#JRCDKPER_CD#.txt
        Plan_err_trailer_template_buffer = StringIO()
        TRAILER_REC = '99'
        # print("SAI",len(TRAILER_REC))
        RecordCount = '00000000002'  # default value which is hardcoded value it reamins same all the time
        filler1 = ' ' * 107
        Plan_err_trailer_template_data = [TRAILER_REC + RecordCount + filler1]
        Plan_err_trailer_template_df = pd.DataFrame(Plan_err_trailer_template_data)
        Plan_err_trailer_template_df.to_csv(Plan_err_trailer_template_buffer, index=False,header=None)

        bucket_name, file_key, s3 = s3_put_incoming_object()
        file_key_plan_err_trailer_template = file_key + 'plan_err_trailer_template_41.txt'
        result = s3.put_object(Bucket=bucket_name, Key=file_key_plan_err_trailer_template,
                               Body=Plan_err_trailer_template_buffer.getvalue())
        res = result.get('ResponseMetadata')
        if res.get('HTTPStatusCode') == 200:

            logger.info("file plan_err_trailer_template_41.txt uploaded successfully")
        else:
            logger.info("file plan_err_trailer_template_41.txt not uploaded")

        # Usage

        print(plan_err_header_41_df)
        print(plan_err_detail_41_data_Set)
        print(plan_trailer_final_df)



        plan_output_buffer = StringIO()
        if plan_err_detail_41_data_Set.empty:
            print(f"The file 'plan_err_detail_41_df' is empty.")
            trg_df = pd.concat([plan_err_header_41_df, plan_err_detail_41_data_Set, plan_trailer_final_df],axis=0,ignore_index=True)

            print(trg_df)
            plan_output_buffer = StringIO()

            trg_df.to_csv(plan_output_buffer,index=False, header=None)
            bucket_name, file_key, s3 = s3_put_object()
            file_key_plan_output = file_key + 'plan_output.txt'
            result = s3.put_object(Bucket=bucket_name, Key=file_key_plan_output,
                                   Body=plan_output_buffer.getvalue())
            res = result.get('ResponseMetadata')
            if res.get('HTTPStatusCode') == 200:

                logger.info("file plan_output.txt uploaded successfully")
            else:
                logger.info("file plan_output.txt not uploaded")

            print("Files concatenated into 'plan_output.txt'.")
        else:
            print(f"The file plan_err_detail_41_df is not empty.")
            trg_df = pd.concat([plan_err_header_41_df, plan_err_detail_41_data_Set, plan_trailer_final_df],axis=0,ignore_index=True)#ignore_index=False)
            print(trg_df)
            plan_output_buffer1 = StringIO()
            trg_df.to_csv(plan_output_buffer1,index=False,header=None)
            #,index=False)#, header=None)
            bucket_name, file_key, s3 = s3_put_object()
            file_key_plan_output = file_key + 'plan_output.txt'
            result = s3.put_object(Bucket=bucket_name, Key=file_key_plan_output,
                                   Body=plan_output_buffer.getvalue())
            res = result.get('ResponseMetadata')
            if res.get('HTTPStatusCode') == 200:

                logger.info("file plan_output.txt uploaded successfully")
            else:
                logger.info("file plan_output.txt not uploaded")

            print("Files concatenated into 'plan_output.txt'.")
    except Exception as e:
        logger.error(f"Error occured while processing plan fourth sequence :{e} ")
        traceback_str=traceback.format_exc()
        print("Error details",traceback_str)

        sys.exit(1)



    bucket_name,file_key,s3=s3_seq_done_object()
    uploadfile_key=file_key + "plan.done"

    result = s3.put_object(Bucket=bucket_name, Key=uploadfile_key, Body='')
    res = result.get('ResponseMetadata')
    if res.get('HTTPStatusCode') == 200:
       logger.info("plan.done file uploaded successfully to s3 ")
    else:
       logger.info("plan.done  file  not uploaded to s3 ")

    logger.info("Plan fourth sequence completed sucessfully")


if __name__ == "__main__":
    event = {'Records': [{
        'body': '{"indId": "0", "gaId": null, "provCompany": null, "accuCode": "PscAfWR", "accuAccessTypeCode": null, "statusCode": "PENDING", "processCode": "CSES_SYNCH", "typeCode": "RKS", "batchNumber": "2422", "runOutput": null, "effdate": "24-Aug-2016", "runDpdateTime": "", "creationDpdateTime": "24-Aug-2016 16:07:01", "lastRunDpDateTime": "", "runEndDateTime": "", "seqnbr": "4", "evId": "0", "externalId": null, "externalSubId": "745638", "rowId": "AABsfQAADAAC4JzAAK"}'}]}
    context = []
    logger.info(lambda_handler(event, context))
