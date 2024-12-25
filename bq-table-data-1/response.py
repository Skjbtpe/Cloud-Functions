from google.cloud import bigquery
import json
import datetime as dt
from utils import Tables,keys_to_remove,table_input_mapping,table_output_mapping,input_param

# job_config = bigquery.QueryJobConfig(
#     # Run at batch priority, which won't count toward concurrent rate limit.
#     priority=bigquery.QueryPriority.BATCH
# )


def payin_transactions(request):
    request_json = request.get_json()
    if "from" in request_json and "to" in request_json and "module" in request_json:
        if "pageCount" in request_json and "pageSize" in request_json:
            offset = (request_json['pageCount']-1) * request_json['pageSize']
            limit = request_json['pageSize']
            limit_offset = f"order by payment_timestamp limit {limit} offset {offset}"
        else:
            limit_offset = "order by payment_timestamp limit 20 offset 0"

        if request_json['module'] == 'PAYMENT_SWIPE':
            txn_type = "SWIPE"
        elif request_json['module'] == 'PAYMENT_QR':
            txn_type = "UPI"
        else:
            txn_type="SWIPE"

        from_date = request_json['from']
        to_date = request_json['to']

        query=f'''SELECT *except(_metadata_timestamp, _metadata_source_type, _metadata_deleted, _metadata_log_file, _metadata_log_position, __deleted, _metadata_uuid) 
                  FROM `bharatpe-analytics-prod.payin.transactions` 
                  WHERE DATE(created_at) >= date('{from_date}') 
                  and DATE(created_at) <= date('{to_date}')
                  and txn_type = '{txn_type}' 
                  and __deleted = 'false'
                '''
        if "merchant_id" in request_json:
            merchant_id = request_json['merchant_id']
            query=query+f"and merchant_id = {merchant_id} "
        if "bank_reference_no" in request_json:
            bank_reference_no = request_json['bank_reference_no']
            query=query+f"and bank_reference_no = '{bank_reference_no}' "
        if "amount" in request_json:
            amount=request_json['amount']
            query=query+f"and amount={amount} "
        if "payment_timestamp" in request_json:
            payment_timestamp=request_json['payment_timestamp']
            query=query+f"and payment_timestamp=timestamp('{payment_timestamp}') "

        query = query + limit_offset
        print(query)
        client=bigquery.Client()
        # query_job=client.query(query,job_config=job_config)
        query_job=client.query(query)
        res=[]
        for i in query_job.result():
            row = dict(i)
            for key in keys_to_remove:
                if key in row:
                    del row[key]
            res.append(row)
        result={'Success':True,'Response':res}     
        return result
    elif ("id" in request_json or "bank_reference_no" in request_json) and "from" not in request_json and "to" not in request_json :
        created_at = (dt.datetime.today() - dt.timedelta(days=270)).strftime('%Y-%m-%d')
        if "id" in request_json and "bank_reference_no" in request_json:
            bank_reference_no = request_json['bank_reference_no']
            query = f"SELECT * FROM `bharatpe-analytics-prod.payin.transactions` WHERE DATE(created_at) >= date('{created_at}') and id = {request_json['id']} and bank_reference_no = '{bank_reference_no}' and __deleted = 'false'"
        elif "id" in request_json:
             query = f"SELECT * FROM `bharatpe-analytics-prod.payin.transactions` WHERE DATE(created_at) >= date('{created_at}') and id = {request_json['id']} and __deleted = 'false'"
        else:
            bank_reference_no = request_json['bank_reference_no']
            query = f"SELECT * FROM `bharatpe-analytics-prod.payin.transactions` WHERE DATE(created_at) >= date('{created_at}') and bank_reference_no = '{bank_reference_no}' and __deleted = 'false'"
        client=bigquery.Client()
        # query_job=client.query(query,job_config=job_config)
        query_job=client.query(query)
        res=[]
        for i in query_job.result():
            row = dict(i)
            for key in keys_to_remove:
                if key in row:
                    del row[key]
            res.append(row)
        result={'Success':True,'Response':res}
        return result
    return json.dumps({'Success':False,'Response':'Invalid-Input please send required fields'})


def payment_settlement_recon(request):
    request_json = request.get_json()
    if request_json and 'table' in request_json and 'created_at' in request_json and any(item in input_param for item in request_json.keys()) and request_json['table'] in Tables:
        table = request_json['table']
        common_column={col:request_json[col] for col in table_input_mapping[table] if col in request_json}

        if common_column:
            if table in ['settlement_request','bank_payout_request'] and 'transaction_date' in common_column:
                common_column['date(settlement_date)']=common_column['transaction_date']
                common_column.pop('transaction_date')
            
            get_column=' and '.join(f"{key}='{val}'" for (key,val) in common_column.items())

            query=f'''SELECT {",".join(table_output_mapping[table])} FROM `bharatpe-analytics-prod.bharatpe_analytics_data.{table}` 
            WHERE DATE(created_at) >= date('{request_json['created_at']}') and {get_column}'''

            client=bigquery.Client()
            # query_job=client.query(query,job_config=job_config)
            query_job=client.query(query)

            res=[]
            for i in query_job.result():
                res.append(dict(i))

            result={'Success':True,'Response':res}
            
            return result
    return json.dumps({'Success':False,'Response':'Invalid-Input'})


def merchant_vpa_profile(request):
    request_json = request.get_json()
    table = request_json['table']
    if "merchant_id" in request_json:
        merchant_id = request_json['merchant_id']
        query = f"SELECT * FROM `bharatpe-analytics-prod.payin.merchant_vpa_profile_temp` WHERE merchant_id = {merchant_id}"
        client=bigquery.Client()
        query_job=client.query(query)
        res=[]
        for i in query_job.result():
            row = dict(i)
            res.append(row)
        result={'Success':True,'Response':res}
        return result
    return json.dumps({'Success':False,'Response':'Invalid-Request please send required fields'})


def missing_transactions_details(request_json):
    try:
        created_at = request_json['created_at']
        ids = request_json['ids']

        query = f"""select *except(_metadata_timestamp,_metadata_source_type,_metadata_deleted,_metadata_log_file,_metadata_log_position,__deleted,_metadata_uuid) 
        from payin.transactions where id in ({','.join(map(str, ids)) }) and status='SUCCESS' and 
        txn_type in  ('UPI', 'PAYMENT_LINK', 'SWIPE') and date(created_at)>'{created_at}'and 
        id not in (select payment_transaction_id from deposit.fp_ledger where payment_transaction_id in ({','.join(map(str, ids))})) and
        ifnull(__deleted,'a')!='true' """

        client=bigquery.Client()
        query_job=client.query(query)
        res=[]
        for i in query_job.result():
            row = dict(i)
            res.append(row)
        result={'Success':True,'Response':res}
        return result
    except Exception as e:
        return json.dumps({'Success':False,'Response':'Invalid-Request please send Correct fields'})

def swipe_inactivity(request_json):
    query = f""" SELECT merchant_id,device_serial FROM `bharatpe-analytics-prod.bharatpe_analytics_data.bharatswipe_terminal` 
    where id not in (
            select distinct vpa_id from `payin.transactions` where date(created_at)>=date_sub(current_date('Asia/Kolkata'),
            interval {request_json['inactivityDays']} Day) and vpa_id is not null); """
    client=bigquery.Client()
    query_job=client.query(query)
    res=[]
    for i in query_job.result():
        row = dict(i)
        res.append(row)
    result={'Success':True,'Response':res}
    return result

def declined_transactions(request_json):
    bank_reference_no=None
    if request_json and 'rejected_bank_reference_no' in request_json:
        bank_reference_no = request_json['rejected_bank_reference_no']
    
    rejected_after=None
    if request_json and 'rejected_after' in request_json:
        rejected_after = request_json['rejected_after']


    offset = (request_json['pageCount']-1) * request_json['pageSize']
    limit = request_json['pageSize']
    limit_offset = f"order by initiation_time desc,bank_reference_no limit {limit} offset {offset}"

    query = f"""WITH merc_vpa AS
                (SELECT merchant_id,
                        full_vpa
                FROM `payin.merchant_static_vpa`
                WHERE merchant_id ={request_json['merchant_id']}
                )

                SELECT a.*except(response_reason),
                        b.description AS rejection_reason
                FROM
                (SELECT b.merchant_id,
                        'FEDERAL' AS payment_gateway,
                        a.npciTxnId AS npci_txn_id,
                        a.payee_vpa,
                        a.payer_vpa,
                        a.payer_name,
                        a.amount,
                        a.auth_response_code,
                        a.payer_acc_type,
                        a.cust_ref_id AS bank_reference_no,
                        timestamp_sub(a.created_at,interval 330 MINUTE) AS initiation_time,
                        timestamp_sub(a.updated_at,interval 330 MINUTE) AS rejection_time,
                        a.response_reason,
                FROM `bharatpe-analytics-prod.payin.pending_request_federal` AS a
                JOIN merc_vpa AS b ON a.payee_vpa=b.full_vpa
                WHERE {[f"date(a.created_at) >='{rejected_after}' and ",""][rejected_after==None]} {[f" a.cust_ref_id = '{bank_reference_no}' and ",""][bank_reference_no==None]} 1=1
                    AND auth_response_code != '00'

                UNION ALL 

                SELECT b.merchant_id,
                                    'YESBANK' AS payment_gateway,
                                    a.npci_txn_id AS npci_txn_id,
                                    a.payee_vpa,
                                    a.payer_vpa,
                                    NULL AS payer_name,
                                    a.amount,
                                    a.auth_response_code,
                                    a.payer_acc_type,
                                    a.bank_reference_no,
                                    timestamp_sub(a.created_at,interval 330 MINUTE) AS initiation_time,
                                    timestamp_sub(a.updated_at,interval 330 MINUTE) AS rejection_time,
                                    a.response_reason,
                FROM `bharatpe-analytics-prod.payin.pending_request_yesbank` AS a
                JOIN merc_vpa AS b ON a.payee_vpa=b.full_vpa
                WHERE {[f"date(a.created_at) >='{rejected_after}' and ",""][rejected_after==None]} {[f" a.bank_reference_no = '{bank_reference_no}' and",""][bank_reference_no==None]} 1=1
                    AND auth_response_code != '00'

                UNION ALL 

                SELECT b.merchant_id,
                                    'INDUSIND' AS payment_gateway,
                                    a.npciTxnId AS npci_txn_id,
                                    a.payee_vpa,
                                    a.payer_vpa,
                                    a.payer_name,
                                    a.amount,
                                    a.auth_response_code,
                                    a.payer_acc_type,
                                    a.cust_ref_id AS bank_reference_no,
                                    timestamp_sub(a.created_at,interval 330 MINUTE) AS initiation_time,
                                    timestamp_sub(a.updated_at,interval 330 MINUTE) AS rejection_time,
                                    a.response_reason,
                FROM 
                `bharatpe-analytics-prod.payin.pending_request_indusind` AS a
                JOIN merc_vpa AS b ON a.payee_vpa=b.full_vpa
                WHERE {[f"date(a.created_at) >='{rejected_after}' and ",""][rejected_after==None]} {[f" a.cust_ref_id = '{bank_reference_no}' and ",""][bank_reference_no==None]} 1=1
                    AND auth_response_code != '00') AS a
                
                LEFT JOIN

                (SELECT actual_reason,
                        description
                FROM
                    (SELECT actual_reason,
                            description,
                            visible,
                            row_number() OVER (PARTITION BY actual_reason
                                                ORDER BY updated_at DESC) AS row_num
                    FROM `bharatpe-analytics-prod.payin.rejection_reason`)
                WHERE row_num=1
                    AND visible="Yes") AS b ON a.response_reason=b.actual_reason {limit_offset};"""

    client=bigquery.Client()
    query_job=client.query(query)
    res=[]
    for i in query_job.result():
        row = dict(i)
        res.append(row)
    result={'Success':True,'Response':res}
    return result

