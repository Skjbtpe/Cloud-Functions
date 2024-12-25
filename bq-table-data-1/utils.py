Tables=[
    'payment_transaction',
    'settlement_request',
    'bank_payout_request',
]

input_param=[
    'transaction_date',
    'bank_reference_no',
    'payment_gateway',
    'settlement_mode'
]

table_input_mapping={
    'payment_transaction':['bank_reference_no','payment_gateway'],
    'settlement_request':['transaction_date','bank_reference_no','payment_gateway','settlement_mode'],
    'bank_payout_request':['transaction_date','bank_reference_no','payment_gateway','settlement_mode'],
}

table_output_mapping={
    'payment_transaction':['id', 'bank_reference_no', 'amount', 'status', 'vpa','payment_gateway'],
    'settlement_request':['id', 'bank_reference_no', 'amount', 'status','internal_account_details_id','payment_gateway'],
    'bank_payout_request':['id', 'bank_reference_no', 'amount', 'status', 'owner','owner_id','internal_account_details_id','payment_gateway'],
}

keys_to_remove = ["__deleted","_metadata_deleted",
                  "_metadata_log_file","_metadata_log_position",
                  "_metadata_source_type","_metadata_timestamp"]
