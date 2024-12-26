## bq-table-data-1

### Overview
The **bq-table-data-1** cloud function is designed to handle and route banking-related requests efficiently. It evaluates incoming HTTP requests and delegates processing to one of several specialized sub-functions, based on the endpoint or condition specified in the request.
     
Different functions used are specified as:
> payin_transactions : Get all the payment transactions details

sample request json :
```
{
    "from": "2024-01-01",
    "to": "2024-01-31",
    "module": "PAYMENT_SWIPE",
    "merchant_id": 12345,
    "pageCount": 1,
    "pageSize": 20
  }
```
     
> merchant_vpa_profile : return details of a particular merchant
'''{
    "table": "settlement_request",
    "created_at": "2024-01-01",
    "transaction_date": "2024-01-01"
}'''

> missing_transactions_details : return details of transactions ids not in **fp_ledger**
'''{
    "merchant_id": 12345,
    "table": "merchant_vpa_profile_temp"
}'''

> swipe_inactivity : return *merchant_id* and *device_serial* of inactive machines from **bharatpeswipe_terminal** BQ table
'''{
    "created_at": "2024-01-01",
    "ids": [101, 102, 103]
  }
'''
> declined_transactions : return the rejection reason of transactions from Top Payment Gateway Providers
'''{
    "inactivityDays": 30
  }'''

> payment_settlement_reco : request to query a BigQuery table for payment and settlement data based on input parameters.
'''{
    "merchant_id": 12345,
    "rejected_bank_reference_no": "ABC123",
    "rejected_after": "2024-01-01",
    "pageCount": 1,
    "pageSize": 20
  }
'''