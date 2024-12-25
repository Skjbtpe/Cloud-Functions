## bq-table-data-1

### Overview
The **bq-table-data-1** cloud function is designed to handle and route banking-related requests efficiently. It evaluates incoming HTTP requests and delegates processing to one of several specialized sub-functions, based on the endpoint or condition specified in the request.
     
     Different functions used are specified as:
     > payin_transactions : Get all the payment transactions details
     
     > merchant_vpa_profile : return details of a particular merchant

     > missing_transactions_details : return details of transactions ids not in *fp_ledger*

     > swipe_inactivity : return *merchant_id* and *device_serial* of inactive machines from *bharatpeswipe_terminal* BQ table

     > declined_transactions : return the rejection reason of transactions from Top Payment Gateway Providers

     > payment_settlement_reco : request to query a BigQuery table for payment and settlement data based on input parameters.
