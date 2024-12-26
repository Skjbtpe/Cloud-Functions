<<<<<<< HEAD
from response import payin_transactions,payment_settlement_recon,merchant_vpa_profile,missing_transactions_details,swipe_inactivity,declined_transactions

def bank_reference_detail(request):

    if request.method=="GET":
        return {"status_code":200}
    else:
        request_json = request.get_json()
        if request_json and 'table' in request_json and request_json['table']=="transactions":
            return payin_transactions(request)
        elif request_json and 'table' in request_json and request_json['table']=="merchant_vpa_profile":
            return merchant_vpa_profile(request)
        elif request_json and 'identifier' in request_json and request_json['identifier']=='missing_transactions':
            return missing_transactions_details(request_json)
        elif request_json and 'inactivityDays' in request_json:
            return swipe_inactivity(request_json)
        elif request_json and ('rejected_after' in request_json or 'rejected_bank_reference_no' in request_json) and 'merchant_id' in request_json and 'pageCount' in request_json and 'pageSize' in request_json:
            return declined_transactions(request_json)
        
        return payment_settlement_recon(request)
=======
from response import payin_transactions,payment_settlement_recon,merchant_vpa_profile,missing_transactions_details,swipe_inactivity,declined_transactions

def bank_reference_detail(request):

    if request.method=="GET":
        return {"status_code":200}
    else:
        request_json = request.get_json()
        if request_json and 'table' in request_json and request_json['table']=="transactions":
            return payin_transactions(request)
        elif request_json and 'table' in request_json and request_json['table']=="merchant_vpa_profile":
            return merchant_vpa_profile(request)
        elif request_json and 'identifier' in request_json and request_json['identifier']=='missing_transactions':
            return missing_transactions_details(request_json)
        elif request_json and 'inactivityDays' in request_json:
            return swipe_inactivity(request_json)
        elif request_json and ('rejected_after' in request_json or 'rejected_bank_reference_no' in request_json) and 'merchant_id' in request_json and 'pageCount' in request_json and 'pageSize' in request_json:
            return declined_transactions(request_json)
        
        return payment_settlement_recon(request)
>>>>>>> 56c69364375a838e8b4592803efdfbd9e4470779
