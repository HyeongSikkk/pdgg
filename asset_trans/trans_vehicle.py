import datetime

def trans_vehicle(asset: dict, match_id: int, elapsed_time: int)-> dict :
    # comment
    """
    유저가 탑승한 탈 것의 종류를 파악
    Args:
        asset (dict): 가공할 로그
        match_id (int): db에 적재된 해당 경기의 id값
        elapsed_time (int): 해당 로그가 발생된 시점의 **경기 진행 시간**

    Returns:
        dict: {
            'match_id': match_id (int),
            'account_id': account_id (int),
            'vehicle_id': vehicle_id (int),
            'elapsed_time': elapsed_time (int)
        }
    """
    # code
    vehicle_id = asset['vehicle']['vehicleId']
    account_id = asset['character']['accountId']
    ride_row = {
        'match_id': match_id,
        'account_id': account_id,
        'vehicle_id': vehicle_id,
        'elapsed_time': elapsed_time
    }
    return ride_row