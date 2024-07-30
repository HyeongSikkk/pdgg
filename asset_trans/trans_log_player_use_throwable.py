import datetime

def trans_log_player_use_throwable(asset: dict, start_time: datetime.datetime, match_id: int)-> dict :
    #comment
    """_summary_

    Args:
        asset (dict): 가공할 로그
        start_time (datetime.datetime): 경기 시작 시간이 담긴 datetime.datetime 객체
        match_id (int): db에 적재된 해당 경기의 id값

    Returns:
        dict: {
            "match_id" : match_id (int),
            "account_id" : account_id (int),
            "weapon_id" : weapon_id (int),
            'x' : x (float),
            'y' : y (float),
            "elapsed_time" : elapsed_time (int),
        }
    """
    
    # code
    account_id = asset["attacker"]["accountId"]  
    weapon_id = asset["weapon"]["itemId"]
    x = asset["attacker"]["location"]["x"]
    y = asset["attacker"]["location"]["y"]
    elapsed_time = (datetime.datetime.strptime(asset['_D'], '%Y-%m-%dT%H:%M:%S.%fZ') - start_time).seconds
    
    throw_row = {
        "match_id" : match_id,
        "account_id" : account_id,
        "weapon_id" : weapon_id,
        'x' : x,
        'y' : y,
        "elapsed_time" : elapsed_time,
    }
    
    return throw_row