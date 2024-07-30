import datetime

def trans_log_player_take_damage(asset: dict, start_time: datetime.datetime, match_id: int, user_id_dict: dict, reason_id_dict: dict)-> dict:
    # comment
    """_summary_

    Args:
        asset (dict): 가공할 로그
        start_time (datetime.datetime): 경기 시작 시간으로 만들어진 datetime.datetime 객체
        match_id (int): db에 적재된 해당 경기의 id값
        user_id_dict (dict): {account_id (str): id (int), ...} 형식으로 이루어진 딕셔너리
        reason_id_dict (dict): {reason_id (str): id (int), ...} 형식으로 이루어진 딕셔너리

    Returns:
        dict: {
            "match_id": match_id (int),
            "attacker": attacker (int),
            "attacker_x": attacker_x (float),
            "attacker_y": attacker_y (float),
            "victim": victim (int),
            "victim_x": victim_x (float),
            "victim_y": victim_y (float),
            "causer_id": causer_id (int),
            "damage_reason_id": damage_reason (int),
            "damage": damage (float),
            "elapsed_time": elapsed_time (int),
        }
    """
    
    #code
    if asset["attacker"] is None :
        attacker = "notexist00000000000000000000000000000000"
        attacker_x = 0
        attacker_y = 0
    else :
        attacker = asset["attacker"]["accountId"]
        attacker_x = asset["attacker"]["location"]["x"]
        attacker_y = asset["attacker"]["location"]["y"]

    if "ai" in attacker[:5] or 'Mons' in attacker[:5]:
        attacker = "ai00000000000000000000000000000000000000"
        
    victim = asset["victim"]["accountId"]
    if "ai" in victim[:5] or 'Mons' in victim[:5]:
        victim = 'ai00000000000000000000000000000000000000'
    
    victim_x = asset["victim"]["location"]["x"]
    victim_y = asset["victim"]["location"]["y"]
    causer_id = asset["damageCauserName"]
    damage_reason = reason_id_dict[asset["damageReason"]]
    damage = asset["damage"]
    elapsed_time = (datetime.datetime.strptime(asset['_D'], '%Y-%m-%dT%H:%M:%S.%fZ') - start_time).seconds

    take_damage_row = {
        "match_id" :match_id,
        "attacker" :user_id_dict[attacker],
        "attacker_x" :attacker_x,
        "attacker_y" :attacker_y,
        "victim" :user_id_dict[victim],
        "victim_x" :victim_x,
        "victim_y" :victim_y,
        "causer_id" :causer_id,
        "damage_reason_id" :damage_reason,
        "damage" :damage,
        "elapsed_time" :elapsed_time,
    }

    return take_damage_row