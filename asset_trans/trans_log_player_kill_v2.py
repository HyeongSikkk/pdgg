import datetime

def trans_log_player_kill_v2(asset: dict, start_time: datetime.datetime, match_id: int, user_id_dict: dict)-> dict:
    # comment
    """_summary_

    Args:
        asset (dict): _description_
        start_time (datetime.datetime): _description_
        match_id (int): _description_
        user_id_dict (dict): _description_

    Returns:
        dict: {
            "match_id": match_id (int),
            "killer": killer (int),
            "killer_weapon_id": killer_weapon_id (str),
            "killer_distance":killer_distance (float),
            "victim": victim (int),
            "victim_weapon_id": victim_weapon_id (str),
            "assisters":assisters (int),
            "elapsed_time":elapsed_time (int),
        }
    """
    
    # code
    ################
    #    killer    #
    ################
    if asset['killer'] is None :
        killer = 'notexist00000000000000000000000000000000'
        killer_weapon_id = "empty"
        killer_distance = 0
    else :
        killer = asset["killer"]["accountId"]
        if "ai." in killer[:5] or "Mons" in killer[:5] :
            killer = 'ai00000000000000000000000000000000000000'
        killer_weapon_id = asset["killerDamageInfo"]["damageCauserName"]
        killer_distance = asset["killerDamageInfo"]["distance"]
        
    ################
    #    victim    #
    ################
    victim = asset['victim']['accountId']
    if "ai." in victim[:5] or "Mons" in victim[:5] :
        victim = 'ai00000000000000000000000000000000000000'
    victim_weapon_id = asset['victimWeapon']
    
    if victim_weapon_id :
        splited_victim_weapon_id = victim_weapon_id.split("_C")[:-1]
        victim_weapon_id = "_C".join(splited_victim_weapon_id) + "_C"
    else :
        victim_weapon_id = "empty"
    
    ################
    #      etc     #
    ################
    assister = asset['assists_AccountId']
    if len(assister) == 0 :
        assister = 2
    else :
        if "ai." in assister[0][:5] or "Mons" in assister[0][:5] :
            assister[0] = 'ai00000000000000000000000000000000000000'
        assister = user_id_dict[assister[0]]
        
    elapsed_time = (datetime.datetime.strptime(asset['_D'], '%Y-%m-%dT%H:%M:%S.%fZ') - start_time).seconds
    
    killv2_row = {
        "match_id" :match_id,
        "killer" :user_id_dict[killer],
        "killer_weapon_id" :killer_weapon_id,
        "killer_distance" :killer_distance,
        "victim" :user_id_dict[victim],
        "victim_weapon_id" :victim_weapon_id,
        "assister" :assister,
        "elapsed_time" :elapsed_time,            
    }
    
    return killv2_row