import datetime
from dbConnect import con, cur

def trans_weapon_parts(asset: dict, start_time: datetime.datetime, match_id: int, part_id_dict: dict)-> dict :
    # comment
    """
    !* asset['character']['account_id']를 인트값으로 변환할 것 *!
    Args:
        asset (dict): 변환할 로그
        start_time (datetime.datetime): 경기 시작 시간이 담긴 datetime.datetime 객체
        match_id (int): db에 적재된 경기 아이디값
        part_id_dict (dict): 부착물을 아이디로 바꿔줄 딕셔너리

    Returns:
        dict: {
            "match_id" : match_id (int),
            'account_id' : account_id (int),
            'weapon_id' : weapon_id (str),
            'parts' : parts (list), [1, 2, 7, ...]
            'elapsed_time' : elapsed_time (int),
        }
    """
    
    #code
    account_id = asset['character']['accountId']
    weapon_id = asset['parentItem']['itemId']
    elapsed_time = (datetime.datetime.strptime(asset['_D'], '%Y-%m-%dT%H:%M:%S.%fZ') - start_time).seconds
    if asset["_T"] == "LogItemAttach" :
        parts = [*asset['parentItem']['attachedItems'], asset['childItem']['itemId']]
    else :
        parts = [i for i in asset['parentItem']['attachedItems'] if i != asset['childItem']['itemId']]
        if len(parts) == 0 :
            parts = ['empty']
    try :
        parts = list(map(lambda x : part_id_dict[x], parts))
    except :
        for idx, part in enumerate(parts) :
            if part not in part_id_dict :
                query = f"""SELECT `id` FROM `weapon_part` WHERE `part_id` = "{part}";"""
                cur.execute(query)
                result = cur.fetchall()
                if not result :
                    query = f"""INSERT INTO `weapon_part` (`type_id`, `part_id`, `name`) VALUES (7, "{part}", "new_data");"""
                    cur.execute(query)
                    con.commit()
                    query = f"""SELECT `id` FROM `weapon_part` WHERE `part_id` = "{part}";"""
                    cur.execute(query)
                    result = cur.fetchall()
                part_id_dict[part] = result[0][0]
            parts[idx] = part_id_dict[part]

    parts.sort()
    parts = list(map(lambda x : str(x), parts))
    parts = ",".join(parts)
    
    weapon_attaches_row = {
        "match_id" : match_id,
        'account_id' : account_id,
        'weapon_id' : weapon_id,
        'parts' : parts,
        'elapsed_time' : elapsed_time,
    }
    return weapon_attaches_row
    