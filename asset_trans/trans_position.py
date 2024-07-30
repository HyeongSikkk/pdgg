import datetime

def trans_position(asset: dict, start_time: datetime.datetime, match_id: int, event_id: int) -> dict:
    # comment
    """
    ! account_id값은 반드시 인트로 변환할 것
    Args:
        asset (dict): 변환할 로그,
        start_time (datetime.datetime): 경기 시작 시간값을 가진 객체,
        match_id (int): db에 적재된 해당 경기의 id값
        event_id (int): db에 적재된 해당 로그의 id값
    
    Returns:
        dict: {
            'match_id' : match_id (int),
            'account_id' : account_id (int),
            'event_id' : event_id (int),
            'elapsed_time' : elapsed_time (int),
            'x' : x (float),
            'y' : y (float),
            'z' : z (float)
        }
    """
    
    # code
    elapsed_time = (datetime.datetime.strptime(asset['_D'], '%Y-%m-%dT%H:%M:%S.%fZ') - start_time).seconds
    event_id = event_id
    account_id = asset['character']['accountId']
    position_row = {
        'match_id' : match_id,
        'account_id' : account_id,
        'event_id' : event_id,
        'elapsed_time' : elapsed_time,
        'x' : asset['character']['location']['x'],
        'y' : asset['character']['location']['y'],
        'z' : asset['character']['location']['z'],
    }
    return position_row