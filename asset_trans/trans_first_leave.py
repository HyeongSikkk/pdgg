import datetime

def trans_first_leave(asset: dict) -> dict :
    # comment
    """
    비행기의 경로를 추정하기 위해,
    유저가 비행기에서 내릴 때의 위치를 파악
    Args:
        asset (dict): 가공할 로그

    Returns:
        dict: {
            'account_id': account_id (int),
            '_D': "yyyy-mm-ddThh:mm:ss.sssZ" (str),
            'x': x (float),
            'y': y (float),
            'z': z (float),        
        }
    """
    # code
    account_id = asset['character']['accountId']
    x = asset['character']['location']['x']
    y = asset['character']['location']['y']
    z = asset['character']['location']['z']
    air_row = {
        'account_id' : account_id,
        '_D' : asset['_D'],
        'x' : x,
        'y' : y,
        'z' : z,
    }
    return air_row