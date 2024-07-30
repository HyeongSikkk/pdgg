def trans_log_match_end(asset: dict, match_id: int, user_id_dict: dict)-> list :
    # comment
    """
    Args:
        asset (dict): 변환할 로그
        match_id (int): db에 적재된 id값
        user_id_dict (dict): 유저 아이디(str)-> 아이디(int) 변환하기 위한 용도

    Returns:
        list: [
            {'match_id': match_id (int), 'team_id': team_id (int), 'account_id': account_id (int)},
            ..., 
            ]
    """
    # code
    teams = []
    for who in asset["characters"] :
        team_id = who["character"]["teamId"]
        account_id = who["character"]["accountId"]
        personal_rank = who['character']['ranking']
        
        if "ai" in account_id[:5] :
            continue
        
        team_row = {
            "match_id" : match_id,
            "team_id" : team_id,
            "account_id" : user_id_dict[account_id],
            'personal_rank' : personal_rank,
        }
    
        teams.append(team_row)
    return teams