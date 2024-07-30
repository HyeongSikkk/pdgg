from dbConnect import con, cur, engine
import asset_trans as ast
from slack_message import send_message
from get_db_dict import weapon_id_dict, part_id_dict, causer_id_dict, reason_id_dict

import datetime
import time
from multiprocessing import Pool
import requests
import pandas as pd
import re
import json

# 정규표현식 컴파일
pattern = re.compile(r'\{.*?\"_T\".*?\}')

# 데이터 수집 코드
def game_data(row) :
    asset_url = row['asset_url']
    match_id = row['match_id']
    try :
        req = requests.get(asset_url)
    except :
        cur.execute(f"""INSERT INTO get_asset_error (match_id, id, asset_url) VALUES({match_id}, "{row['id']}", "{asset_url}");""")
        cur.execute(f'DELETE FROM get_asset WHERE match_id = "{match_id}";')
        con.commit()
        return None
    if req.status_code != 200 :
        cur.execute(f"""INSERT INTO get_asset_error (match_id, id, asset_url) VALUES({match_id}, "{row['id']}", "{asset_url}");""")
        cur.execute(f'DELETE FROM get_asset WHERE match_id = "{match_id}";')
        con.commit()
        return None

    is_json = True
    try :
        assets = req.json()
    except :
        is_json = False
        assets = pattern.findall(req.text)

    record_bool = {}
    is_match_start = False

    # 유저 아이디값 관련
    user_id_dict = {
        'ai00000000000000000000000000000000000000' : 1,
        'notexist00000000000000000000000000000000' : 2,
    }
    # 오브젝트 관련
    pc = []
    gsp = []
    air = []
    redeploys = []
    zone_list = []
    first_leave = {}

    # 이동경로
    positions = []

    # 탈 것
    rides = []

    # 교전
    take_damages = []
    attacks = []
    throws = []
    killv2s = []

    # 파츠
    weapon_attaches = []

    # 승자와 팀정보
    winners = []
    teams = []
    
    for asset in assets :
        if not is_json :
            match = match.replace(':-nan', ':null')
            match = match.replace(':nan', ':null')
            asset = json.loads(match)  
                    
        # positions 기록시작 유무 딕셔너리 만들기
        if asset['_T'] == "LogPlayerCreate" :
            if asset['character']['accountId'] not in record_bool :
                account_id = asset['character']['accountId']
                
                # 기록 시작, 비행기 내림여부 딕셔너리 제작
                record_bool[account_id] = False
                first_leave[account_id] = True
                
                # user 테이블에 해당 아이디가 없다면 삽입
                if "ai." in account_id[:5] or "Mons" in account_id[:5] :
                    continue
                query = f"""SELECT `id` FROM `user` WHERE `account_id` = "{account_id}";"""
                cur.execute(query)
                result = cur.fetchall()
                if not result :
                    query= f"""INSERT INTO `user` (`account_id`) VALUES ("{account_id}");"""
                    cur.execute(query)
                    con.commit()
                
                
                # user 테이블에서 account_id값의 id값을 반환
                query = f"""SELECT `id` FROM `user` WHERE `account_id` = "{account_id}";"""
                cur.execute(query)
                user_id_dict[account_id] = cur.fetchall()[0][0]

        # 경기 시작 시간
        elif asset['_T'] == "LogMatchStart" :
            start_time = datetime.datetime.strptime(asset['_D'], '%Y-%m-%dT%H:%M:%S.%fZ')
            is_match_start = True
        
        # 이동경로 관련
        elif asset['_T'] == 'LogParachuteLanding' :
            account_id = asset['character']['accountId']
            if "ai." in account_id[:5] or "Mons" in account_id[:5] :
                continue
            asset['character']['accountId'] = user_id_dict[account_id]
            record_bool[account_id] = True
            position_row = ast.trans_position(asset, start_time, match_id, 1)
            positions.append(position_row)

        # 이동경로 관련
        elif asset['_T'] == 'LogPlayerPosition' :
            account_id = asset['character']['accountId']

            if "ai." in account_id[:5] or "Mons" in account_id[:5] :
                continue
            
            elif record_bool[account_id] :
                asset['character']['accountId'] = user_id_dict[account_id]
                position_row = ast.trans_position(asset, start_time, match_id, 2)
                positions.append(position_row)

        # 이동경로 관련
        elif asset['_T'] == 'LogVehicleLeave' :
            account_id = asset['character']['accountId']
            if "ai." in account_id[:5] or "Mons" in account_id[:5] :
                continue
            asset['character']['accountId'] = user_id_dict[account_id]
            
            if record_bool[account_id] :
                position_row = ast.trans_position(asset, start_time, match_id, 6)
                positions.append(position_row)
        

            elif asset['vehicle']['vehicleType'] == 'TransportAircraft' and first_leave[account_id]:
                first_leave[account_id] = False
                air_row = ast.trans_first_leave(asset)
                air.append(air_row)

        # 이동경로, 탈것 관련
        elif asset['_T'] == 'LogVehicleRide' :
            account_id = asset['character']['accountId']
            
            if "ai." in account_id[:5] or "Mons" in account_id[:5] :
                continue
            
            elif record_bool[account_id] :
                asset['character']['accountId'] = user_id_dict[account_id]
                position_row = ast.trans_position(asset, start_time, match_id, 5)
                positions.append(position_row)
                
                elapsed_time = (datetime.datetime.strptime(asset['_D'], '%Y-%m-%dT%H:%M:%S.%fZ') - start_time).seconds
                ride_row = ast.trans_vehicle(asset, match_id, elapsed_time)
                rides.append(ride_row)

        # 이동경로 관련
        elif asset['_T'] == 'LogSwimStart' :
            account_id = asset['character']['accountId']
            
            if "ai." in account_id[:5] or "Mons" in account_id[:5] :
                continue
            
            elif record_bool[account_id] :
                asset['character']['accountId'] = user_id_dict[account_id]
                position_row = ast.trans_position(asset, start_time, match_id, 4)
                positions.append(position_row)
        
        # 이동경로 관련
        elif asset['_T'] == 'LogSwimEnd' :
            account_id = asset['character']['accountId']
            
            if "ai." in account_id[:5] or "Mons" in account_id[:5] :
                continue
            
            elif record_bool[account_id] :
                asset['character']['accountId'] = user_id_dict[account_id]
                position_row = ast.trans_position(asset, start_time, match_id, 3)
                positions.append(position_row)
        
        # take_damage
        elif asset['_T'] == 'LogPlayerTakeDamage' :
            take_damage_row = ast.trans_log_player_take_damage(asset, start_time, match_id, user_id_dict, reason_id_dict)
            take_damages.append(take_damage_row)


        # attack    
        elif asset["_T"] ==  'LogPlayerAttack' :
            if is_match_start :
                account_id = asset["attacker"]["accountId"]
                if "ai" in account_id[:5] or account_id == "" or "Mons" in account_id[:5] :
                    continue
                
                asset["attacker"]["accountId"] = user_id_dict[account_id]
                attack_row = ast.trans_log_player_attack(asset, start_time, match_id)
                attacks.append(attack_row)
        
        
        elif asset["_T"] == 'LogPlayerUseThrowable' :
            
            if is_match_start :
                account_id = asset["attacker"]["accountId"]
                if "ai" in account_id[:5] or account_id == "" or "Mons" in account_id[:5] :
                    continue
                
                asset["attacker"]["accountId"] = user_id_dict[account_id]
                throw_row = ast.trans_log_player_use_throwable(asset, start_time, match_id)
                throws.append(throw_row)
        
        elif asset["_T"] == "LogMatchEnd" :
            teams = ast.trans_log_match_end(asset, match_id, user_id_dict)
            

        elif asset["_T"] == "LogPlayerKillV2" :
            killv2_row = ast.trans_log_player_kill_v2(asset, start_time, match_id, user_id_dict)
            killv2s.append(killv2_row)

        # 오브젝트 관련 파싱
        # 자기장 파악
        elif asset['_T'] == 'LogPhaseChange' :
            ph_row = {
                'phase' : asset['phase'],
                '_D' : asset['_D']
            }
            pc.append(ph_row)
            
        # 자기장 파악
        elif asset['_T'] == 'LogGameStatePeriodic' :
            small_row = {
                'elapsed_time' : asset['gameState']['elapsedTime'],
                'radius' : asset['gameState']['safetyZoneRadius'],
                '_D' : asset['_D']
            }
            gsp_row = {**small_row, **asset['gameState']['safetyZonePosition']}
            gsp.append(gsp_row)

        # 부활한 유저
        # elif asset['_T'] == 'LogPlayerRedeployBRStart' :
        #     for who in asset['characters'] :
        #         redeploys.append(who['accountId'])

        # 부착물
        elif asset["_T"] == "LogItemAttach" :
            account_id = asset['character']['accountId']
            if "ai" in account_id[:5] or account_id == "" or "Mons" in account_id[:5] :
                continue
            asset['character']['accountId'] = user_id_dict[account_id]
            weapon_attaches_row = ast.trans_weapon_parts(asset, start_time, match_id, part_id_dict)
            weapon_attaches.append(weapon_attaches_row)
        
        # 부착물
        elif asset["_T"] == "LogItemDetach" :
            account_id = asset['character']['accountId']
            if "ai" in account_id[:5] or account_id == "" or "Mons" in account_id[:5] :
                continue
            asset['character']['accountId'] = user_id_dict[account_id]
            weapon_attaches_row = ast.trans_weapon_parts(asset, start_time, match_id, part_id_dict)
            weapon_attaches.append(weapon_attaches_row)

    # 오브젝트 관련 데이터 만들기
    zone = pd.DataFrame(gsp)
    pcs = pd.DataFrame(pc)

    pcs = pcs.sort_values(by = '_D', ascending = False).drop_duplicates('phase').sort_values(by = '_D')
    zone['phase'] = 0

    for index, row in pcs.iterrows() :
        filter1 = zone['_D'] >= row['_D']
        zone.loc[filter1, 'phase'] = row['phase']

    zone['match_id'] = match_id
    columns = [
        'match_id',
        'phase',
        'radius',
        'x',
        'y',
        'z',
        'elapsed_time'
    ]
    zone = zone[columns]
    try :
        zone.to_sql(name = "zone", con = engine, schema = 'pdgg', if_exists = 'append', index = False)
        con.commit()
    except :
        send_message(f"{match_id} 경기 zone 에러 발생")
    
    firstX = air[0]['x']
    firstY = air[0]['y']
    lastX = air[-1]['x']
    lastY = air[-1]['y']
    airplane = {
        'match_id': match_id,
        'start_x': firstX,
        'start_y': firstY,
        'end_x': lastX,
        'end_y': lastY,
    }
        
    try :
        pd.DataFrame([airplane]).to_sql(name = 'airplane', con = engine, schema = 'pdgg', if_exists = 'append', index = False)
    except :
        send_message(f"{match_id} 경기 airplane 에러 발생")
        pd.DataFrame([airplane]).to_csv(f"C:/workspace/pdgg/error_data/{match_id}_object.csv", index=False)

    con.commit()

    # position
    try :
        position_df = pd.DataFrame(positions)
        position_df.to_sql(name='position', con = engine, schema='pdgg', if_exists='append', index=False)
        con.commit()
    except :
        send_message(f"{match_id} 경기 position 에러 발생")
        pd.DataFrame(positions).to_csv(f"C:/workspace/pdgg/error_data/{match_id}_position.csv", index=False)

    # participant
    try :
        participant_df = pd.DataFrame(teams, columns = ['match_id', 'team_id', 'account_id', 'personal_rank'])
        participant_df = participant_df.sort_values(by='personal_rank')
        participant_df.reset_index(drop=True, inplace = True)
        participant_df['personal_rank'] = participant_df.index+1
        rank_dict = {team: idx+1 for idx, team in enumerate(participant_df['team_id'].unique())}
        participant_df['team_rank'] = participant_df['team_id'].apply(lambda x : rank_dict[x])
        participant_df.to_sql(name="participant", con=engine, schema='pdgg', if_exists='append', index=False)
    except :
        send_message(f"{match_id} 경기 personal 에러 발생")
        pd.DataFrame(teams, columns = ['match_id', 'team_id', 'account_id', 'personal_rank']).to_csv(f"C:/workspace/pdgg/error_data/{match_id}_personal.csv", index=False)
            
    #ride
    try :
        if not len(rides) == 0 :
            ride_df = pd.DataFrame(rides)
            vehicles = list(ride_df['vehicle_id'].unique())
            for vehicle in vehicles :
                query = f"""SELECT vehicle_id, id FROM vehicle WHERE vehicle_id = "{vehicle}";"""
                cur.execute(query)
                result = cur.fetchall()
                if not result :
                    query = f"""INSERT INTO `vehicle` (`vehicle_id`, `name`) VALUES ("{vehicle}", "new_data");"""
                    cur.execute(query)
                    con.commit()
            cur.execute("""SELECT vehicle_id, id FROM vehicle;""")
            vehicle_query_result = cur.fetchall()
            vehicle_id = {}
            for row in vehicle_query_result :
                vehicle_id[row[0]] = row[1]
            ride_df['vehicle_id'] = ride_df['vehicle_id'].apply(lambda x : vehicle_id[x])
            ride_df.to_sql(name='ride', con=engine, schema='pdgg', if_exists = 'append', index=False)
    except :
        send_message(f"{match_id} 경기 ride 에러 발생")
        pd.DataFrame(rides).to_csv(f"C:/workspace/pdgg/error_data/{match_id}_ride.csv", index=False)

    # attack
    try :
        attack_df = pd.DataFrame(attacks)
        attack_df = attack_df[attack_df['weapon_id'] != '']
        weapon_list = list(attack_df['weapon_id'].unique())
        weapon_list.sort()
        for weapon in weapon_list :
            # 데이터 삽입
            query = f"""SELECT weapon_name FROM `weapon` WHERE `weapon_id` = "{weapon}";"""
            cur.execute(query)
            result = cur.fetchall()
            if not result : 
                query = f"INSERT INTO `weapon` (`bullet_type`, `weapon_type`, `weapon_id`, `weapon_name`) VALUES (14, 9, '{weapon}', 'new_data');"
                cur.execute(query)
                con.commit()
            
            query = f"SELECT id FROM `weapon` WHERE weapon_id = '{weapon}';"
            cur.execute(query)
            weapon_id_dict[weapon] = cur.fetchall()[0][0]

        attack_df['weapon_id'] = attack_df['weapon_id'].apply(lambda x: weapon_id_dict[x])
        attack_df.to_sql(name="attack", con=engine, schema = "pdgg", if_exists='append', index=False)
    except :
        send_message(f"{match_id} 경기 attack 에러 발생")
        pd.DataFrame(attacks).to_csv(f"C:/workspace/pdgg/error_data/{match_id}_attack.csv", index=False)
        
    # throw
    try :
        throw_df = pd.DataFrame(throws)
        throw_df['weapon_id'] = throw_df['weapon_id'].apply(lambda x : weapon_id_dict[x])
        throw_df.to_sql(name='throw', schema='pdgg', con=engine, if_exists='append', index=False)
        con.commit()
    except :
        send_message(f"{match_id} 경기 throw 에러 발생")
        pd.DataFrame(throws).to_csv(f"C:/workspace/pdgg/error_data/{match_id}_throw.csv", index=False)

    # take_damage
    try :
        take_damage_df = pd.DataFrame(take_damages)
        causer_list = list(take_damage_df['causer_id'].unique())
        for causer in causer_list :
            # 데이터 삽입
            query = f"""SELECT `name` FROM `damage_causer` WHERE `causer_id` = "{causer}";"""
            cur.execute(query)
            result = cur.fetchall()
            if not result :
                query = f"""INSERT INTO `damage_causer` (`causer_id`, `name`, `causer_type_id`) VALUES ('{causer}', "new_data",5);"""
                cur.execute(query)
                con.commit()
            
            query = f"SELECT id FROM `damage_causer` WHERE causer_id = '{causer}';"
            cur.execute(query)
            causer_id_dict[causer] = cur.fetchall()[0][0]
        con.commit()

        take_damage_df['causer_id'] = take_damage_df['causer_id'].apply(lambda x : causer_id_dict[x])
        take_damage_df.to_sql(name="take_damage", con=engine, schema = "pdgg", if_exists='append', index=False)
        con.commit()
    except :
        send_message(f"{match_id} 경기 take_damage 에러 발생")
        pd.DataFrame(take_damages).to_csv(f"C:/workspace/pdgg/error_data/{match_id}_take_damage.csv", index=False)

    # killv2
    try :
        killv2_df = pd.DataFrame(killv2s)
        killer_causer_list = set(killv2_df['killer_weapon_id'].unique())
        victim_causer_list = set(killv2_df['victim_weapon_id'].unique())
        causer_list = killer_causer_list.union(victim_causer_list)
        for causer in causer_list :
            # 데이터 삽입
            query = f"""SELECT `name` FROM `damage_causer` WHERE `causer_id` = "{causer}";"""
            cur.execute(query)
            result = cur.fetchall()
            if not result :
                query = f"""INSERT INTO `damage_causer` (`causer_id`, `name`, `causer_type_id`) VALUES ('{causer}', "new_data",5);"""
                cur.execute(query)
                con.commit()
            
            query = f"SELECT id FROM `damage_causer` WHERE causer_id = '{causer}';"
            cur.execute(query)
            causer_id_dict[causer] = cur.fetchall()[0][0]
        con.commit()
        killv2_df['killer_weapon_id'] = killv2_df['killer_weapon_id'].apply(lambda x : causer_id_dict[x])
        killv2_df['victim_weapon_id'] = killv2_df['victim_weapon_id'].apply(lambda x : causer_id_dict[x])
        killv2_df.to_sql(name='killv2', con=engine, schema='pdgg', if_exists='append', index=False)
        con.commit()
    except :
        send_message(f"{match_id} 경기 killv2 에러 발생")
        pd.DataFrame(killv2s).to_csv(f"C:/workspace/pdgg/error_data/{match_id}_killv2.csv", index=False)

    # attach
    try :
        weapon_attach_df = pd.DataFrame(weapon_attaches)
        weapon_list = list(weapon_attach_df['weapon_id'].unique())
        weapon_list.sort()
        for weapon in weapon_list :
            query = f"""SELECT `weapon_name` FROM `weapon` WHERE `weapon_id` = "{weapon}";"""
            cur.execute(query)
            result = cur.fetchall()
            if not result :
                # 데이터 삽입
                query = f"INSERT INTO `weapon` (`bullet_type`, `weapon_type`, `weapon_id`, `weapon_name`) VALUES (14, 9, '{weapon}', 'new_data');"
                cur.execute(query)
                con.commit()
            
            query = f"SELECT id FROM `weapon` WHERE weapon_id = '{weapon}';"
            cur.execute(query)
            weapon_id_dict[weapon] = cur.fetchall()[0][0]
        weapon_attach_df['weapon_id'] = weapon_attach_df['weapon_id'].apply(lambda x : weapon_id_dict[x])
        weapon_attach_df.to_sql(name="attachment", con = engine, schema='pdgg', if_exists='append', index=False)
        con.commit()
    except :
        send_message(f"{match_id} 경기 attachment 에러 발생")
        pd.DataFrame(weapon_attaches).to_csv(f"C:/workspace/pdgg/error_data/{match_id}_attachment.csv", index=False)
    cur.execute(f"delete from get_asset where match_id = {match_id};")
    con.commit()
    
if __name__ == '__main__' :
    while True :
        if datetime.datetime.today().minute%1 == 0 :
            cur.execute("SELECT match_id, id, asset_url FROM get_asset;")
            get_assets = cur.fetchall()
            get_assets = list(map(lambda x : {'match_id' : x[0], 'id' : x[1], 'asset_url' : x[2]}, get_assets))
            
            pool = Pool()
            result = []
            result.append(pool.map(game_data, get_assets))
            pool.close()
            pool.join()
        else :
            time.sleep(60)