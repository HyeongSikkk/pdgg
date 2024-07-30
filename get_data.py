# 직접 만든 모듈
from authors import authors
from multi_tool import multi_tool
from get_funcs import get_users, get_match
from dbConnect import con, cur, engine

# 외부 패키지
import pandas as pd
import datetime
import json
import re
import tqdm
import time
from slack_message import send_message
# authors = authors[:5]

while True :
    query = f"""
    SELECT
        `user`.`account_id`
    FROM
        `user`
        JOIN `participant` ON
            `participant`.`account_id`= `user`.`id`
    ORDER BY
        RAND()
    LIMIT
        {len(authors) * 10}
    ;"""
    cur.execute(query)
    target_users = cur.fetchall()
    idx = 0
    users_matches = []

    # api 교차사용
    num_api = 0
    # api 호출속도
    count_apis = len(authors)
    max_tries = count_apis * 10
    tries = 0
    cur_minute = datetime.datetime.today().minute
    start = time.time()
    while idx < len(target_users) :        

        # 제한 속도 걸렸을 경우 속도 제한
        if tries == max_tries :
            sleep_second = 60 - datetime.datetime.today().second

        # 매 분마다 속도제한 초기화
        if cur_minute != datetime.datetime.today().minute :
            cur_minute = datetime.datetime.today().minute
            tries = 0

        api = authors[num_api%count_apis]
        account_ids = ''
        for a in target_users[idx:idx+10] :
            account_ids += ','+a[0]
        result = get_users(account_ids[1:], api)
        if type(result) == list :
            users_matches += result
        idx += 10
        tries += 1
        num_api += 1
    matches = set(map(lambda x : x['id'], users_matches))
    pd.DataFrame(matches, columns = ['id']).to_sql(name = 'test_exist_match_id', con = engine, schema = 'pdgg', if_exists = 'append', index = False)
    con.commit()
    cur.execute("""
        SELECT 
            sub.id,
            main.map_name_id AS map_name_id
        FROM 
            test_exist_match_id as sub 
            LEFT OUTER JOIN match_summary as main ON 
                sub.id = main.id 
        WHERE 
            map_name_id is NULL;""")
    matches = cur.fetchall()
    cur.execute("DELETE FROM test_exist_match_id;")
    cur.fetchall()
    con.commit()

    match_datas = []
    num_api = 0
    # api 호출속도
    count_apis = len(authors)
    max_tries = count_apis * 10
    tries = 0
    cur_minute = datetime.datetime.today().minute
    start = time.time()
    for match_id in tqdm.tqdm(matches) :
        # 제한 속도 걸렸을 경우 속도 제한
        if tries == max_tries :
            match_datas = []
            sleep_second = 60 - datetime.datetime.today().second
            time.sleep(sleep_second)

        # 매 분마다 속도제한 초기화
        if cur_minute != datetime.datetime.today().minute :
            cur_minute = datetime.datetime.today().minute
            tries = 0

        api = authors[num_api%count_apis]
        result = get_match(match_id[0], api)
        if type(result) == list :
            match_datas += result
        tries += 1
        num_api += 1
    df = pd.DataFrame(match_datas)
    try :
        df.to_sql(name = 'match_summary', con = engine, schema = 'pdgg', if_exists = 'append', index = False)
    except :
        for idx, row in df.iterrows() :
            try :
                cur.execute(f"""INSERT INTO match_summary VALUES ("{row['id']}", "{row['map_name_id']}", "{row['game_mode_id']}", "{row['match_type']}", "{row['created_at']}", "{row['asset_url']}");""")
                con.commit()
            except :
                send_message(f"""error : \nINSERT INTO match_summary VALUES ("{row['id']}", "{row['map_name']}", "{row['game_mode']}", "{row['match_type']}", "{row['created_at']}", "{row['asset_url']}")""")
    con.commit()