#!/usr/bin/env python
#coding: UTF-8

'''
netkeibaから競馬データをスクレイピングする
'''


'''
memo
・生年月日をdatatimeに
・値に空白が入る場合が有る
'''

import urllib as ul
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
import os
import re
import time
import datetime
import pdb
import csv
import psycopg2
import itertools
#import multiprocessing as mp
#from prettyprint import pp, pp_str


__PROC__ = 1


# dataをcsvファイルに出力
def data_to_csv(horse_data, year):
    horse_table = pd.DataFrame(horse_data).T
    renames = []
    for i in range(len(horse_table.columns)):
        renames.append(horse_table.columns[i].encode('utf-8'))
    horse_table.columns = renames
    for i in range(len(horse_table)):
        horse_table.iloc[i] = horse_table.iloc[i].apply(lambda x: x.encode('utf-8') if type(x)!=float and type(x)!=int else x)
    horse_table.to_csv("{0}_horse_table.csv".format(year), index=True, header=True)



# 馬の情報を集める
def makeHorseDB(year, conn, cur):
    horse_url = 'http://db.netkeiba.com/horse/'
    idx_from = 100000
    idx_to   = 111000

    # cur.execute('create table horse(id varchar(20), name varchar(30), tihou_horse_flg int, foreign_horse_flg int, sex varchar(10), hair varchar(30), father varchar(30), mother varchar(30), m_father varchar(30), birthday varchar(30), tyokyoshi text, owner text, price text);')
    # conn.commit()

    pd_horse = pd.DataFrame([], columns=['horse_id', 'name', 'tihou_horse_flg', 'foreign_horse_flg', 'sex', 'hair', 'father', 'mother', 'm_father', 'birthday', 'tyokyoshi', 'owner', 'price'])

    # f = open('{0}_horse_performance.csv'.format(year), 'ab')
    # csvWriter = csv.writer(f)
    header = ['horse_name', 'goal', 'race_date', 'area', 'whether', 'race_num', 'race_name', 'num_horses', 'waku_ban', 'uma_ban', 'odds', 'popularity', 'jockey', 'basis_w', 'race_type', 'distance', 'baba', 'race_time', 'arrive', 'passing', 'pace', 'last_time', 'horse_w', 'w_change', 'prize']
    # csvWriter.writerow(header)

    prof_keys = [
        '馬名',
        '父',
        '母',
        '母父',
        '生年月日',
        '調教師',
        '馬主',
        'セリ取引価格'
    ]


    for idx in range(idx_from, idx_to+1):
        if idx%100==0:
            print("ID： "+str(idx))
        try:
            # 過度なアクセスを抑えるために待ち時間を設定
            time.sleep(2)

            # webからhtmlを取得
            idx = str(year) + str(idx).zfill(6)
            url = horse_url + idx
            src_html = ul.request.urlopen(url).read()
            root = BeautifulSoup(src_html)

            # DBが存在するか
            # if root.find('div', class_='horse_title').find('h1').text:
            if root.find('div', class_='horse_title') is None:
                print('{0} is not found'.format(idx))
                continue

            horse_data = {}
            horse_name = ''
            tihou_horse_flg   = 0
            foreign_horse_flg = 0


            # プロフィール情報を取得
            for prof_key in prof_keys:
                try:
                    if prof_key == '馬名':
                        ex_horse_name = root.find('div', class_='horse_title').find('h1').text.strip('\u3000').strip(' ')
                        # 地方馬
                        if '□' == ex_horse_name[0] and '地' == ex_horse_name[1]:
                            horse_name = ex_horse_name[2:]
                            tihou_horse_flg = 1
                        # 外国馬
                        elif '□' == ex_horse_name[0] and '外' == ex_horse_name[1]:
                            horse_name = ex_horse_name[2:]
                            foreign_horse_flg = 1
                        # 中央馬
                        else:
                            horse_name = ex_horse_name

                        horse_sex  = root.find('div', class_='horse_title').find('p', class_='txt_01').text.split('\u3000')[1]
                        horse_hair = root.find('div', class_='horse_title').find('p', class_='txt_01').text.split('\u3000')[2]

                        horse_data['name'] = horse_name
                        horse_data['tihou_horse_flg']   = tihou_horse_flg
                        horse_data['foreign_horse_flg'] = foreign_horse_flg
                        horse_data['sex']  = horse_sex
                        horse_data['hair'] = horse_hair

                    elif prof_key == '父':
                        horse_data['father'] = root.find('dl', class_='fc').find_all('td', class_='b_ml')[0].text.strip('\n')

                    elif prof_key == '母':
                        horse_data['mother'] = root.find('dl', class_='fc').find_all('td', class_='b_fml')[1].text.strip('\n')

                    elif prof_key == '母父':
                        horse_data['m_father'] = root.find('dl', class_='fc').find_all('td', class_='b_ml')[2].text.strip('\n')

                    elif prof_key == '生年月日':
                        horse_data['birthday'] = root.find('table', class_='db_prof_table').find_all('td')[0].text

                    elif prof_key == '調教師':
                        horse_data['tyokyoshi'] = root.find('table', class_='db_prof_table').find_all('td')[1].text

                    elif prof_key == '馬主':
                        horse_data['owner'] = root.find('table', class_='db_prof_table').find_all('td')[2].text

                    elif prof_key == 'セリ取引価格':
                        horse_data['price'] = root.find('table', class_='db_prof_table').find_all('td')[5].text
                except:
                    pass

            # pandasに追加
            series = pd.Series([idx, horse_data['name'], horse_data['tihou_horse_flg'], horse_data['foreign_horse_flg'], horse_data['sex'], horse_data['hair'], horse_data['father'], horse_data['mother'], horse_data['m_father'], horse_data['birthday'], horse_data['tyokyoshi'], horse_data['owner'], horse_data['price']], index=pd_horse.columns)
            pd_horse = pd_horse.append(series, ignore_index = True)

        except Exception as e:
            print(idx, e)

    # DB追加
    # engine=create_engine("postgresql://sou@localhost:5432/keiba")
    engine=create_engine("postgresql://{0}@{1}:{2}/keiba".format(os.environ["PSQL_USER"], os.environ["PSQL_HOST"], os.environ["PSQL_PORT"], os.environ["PSQL_DB"],))
    pd_horse.to_sql('horse', engine, if_exists='append', index=False)




# レース情報を集める
def makeRaceDB(year, conn, cur):
    race_url = 'http://db.netkeiba.com/race/'
    # 1:札幌, 2:函館, 3:福島, 4:新潟, 5:東京, 6:中山, 7:中京, 8:京都, 9:阪神, 10:小倉
    place_l = [str(i).zfill(2) for i in range(1, 11)]
    time_l  = [str(i).zfill(2) for i in range(1, 7)]
    day_l   = [str(i).zfill(2) for i in range(1, 13)]
    race_l  = [str(i).zfill(2) for i in range(1, 13)]

    pd_result = pd.DataFrame([], columns=['horse_id', 'race_id', 'goal', 'waku_ban', 'uma_ban', 'horse_name', 'horse_sex', 'horse_age', 'weight', 'jockey', 'race_time', 'margin', 'passing_first', 'passing_second', 'passing_third', 'passing_forth', 'rise', 'win_odds', 'popularity', 'horse_weight', 'horse_weight_change', 'tyokyoshi', 'owner', 'prize'])

    pd_race = pd.DataFrame([], columns=['race_id', 'race_name', 'race_year', 'race_place', 'race_time', 'race_day', 'race_order', 'race_type', 'race_direction', 'race_distance', 'race_whether', 'race_baba', 'lap', 'pace'])

    pd_odds = pd.DataFrame([], columns=['race_id', 'type', 'kaime', 'odds', 'popularity'])

    for place, r_time, day, race_order in itertools.product(place_l, time_l, day_l, race_l):
        # try:
            # 過度なアクセスを抑えるために待ち時間を設定
            time.sleep(2)

            race_id = str(year) + place + r_time + day + race_order
            race_id = '200103020105'
            url     = race_url + race_id
            src_html = ul.request.urlopen(url).read()
            root = BeautifulSoup(src_html)

            # DBが存在しない場合
            # if root.find('div', class_='horse_title') is None:
            #     print('{0} is not found'.format(idx))
            #     continue

            # レース情報を取得
            race_info = root.find('dl', class_='racedata')
            race_name = race_info.find('h1').text   # レース名
            race_type = race_info.find('span').text.replace('\xa0', '').split('/')[0][0]        # 芝 or ダート
            race_direction = race_info.find('span').text.replace('\xa0', '').split('/')[0][1]   # 左回り or 右回り
            race_distance  = int(race_info.find('span').text.replace('\xa0', '').split('/')[0][::-1][:5][::-1].strip('m'))
            race_whether   = race_info.find('span').text.replace('\xa0', '').split('/')[1].split(':')[1].strip(' ')   # 天候
            race_baba      = race_info.find('span').text.replace('\xa0', '').split('/')[2].split(':')[1].strip(' ')   # 馬場状況

            # レース結果を取得
            results = root.find('table', class_='race_table_01 nk_tb_common').find_all('tr')[1:]
            for result in results:
                result_record = result.find_all('td')
                goal = result_record[0].text         # 着順（「取」・「除」が存在）
                waku_ban = result_record[1].text     # 枠順
                uma_ban  = result_record[2].text     # 馬番
                horse_name = result_record[3].text.strip('\n')   # 馬名
                horse_id  = result_record[3].a.get('href').split('/')[-2]   # 馬ID（URLの最後）
                horse_sex = result_record[4].text[0]     # 性別
                horse_age = result_record[4].text[1:]    # 年齢
                weight    = float(result_record[5].text)   # 斤量
                jockey    = result_record[6].text.strip('\n')   # 騎手
                race_time = float(result_record[7].text.split(':')[0])*60 + float(result_record[7].text.split(':')[1]) if len(result_record[7].text) > 0 else 0  # レースタイム
                margin    = result_record[8].text    # 着差
                passing_first  = ''    # 第１コーナー通過順位（最終コーナー３個前）
                passing_second = ''    # 第２コーナー通過順位（最終コーナー２個前）
                passing_third  = ''    # 第３コーナー通過順位（最終コーナー１個前）
                passing_forth  = ''    # 第４コーナー通過順位（最終コーナー）
                if len(result_record[10].text) > 0:
                    for i_pass, passing in enumerate(result_record[10].text.split('-')[::-1]):
                        if i_pass == 0: passing_forth  = passing
                        if i_pass == 1: passing_third  = passing
                        if i_pass == 2: passing_second = passing
                        if i_pass == 3: passing_first  = passing
                rise     = float(result_record[11].text) if len(result_record[7].text) > 0 else 0    # 上がり３ハロンタイム
                win_odds = float(result_record[12].text) if len(result_record[7].text) > 0 else 0   # 単勝オッズ
                popularity   = int(result_record[13].text) if len(result_record[7].text) > 0 else 0  # 単勝人気
                horse_weight        = 0
                horse_weight_change = 0
                if result_record[14].text != '計不' and len(result_record[14].text) > 0:
                    horse_weight = int(result_record[14].text.split('(')[0])   # 馬体重
                    horse_weight_change = int(result_record[14].text.split('(')[1].strip(')'))   # 馬体重増減
                tyokyoshi = result_record[18].text.strip('\n')   # 調教師
                owner     = result_record[19].text.strip('\n')   # 馬主
                prize = float(result_record[20].text.replace(',', '')) if len(result_record[20])>0 else 0    # 賞金（万円）

                result_series = pd.Series([horse_id, race_id, goal, waku_ban, uma_ban, horse_name, horse_sex, horse_age, weight, jockey, race_time, margin, passing_first, passing_second, passing_third, passing_forth, rise, win_odds, popularity, horse_weight, horse_weight_change, tyokyoshi, owner, prize], index=pd_result.columns)
                pd_result = pd_result.append(result_series, ignore_index = True)


            # 払い戻し情報を取得
            odds_table = root.find('dl', class_='pay_block').find('dd').find_all('tr')
            kaime_list = [str(x) for x in odds_table[1].find_all('td')[0].contents if 'br' not in str(x)]
            for odds_row in odds_table:
                baken_type = odds_row.find('th').text
                kaime_list = [str(x) for x in odds_row.find_all('td')[0].contents if 'br' not in str(x)]
                odds_list  = [int(str(x).replace(',', '')) for x in odds_row.find_all('td')[1].contents if 'br' not in str(x)]
                popularity_list = [int(x) for x in odds_row.find_all('td')[2].contents if 'br' not in str(x)]
                for kaime, odds, popularity in zip(kaime_list, odds_list, popularity_list):
                    odds_series = pd.Series([race_id, baken_type, kaime, odds, popularity], index=pd_odds.columns)
                    pd_odds = pd_odds.append(odds_series, ignore_index = True)


            # ラップタイムを取得
            lap_time = root.find('table', summary='ラップタイム').find_all('td')
            lap  = lap_time[0].text   # ラップ
            pace = lap_time[1].text   # ペース
            race_series = pd.Series([race_id, race_name, year, place, r_time, day, race_order, race_type, race_direction, race_distance, race_whether, race_baba, lap, pace], index=pd_race.columns)
            pd_race = pd_race.append(race_series, ignore_index = True)

        # except Exception as e:
            # print(e)

    # DB追加
    result_engine = create_engine("postgresql://sou@localhost:5432/keiba")
    race_engine   = create_engine("postgresql://sou@localhost:5432/keiba")
    odds_engine   = create_engine("postgresql://sou@localhost:5432/keiba")
    # result_engine = create_engine("postgresql://{0}@{1}:{2}/keiba".format(os.environ["PSQL_USER"], os.environ["PSQL_HOST"], os.environ["PSQL_PORT"], os.environ["PSQL_DB"],))
    # race_engine   = create_engine("postgresql://{0}@{1}:{2}/keiba".format(os.environ["PSQL_USER"], os.environ["PSQL_HOST"], os.environ["PSQL_PORT"], os.environ["PSQL_DB"],))
    # odds_engine   = create_engine("postgresql://{0}@{1}:{2}/keiba".format(os.environ["PSQL_USER"], os.environ["PSQL_HOST"], os.environ["PSQL_PORT"], os.environ["PSQL_DB"],))
    pd_result.to_sql('result', result_engine, if_exists='append', index=False)
    pd_race.to_sql('race', race_engine, if_exists='append', index=False)
    pd_odds.to_sql('odds', odds_engine, if_exists='append', index=False)


if __name__ == '__main__':
    year_l = [i for i in range(2001,2016)]
    #pool = mp.Pool(__PROC__)C
    #pool.map(makeHorseDB, year_l)

    # conn = psycopg2.connect("dbname={0} host={1} user={2} port={3} password={4}".format(os.environ["PSQL_DB"], os.environ["PSQL_HOST"], os.environ["PSQL_USER"], os.environ["PSQL_PORT"], os.environ["PSQL_PASS"]))
    conn = psycopg2.connect("dbname=keiba host=localhost user=sou")
    cur = conn.cursor()

    for year in year_l:
        # makeHorseDB(year, conn, cur)
        makeRaceDB(year, conn, cur)

    cur.close()
    conn.close()









