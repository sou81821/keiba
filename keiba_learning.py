#!/usr/bin/env python
#coding: UTF-8

'''
netkeibaから取得したデータを機械学習にかける
'''

import numpy as np
import pandas as pd
import pdb
from sklearn.ensemble import RandomForestClassifier
import plotly.plotly as py
from plotly.graph_objs import *


def pre_process(horse_prof, performance):
    # レースの日付を月だけにする

    # 着順の処理（1着なら1, それ以外なら0）
    performance["goal"] = performance["goal"].apply(lambda x:1 if x==1 else 0)

    # 馬場状況の処理（良：1, 稍：2, 重：3, 不：4）
    performance["baba"] = performance["baba"].apply(lambda x:1 if x=="良" else 2 if x=="稍" else 3 if x=="重" else 4)

    # 各コーナーの追加位置を，最終コーナーの位置のみにする
    performance["passing"] = performance["passing"].apply(lambda x:x.split("-")[-1])

    # カテゴリ変数はダミー変数にしておく
    dummy_race_type = pd.get_dummies(performance["race_type"])
    dummy_whether   = pd.get_dummies(performance["whether"])

    pdb.set_trace()

    return horse_prof, performance



if __name__ == '__main__':
    horse_prof  = pd.read_csv("keiba_data/2010_horse_table.csv")
    performance = pd.read_csv("keiba_data/2010_horse_performance.csv")

    goal_w = performance[["goal", "w_change"]]
    goal_w[goal_w["goal"]==1]["w_change"]

    pdb.set_trace()

    # データの前処理を行う
    horse_prof, performance = pre_process(horse_prof, performance)

    # performanceの内，実際に入力データとして扱うものに絞る
    performance = performance[["race_date", "area", "whether", "race_num", "num_horses", "waku_ban", "uma_ban", "popularity", "jockey", "basis_w", "race_type", "distance", "baba", "horse_w", "w_change"]]





