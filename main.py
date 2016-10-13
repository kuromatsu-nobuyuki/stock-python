# -*- coding: utf-8 -*-
import sys
sys.path.append("/opt/conda/envs/python2/lib/python2.7/site-packages/")
import urllib2
import datetime
import os
import time
import numpy as np
import pandas as pd

base = "http://k-db.com/stocks/"
dataDir = "/data/data"
featureDir = "/data/inputs"

def download(fileURL, filePath):
    try:
        csv = urllib2.urlopen(fileURL)
        l_file = open(filePath, "w")
        l_file.write(csv.read())
        l_file.close()
    except e:
        print("Error " + filePath + " can not created, code " + str(e.code))
        return e.code

# ゼロパディング
def zeroPadding(num):
    return '{0:02d}'.format(num)

# ゼロパディングで日付を返す
def date_to_string(date):
    return "{0:04d}".format(date.year)+ "-" + "{0:02d}".format(date.month) + "-" + "{0:02d}".format(date.day)

# 取得するデータのurlを返す
def get_CSV_URL(date):
    return base + date_to_string(date) + "?download=csv"

# 今日の分のデータのファイルパスを返す
def makeFilePath(date):
    weekday = date.weekday()
    return dataDir + "/" + date_to_string(date) + "-" + str(date.weekday()) + ".csv"

# path以下のうち更新日が最も新しいファイル or ディレクトリのパスを返す
def getLatestFile(path):
    ret = None
    last_updated_time = None
    for item in os.listdir(path):
        itempath = os.path.join(path, item)
        file_updated = os.path.getmtime(itempath)
        if last_updated_time == None or last_updated_time < file_updated:
            ret = itempath
            last_updated_time = file_updated
    return ret

# 与えられたパスにあるcsvをpandasにして日付と曜日情報を加えて返す
def pathToPandas(path):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return pd.DataFrame()
    data = pd.read_csv(path, encoding='Shift_JIS')
    filename = path.split("/")[3]
    data['年'] = int(filename[0:4])
    data['月'] = int(filename[5:7])
    data['日'] = int(filename[8:10])
    data['曜日'] = int(filename[11])
    return data

# 今日の分の株価データを取得する
def getTodaysFile(date):
    print("donwload " + date_to_string(date) + "...\n")
    csvFilePath = makeFilePath(date)
    url = get_CSV_URL(date)
    retry = 0
    code = download(url, csvFilePath)
    while(code == 503 and (retry < 5)):
        retry += 1
        time.sleep(3)
        print("retry " + csvFilePath + "(" + str(retry) + ")")
        code = download(url, csvFilePath)
    return

# 存在しない場合ディレクトリを作る
def makeDir(path):
    if not os.path.exists(path):
        os.makedirs(path)
        
# 特徴量のデータのパスを返す
def getFeaturesDataPath(dirpath, code):
    if dirpath == None:
        return None
    return dirpath + "/" + code + "/feature.csv"

# 特徴量のデータを作る。前日との差分の計算が発生する。
def makeFeature(date):
    today = zeroPadding(date.year) + "-" + zeroPadding(date.month) + "-" + zeroPadding(date.day)
    stockdatapath = dataDir + "/" + today + "-" + str(date.weekday()) + ".csv"
    data = pathToPandas(stockdatapath).dropna()
    # 土日はデータがないので空っぽになるから何もしない
    if data.empty:
        print(stockdatapath + " is not found")
        sys.exit()
    # 株価データの文字コード変換。面倒なので一度ファイルに書き出してデコード
    data.to_csv("/data/tmp.csv", index=False, encoding='utf-8')
    data = pd.read_csv("/data/tmp.csv", encoding='utf-8')
    # 前日のデータが置かれているディレクトリを見つける
    pastdataDir = getLatestFile(featureDir)
    # 特徴量のディレクトリの作成
    todaysDir = featureDir + "/" + today
    makeDir(todaysDir)
    
    writeEachCodeData(data, pastdataDir, todaysDir)
    return

# 各コードのディレクトリにcsvを作成する。pastdataDirがNoneなら初めて作成することを示す。
# data=今日の全コードの株価データ, pastdataDir=前回の取引データがあるディレクトリ
# todaysDir=今日の分のデータの作成場所
def writeEachCodeData(data, pastdataDir, todaysDir):
    targetColumns = [u'始値', u'高値', u'安値', u'終値', u'出来高', u'売買代金']
    size = len(targetColumns)
    col_range =  range(0, size)
    
    for key, row in data.iterrows():
        code = row[u"コード"]
        print(code)
        pastCSVPath = getFeaturesDataPath(pastdataDir, code)
        CSVPath = getFeaturesDataPath(todaysDir, code)
        outDir = todaysDir + "/" + code
        makeDir(outDir)
        if not os.path.exists(pastCSVPath):
            # 過去に一度も株価データの差分を求めていない
            result = pd.pandas.DataFrame(row).T
            result.to_csv(path_or_buf=CSVPath, index=False, encoding='utf-8')
            continue

        pastdata = pd.read_csv(pastCSVPath, index_col=False, encoding='utf-8')
        yesterday = pastdata.tail(1)
        for i in col_range:
            col_name = targetColumns[i]
            substruct_col_name = col_name + u'前日比'
            row[substruct_col_name] = row[col_name] - yesterday[col_name].values[0]
            todayDF = pd.pandas.DataFrame(row).T
        result = pastdata.append(todayDF)
            
        result.to_csv(path_or_buf=CSVPath, index=False, encoding='utf-8')
        
    return

# test make features
if __name__ == "__main__":
    args = sys.argv
    print(args[1])
    if not len(args) == 4:
        print("args error:" + str(args))
        sys.exit(1)
    today = datetime.datetime(int(args[1]), int(args[2]), int(args[3]))
    print today

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
