import pandas as pd
# import modin.pandas as pd
import warnings
import sys
import datetime
import os
# coding=utf-8
import pickle
import ray
import numpy as np
import dtreeLoopMD as md
import dtreeLoopPM as pm
import dtreeLoopPMN as pmn
import shutil


warnings.filterwarnings('ignore')

def calcRatioLoop(data):
    df = data

    if df.value_counts('pur_gubn5')[0] >= 8000:
        paramLimitRatio = 0.1  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 3500:
        paramLimitRatio = 0.2  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 2500 and df.value_counts('pur_gubn5')[0] <= 3499:
        paramLimitRatio = 0.3  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 1500 and df.value_counts('pur_gubn5')[0] <= 2499:
        paramLimitRatio = 0.4  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 900 and df.value_counts('pur_gubn5')[0] <= 1499:
        paramLimitRatio = 0.7  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 600 and df.value_counts('pur_gubn5')[0] <= 899:
        paramLimitRatio = 0.9  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 400 and df.value_counts('pur_gubn5')[0] <= 599:
        paramLimitRatio = 1.2  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 250 and df.value_counts('pur_gubn5')[0] <= 399:
        paramLimitRatio = 1.5  # 초기데이터 중 dcnt대비
    elif df.value_counts('pur_gubn5')[0] >= 50 and df.value_counts('pur_gubn5')[0] <= 249:
        paramLimitRatio = 2.2  # 초기데이터 중 dcnt대비

    return paramLimitRatio

def executeMD(name, path, paramLevel, paramLastRatio, paramLoop, paramRandomRatio, paramMemoryYn, data):
    ray.init(num_cpus=20, log_to_driver=False)
    branch = 11

    # 분석할 데이터
    # if name == 'kbuy2':
    #     data = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/" + "kbuy2" + "_com_loop2.csv")
    #     startLoopNum = 2
    # else:

    for i in range(len(data.columns)):
        if data.columns[i] == 'yyyymmdd':
            stopCol = i

    data = data.iloc[:, stopCol:]
    # startLoopNum = 0
    realInitData = data

    # 검증할 데이터
    initData = pd.read_csv(path + name + '_11year.csv')

    initBcnt = data['pur_gubn5'].value_counts()[1]
    initDcnt = data['pur_gubn5'].value_counts()[0]
    lastFinal = pd.DataFrame()
    tmpFF = pd.DataFrame()
    tmpLoopFinal = pd.DataFrame()
    mod = sys.modules[__name__]
    data = data.sample(frac=paramRandomRatio, random_state=104)
    # vLoop = 0
    # for vLoop in range(startLoopNum, paramLoop):

    vLoop = paramLoop

    # random으로 데이터 봅기

    limitCnt = initDcnt * calcRatioLoop(data) * 0.01
    print('limitCnt : ' + str(limitCnt))

    # vLoop = 0
    tmpDcnt = data['pur_gubn5'].value_counts()[0]
    tmpBcnt = data['pur_gubn5'].value_counts()[1]

    ######################################################################################################################################
    # 4레벨까지 원래 도는 부분
    ######################################################################################################################################
    # PICKLE FILE DELETE
    md.createFolder("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/" + name + 'New')
    md.removeAllFile("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/" + name + 'New')

    lastFinal = lastFinal.append(
        md.makeLevel(vLoop, paramLevel, paramLastRatio, limitCnt, name, data, '', 'N', paramMemoryYn))

    lastFinal.to_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/loopFinal_[MD]" + name + "_com_loop" + str(vLoop) + ".csv")
    # data.value_counts('pur_gubn5')

    return lastFinal

def executePM(name, path, paramLevel, paramLastRatio, paramLoop, paramRandomRatio, paramMemoryYn, data):
    ray.init(num_cpus=20, log_to_driver=False)
    branch = 11

    # 분석할 데이터
    for i in range(len(data.columns)):
        if data.columns[i] == 'yyyymmdd':
            stopCol = i

    data = data.iloc[:, stopCol:]

    data = data.sample(frac=paramRandomRatio, random_state=104)
    # 검증할 데이터
    initData = pd.read_csv(path + name + '_11year.csv')

    initBcnt = data['pur_gubn5'].value_counts()[1]
    initDcnt = data['pur_gubn5'].value_counts()[0]
    lastFinal = pd.DataFrame()
    tmpFF = pd.DataFrame()
    tmpLoopFinal = pd.DataFrame()
    mod = sys.modules[__name__]

    vLoop = paramLoop

    # for vLoop in range(0, paramLoop):
    # random으로 데이터 봅기
    # data = data.sample(frac=paramRandomRatio, random_state=104)
    limitCnt = initDcnt * calcRatioLoop(data) * 0.01
    print('limitCnt : ' + str(limitCnt))

    # vLoop = 0
    tmpDcnt = data['pur_gubn5'].value_counts()[0]
    tmpBcnt = data['pur_gubn5'].value_counts()[1]

    ######################################################################################################################################
    # 4레벨까지 원래 도는 부분
    ######################################################################################################################################
    # PICKLE FILE DELETE
    pm.createFolder("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/" + name + 'New')
    pm.removeAllFile("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/" + name + 'New')

    lastFinal = lastFinal.append(
        pm.makeLevel(vLoop, paramLevel, paramLastRatio, limitCnt, name, data, '', 'N', paramMemoryYn))

    lastFinal.to_csv(
        "C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/loopFinal_[PM]" + name + "_com_loop" + str(vLoop) + ".csv")
    # data.value_counts('pur_gubn5')

    return lastFinal

def executePMN(name, path, paramLevel, paramLastRatio, paramLoop, paramRandomRatio, paramMemoryYn, data):
    ray.init(num_cpus=20, log_to_driver=False)
    branch = 11
    # paramLoop = 0
    # 분석할 데이터
    for i in range(len(data.columns)):
        if data.columns[i] == 'yyyymmdd':
            stopCol = i

    data = data.iloc[:, stopCol:]

    data = data.sample(frac=paramRandomRatio, random_state=104)
    realInitData = data

    # 검증할 데이터
    initData = pd.read_csv(path + name + '_11year.csv')

    initBcnt = data['pur_gubn5'].value_counts()[1]
    initDcnt = data['pur_gubn5'].value_counts()[0]
    lastFinal = pd.DataFrame()
    tmpFF = pd.DataFrame()
    tmpLoopFinal = pd.DataFrame()
    mod = sys.modules[__name__]

    # vLoop = 0
    vLoop = paramLoop
    # for vLoop in range(0, paramLoop):
    # random으로 데이터 봅기
    limitCnt = initDcnt * calcRatioLoop(data) * 0.01
    print('limitCnt : ' + str(limitCnt))

    # vLoop = 0
    tmpDcnt = data['pur_gubn5'].value_counts()[0]
    tmpBcnt = data['pur_gubn5'].value_counts()[1]

    ######################################################################################################################################
    # 4레벨까지 원래 도는 부분
    ######################################################################################################################################
    # PICKLE FILE DELETE
    pmn.createFolder("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/" + name + 'New')
    pmn.removeAllFile("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/" + name + 'New')

    lastFinal = lastFinal.append(
        pmn.makeLevel(vLoop, paramLevel, paramLastRatio, limitCnt, name, data, '', 'N', paramMemoryYn))

    lastFinal = lastFinal.astype(
        {'dcnt': 'int', 'bcnt': 'int', 'bvsd': 'float', 'dvsb': 'float', 'entr': 'float'})

    lastFinal.to_csv(
            "C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/loopFinal_[PMN]" + name + "_com_loop" + str(vLoop) + ".csv")

    return lastFinal

def makeFinalResult(name):
    lists = os.listdir("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/")
    file_list_rslt = [file for file in lists if file.startswith('[Final]' + name)]

    # 결과 합치기
    fResultMid = pd.DataFrame()

    for i in range(0, len(file_list_rslt)):
        # print("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/" + file_list_rslt[i])
        tmp = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/" + file_list_rslt[i])
        fResultMid = fResultMid.append(tmp)
        print('------------------------------- / ' + file_list_rslt[i])

    fResultMid.to_csv(path + name + "_ddelTreeLoopFinal_result_{}.csv".format(datetime.datetime.today().strftime(
                        "%Y%m%d%H%M%S")))

    lists2 = os.listdir("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/")
    file_list_rslt2 = [file for file in lists2 if file.endswith('_result.csv')]

    for i in range(0, len(file_list_rslt2)):
        os.remove("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/" + file_list_rslt2[i])

if __name__ == '__main__':
    #####################################################################################################
    # 파라미터 세팅   초기 조건 만들지 안고 계속 11개 가지 치기
    # 해당 가지에서 조건 추출 후 컬럼 삭제
    #####################################################################################################
    paramLevel = 7  # 초기 LEVEL수
    paramLastRatio = 2  # 마지막 레벨에서 dvsb가 x:1 이상인 것만 돌리게 하는 조건
    paramLoop = 5  # loop Count Setting
    paramRandomRatio = 1  # 전체 data에서 random으로 뽑을 비율 (2022.06.15 Add)
    paramMemoryYn = 'Y'   # Pickle파일을 메모리로 사용할 것인지 여부
    #####################################################################################################

    #####################################################################################################
    # paramName,paramLoopDetail = 'sjtabuy', 2
    #####################################################################################################
    # name = ['sjtabuy', 'ncbuy2', 'ncbuy3', 'kbuy1', 'kbuy2' , 'sbuy2', 'sbuy3']  # 사용자 지정 명
    name=['sjtabuy']
    # name = ['kbuy2','sjtabuy', 'ncbuy2', 'ncbuy3', 'kbuy1',  'sbuy2', 'sbuy3']  # 사용자 지정 명
    path = "C:/Users/Shine_anal/Desktop/inott/"  # 사용자 지정명 + _com.csv 파일이 존재하는 폴더 (분석할 csv파일)
    
    ray.shutdown()
    for paramName in name:
        startLoop = 0
        
        for paramLoopDetail in range(startLoop, paramLoop):
            if paramLoopDetail == 0:
                data = pd.read_csv(path + paramName + '_com.csv')
            else:
                stopDcnt = data.value_counts('pur_gubn5')[0]
                data = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/" + paramName + "_com_loop" + str(paramLoopDetail - 1) + ".csv")

                if data.value_counts('pur_gubn5')[0] < 100 or data.value_counts('pur_gubn5')[0] > stopDcnt * 0.7:
                    break
        
            ratioDfMD = executeMD(paramName, path, paramLevel, paramLastRatio, paramLoopDetail, paramRandomRatio, paramMemoryYn, data)
            ray.shutdown()
            ratioDfPM = executePM(paramName, path, paramLevel, paramLastRatio, paramLoopDetail, paramRandomRatio, paramMemoryYn, data)
            ray.shutdown()
            ratioDfPMN = executePMN(paramName, path, paramLevel, paramLastRatio, paramLoopDetail, paramRandomRatio, paramMemoryYn, data)
            ray.shutdown()
            fResultMid = md.makeFinalSet(path, paramName, paramLoopDetail)
            ray.shutdown()
        
        makeFinalResult(paramName)
    
    #####################################################################################################
    # 최종결과파일명   사용자 지정 이름__ddelTreeLoopFinal_result.csv
    #####################################################################################################
    print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
    print('최종결과파일은 ' + path + name + '_ddelTreeLoopFinal_result.csv  입니다.')
    print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
    #####################################################################################################

