# coding=utf-8
import pandas as pd
# import modin.pandas as pd
import warnings
import sys
import datetime
import os
import pickle
import ray
import numpy as np
import dtreeLoopMD as md
import dtreeLoopPM as pm
import dtreeLoopPMN as pmn

warnings.filterwarnings('ignore')


def executeMD(name, path, paramLevel, paramLastRatio, paramLoop, paramRandomRatio, paramMemoryYn):
    ray.init(num_cpus=20, log_to_driver=False)
    branch = 11

    # 분석할 데이터
    # if name == 'kbuy2':
    #     data = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/" + "kbuy2" + "_com_loop2.csv")
    #     startLoopNum = 2
    # else:
    data = pd.read_csv(path + name + '_com.csv')
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

    lastFinal = lastFinal.astype(
        {'dcnt': 'int', 'bcnt': 'int', 'bvsd': 'float', 'dvsb': 'float', 'entr': 'float'})

    lastFinal = lastFinal[lastFinal['condi'] != '']
    lastFinal = lastFinal[lastFinal['dvsb'] >= limitCnt]

    tmpLoopFinal = tmpLoopFinal.append(lastFinal)
    # lastFinal = pd.read_csv('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/[Loop - 1][MD][tree9]kbuy2_result.csv')
    lastFinal['rDcnt'] = 0
    lastFinal['rBcnt'] = 0
    lastFinal['rDvsb'] = 0
    lastFinal['vIndex'] = ''

    lastFinal = lastFinal.dropna()

    # _com 데이터에서 만들어낸 조건들을 11year 데이터에 대입해본다.
    for m in range(0, len(lastFinal)):
        print(str(m) + ' / ' + str(len(lastFinal)) + ' - ' + lastFinal.iloc[m]['condi'])
        lvindex, lvdcnt, lvbcnt, lvdvsb, lvData = md.checkCondition(initData, lastFinal.iloc[m]['condi'])
        lastFinal['rDcnt'].iloc[m] = lvdcnt
        lastFinal['rBcnt'].iloc[m] = lvbcnt
        lastFinal['rDvsb'].iloc[m] = lvdvsb
        lastFinal['vIndex'].iloc[m] = lvindex

        # lvindex, lvdcnt, lvbcnt, lvdvsb, lvData = md.checkCondition(data, lastFinal.iloc[m]['condi'])
        # lastFinal['index'].iloc[m] = lvindex

    # loop 돌기 전 입력한 비율 값 보다 큰 rDcnt & rBcnt가 0인 조건을 만족하는 data만 빼낸다.
    lastFinal = lastFinal[(lastFinal['rDcnt'] >= limitCnt) & (lastFinal['rBcnt'] < 1)]

    data['delYn'] = 'N'

    if len(lastFinal) > 0:
        for z in range(0, len(lastFinal)):
            # z=0
            try:
                # data = realInitData.drop(index=md.checkCondition(realInitData, lastFinal.iloc[z].condi)[0])
                data['delYn'].iloc[lastFinal['index'].iloc[z]] = 'Y'
            except:
                pass

    data = data[data['delYn'] == 'N'] # 30744 - 25420 / 5324
    data.to_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/[MD]" + name + "_com_loop" + vLoop + ".csv")
    # data.value_counts('pur_gubn5')
    tmpLoopFinal.to_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/[MD]" + name + "_result.csv")

    return initData, limitCnt

def executePM(name, path, paramLevel, paramLastRatio, paramLoop, paramRandomRatio, paramMemoryYn):
    ray.init(num_cpus=20, log_to_driver=False)
    branch = 11

    # 분석할 데이터
    data = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/[MD]" + name + "_com_loop" + str(paramLoop) + ".csv")
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

    lastFinal = lastFinal.astype(
        {'dcnt': 'int', 'bcnt': 'int', 'bvsd': 'float', 'dvsb': 'float', 'entr': 'float'})

    lastFinal = lastFinal[lastFinal['condi'] != '']

    tmpLoopFinal = tmpLoopFinal.append(lastFinal)

    lastFinal['rDcnt'] = 0
    lastFinal['rBcnt'] = 0
    lastFinal['rDvsb'] = 0
    lastFinal['vIndex'] = ''

    # _com 데이터에서 만들어낸 조건들을 11year 데이터에 대입해본다.
    for m in range(0, len(lastFinal)):
        print(str(m) + ' / ' + str(len(lastFinal)) + lastFinal.iloc[m]['condi'])
        lvindex, lvdcnt, lvbcnt, lvdvsb, lvData = pm.checkCondition(initData, lastFinal.iloc[m]['condi'])
        lastFinal['rDcnt'].iloc[m] = lvdcnt
        lastFinal['rBcnt'].iloc[m] = lvbcnt
        lastFinal['rDvsb'].iloc[m] = lvdvsb
        lastFinal['vIndex'].iloc[m] = lvindex

    # loop 돌기 전 입력한 비율 값 보다 큰 rDcnt & rBcnt가 0인 조건을 만족하는 data만 빼낸다.
    lastFinal = lastFinal[(lastFinal['rDcnt'] >= limitCnt) & (lastFinal['rBcnt'] < 1)]

    data['delYn'] = 'N'

    if len(lastFinal) > 0:
        for z in range(0, len(lastFinal)):
            try:
                # data = realInitData.drop(index=pm.checkCondition(realInitData, lastFinal.iloc[z].condi)[0])
                data['delYn'].iloc[lastFinal['index'].iloc[z]] = 'Y'
            except:
                pass
    else:
        pass

    data = data[data['delYn'] == 'N']
    data.to_csv(
        "C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/[PM]" + name + "_com_loop" + vLoop + ".csv")
    # data.value_counts('pur_gubn5')
    tmpLoopFinal.to_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/[PM]" + name + "_result.csv")

    return initData, limitCnt

def executePMN(name, path, paramLevel, paramLastRatio, paramLoop, paramRandomRatio, paramMemoryYn):
    ray.init(num_cpus=20, log_to_driver=False)
    branch = 11

    # 분석할 데이터
    data = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/[PM]" + name + "_com_loop" + str(paramLoop) + ".csv")
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

    lastFinal = lastFinal[lastFinal['condi'] != '']

    tmpLoopFinal = tmpLoopFinal.append(lastFinal)

    lastFinal['rDcnt'] = 0
    lastFinal['rBcnt'] = 0
    lastFinal['rDvsb'] = 0
    lastFinal['vIndex'] = ''

    # _com 데이터에서 만들어낸 조건들을 11year 데이터에 대입해본다.
    for m in range(0, len(lastFinal)):
        print(str(m) + ' / ' + str(len(lastFinal)) + lastFinal.iloc[m]['condi'])
        lvindex, lvdcnt, lvbcnt, lvdvsb, lvData = pmn.checkCondition(initData, lastFinal.iloc[m]['condi'])
        lastFinal['rDcnt'].iloc[m] = lvdcnt
        lastFinal['rBcnt'].iloc[m] = lvbcnt
        lastFinal['rDvsb'].iloc[m] = lvdvsb
        lastFinal['vIndex'].iloc[m] = lvindex

    # loop 돌기 전 입력한 비율 값 보다 큰 rDcnt & rBcnt가 0인 조건을 만족하는 data만 빼낸다.
    lastFinal = lastFinal[(lastFinal['rDcnt'] >= limitCnt) & (lastFinal['rBcnt'] < 1)]

    data['delYn'] = 'N'

    if len(lastFinal) > 0:
        for z in range(0, len(lastFinal)):
            try:
                # data = realInitData.drop(index=pmn.checkCondition(realInitData, lastFinal.iloc[z].condi)[0])
                data['delYn'].iloc[lastFinal['index'].iloc[z]] = 'Y'
            except:
                pass
    else:
        pass

    data = data[data['delYn'] == 'N']

    try:
        data.to_csv(
            "C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/[PMN]" + name + "_com_loop" + vLoop + ".csv")
    except:
        pass

    try:
        tmpLoopFinal.to_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/[PMN]" + name + "_result.csv")
    except:
        pass

    return initData, limitCnt

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


if __name__ == '__main__':
    #####################################################################################################
    # 파라미터 세팅   초기 조건 만들지 안고 계속 11개 가지 치기
    # 해당 가지에서 조건 추출 후 컬럼 삭제
    #####################################################################################################
    paramLevel = 8  # 초기 LEVEL수
    paramLastRatio = 2  # 마지막 레벨에서 dvsb가 x:1 이상인 것만 돌리게 하는 조건
    paramLoop = 5  # loop Count Setting
    paramRandomRatio = 1  # 전체 data에서 random으로 뽑을 비율 (2022.06.15 Add)
    paramMemoryYn = 'Y'   # Pickle파일을 메모리로 사용할 것인지 여부
    #####################################################################################################

    #####################################################################################################
    # kbuy2
    #####################################################################################################
    name = ['kbuy2','kbuy1','sbuy3','sbuy2','ncbuy3','ncbuy2','sjtabuy']  # 사용자 지정 명
    path = "C:/Users/Shine_anal/Desktop/inott/"  # 사용자 지정명 + _com.csv 파일이 존재하는 폴더 (분석할 csv파일)

    for paramName in name:
        for paramLoopDetail in range(0, paramLoop):
            initDataMD, limitCntMD = executeMD(paramName, path, paramLevel, paramLastRatio, paramLoopDetail, paramRandomRatio, paramMemoryYn)
            initDataPM, limitCntPM = executePM(paramName, path, paramLevel, paramLastRatio, paramLoopDetail, paramRandomRatio, paramMemoryYn)
            initDataPMN, limitCntPMN = executePMN(paramName, path, paramLevel, paramLastRatio, paramLoopDetail, paramRandomRatio, paramMemoryYn)
            fResultMid = md.makeFinalSet(path, paramName, paramLastRatio, initData, limitCnt)
            ray.shutdown()
    #####################################################################################################
    # 최종결과파일명   사용자 지정 이름__ddelTreeLoopFinal_result.csv
    #####################################################################################################
    print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
    print('최종결과파일은 ' + path + name + '_ddelTreeLoopFinal_result.csv  입니다.')
    print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
    #####################################################################################################
