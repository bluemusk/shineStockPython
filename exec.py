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
import ddelTreeLoopFinal as jj

warnings.filterwarnings('ignore')


def execute(name, path, paramLevel, paramAftLevel, paramLimitRatio, paramLastRatio, paramLoop):
    ray.init(num_cpus=20, log_to_driver=False)
    branch = 11

    # 분석할 데이터
    data = pd.read_csv(path + name + '_com.csv')
    # 검증할 데이터
    initData = pd.read_csv(path + name + '_11year.csv')
    realInitData = initData

    initBcnt = data['pur_gubn5'].value_counts()[1]
    initDcnt = data['pur_gubn5'].value_counts()[0]
    limitCnt = initDcnt * paramLimitRatio * 0.01
    realFinal = pd.DataFrame()
    lastFinal = pd.DataFrame()
    realFinal['index'] = ''
    tmpFF = pd.DataFrame()
    tmpLoopFinal = pd.DataFrame()
    print('limitCnt : ' + str(limitCnt))
    mod = sys.modules[__name__]

    for vLoop in range(0, paramLoop):
        # vLoop = 0
        tmpDcnt = data['pur_gubn5'].value_counts()[0]
        tmpBcnt = data['pur_gubn5'].value_counts()[1]

        if tmpDcnt < 50:
            break

        ######################################################################################################################################
        # 4레벨까지 원래 도는 부분
        ######################################################################################################################################
        # PICKLE FILE DELETE
        jj.createFolder("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/" + name + 'New')
        jj.removeAllFile("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/" + name + 'New')

        realFinal = realFinal.append(
            jj.makeLevel(vLoop, paramLevel, paramLastRatio, limitCnt, name, branch, data, '', 'N'))

        realFinal = realFinal.astype(
            {'dcnt': 'int', 'bcnt': 'int', 'bvsd': 'float', 'dvsb': 'float', 'entr': 'float'})

        # 4레벨까지 돈 결과에서 bcnt가 0인 녀석들을 모아서 넣는다.
        lastFinal = realFinal[(realFinal['bcnt'] < 1) & (realFinal['dcnt'] > 0.8)]
        # realFinal에서 bcnt가 0인 녀석들을 빼낸다.
        realFinal = realFinal[~((realFinal['bcnt'] < 1) & (realFinal['dcnt'] > 0.8))]
        # realFinal에서 초기 세팅한 limitCnt dcnt가 큰 녀석들만 남긴다.
        realFinal = realFinal[realFinal['dcnt'] >= limitCnt]

        realFinal = realFinal.sort_values('dvsb', ascending=False)

        lvl4Conds = pd.DataFrame()
        lvl4Data = data

        # 각 조건 별로 bcnt, dcnt, dvsb를 다시 계산
        if len(realFinal) > 0:
            for y in range(0, len(realFinal)):
                realFinal = realFinal.sort_values('dvsb', ascending=False)
                try:
                    if realFinal.iloc[0].dcnt > 0:
                        pass
                except:
                    break

                if realFinal.iloc[0].dcnt >= limitCnt:
                    # realFinal을 dvsb 내림차순으로 정렬
                    realFinal = realFinal[realFinal['dvsb'] >= 0.6]
                    realFinal = realFinal.sort_values('dvsb', ascending=False)

                    for x in range(0, len(realFinal)):
                        # x=0
                        # limitCnt 보다 큰 녀석들 중 dvsb가 가장 큰 녀석을 뽑고 데이터도 빼낸다.
                        lvindex, lvdcnt, lvbcnt, lvdvsb, lvData = jj.checkCondition(lvl4Data,
                                                                                    realFinal.iloc[x]['condi'])
                        realFinal['index'].iloc[x] = lvindex
                        realFinal['dcnt'].iloc[x] = lvdcnt
                        realFinal['bcnt'].iloc[x] = lvbcnt
                        realFinal['dvsb'].iloc[x] = lvdvsb

                        print(str(y) + '-' + str(x) + ' - bcnt : ' + str(lvbcnt) + ' / dcnt : ' + str(
                            lvdcnt) + ' / dvsb : ' + str(round(lvdcnt / lvbcnt, 2)) + ' / ' + realFinal.iloc[x][
                                  'condi'])
                    # kk = realFinal[realFinal['condi'] == 'dis5_ang1_dis20_ang1 > -5.7 AND atr_ang3_1bd > 11.7 AND dis5_ang1 <= 8.8 AND dis10_dis60 > -34.9']
                    ################################################################################################################
                    # realFinal을 dvsb 내림차순으로 정렬
                    realFinal = realFinal.sort_values('dvsb', ascending=False)

                    # lvl4Conds에 가장 좋은 조건을 저장한다
                    lvl4Conds = lvl4Conds.append(
                        pd.DataFrame([(realFinal.iloc[0]['condi'], realFinal.iloc[0]['bcnt'], realFinal.iloc[0]['dcnt'],
                                       realFinal.iloc[0]['dvsb'])]
                                     , columns=['condi', 'bcnt', 'dcnt', 'dvsb'])
                    )

                    # 최종 결과 파일에 저장
                    lastFinal = lastFinal.append(realFinal.iloc[0])

                    # dvsb가 가장 좋은 조건의 데이터를 빼낸다
                    try:
                        lvl4Data = lvl4Data.drop(index=realFinal.iloc[0]['index'])
                    except:
                        pass

                    # realFinal에서 가장 좋은 조건 삭제
                    realFinal = realFinal[~(realFinal['condi'] == realFinal.iloc[0]['condi'])]

                    ################################################################################################################
                    # lvl4Cond에 설정한 dvsb보다 크고 dcnt가 가장 큰 조건을 저장한다..
                    tmpRealFinal = realFinal[realFinal['dvsb'] >= paramLastRatio]

                    if len(tmpRealFinal) > 0:
                        # realFinal을 dcnt 내림차순으로 정렬
                        tmpRealFinal = tmpRealFinal.sort_values(['dcnt', 'bcnt'], ascending=[False, True])

                        lvl4Conds = lvl4Conds.append(
                            pd.DataFrame([(tmpRealFinal.iloc[0]['condi'], tmpRealFinal.iloc[0]['bcnt'],
                                           tmpRealFinal.iloc[0]['dcnt'],
                                           tmpRealFinal.iloc[0]['dvsb'])]
                                         , columns=['condi', 'bcnt', 'dcnt', 'dvsb'])
                        )

                        # 최종 결과 파일에 저장
                        lastFinal = lastFinal.append(tmpRealFinal.iloc[0])

                        # realFinal에서 가장 좋은 조건 삭제
                        realFinal = realFinal[~(realFinal['condi'] == tmpRealFinal.iloc[0]['condi'])]
                    ################################################################################################################

                else:
                    break
        ######################################################################################################################################
        # 4레벨 이후 2개 레벨 더 도는 부분
        ######################################################################################################################################
        # vLoop = 0
        for u in range(0, len(lvl4Conds)):
            # PICKLE FILE DELETE
            jj.createFolder("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/" + name + 'New')
            jj.removeAllFile("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/" + name + 'New')
            print(str(u) + ' - ' + lvl4Conds.iloc[u].condi)
            tempGG = jj.makeLevel(vLoop, paramAftLevel, paramLastRatio, limitCnt, name, branch,
                                  jj.checkCondition(data, lvl4Conds.iloc[u].condi)[4], lvl4Conds.iloc[u].condi, 'Y')
            lastFinal = lastFinal.append(tempGG)

        lastFinal = lastFinal.astype(
            {'dcnt': 'int', 'bcnt': 'int', 'bvsd': 'float', 'dvsb': 'float', 'entr': 'float'})

        lastFinal = lastFinal.sort_values('dvsb', ascending=False)
        lastFinal = lastFinal[lastFinal['dvsb'] >= paramLastRatio]

        try:
            lastFinal.to_csv(
                "C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/" + name + "_" + str(
                    vLoop) + "_result.csv")
        except:
            pass

        # lastFinal = pd.read_csv(
        #         "C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/" + name + "_" + str(
        #             vLoop) + "_result.csv")

        tmpLoopFinal = tmpLoopFinal.append(lastFinal)

        lastFinal['rDcnt'] = 0
        lastFinal['rBcnt'] = 0
        lastFinal['rDvsb'] = 0

        # _com 데이터에서 만들어낸 조건들을 11year 데이터에 대입해본다.
        for m in range(0, len(lastFinal)):
            print(str(m) + ' / ' + lastFinal.iloc[m]['condi'])
            lvindex, lvdcnt, lvbcnt, lvdvsb, lvData = jj.checkCondition(initData, lastFinal.iloc[m]['condi'])
            lastFinal['rDcnt'].iloc[m] = lvdcnt
            lastFinal['rBcnt'].iloc[m] = lvbcnt
            lastFinal['rDvsb'].iloc[m] = lvdvsb

        # loop 돌기 전 입력한 비율 값 보다 큰 rDcnt & rBcnt가 0인 조건을 만족하는 data만 빼낸다.
        lastFinal = lastFinal[(lastFinal['rDcnt'] >= limitCnt) & (lastFinal['rBcnt'] < 1)]

        if len(lastFinal) > 0:
            for z in range(0, len(lastFinal)):
                try:
                    data = data.drop(index=jj.checkCondition(data, lastFinal.iloc[z].condi)[0])
                except:
                    pass
        else:
            break

    tmpLoopFinal.to_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickle/RESULTNEW/" + name + "_result.csv")
    fResultMid = jj.makeFinalSet(path, name, paramLastRatio, realInitData)
    ray.shutdown()
    #####################################################################################################
    # 최종결과파일명   사용자 지정 이름__ddelTreeLoopFinal_result.csv
    #####################################################################################################
    print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
    print('최종결과파일은 ' + path + name + '_ddelTreeLoopFinal_result.csv  입니다.')
    print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
    #####################################################################################################


def calcRatio(path, name):
    df = pd.read_csv(path + name + '_com.csv')

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
    paramLevel = 5  # 초기 LEVEL수
    paramAftLevel = 2  # 초기 LEVEL 이후 돌려야할 LEVEL수
    paramLastRatio = 2  # 마지막 레벨에서 dvsb가 x:1 이상인 것만 돌리게 하는 조건
    paramLoop = 1  # loop Count Setting
    #####################################################################################################

    #####################################################################################################
    # kbuy2
    #####################################################################################################
    name = 'kbuy2'  # 사용자 지정 명
    path = "C:/Users/Shine_anal/Desktop/inott/"  # 사용자 지정명 + _com.csv 파일이 존재하는 폴더 (분석할 csv파일)

    paramLimitRatio = calcRatio(path, name)  # 초기데이터 중 dcnt대비 곱하기 x 개 이상의 가지만 돌리게 하는 조건 (퍼센트 임!!)
    execute(name, path, paramLevel, paramAftLevel, paramLimitRatio, paramLastRatio, paramLoop)

    # #####################################################################################################
    # # kbuy1
    # #####################################################################################################
    # # [0][2022 - 06 - 02 17: 08: 38] sourceData: lvl_0_1 / dvsb: 0.25 / bvsd: 4.77 / prevBcnt: 25420 / prevDcnt: 5324 / entropy: 0.66

    # name = 'kbuy1'  # 사용자 지정 명
    # path = "C:/Users/Shine_anal/Desktop/inott/"  # 사용자 지정명 + _com.csv 파일이 존재하는 폴더 (분석할 csv파일)

    # paramLimitRatio = calcRatio(path, name) # 초기데이터 중 dcnt대비 곱하기 x 개 이상의 가지만 돌리게 하는 조건 (퍼센트 임!!)
    # execute(name, path, paramLevel, paramAftLevel, paramLimitRatio, paramLastRatio, paramLoop)

    # #####################################################################################################
    # # ncbuy3
    # #####################################################################################################
    # name = 'ncbuy3'  # 사용자 지정 명
    # path = "C:/Users/Shine_anal/Desktop/inott/"  # 사용자 지정명 + _com.csv 파일이 존재하는 폴더 (분석할 csv파일)

    # paramLimitRatio = calcRatio(path, name) # 초기데이터 중 dcnt대비 곱하기 x 개 이상의 가지만 돌리게 하는 조건 (퍼센트 임!!)
    # execute(name, path, paramLevel, paramAftLevel, paramLimitRatio, paramLastRatio, paramLoop)

    # #####################################################################################################
    # # sbuy3
    # #####################################################################################################
    # name = 'sbuy3'  # 사용자 지정 명
    # path = "C:/Users/Shine_anal/Desktop/inott/"  # 사용자 지정명 + _com.csv 파일이 존재하는 폴더 (분석할 csv파일)

    # paramLimitRatio = calcRatio(path, name) # 초기데이터 중 dcnt대비 곱하기 x 개 이상의 가지만 돌리게 하는 조건 (퍼센트 임!!)
    # execute(name, path, paramLevel, paramAftLevel, paramLimitRatio, paramLastRatio, paramLoop)

    # #####################################################################################################
    # # sbuy2
    # #####################################################################################################
    # name = 'sbuy2'  # 사용자 지정 명
    # path = "C:/Users/Shine_anal/Desktop/inott/"  # 사용자 지정명 + _com.csv 파일이 존재하는 폴더 (분석할 csv파일)

    # paramLimitRatio = calcRatio(path, name) # 초기데이터 중 dcnt대비 곱하기 x 개 이상의 가지만 돌리게 하는 조건 (퍼센트 임!!)
    # execute(name, path, paramLevel, paramAftLevel, paramLimitRatio, paramLastRatio, paramLoop)

    # #####################################################################################################
    # # ncbuy2
    # #####################################################################################################
    # name = 'ncbuy2'  # 사용자 지정 명
    # path = "C:/Users/Shine_anal/Desktop/inott/"  # 사용자 지정명 + _com.csv 파일이 존재하는 폴더 (분석할 csv파일)

    # paramLimitRatio = calcRatio(path, name) # 초기데이터 중 dcnt대비 곱하기 x 개 이상의 가지만 돌리게 하는 조건 (퍼센트 임!!)
    # execute(name, path, paramLevel, paramAftLevel, paramLimitRatio, paramLastRatio, paramLoop)

    # #####################################################################################################
    # # sjtabuy
    # #####################################################################################################
    # name = 'sjtabuy'  # 사용자 지정 명
    # path = "C:/Users/Shine_anal/Desktop/inott/"  # 사용자 지정명 + _com.csv 파일이 존재하는 폴더 (분석할 csv파일)

    # paramLimitRatio = calcRatio(path, name) # 초기데이터 중 dcnt대비 곱하기 x 개 이상의 가지만 돌리게 하는 조건 (퍼센트 임!!)
    # execute(name, path, paramLevel, paramAftLevel, paramLimitRatio, paramLastRatio, paramLoop)