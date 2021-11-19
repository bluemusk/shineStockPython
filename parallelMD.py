# coding=utf-8
import sys
import pandas as pd
import datetime
import numpy as np

def makeMatrix(data, aggr_df, conData, leafType, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum, condition, datBool, lvl, ratDf, invYn, treeNm):
    # data = tmp[8]
    # aggr_df = tmp[9]
    # conData = tmp[1]
    # leafType = 'D'
    # rRatYn = 'Y'
    # rRatNum = 1
    # rRtYn = 'Y'
    # rRtLvl = 3
    # rRtNum = 1
    # serial = 2
    # branch = 4
    # lvl = 2
    # condition = tmp[6]
    # datBool = tmp[0]
    # # ratDf = tmp[7]
    # invYn = 'N'
    # treeNm = 'tree1'
    #-----------------------------------------
    # data = tmp[8][~tmpInv[0]]
    # aggr_df = tmp[8][~tmpInv[0]]['pur_gubn5']
    # conData = tmpInv[1]
    # leafType = 'A'
    # rRatYn = 'Y'
    # rRatNum = 1
    # rRtYn = 'Y'
    # rRtLvl = 3
    # rRtNum = 1
    # serial = 2
    # branch = 4
    # lvl = 2
    # condition = tmpInv[6]
    # datBool = ~tmpInv[0]
    # ratDf = ''
    # invYn = 'Y'
    # treeNm = 'tree1'
    try:
        prevDcnt = data.iloc[:, 2].value_counts()[0]
    except Exception as e:
        prevDcnt = 0.8

    try:
        prevBcnt = data.iloc[:, 2].value_counts()[1]
    except Exception as e:
        prevBcnt = 0.8

    try:
        if (leafType == 'A' and len(data) > 0) or invYn == 'Y':
            meltDt = data.iloc[:, 2:data.shape[1]].melt(id_vars='pur_gubn5')
            meltSummary = pd.DataFrame(meltDt.value_counts().reset_index(name='counts')).sort_values(
                ['variable', 'value'])
            tmpFinal = meltSummary.pivot(index=['variable', 'value'], columns='pur_gubn5', values='counts')

            tmpFinal.columns = tmpFinal.columns.values
            tmpFinal.reset_index(level=0, inplace=True)
            tmpFinal.reset_index(level=0, inplace=True)

            tmpFinal.columns = ['cal', 'tree', 'dcnt', 'bcnt']

            tmpFinal[tmpFinal['bcnt'] == 0]['bcnt'] = 0.8
            tmpFinal[tmpFinal['dcnt'] == 0]['dcnt'] = 0.8

            tmpFinal['bvsd'] = tmpFinal['bcnt'] / tmpFinal['dcnt']
            tmpFinal['dvsb'] = tmpFinal['dcnt'] / tmpFinal['bcnt']
            tmpFinal['prevBcnt'] = prevBcnt
            tmpFinal['prevDcnt'] = prevDcnt
            tmpFinal = tmpFinal.assign(rGr=lambda x: x.dvsb - x.prevDcnt / x.prevBcnt)
            tmpFinal = tmpFinal.assign(bDc=lambda x: x.prevBcnt - x.bcnt)
            tmpFinal = tmpFinal.assign(dDc=lambda x: x.prevDcnt - x.dcnt)
            tmpFinal = tmpFinal.assign(rRt=lambda x: (x.prevBcnt - x.bcnt) / (x.prevDcnt - x.dcnt))
            tmpFinal = tmpFinal.assign(
                sRt=lambda x: (x.prevBcnt - x.bcnt) * ((x.prevBcnt - x.bcnt) / (x.prevDcnt - x.dcnt)))
            tmpFinal = tmpFinal.assign(
                pRt=lambda x: (x.prevBcnt - x.bcnt) * ((x.prevBcnt - x.bcnt) / (x.prevDcnt - x.dcnt)))
            tmpFinal = tmpFinal.assign(qRt=lambda x: ((x.prevBcnt - x.bcnt) * 100 / x.prevBcnt) * (
                    (x.prevBcnt - x.bcnt) / (x.prevDcnt - x.dcnt)))
            tmpFinal = tmpFinal.assign(fRt=lambda x: (x.dvsb - x.prevDcnt / x.prevBcnt) / (x.prevDcnt / x.prevBcnt) * (
                    (x.prevBcnt - x.bcnt) / (x.prevDcnt - x.dcnt)))
            tmpFinal = tmpFinal.assign(rRat=lambda x: (x.prevBcnt - x.bcnt) / x.prevBcnt * 100)
            tmpFinal = tmpFinal.assign(rrr=lambda x: (x.dvsb - x.prevDcnt / x.prevBcnt) / (x.prevDcnt / x.prevBcnt))

            if rRatYn == 'Y':
                tmpFinal = tmpFinal[tmpFinal['rRat'] >= rRatNum]

            if rRtYn == 'Y' and lvl >= rRtLvl:
                tmpFinal = tmpFinal[tmpFinal['rRat'] >= rRtNum]

            pos = pd.merge(tmpFinal[tmpFinal['cal'] == 1], conData[conData['cal'] == 'GT'], left_on='tree',
                           right_on='colname', how='inner').sort_values('tree')
            neg = pd.merge(tmpFinal[tmpFinal['cal'] == 0], conData[conData['cal'] == 'LT'], left_on='tree',
                           right_on='colname', how='inner').sort_values('tree')
            midSet = pos
            midSet = midSet.append(neg)
        else:
            midSet = ratDf

    except Exception as e:
        midSet = ratDf
        pass

    try:
        if len(midSet) > 0:
            # 가지별로 계산하는 것을 makeMatrix에서 처리하겠음
            if treeNm == 'tree1':
                if leafType == 'A':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / bcnt asc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('bcnt')
                elif leafType == 'C':
                    midSet = midSet[midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt'] & midSet['rRat'] > 20]
                    midSet = midSet.sort_values('dcnt', ascending=False)
                elif leafType == 'D':
                    midSet = midSet[(midSet['dvsb'] > 0) & (midSet['prevDcnt'] > 0)]
                    midSet = midSet.sort_values('bcnt', ascending=False)
                elif leafType == 'E':
                    # bcnt != 0 & dcnt != 0 & bcnt <= totCnt * 0.25 /	dcnt desc
                    midSet = midSet[midSet['bcnt'] <= (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.25]
                    midSet = midSet.sort_values('dcnt', ascending=False)
            elif treeNm == 'tree2':
                if leafType == 'A':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / bcnt asc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('bcnt')
                elif leafType == 'C':
                    midSet = midSet[midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt'] & midSet['rRat'] > 20]
                    midSet = midSet.sort_values('dcnt', ascending=False)
                elif leafType == 'D':
                    midSet = midSet[(midSet['dvsb'] > 0) & (midSet['prevDcnt'] > 0)]
                    midSet = midSet.sort_values('bcnt', ascending=False)
            elif treeNm == 'tree3':
                if leafType == 'A':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / bcnt asc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('bcnt')
                elif leafType == 'C':
                    midSet = midSet[midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt'] & midSet['rRat'] > 20]
                    midSet = midSet.sort_values('dcnt', ascending=False)
                elif leafType == 'D':
                    midSet = midSet[(midSet['dvsb'] > 0) & (midSet['prevDcnt'] > 0)]
                    midSet = midSet.sort_values('bcnt', ascending=False)
            elif treeNm == 'tree4':
                if leafType == 'A':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / bcnt asc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('bcnt')
                elif leafType == 'C':
                    midSet = midSet[midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt'] & midSet['rRat'] > 20]
                    midSet = midSet.sort_values('dcnt', ascending=False)
                elif leafType == 'D':
                    midSet = midSet[(midSet['dvsb'] > 0) & (midSet['prevDcnt'] > 0)]
                    midSet = midSet.sort_values('bcnt', ascending=False)
            elif treeNm == 'tree5':
                if leafType == 'A':
                    midSet = midSet[midSet['rRat'] > 20]
                    midSetChk = midSet[midSet['rRt'] > midSet['dvsb']]

                    if midSetChk['dvsb'].count() > 0:
                        midSet = midSet.sort_values('rRt', ascending=False)
                    else:
                        midSet = midSet.sort_values('sRt', ascending=False)
                elif leafType == 'B':
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'C':
                    midSet = midSet.sort_values('bcnt')
                elif leafType == 'D':
                    midSet = midSet[midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt']]
                    midSet = midSet.sort_values('dcnt', ascending=False)
            elif treeNm == 'tree6':
                if lvl > 0 and lvl <= 3:
                    midSet = midSet[midSet['rRat'] > 35]
                elif lvl > 3 and lvl <= 6:
                    midSet = midSet[midSet['rRat'] > 25]
                elif lvl > 6 and lvl <= 9:
                    midSet = midSet[midSet['rRat'] > 15]

                midSet = midSet.sort_values('rRt', ascending=False)
            elif treeNm == 'tree7':
                if leafType == 'A':
                    if lvl > 0 and lvl <= 2:
                        midSet = midSet[midSet['rRat'] > 40]
                    elif lvl > 2 and lvl <= 4:
                        midSet = midSet[midSet['rRat'] > 30]
                    elif lvl > 4 and lvl <= 6:
                        midSet = midSet[midSet['rRat'] > 20]
                    elif lvl > 6:
                        midSet = midSet[midSet['rRat'] > 7]

                    midSet = midSet.sort_values('sRt', ascending=False)
                elif leafType == 'B':
                    if lvl > 0 and lvl <= 2:
                        midSet = midSet[midSet['rRat'] > 35]
                    elif lvl > 2 and lvl <= 4:
                        midSet = midSet[midSet['rRat'] > 25]
                    elif lvl > 4 and lvl <= 6:
                        midSet = midSet[midSet['rRat'] > 15]
                    elif lvl > 6:
                        midSet = midSet[midSet['rRat'] > 5]

                    midSet = midSet.sort_values('rRt', ascending=False)
            elif treeNm == 'tree8':
                if leafType == 'A':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    if lvl > 0 and lvl <= 2:
                        midSet = midSet[midSet['rRat'] > 40]
                    elif lvl > 2 and lvl <= 4:
                        midSet = midSet[midSet['rRat'] > 30]
                    elif lvl > 4 and lvl <= 6:
                        midSet = midSet[midSet['rRat'] > 20]
                    elif lvl > 6:
                        midSet = midSet[midSet['rRat'] > 7]

                    midSet = midSet.sort_values('rRt', ascending=False)
                elif leafType == 'C':
                    midSet = midSet[midSet['rRat'] > 20]
                    midSetChk = midSet[midSet['rRt'] > midSet['dvsb']]

                    if midSetChk['dvsb'].count() > 0:
                        midSet = midSet.sort_values('rRt', ascending=False)
                    else:
                        midSet = midSet.sort_values('sRt', ascending=False)
            elif treeNm == 'tree9':
                if leafType == 'A':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet[midSet['rRat'] > 20]
                    midSetChk = midSet[midSet['rRt'] > midSet['dvsb']]

                    if midSetChk['dvsb'].count() > 0:
                        midSet = midSet.sort_values('rRt', ascending=False)
                    else:
                        midSet = midSet.sort_values('sRt', ascending=False)
                elif leafType == 'C':
                    midSet = midSet[midSet['rRt'] > midSet['rRt'].mean()]
                    midSet = midSet.sort_values('dcnt', ascending=False)
                elif leafType == 'D':
                    midSet = midSet[(midSet['dvsb'] > 0) & (midSet['prevDcnt'] > 0)]
                    midSet = midSet.sort_values('bcnt', ascending=False)

            # A,B,C,D 공통 시작
            conH = midSet.iloc[0]['tree']
            rConH = midSet.iloc[0]['condi']
            valH = midSet.iloc[0]['value']
            calX = midSet.iloc[0]['cal_x']
            calH = midSet.iloc[0]['cal_y']

            tmpBool = (data[conH] == calX)

            if lvl == 1 and invYn == 'N':
                if calH == 'GT':
                    condition = rConH + ' > ' + str(valH)
                else:
                    condition = rConH + ' <= ' + str(valH)
            else:
                if calH == 'GT':
                    condition = condition + ' AND ' + rConH + ' > ' + str(valH)
                else:
                    condition = condition + ' AND ' + rConH + ' <= ' + str(valH)
            # A,B,C,D 공통부분 종료
            aggr_df = aggr_df[tmpBool]
            data = data[tmpBool]
            tmpRt = midSet.iloc[0]
            conTmp = conData[~((conData['colname'] == midSet.iloc[0]['tree']) & (conData['cal'] == midSet.iloc[0]['cal_y']))]
            data = data.drop(conH, axis=1)

            # 중복조건 있는지 확인
            if leafType == 'B' or leafType == 'C' or leafType == 'D':
                condition, tmpBool = chkDupCond(data, midSet, condition, tmpBool)

        else:
            tmpBool = pd.DataFrame()
            tmpRt = pd.DataFrame([(0,0,0,0)], columns=['bcnt','dcnt','bvsd','dvsb'])
            conTmp = conData
    except Exception as e:
        tmpBool = pd.DataFrame()
        tmpRt = pd.DataFrame([(0,0,0,0)], columns=['bcnt','dcnt','bvsd','dvsb'])
        conTmp = conData

    return data, aggr_df, tmpRt, tmpBool, condition, conTmp, midSet

def chkDupCond(data, midSet, condition, tmpBool):
    # 중복조건 있는지 확인
    chkAddiCon = midSet['dvsb'] >= midSet.iloc[0]['dvsb']
    tfCnt = chkAddiCon.value_counts()

    if tfCnt.loc[True] > 1 and tfCnt.loc[True] < 10:
        addiCon = midSet[chkAddiCon]

        for x in range(0, addiCon.shape[0]):
            conD = addiCon.iloc[x]['tree']
            rConD = addiCon.iloc[x]['condi']
            valD = addiCon.iloc[x]['value']
            calD = addiCon.iloc[x]['cal_y']
            calX = addiCon.iloc[x]['cal_x']

            dupBool = (data[conD] == calX)

            if x == 0:
                if calD == 'GT':
                    dupCondition = '(' + rConD + ' > ' + str(valD)
                else:
                    dupCondition = '(' + rConD + ' <= ' + str(valD)
            elif x != addiCon.shape[0] - 1:
                if calD == 'GT':
                    dupCondition = dupCondition + ' OR ' + rConD + ' > ' + str(valD)
                else:
                    dupCondition = dupCondition + ' OR ' + rConD + ' <= ' + str(valD)

            dupCondition = dupCondition + ' OR ' + condition + ')'

            condition = dupCondition
            tmpBool = tmpBool | dupBool
    else:
        condition = condition
        tmpBool = tmpBool

    return condition, tmpBool

def initCond(df, dfCon):
    cond = dfCon.iloc[:, 1]
    condDf = pd.DataFrame()
    newDf = pd.DataFrame()

    newDf['ymd'] = df['yyyymmdd']
    newDf['stCd'] = df['stock_code']
    newDf['pur_gubn5'] = df['pur_gubn5']

    for i in range(0, cond.shape[0]):
        con = cond.iloc[i].split('>')[0]
        try:
            val = float(cond.iloc[i].split('>')[1])
        except Exception as e:
            val = cond.iloc[i].split('>')[1]
            pass
        colname = 'tree' + str(i)
        condDf = condDf.append(pd.DataFrame([(con, val, 'GT', colname)], columns=['condi', 'value', 'cal', 'colname']))
        condDf = condDf.append(pd.DataFrame([(con, val, 'LT', colname)], columns=['condi', 'value', 'cal', 'colname']))
        condDf = condDf[~condDf['condi'].str.contains('-')]

        if '-' not in str(con):
            # 조건을 만족하면(condi > value) 값에 1을 넣고 아니면 0
            if if_float(val):
                tmpBoolP = df[con] > val
            else:
                tmpBoolP = df[con] > df[val]

            newDf[colname] = tmpBoolP.apply(lambda x: 1 if x == True else 0)
            # newDf.append(tmpBoolP.apply(lambda x: 1 if x == True else 0), colname=['tree' + str(i)], ignore_index=True)

    return newDf,condDf

def if_float(string):
    try:
        float(string)
        return True
    except ValueError:
        return False

def drawTree(data, level, limitRatio, initConds, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum ):
    #####################################################################################
    #  트리를 그리는 로직
    # level = 2
    # data = newDff
    # initConds = dfConF
    # rRatYn = 'Y'
    # rRatNum = 1
    # rRtYn = 'Y'
    # rRtLvl = 3
    # rRtNum = 1
    #####################################################################################
    mod = sys.modules[__name__]

    fResult = pd.DataFrame()

    trees = pd.DataFrame([('tree1', 5)], columns=('treeNm', 'branch'))
    trees = trees.append(pd.DataFrame([('tree2', 4)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree3', 4)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree4', 4)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree5', 4)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree6', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree7', 3)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree8', 3)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree9', 4)], columns=('treeNm', 'branch')))
    # row = 0
    # i = 2 j = 2 row = 0
    try:
        for row in range(0, trees.shape[0]):
            # print('tree forloop')
            for i in range(0, level + 1):
                # print('level forloop')
                leaf = trees.iloc[row].branch ** i

                if i == 0:
                    data['x'] = True
                    datBool = data['x']
                    data = data.drop('x', axis=1)
                    setattr(mod, 'lvl_0_1', [datBool, initConds, '', '', '', '', '', '',data,data.iloc[:, 2]])
                else:
                    for j in range(1, leaf + 1):
                        # print('leaf forloop')
                        if trees.iloc[row].treeNm == 'tree1':
                            #-----------------------------------------------------------------------------------------------
                            print("[" + datetime.datetime.today().strftime(
                                "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(j))
                            # -----------------------------------------------------------------------------------------------
                            # tmp = getattr(mod, 'lvl_1_5')
                            if j % trees.iloc[row].branch == 1:
                                tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                                try:
                                    prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                                except Exception as e:
                                    prevDcnt = 0
                                    pass

                                try:
                                    prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                                except Exception as e:
                                    prevBcnt = 0
                                    pass

                                print("[" + datetime.datetime.today().strftime(
                                    "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(j) + " - " + trees.iloc[row].treeNm + " / prevBcnt : " + str(
                                    prevBcnt) + " / prevDcnt : " + str(
                                    prevDcnt) )
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmp[7], 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 2:
                                #j=2
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(i , j-1))
                                tmpInvCon = ''

                                if tmpInv[6].find('>') > 0:
                                    tmpInvCon = tmpInv[6].replace('>', '<=')
                                else:
                                    tmpInvCon = tmpInv[6].replace('<=', '>')

                                if tmpInvCon.find('AND') > 0:
                                    tmpInvCon = tmpInvCon.replace('AND', 'OR')
                                else:
                                    tmpInvCon = tmpInvCon.replace('OR', 'AND')

                                tmpInvCon = '(' + tmpInvCon + ')'

                                if i == 1:
                                    tmpDat, tmpAggrDf,tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(tmp[8].drop(tmpInv[8].index), tmp[8].drop(tmpInv[8].index)['pur_gubn5'], tmpInv[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 3: # j = 3
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j-1))
                                tmpInvCon = ''
                                if tmpInv[6].find('>') > 0:
                                    tmpInvCon = tmpInv[6].replace('>', '<=')
                                else:
                                    tmpInvCon = tmpInv[6].replace('<=', '>')

                                if tmpInvCon.find('AND') > 0:
                                    tmpInvCon = tmpInvCon.replace('AND', 'OR')
                                else:
                                    tmpInvCon = tmpInvCon.replace('OR', 'AND')

                                tmpInvCon = '(' + tmpInvCon + ')'

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8].drop(tmpInv[8].index), tmp[8].drop(tmpInv[8].index)['pur_gubn5'], tmpInv[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 4:
                                tmpDat, tmpAggrDf,tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(tmp[8], tmp[9], tmp[1], 'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 0:
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j-1))
                                tmpInvCon = ''
                                if tmpInv[6].find('>') > 0:
                                    tmpInvCon = tmpInv[6].replace('>', '<=')
                                else:
                                    tmpInvCon = tmpInv[6].replace('<=', '>')

                                if tmpInvCon.find('AND') > 0:
                                    tmpInvCon = tmpInvCon.replace('AND', 'OR')
                                else:
                                    tmpInvCon = tmpInvCon.replace('OR', 'AND')

                                tmpInvCon = '(' + tmpInvCon + ')'

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(tmp[8].drop(tmpInv[8].index), tmp[8].drop(tmpInv[8].index)['pur_gubn5'], tmpInv[1], 'E', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'E', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                        elif trees.iloc[row].treeNm == 'tree2':
                            # -----------------------------------------------------------------------------------------------
                            print("[" + datetime.datetime.today().strftime(
                                "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(j))
                            # -----------------------------------------------------------------------------------------------
                            # tmp = getattr(mod, 'lvl_1_5')
                            if j % trees.iloc[row].branch == 1:
                                tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                                try:
                                    prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                                except Exception as e:
                                    prevDcnt = 0
                                    pass

                                try:
                                    prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                                except Exception as e:
                                    prevBcnt = 0
                                    pass

                                print("[" + datetime.datetime.today().strftime(
                                    "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(
                                    j) + " - " + trees.iloc[row].treeNm + " / prevBcnt : " + str(
                                    prevBcnt) + " / prevDcnt : " + str(
                                    prevDcnt) )
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 2:
                                # j=2
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                tmpInvCon = ''

                                if tmpInv[6].find('>') > 0:
                                    tmpInvCon = tmpInv[6].replace('>', '<=')
                                else:
                                    tmpInvCon = tmpInv[6].replace('<=', '>')

                                if tmpInvCon.find('AND') > 0:
                                    tmpInvCon = tmpInvCon.replace('AND', 'OR')
                                else:
                                    tmpInvCon = tmpInvCon.replace('OR', 'AND')

                                tmpInvCon = '(' + tmpInvCon + ')'

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8].drop(tmpInv[8].index), tmp[8].drop(tmpInv[8].index)['pur_gubn5'], tmpInv[1],
                                        'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 3:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 0:
                                # i=1 j=4
                                tmpInv1 = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                tmpInvCon1 = ''
                                if tmpInv1[6].find('>') > 0:
                                    tmpInvCon1 = tmpInv1[6].replace('>', '<=')
                                else:
                                    tmpInvCon1 = tmpInv1[6].replace('<=', '>')

                                if tmpInvCon1.find('AND') > 0:
                                    tmpInvCon1 = tmpInvCon.replace('AND', 'OR')
                                else:
                                    tmpInvCon1 = tmpInvCon.replace('OR', 'AND')

                                tmpInvCon1 = '(' + tmpInvCon1 + ')'
                                
                                # inv_B
                                tmpInv2 = getattr(mod, 'lvl_{}_{}'.format(i, j - 2))
                                tmpInvCon2 = ''
                                if tmpInv2[6].find('>') > 0:
                                    tmpInvCon2 = tmpInv2[6].replace('>', '<=')
                                else:
                                    tmpInvCon2 = tmpInv2[6].replace('<=', '>')

                                if tmpInvCon2.find('AND') > 0:
                                    tmpInvCon2 = tmpInvCon.replace('AND', 'OR')
                                else:
                                    tmpInvCon2 = tmpInvCon.replace('OR', 'AND')

                                tmpInvCon2 = '(' + tmpInvCon2 + ')'

                                tmpInvCon = tmpInvCon1 + ' AND ' + tmpInvCon2

                                expData = pd.DataFrame(tmpInv1[8].index, columns=['ex'])
                                expData = expData.append(pd.DataFrame(tmpInv2[8].index, columns=['ex']))

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8].drop(expData.ex.unique()), tmp[8].drop(expData.ex.unique())['pur_gubn5'], tmpInv[1],
                                        'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                        elif trees.iloc[row].treeNm == 'tree3':
                            # -----------------------------------------------------------------------------------------------
                            print("[" + datetime.datetime.today().strftime(
                                "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(j))
                            # -----------------------------------------------------------------------------------------------
                            # tmp = getattr(mod, 'lvl_1_5')
                            if j % trees.iloc[row].branch == 1:
                                tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                                try:
                                    prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                                except Exception as e:
                                    prevDcnt = 0
                                    pass

                                try:
                                    prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                                except Exception as e:
                                    prevBcnt = 0
                                    pass

                                print("[" + datetime.datetime.today().strftime(
                                    "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(
                                    j) + " - " + trees.iloc[row].treeNm + " / prevBcnt : " + str(
                                    prevBcnt) + " / prevDcnt : " + str(
                                    prevDcnt) )
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 2:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 3:  # j = 3
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 0:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                        elif trees.iloc[row].treeNm == 'tree4':
                            # -----------------------------------------------------------------------------------------------
                            print("[" + datetime.datetime.today().strftime(
                                "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(j))
                            # -----------------------------------------------------------------------------------------------
                            # tmp = getattr(mod, 'lvl_1_5')
                            if j % trees.iloc[row].branch == 1:
                                tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                                try:
                                    prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                                except Exception as e:
                                    prevDcnt = 0
                                    pass

                                try:
                                    prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                                except Exception as e:
                                    prevBcnt = 0
                                    pass

                                print("[" + datetime.datetime.today().strftime(
                                    "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(
                                    j) + " - " + trees.iloc[row].treeNm + " / prevBcnt : " + str(
                                    prevBcnt) + " / prevDcnt : " + str(
                                    prevDcnt) )
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 2:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 3:  # j = 3
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 0:
                                # inv_C
                                tmpInv1 = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                tmpInvCon1 = ''
                                if tmpInv1[6].find('>') > 0:
                                    tmpInvCon1 = tmpInv1[6].replace('>', '<=')
                                else:
                                    tmpInvCon1 = tmpInv1[6].replace('<=', '>')

                                if tmpInvCon1.find('AND') > 0:
                                    tmpInvCon1 = tmpInvCon.replace('AND', 'OR')
                                else:
                                    tmpInvCon1 = tmpInvCon.replace('OR', 'AND')

                                tmpInvCon1 = '(' + tmpInvCon1 + ')'

                                # # inv_B
                                # tmpInv2 = getattr(mod, 'lvl_{}_{}'.format(i, j - 2))
                                # tmpInvCon2 = ''
                                # if tmpInv2[6].find('>') > 0:
                                #     tmpInvCon2 = tmpInv2[6].replace('>', '<=')
                                # else:
                                #     tmpInvCon2 = tmpInv2[6].replace('<=', '>')
                                #
                                # if tmpInvCon2.find('AND') > 0:
                                #     tmpInvCon2 = tmpInvCon.replace('AND', 'OR')
                                # else:
                                #     tmpInvCon2 = tmpInvCon.replace('OR', 'AND')
                                #
                                # tmpInvCon2 = '(' + tmpInvCon2 + ')'

                                # # inv_A
                                # tmpInv3 = getattr(mod, 'lvl_{}_{}'.format(i, j - 3))
                                # tmpInvCon3 = ''
                                # if tmpInv3[6].find('>') > 0:
                                #     tmpInvCon3 = tmpInv3[6].replace('>', '<=')
                                # else:
                                #     tmpInvCon3 = tmpInv3[6].replace('<=', '>')
                                #
                                # if tmpInvCon3.find('AND') > 0:
                                #     tmpInvCon3 = tmpInvCon.replace('AND', 'OR')
                                # else:
                                #     tmpInvCon3 = tmpInvCon.replace('OR', 'AND')
                                #
                                # tmpInvCon3 = '(' + tmpInvCon3 + ')'

                                tmpInvCon = tmpInvCon1 #+ ' AND ' + tmpInvCon2 + ' AND ' + tmpInvCon3

                                expData = pd.DataFrame(tmpInv1[8].index, columns=['ex'])
                                # expData = expData.append(pd.DataFrame(tmpInv2[8].index, columns=['ex']))
                                # expData = expData.append(pd.DataFrame(tmpInv3[8].index, columns=['ex']))

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8].drop(expData.ex.unique()), tmp[8].drop(expData.ex.unique())['pur_gubn5'],
                                        tmpInv[1],
                                        'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                        elif trees.iloc[row].treeNm == 'tree5':
                            # -----------------------------------------------------------------------------------------------
                            print("[" + datetime.datetime.today().strftime(
                                "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(j))
                            # -----------------------------------------------------------------------------------------------
                            # tmp = getattr(mod, 'lvl_1_5')
                            if j % trees.iloc[row].branch == 1:
                                tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                                try:
                                    prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                                except Exception as e:
                                    prevDcnt = 0
                                    pass

                                try:
                                    prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                                except Exception as e:
                                    prevBcnt = 0
                                    pass

                                print("[" + datetime.datetime.today().strftime(
                                    "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(
                                    j) + " - " + trees.iloc[row].treeNm + " / prevBcnt : " + str(
                                    prevBcnt) + " / prevDcnt : " + str(
                                    prevDcnt) )
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 2:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 3:  # j = 3
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 0:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                        elif trees.iloc[row].treeNm == 'tree6':
                            # -----------------------------------------------------------------------------------------------
                            print("[" + datetime.datetime.today().strftime(
                                "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(j))
                            # -----------------------------------------------------------------------------------------------
                            # tmp = getattr(mod, 'lvl_1_5')
                            if j % trees.iloc[row].branch == 1:
                                tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                                try:
                                    prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                                except Exception as e:
                                    prevDcnt = 0
                                    pass

                                try:
                                    prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                                except Exception as e:
                                    prevBcnt = 0
                                    pass

                                print("[" + datetime.datetime.today().strftime(
                                    "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(
                                    j) + " - " + trees.iloc[row].treeNm + " / prevBcnt : " + str(
                                    prevBcnt) + " / prevDcnt : " + str(
                                    prevDcnt) )
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 0:
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                tmpInvCon = ''

                                if tmpInv[6].find('>') > 0:
                                    tmpInvCon = tmpInv[6].replace('>', '<=')
                                else:
                                    tmpInvCon = tmpInv[6].replace('<=', '>')

                                if tmpInvCon.find('AND') > 0:
                                    tmpInvCon = tmpInvCon.replace('AND', 'OR')
                                else:
                                    tmpInvCon = tmpInvCon.replace('OR', 'AND')

                                tmpInvCon = '(' + tmpInvCon + ')'

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8].drop(tmpInv[8].index), tmp[8].drop(tmpInv[8].index)['pur_gubn5'], tmpInv[1],
                                        'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                        elif trees.iloc[row].treeNm == 'tree7':
                            # -----------------------------------------------------------------------------------------------
                            print("[" + datetime.datetime.today().strftime(
                                "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(j))
                            # -----------------------------------------------------------------------------------------------
                            # tmp = getattr(mod, 'lvl_1_5')
                            if j % trees.iloc[row].branch == 1:
                                tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                                try:
                                    prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                                except Exception as e:
                                    prevDcnt = 0
                                    pass

                                try:
                                    prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                                except Exception as e:
                                    prevBcnt = 0
                                    pass

                                print("[" + datetime.datetime.today().strftime(
                                    "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(
                                    j) + " - " + trees.iloc[row].treeNm + " / prevBcnt : " + str(
                                    prevBcnt) + " / prevDcnt : " + str(
                                    prevDcnt) )
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 2:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 0:
                                tmpInv1 = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                tmpInvCon1 = ''
                                if tmpInv1[6].find('>') > 0:
                                    tmpInvCon1 = tmpInv1[6].replace('>', '<=')
                                else:
                                    tmpInvCon1 = tmpInv1[6].replace('<=', '>')

                                if tmpInvCon1.find('AND') > 0:
                                    tmpInvCon1 = tmpInvCon1.replace('AND', 'OR')
                                else:
                                    tmpInvCon1 = tmpInvCon1.replace('OR', 'AND')

                                tmpInvCon1 = '(' + tmpInvCon1 + ')'

                                tmpInvCon = tmpInvCon1 #+ ' AND ' + tmpInvCon2

                                expData = pd.DataFrame(tmpInv1[8].index, columns=['ex'])
                                # expData = expData.append(pd.DataFrame(tmpInv2[8].index, columns=['ex']))

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8].drop(expData.ex.unique()), tmp[8].drop(expData.ex.unique())['pur_gubn5'],
                                        tmpInv1[1],
                                        'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv1[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                        elif trees.iloc[row].treeNm == 'tree8':
                            # -----------------------------------------------------------------------------------------------
                            print("[" + datetime.datetime.today().strftime(
                                "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(j))
                            # -----------------------------------------------------------------------------------------------
                            # tmp = getattr(mod, 'lvl_1_5')
                            if j % trees.iloc[row].branch == 1:
                                tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                                try:
                                    prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                                except Exception as e:
                                    prevDcnt = 0
                                    pass

                                try:
                                    prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                                except Exception as e:
                                    prevBcnt = 0
                                    pass

                                print("[" + datetime.datetime.today().strftime(
                                    "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(
                                    j) + " - " + trees.iloc[row].treeNm + " / prevBcnt : " + str(
                                    prevBcnt) + " / prevDcnt : " + str(
                                    prevDcnt) )
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 2:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 0:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                        elif trees.iloc[row].treeNm == 'tree9':
                            # -----------------------------------------------------------------------------------------------
                            print("[" + datetime.datetime.today().strftime(
                                "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(j))
                            # -----------------------------------------------------------------------------------------------
                            # tmp = getattr(mod, 'lvl_1_5')
                            if j % trees.iloc[row].branch == 1:
                                tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                                try:
                                    prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                                except Exception as e:
                                    prevDcnt = 0
                                    pass

                                try:
                                    prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                                except Exception as e:
                                    prevBcnt = 0
                                    pass

                                print("[" + datetime.datetime.today().strftime(
                                    "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(
                                    j) + " - " + trees.iloc[row].treeNm + " / prevBcnt : " + str(
                                    prevBcnt) + " / prevDcnt : " + str(
                                    prevDcnt) )
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 2:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 3:  #
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 2))
                                tmpInvCon = ''
                                if tmpInv[6].find('>') > 0:
                                    tmpInvCon = tmpInv[6].replace('>', '<=')
                                else:
                                    tmpInvCon = tmpInv[6].replace('<=', '>')

                                if tmpInvCon.find('AND') > 0:
                                    tmpInvCon = tmpInvCon.replace('AND', 'OR')
                                else:
                                    tmpInvCon = tmpInvCon.replace('OR', 'AND')

                                tmpInvCon = '(' + tmpInvCon + ')'

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8].drop(tmpInv[8].index), tmp[8].drop(tmpInv[8].index)['pur_gubn5'], tmpInv[1],
                                        'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            # -----------------------------------------------------------------------------------------------
                            elif j % trees.iloc[row].branch == 0:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            # -----------------------------------------------------------------------------------------------

                        if i >= 3: # 초기 1,2,3레벨일때는 데이터 셋이 크니까 없애버려
                            setattr(mod, 'lvl_{}_{}'.format(i, j),
                                    [pd.DataFrame(), pd.DataFrame(), tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb,
                                     tmpCon,
                                     tmpRatList, pd.DataFrame(), pd.DataFrame()])

                        if i != level:
                            # print("here2!!")
                            setattr(mod, 'lvl_{}_{}'.format(i, j),
                                    [tmpBool, tmpConDf, tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb, tmpCon,
                                     tmpRatList, tmpDat, tmpAggrDf])
                        else: # 마지막 레벨일때는 램을 많이 먹는 데이터셋은 저장하지 않음
                            # print("here3!!")
                            setattr(mod, 'lvl_{}_{}'.format(i, j),
                                    [pd.DataFrame(), pd.DataFrame(), tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb, tmpCon,
                                     tmpRatList, pd.DataFrame(), pd.DataFrame()])


            # mod = sys.modules[__name__]
            for i in range(0, level + 1):
                leaf = trees.iloc[row].branch ** i
                for j in range(1, leaf + 1):
                    tmp = getattr(mod, 'lvl_{}_{}'.format(i, j))
                    # print('lvl_'+str(i)+'_'+str(j))
                    # print(tmp[3].value_counts('pur_gubn5')[0])
                    try:
                        bcnt = int(tmp[2])
                        if bcnt == 0:
                            bcnt = 0.8
                    except Exception as e:
                        bcnt = 1
                        pass

                    try:
                        dcnt = int(tmp[3])
                        if dcnt == 0:
                            dcnt = 0.8
                    except Exception as e:
                        dcnt = 1
                        pass

                    fResult = fResult.append(
                        pd.DataFrame([(trees.iloc[row].treeNm, 'lvl_' + str(i) + '_' + str(j), tmp[6], bcnt, dcnt, bcnt / dcnt, dcnt / bcnt)],
                                     columns=['treeNm', 'lvl', 'condi', 'bcnt', 'dcnt', 'bvsd', 'dvsb']))
                    delattr(mod, 'lvl_{}_{}'.format(i, j))
    except Exception as e:
        print(e)
        setattr(mod, 'lvl_{}_{}'.format(i, j),
                [tmpBool, tmpConDf, tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb, tmpCon, tmpRatList, tmpDat,
                 tmpAggrDf])

    return fResult

# Data Loading
df = pd.read_csv('C:/Users/Shine_anal/Desktop/kbuy1.csv')
dfCon = pd.read_csv('C:/Users/Shine_anal/Desktop/dtreeCon.csv')

# 조건 이니셜라이즈
newDff, dfConF = initCond(df, dfCon)

# 실행 구문
fResult = drawTree(newDff, 7, 4, dfConF, 'Y', 1, 'Y', 3, 1)

fResult = fResult.iloc[:,2:fResult.shape[1]].drop_duplicates()
fResult = fResult.sort_values('bvsd', ascending=False)
fResult.to_csv("C:/Users/Shine_anal/Desktop/kbuy1_20210906_dtree.csv")

(sma10_ang1__sma20_ang2 > 0.0) AND close_price_ang2 > 0.0 AND high5d_low5d_rate <= 28.0 AND fast3D_ang2 <= 3.0 AND fast5D > m14_slow3k AND dis60_ang1_dis20_ang2 <= 9.0 AND DIp_ang1_Adx_ang1 <= 8.0 AND nmind_nmind_sig <= 32.0

df[
(
        (   (df['sma10_ang1__sma20_ang2'] > 0.0)
& (df['fast5D'] > df['m14_slow3k'] ))
& (df['close_price_ang2'] > 0.0  )
& (df['high5d_low5d_rate'] <= 28)
& (df['fast3D_ang2'] <= 3.0)
 & (df['dis60_ang1_dis20_ang2'] <= 9.0)
& (df['DIp_ang1_Adx_ang1'] <= 8 )
& (df['nmind_nmind_sig'] <= 32 )
)
].value_counts('pur_gubn5')

pur_gubn5
0    14
1     1