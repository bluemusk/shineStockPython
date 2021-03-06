# coding=utf-8
import sys
import pandas as pd
import datetime
import os
import pickle
import warnings

warnings.filterwarnings('ignore')


def makeMatrix(data, aggr_df, conData, leafType, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum, condition, datBool, lvl, ratDf,
               invYn, treeNm):
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
    # -----------------------------------------
    # data = newDff
    # data['x'] = True
    # datBool = data['x']
    # data = data.drop('x', axis=1)
    # data = data.drop(tmpInv[8].index)
    # aggr_df = data['pur_gubn5']
    # conData = tmpInv[1]
    # leafType = 'C'
    # rRatYn = 'Y'
    # rRatNum = 1
    # rRtYn = 'Y'
    # rRtLvl = 3
    # rRtNum = 1
    # serial = 2
    # branch = 4
    # lvl = 1
    # condition = tmpInvCon
    # datBool = ~tmpInv[0]
    # ratDf = ''
    # invYn = 'Y'
    # treeNm = trees.iloc[row].treeNm
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
            # meltDt = data.iloc[1:10, 2:5].melt(id_vars='pur_gubn5')
            meltSummary = pd.DataFrame(meltDt.value_counts().reset_index(name='counts')).sort_values(
                ['variable', 'value'])
            tmpFinal = meltSummary.pivot(index=['variable', 'value'], columns='pur_gubn5', values='counts')

            tmpFinal.columns = tmpFinal.columns.values
            tmpFinal.reset_index(level=0, inplace=True)
            tmpFinal.reset_index(level=0, inplace=True)

            tmpFinal.columns = ['cal', 'tree', 'dcnt', 'bcnt']
            # x = pd.DataFrame(tmpFinal.value_counts('tree'))
            # x.columns = ['cnt']
            # x[x['cnt'] == 2]

            tmpFinal = tmpFinal.fillna(0.8)
            # tmpFinal[tmpFinal['bcnt'] == 0]['bcnt'] = 0.8
            # tmpFinal[tmpFinal['dcnt'] == 0]['dcnt'] = 0.8

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
            # ???????????? ???????????? ?????? makeMatrix?????? ???????????????
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
                    midSet = midSet[(midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt'])]
                    midSet = midSet[midSet['rRat'] > 20]
                    midSet = midSet.sort_values('dcnt', ascending=False)
                elif leafType == 'D':
                    midSet = midSet[(midSet['dvsb'] > 0)]
                    midSet = midSet[(midSet['prevDcnt'] > 0)]
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
                    midSet = midSet[(midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt'])]
                    midSet = midSet[midSet['rRat'] > 20]
                    midSet = midSet.sort_values('dcnt', ascending=False)
                elif leafType == 'D':
                    midSet = midSet[(midSet['dvsb'] > 0)]
                    midSet = midSet[(midSet['prevDcnt'] > 0)]
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
                    midSet = midSet[(midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt'])]
                    midSet = midSet[midSet['rRat'] > 20]
                    midSet = midSet.sort_values('dcnt', ascending=False)
                elif leafType == 'D':
                    midSet = midSet[(midSet['dvsb'] > 0)]
                    midSet = midSet[(midSet['prevDcnt'] > 0)]
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
                    midSet = midSet[(midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt'])]
                    midSet = midSet[midSet['rRat'] > 20]
                    midSet = midSet.sort_values('dcnt', ascending=False)
                elif leafType == 'D':
                    midSet = midSet[(midSet['dvsb'] > 0)]
                    midSet = midSet[(midSet['prevDcnt'] > 0)]
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
            elif treeNm == 'tree10':
                if leafType == 'A':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / bcnt asc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('bcnt')
                elif leafType == 'C':
                    midSet = midSet[(midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt'])]
                    midSet = midSet[midSet['rRat'] > 20]
                    midSet = midSet.sort_values('dcnt', ascending=False)
                elif leafType == 'D':
                    midSet = midSet[(midSet['dvsb'] > 0)]
                    midSet = midSet[(midSet['prevDcnt'] > 0)]
                    midSet = midSet.sort_values('bcnt', ascending=False)
                elif leafType == 'E':
                    # bcnt != 0 & dcnt != 0 & bcnt <= totCnt * 0.25 /	dcnt desc
                    midSet = midSet[midSet['bcnt'] <= (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.25]
                    midSet = midSet.sort_values('dcnt', ascending=False)

            # A,B,C,D ?????? ??????
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
            # A,B,C,D ???????????? ??????
            aggr_df = aggr_df[tmpBool]
            data = data[tmpBool]
            tmpRt = midSet.iloc[0]
            conTmp = conData[
                ~((conData['colname'] == midSet.iloc[0]['tree']) & (conData['cal'] == midSet.iloc[0]['cal_y']))]
            data = data.drop(conH, axis=1)

            # ???????????? ????????? ??????
            # if leafType == 'B' or leafType == 'C' or leafType == 'D':
            #     condition, tmpBool = chkDupCond(data, midSet, condition, tmpBool)

        else:
            tmpBool = pd.DataFrame()
            tmpRt = pd.DataFrame([(0.8, 0.8, 0, 0)], columns=['bcnt', 'dcnt', 'bvsd', 'dvsb'])
            conTmp = conData
    except Exception as e:
        tmpBool = pd.DataFrame()
        tmpRt = pd.DataFrame([(0.8, 0.8, 0, 0)], columns=['bcnt', 'dcnt', 'bvsd', 'dvsb'])
        conTmp = conData

    return data, aggr_df, tmpRt, tmpBool, condition, conTmp, midSet


def chkDupCond(data, midSet, condition, tmpBool):
    # ???????????? ????????? ??????
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
    # cond = dfCon.iloc[:, 1]
    cond = dfCon
    condDf = pd.DataFrame()
    newDf = pd.DataFrame()

    newDf['ymd'] = df['yyyymmdd']
    newDf['stCd'] = df['stock_code']
    newDf['pur_gubn5'] = df['pur_gubn5']
    
    for i in range(0, cond.shape[0]):
        con = cond.iloc[i, 0].split('>')[0]
        try:
            val = float(cond.iloc[i, 0].split('>')[1])
        except Exception as e:
            val = cond.iloc[i, 0].split('>')[1]
            pass
        
        colname = "tree" + str(i)
        condDf = condDf.append(pd.DataFrame([(con, val, 'GT', colname)], columns=['condi', 'value', 'cal', 'colname']))
        condDf = condDf.append(pd.DataFrame([(con, val, 'LT', colname)], columns=['condi', 'value', 'cal', 'colname']))
        condDf = condDf[~condDf['condi'].str.contains('-')]

        if '-' not in str(con):
            # ????????? ????????????(condi > value) ?????? 1??? ?????? ????????? 0
            if if_float(val):
                tmpBoolP = df[con] > val
            else:
                tmpBoolP = df[con] > df[val]

            newDf[colname] = tmpBoolP.apply(lambda x: 1 if x == True else 0)
            # newDf.append(tmpBoolP.apply(lambda x: 1 if x == True else 0), colname=['tree' + str(i)], ignore_index=True)

    return newDf, condDf


def if_float(string):
    try:
        float(string)
        return True
    except ValueError:
        return False


def drawTree(data, level, limitRatio, initConds, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum):
    #####################################################################################
    #  ????????? ????????? ??????
    # level = 2
    # data = newDff
    # initConds = dfConF
    # rRatYn = 'Y'
    # rRatNum = 1
    # rRtYn = 'Y'
    # rRtLvl = 3
    # rRtNum = 1
    # limitCntRatio = 0.3
    #####################################################################################
    mod = sys.modules[__name__]

    initDcnt = data.iloc[:, 2].value_counts()[0]
    # limitCnt = initDcnt * limitCntRatio * 0.01
    fResultT = pd.DataFrame()

    trees = pd.DataFrame([('tree1', 5)], columns=('treeNm', 'branch'))
    # trees = pd.DataFrame([('tree8', 3)], columns=('treeNm', 'branch'))
    trees = trees.append(pd.DataFrame([('tree2', 4)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree3', 4)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree4', 4)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree5', 4)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree6', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree7', 3)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree8', 3)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree9', 4)], columns=('treeNm', 'branch')))
    # trees = trees.append(pd.DataFrame([('tree10', 5)], columns=('treeNm', 'branch')))
    # i, j, row =  1, 3, 0
    for row in range(0, trees.shape[0]):
        # print('tree forloop')
        for i in range(0, level + 1):
            # print('level forloop')
            leaf = trees.iloc[row].branch ** i

            if i == 0:
                data['x'] = True
                datBool = data['x']
                data = data.drop('x', axis=1)

                if trees.iloc[row].treeNm != 'tree10':
                    setattr(mod, 'lvl_0_1', [datBool, initConds, 0.8, 0.8, 0.8, 0.8, '', '', data, data.iloc[:, 2]])
                else:
                    with open('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickleDtree/lvl_0_1.pkl', 'wb') as f:
                        pickle.dump([datBool, initConds, 0.8, 0.8, 0.8, 0.8, '', '', data, data.iloc[:, 2]], f)
            else:
                for j in range(1, leaf + 1):
                    if trees.iloc[row].treeNm == 'tree1':
                        # tmp = getattr(mod, 'lvl_1_5')
                        if j % trees.iloc[row].branch == 1:
                            tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            if prevDcnt > 0:
                                print("[" + datetime.datetime.today().strftime(
                                    "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(j) + " - " + trees.iloc[
                                          row].treeNm + " / prevBcnt : " + str(
                                    prevBcnt) + " / prevDcnt : " + str(
                                    prevDcnt))
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 2:
                            if prevDcnt > 0:
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                tmpInvCon = ''
                                tmpInvCon = changeCondition(tmpInv[6])
                                tmpInvCon = '(' + tmpInvCon + ')'

                                if i == 1:
                                    try:
                                        tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                            data.drop(tmpInv[8].index), data.drop(tmpInv[8].index)['pur_gubn5'],
                                            tmpInv[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                            rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                    except Exception as e:
                                        print(e)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 3:  # j = 3
                            if prevDcnt > 0:
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                tmpInvCon = ''
                                tmpInvCon = changeCondition(tmpInv[6])
                                tmpInvCon = '(' + tmpInvCon + ')'

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        data.drop(tmpInv[8].index), data.drop(tmpInv[8].index)['pur_gubn5'],
                                        tmpInv[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 4:
                            if prevDcnt > 0:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            if prevDcnt > 0:
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                tmpInvCon = ''
                                tmpInvCon = changeCondition(tmpInv[6])
                                tmpInvCon = '(' + tmpInvCon + ')'

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        data.drop(tmpInv[8].index), data.drop(tmpInv[8].index)['pur_gubn5'],
                                        tmpInv[1], 'E', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'E', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                    elif trees.iloc[row].treeNm == 'tree2':
                        # tmp = getattr(mod, 'lvl_1_5')
                        if j % trees.iloc[row].branch == 1:
                            tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            if prevDcnt > 0:
                                print("[" + datetime.datetime.today().strftime(
                                    "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(
                                    j) + " - " + trees.iloc[row].treeNm + " / prevBcnt : " + str(
                                    prevBcnt) + " / prevDcnt : " + str(
                                    prevDcnt))
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 2:
                            if prevDcnt > 0:
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                tmpInvCon = ''
                                tmpInvCon = changeCondition(tmpInv[6])
                                tmpInvCon = '(' + tmpInvCon + ')'

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        data.drop(tmpInv[8].index), data.drop(tmpInv[8].index)['pur_gubn5'],
                                        tmpInv[1],
                                        'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 3:
                            if prevDcnt > 0:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            if prevDcnt > 0:
                                tmpInv1 = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                tmpInvCon1 = ''
                                tmpInvCon1 = changeCondition(tmpInv[6])
                                tmpInvCon1 = '(' + tmpInvCon1 + ')'

                                # inv_B
                                tmpInv2 = getattr(mod, 'lvl_{}_{}'.format(i, j - 2))
                                tmpInvCon2 = ''
                                tmpInvCon2 = changeCondition(tmpInv2[6])
                                tmpInvCon2 = '(' + tmpInvCon2 + ')'

                                tmpInvCon = tmpInvCon1 + ' AND ' + tmpInvCon2

                                expData = pd.DataFrame(tmpInv1[8].index, columns=['ex'])
                                expData = expData.append(pd.DataFrame(tmpInv2[8].index, columns=['ex']))

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        data.drop(expData.ex.unique()), data.drop(expData.ex.unique())['pur_gubn5'],
                                        tmpInv[1],
                                        'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                    elif trees.iloc[row].treeNm == 'tree3':
                        # tmp = getattr(mod, 'lvl_1_5')
                        if j % trees.iloc[row].branch == 1:
                            tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            if prevDcnt > 0:
                                print("[" + datetime.datetime.today().strftime(
                                    "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(
                                    j) + " - " + trees.iloc[row].treeNm + " / prevBcnt : " + str(
                                    prevBcnt) + " / prevDcnt : " + str(
                                    prevDcnt))
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 2:
                            if prevDcnt > 0:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 3:  # j = 3
                            if prevDcnt > 0:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            if prevDcnt > 0:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                    elif trees.iloc[row].treeNm == 'tree4':
                        # tmp = getattr(mod, 'lvl_1_5')
                        if j % trees.iloc[row].branch == 1:
                            tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            if prevDcnt > 0:
                                print("[" + datetime.datetime.today().strftime(
                                    "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(
                                    j) + " - " + trees.iloc[row].treeNm + " / prevBcnt : " + str(
                                    prevBcnt) + " / prevDcnt : " + str(
                                    prevDcnt))
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 2:
                            if prevDcnt > 0:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 3:  # j = 3
                            if prevDcnt > 0:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            if prevDcnt > 0:
                                tmpInv1 = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                tmpInvCon1 = ''
                                tmpInvCon1 = changeCondition(tmpInv[6])
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

                                tmpInvCon = tmpInvCon1  # + ' AND ' + tmpInvCon2 + ' AND ' + tmpInvCon3

                                expData = pd.DataFrame(tmpInv1[8].index, columns=['ex'])
                                # expData = expData.append(pd.DataFrame(tmpInv2[8].index, columns=['ex']))
                                # expData = expData.append(pd.DataFrame(tmpInv3[8].index, columns=['ex']))

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        data.drop(expData.ex.unique()), data.drop(expData.ex.unique())['pur_gubn5'],
                                        tmpInv[1],
                                        'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                    elif trees.iloc[row].treeNm == 'tree5':
                        if j % trees.iloc[row].branch == 1:
                            tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            if prevDcnt > 0:
                                print("[" + datetime.datetime.today().strftime(
                                    "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(
                                    j) + " - " + trees.iloc[row].treeNm + " / prevBcnt : " + str(
                                    prevBcnt) + " / prevDcnt : " + str(
                                    prevDcnt))
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 2:
                            if prevDcnt > 0:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 3:  # j = 3
                            if prevDcnt > 0:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            if prevDcnt > 0:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                    elif trees.iloc[row].treeNm == 'tree6':
                        # tmp = getattr(mod, 'lvl_1_5')
                        if j % trees.iloc[row].branch == 1:
                            tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            if prevDcnt > 0:
                                print("[" + datetime.datetime.today().strftime(
                                    "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(
                                    j) + " - " + trees.iloc[row].treeNm + " / prevBcnt : " + str(
                                    prevBcnt) + " / prevDcnt : " + str(
                                    prevDcnt))
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            if prevDcnt > 0:
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                tmpInvCon = ''
                                tmpInvCon = changeCondition(tmpInv[6])
                                tmpInvCon = '(' + tmpInvCon + ')'

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        data.drop(tmpInv[8].index), data.drop(tmpInv[8].index)['pur_gubn5'],
                                        tmpInv[1],
                                        'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                    elif trees.iloc[row].treeNm == 'tree7':
                        # tmp = getattr(mod, 'lvl_1_5')
                        if j % trees.iloc[row].branch == 1:
                            tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            if prevDcnt > 0:
                                print("[" + datetime.datetime.today().strftime(
                                    "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(
                                    j) + " - " + trees.iloc[row].treeNm + " / prevBcnt : " + str(
                                    prevBcnt) + " / prevDcnt : " + str(
                                    prevDcnt))
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 2:
                            if prevDcnt > 0:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            if prevDcnt > 0:
                                tmpInv1 = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                                tmpInvCon = ''
                                tmpInvCon = changeCondition(tmpInv[6])
                                tmpInvCon = '(' + tmpInvCon + ')'

                                expData = pd.DataFrame(tmpInv1[8].index, columns=['ex'])
                                # expData = expData.append(pd.DataFrame(tmpInv2[8].index, columns=['ex']))

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        data.drop(expData.ex.unique()), data.drop(expData.ex.unique())['pur_gubn5'],
                                        tmpInv1[1],
                                        'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv1[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                    elif trees.iloc[row].treeNm == 'tree8':
                        # tmp = getattr(mod, 'lvl_1_5')
                        if j % trees.iloc[row].branch == 1:
                            tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            if prevDcnt > 0:
                                print("[" + datetime.datetime.today().strftime(
                                    "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(
                                    j) + " - " + trees.iloc[row].treeNm + " / prevBcnt : " + str(
                                    prevBcnt) + " / prevDcnt : " + str(
                                    prevDcnt))
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 2:
                            if prevDcnt > 0:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            if prevDcnt > 0:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                    elif trees.iloc[row].treeNm == 'tree9':
                        # tmp = getattr(mod, 'lvl_1_5')
                        if j % trees.iloc[row].branch == 1:
                            tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            if prevDcnt > 0:
                                print("[" + datetime.datetime.today().strftime(
                                    "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(
                                    j) + " - " + trees.iloc[row].treeNm + " / prevBcnt : " + str(
                                    prevBcnt) + " / prevDcnt : " + str(
                                    prevDcnt))
                                tmpNxtRat = pd.DataFrame()
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                                tmpNxtRat = tmpRatList
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 2:
                            if prevDcnt > 0:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 3:
                            if prevDcnt > 0:
                                tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 2))
                                tmpInvCon = ''
                                tmpInvCon = changeCondition(tmpInv[6])
                                tmpInvCon = '(' + tmpInvCon + ')'

                                if i == 1:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        data.drop(tmpInv[8].index), data.drop(tmpInv[8].index)['pur_gubn5'],
                                        tmpInv[1],
                                        'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)
                                else:
                                    tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                        tmp[8], tmp[9], tmp[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                        rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            if prevDcnt > 0:
                                tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                    tmp[8], tmp[9], tmp[1], 'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                    rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            else:
                                setattr(mod, 'lvl_{}_{}'.format(i, j),
                                        [pd.DataFrame(), pd.DataFrame(), 0.8, 0.8, 0.8, 0.8, tmpCon,
                                         tmpRatList, pd.DataFrame(), pd.DataFrame()])
                        # -----------------------------------------------------------------------------------------------
                    elif trees.iloc[row].treeNm == 'tree10':
                        # tmp = getattr(mod, 'lvl_1_5')
                        if j % trees.iloc[row].branch == 1:
                            with open("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickleDtree/lvl_{}_{}.pkl".format(i - 1,
                                                                                                                 j //
                                                                                                                 trees.iloc[
                                                                                                                     row].branch + 1),
                                      "rb") as fr:
                                tmp = pickle.load(fr)

                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            print("[" + datetime.datetime.today().strftime(
                                "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(j) + " - " + trees.iloc[
                                      row].treeNm + " / prevBcnt : " + str(
                                prevBcnt) + " / prevDcnt : " + str(
                                prevDcnt))
                            tmpNxtRat = pd.DataFrame()
                            tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                            tmpNxtRat = tmpRatList

                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 2:
                            with open("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickleDtree/lvl_{}_{}.pkl".format(i,
                                                                                                                 j - 1),
                                      "rb") as fr:
                                tmpInv = pickle.load(fr)
                            tmpInvCon = ''
                            tmpInvCon = changeCondition(tmpInv[6])
                            tmpInvCon = '(' + tmpInvCon + ')'

                            tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                data.drop(tmpInv[8].index), data.drop(tmpInv[8].index)['pur_gubn5'],
                                tmpInv[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)

                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 3:  # j = 3
                            # tmpInv = getattr(mod, 'lvl_{}_{}'.format(i, j - 1))
                            with open("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickleDtree/lvl_{}_{}.pkl".format(i,
                                                                                                                 j - 1),
                                      "rb") as fr:
                                tmpInv = pickle.load(fr)
                            tmpInvCon = ''
                            tmpInvCon = changeCondition(tmpInv[6])
                            tmpInvCon = '(' + tmpInvCon + ')'

                            tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                data.drop(tmpInv[8].index), data.drop(tmpInv[8].index)['pur_gubn5'],
                                tmpInv[1], 'C', rRatYn, rRatNum, rRtYn, rRtLvl,
                                rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)

                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 4:
                            tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                tmp[8], tmp[9], tmp[1], 'D', rRatYn, rRatNum, rRtYn, rRtLvl,
                                rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            with open("C:/Users/Shine_anal/PycharmProjects/anlaysis/pickleDtree/lvl_{}_{}.pkl".format(i,
                                                                                                                 j - 1),
                                      "rb") as fr:
                                tmpInv = pickle.load(fr)
                            tmpInvCon = ''
                            tmpInvCon = changeCondition(tmpInv[6])
                            tmpInvCon = '(' + tmpInvCon + ')'

                            tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrix(
                                data.drop(tmpInv[8].index), data.drop(tmpInv[8].index)['pur_gubn5'],
                                tmpInv[1], 'E', rRatYn, rRatNum, rRtYn, rRtLvl,
                                rRtNum, tmpInvCon, ~tmpInv[0], i, '', 'Y', trees.iloc[row].treeNm)

                    if trees.iloc[row].treeNm != 'tree10':
                        setattr(mod, 'lvl_{}_{}'.format(i, j),
                                [tmpBool, tmpConDf, tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb, tmpCon,
                                 tmpRatList, tmpDat, tmpAggrDf])
                    else:
                        with open('C:/Users/Shine_anal/PycharmProjects/anlaysis/pickleDtree/lvl_{}_{}.pkl'.format(i, j),
                                  'wb') as f:
                            pickle.dump([tmpBool, tmpConDf, tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb, tmpCon,
                                         tmpRatList, tmpDat, tmpAggrDf], f)

                    try:
                        bcnt = float(tmpRat.bcnt)
                    except Exception as e:
                        bcnt = 0.8
                        pass

                    try:
                        dcnt = float(tmpRat.dcnt)
                    except Exception as e:
                        dcnt = 0.8
                        pass

                    print("[" + datetime.datetime.today().strftime(
                        "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(j) + " - " + trees.iloc[
                              row].treeNm + " / bcnt : " + str(
                        bcnt) + " / dcnt : " + str(
                        dcnt))

                    try:
                        fResultT = fResultT.append(
                            pd.DataFrame(
                                [('lvl_' + str(i) + '_' + str(j), tmpCon, bcnt, dcnt, bcnt / dcnt, dcnt / bcnt)],
                                columns=['lvl', 'condi', 'bcnt', 'dcnt', 'bvsd', 'dvsb']))
                    except Exception as e:
                        pass

                    if trees.iloc[row].treeNm != 'tree10':
                        if i >= 2:  # ?????? 1,2,3??????????????? ????????? ?????? ????????? ????????????
                            leaf2 = trees.iloc[row].branch ** (i - 2)
                            try:
                                for l in range(1, leaf2 + 1):
                                    delattr(mod, 'lvl_{}_{}'.format(i - 2, l))
                            except Exception as e:
                                pass

                        if j >= trees.iloc[row].branch and j % trees.iloc[row].branch == 0:
                            try:
                                delattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch))
                            except Exception as e:
                                print(e)
                                pass

                        if i == level:
                            try:
                                delattr(mod, 'lvl_{}_{}'.format(i, j - trees.iloc[row].branch + 1))
                            except Exception as e:
                                pass
                    else:
                        if i >= 2:  # ?????? 1,2,3??????????????? ????????? ?????? ????????? ????????????
                            leaf2 = trees.iloc[row].branch ** (i - 2)
                            try:
                                for l in range(1, leaf2 + 1):
                                    # delattr(mod, 'lvl_{}_{}'.format(i-2, l))
                                    if os.path.isfile(
                                            'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickleDtree/lvl_{}_{}.pkl'.format(
                                                i - 2,
                                                j)):
                                        os.remove(
                                            'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickleDtree/lvl_{}_{}.pkl'.format(
                                                i - 2,
                                                j))
                            except Exception as e:
                                pass

                        if j >= trees.iloc[row].branch and j % trees.iloc[row].branch == 0:
                            try:
                                # delattr(mod, 'lvl_{}_{}'.format(i - 1, j // branch))
                                if os.path.isfile(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickleDtree/lvl_{}_{}.pkl'.format(
                                            i - 1,
                                            j // trees.iloc[row].branch)):
                                    os.remove(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickleDtree/lvl_{}_{}.pkl'.format(
                                            i - 1,
                                            j // trees.iloc[row].branch))
                            except Exception as e:
                                print(e)
                                pass

                        if i == level:
                            try:
                                # delattr(mod, 'lvl_{}_{}'.format(i, j - branch + 1))
                                if os.path.isfile(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickleDtree/lvl_{}_{}.pkl'.format(i,
                                                                                                                   j -
                                                                                                                   trees.iloc[
                                                                                                                       row].branch + 1)):
                                    os.remove(
                                        'C:/Users/Shine_anal/PycharmProjects/anlaysis/pickleDtree/lvl_{}_{}.pkl'.format(i,
                                                                                                                   j -
                                                                                                                   trees.iloc[
                                                                                                                       row].branch + 1))
                            except Exception as e:
                                pass

    return fResultT


def makeMatrixPM(data, aggr_df, conData, leafType, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum, condition, datBool, lvl,
                 ratDf, invYn, treeNm):
    # data = tmp[8]
    # aggr_df = tmp[9]
    # conData = tmp[1]
    # leafType = 'B'
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
    # ratDf = tmp[7]
    # invYn = 'N'
    # treeNm = 'tree1'
    # -----------------------------------------
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
        if len(data) > 0:
            meltDt = data.iloc[:, 2:data.shape[1]].melt(id_vars='pur_gubn5')
            meltSummary = pd.DataFrame(meltDt.value_counts().reset_index(name='counts')).sort_values(
                ['variable', 'value'])
            tmpFinal = meltSummary.pivot(index=['variable', 'value'], columns='pur_gubn5', values='counts')

            tmpFinal.columns = tmpFinal.columns.values
            tmpFinal.reset_index(level=0, inplace=True)
            tmpFinal.reset_index(level=0, inplace=True)

            tmpFinal.columns = ['cal', 'tree', 'dcnt', 'bcnt']

            tmpFinal = tmpFinal.fillna(0.8)
            # tmpFinal[tmpFinal['bcnt'] == 0]['bcnt'] = 0.8
            # tmpFinal[tmpFinal['dcnt'] == 0]['dcnt'] = 0.8

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
            # ???????????? ???????????? ?????? makeMatrixPM?????? ???????????????
            if treeNm == 'tree1':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('bvsd', ascending=False)
                    else:
                        # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / bcnt asc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.3]
                    midSet = midSet.sort_values('rrr', ascending=False)
            elif treeNm == 'tree2':
                if leafType == 'A':
                    if lvl == 1:
                        # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / bcnt asc
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('bcnt')
                    else:
                        # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / bcnt asc
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.3]
                    midSet = midSet.sort_values('bcnt')
            elif treeNm == 'tree3':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet[midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt']]
                        midSet = midSet.sort_values('dcnt', ascending=False)
                    else:
                        # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet[midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt']]
                    midSet = midSet.sort_values('dcnt', ascending=False)
            elif treeNm == 'tree4':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet.sort_values('bcnt', ascending=False)
                    else:
                        # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet.sort_values('bcnt', ascending=False)
            elif treeNm == 'tree5':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet[midSet['bcnt'] <= (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.25]
                        midSet = midSet.sort_values('dcnt', ascending=False)
                    else:
                        # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet[midSet['bcnt'] <= (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.25]
                    midSet = midSet.sort_values('dcnt', ascending=False)
            elif treeNm == 'tree6':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet[midSet['rRat'] > 20]
                        midSetChk = midSet[midSet['rRt'] > midSet['dvsb']]

                        if midSetChk['dvsb'].count() > 0:
                            midSet = midSet.sort_values('rRt', ascending=False)
                        else:
                            midSet = midSet.sort_values('sRt', ascending=False)
                    else:
                        # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet[midSet['rRat'] > 20]
                    midSetChk = midSet[midSet['rRt'] > midSet['dvsb']]

                    if midSetChk['dvsb'].count() > 0:
                        midSet = midSet.sort_values('rRt', ascending=False)
                    else:
                        midSet = midSet.sort_values('sRt', ascending=False)
            elif treeNm == 'tree7':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet[midSet['rRat'] > 35]
                        midSet = midSet.sort_values('rRt', ascending=False)
                    else:
                        # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('dvsb', ascending=False)
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
                    if lvl == 1:
                        midSet = midSet[midSet['rRat'] > 40]
                        midSet = midSet.sort_values('sRt', ascending=False)
                    else:
                        # bcnt != 0 & dcnt != 0 & (bcnt + dcnt) > totCnt * 0.2  / dvsb desc
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
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

                    midSet = midSet.sort_values('sRt', ascending=False)

            # A,B,C,D ?????? ??????
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
            # A,B,C,D ???????????? ??????
            aggr_df = aggr_df[tmpBool]
            data = data[tmpBool]
            tmpRt = midSet.iloc[0]
            conTmp = conData[~((conData['colname'] == midSet.iloc[0]['tree']))]
            data = data.drop(conH, axis=1)

            # ???????????? ????????? ??????
            if leafType == 'B' or leafType == 'C' or leafType == 'D':
                condition, tmpBool = chkDupCond(data, midSet, condition, tmpBool)

        else:
            tmpBool = pd.DataFrame()
            tmpRt = pd.DataFrame([(0.8, 0.8, 0, 0)], columns=['bcnt', 'dcnt', 'bvsd', 'dvsb'])
            conTmp = conData
    except Exception as e:
        tmpBool = pd.DataFrame()
        tmpRt = pd.DataFrame([(0.8, 0.8, 0, 0)], columns=['bcnt', 'dcnt', 'bvsd', 'dvsb'])
        conTmp = conData

    return data, aggr_df, tmpRt, tmpBool, condition, conTmp, midSet


def drawTreePM(data, level, limitRatio, initConds, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum):
    #####################################################################################
    #  ????????? ????????? ??????
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

    trees = pd.DataFrame([('tree1', 2)], columns=('treeNm', 'branch'))
    trees = trees.append(pd.DataFrame([('tree2', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree3', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree4', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree5', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree6', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree7', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree8', 2)], columns=('treeNm', 'branch')))
    # row = 0
    # i = 2 j = 2 row = 0
    try:
        for row in range(0, trees.shape[0]):
            for i in range(0, level + 1):
                leaf = trees.iloc[row].branch ** i

                if i == 0:
                    data['x'] = True
                    datBool = data['x']
                    data = data.drop('x', axis=1)
                    setattr(mod, 'lvl_0_1', [datBool, initConds, 0.8, 0.8, 0.8, 0.8, '', '', data, data.iloc[:, 2]])
                else:
                    for j in range(1, leaf + 1):
                        # -----------------------------------------------------------------------------------------------
                        print("[" + datetime.datetime.today().strftime(
                            "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(j))
                        # -----------------------------------------------------------------------------------------------
                        if j % trees.iloc[row].branch == 1:
                            tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))

                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            print("[" + datetime.datetime.today().strftime(
                                "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(j) + " - " + trees.iloc[
                                      row].treeNm + " / prevBcnt : " + str(
                                prevBcnt) + " / prevDcnt : " + str(
                                prevDcnt))
                            tmpNxtRat = pd.DataFrame()
                            tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrixPM(
                                tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                rRtNum, tmp[6], tmp[0], i, tmp[7], 'N', trees.iloc[row].treeNm)
                            tmpNxtRat = tmpRatList
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrixPM(
                                tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)

                        if i >= 3:  # ?????? 1,2,3??????????????? ????????? ?????? ????????? ????????????
                            setattr(mod, 'lvl_{}_{}'.format(i, j),
                                    [pd.DataFrame(), pd.DataFrame(), tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb,
                                     tmpCon,
                                     tmpRatList, pd.DataFrame(), pd.DataFrame()])

                        if i != level:
                            # print("here2!!")
                            setattr(mod, 'lvl_{}_{}'.format(i, j),
                                    [tmpBool, tmpConDf, tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb, tmpCon,
                                     tmpRatList, tmpDat, tmpAggrDf])
                        else:  # ????????? ??????????????? ?????? ?????? ?????? ??????????????? ???????????? ??????
                            # print("here3!!")
                            setattr(mod, 'lvl_{}_{}'.format(i, j),
                                    [pd.DataFrame(), pd.DataFrame(), tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb,
                                     tmpCon,
                                     tmpRatList, pd.DataFrame(), pd.DataFrame()])

            # mod = sys.modules[__name__]
            for i in range(0, level + 1):
                leaf = trees.iloc[row].branch ** i
                for j in range(1, leaf + 1):
                    tmp = getattr(mod, 'lvl_{}_{}'.format(i, j))
                    # print('lvl_'+str(i)+'_'+str(j))
                    # print(tmp[3].value_counts('pur_gubn5')[0])
                    try:
                        bcnt = float(tmp[2])
                        if bcnt == 0:
                            bcnt = 0.8
                    except Exception as e:
                        bcnt = 0.8
                        pass

                    try:
                        dcnt = float(tmp[3])
                        if dcnt == 0:
                            dcnt = 0.8
                    except Exception as e:
                        dcnt = 0.8
                        pass

                    fResult = fResult.append(
                        pd.DataFrame([('lvl_' + str(i) + '_' + str(j), tmp[6], bcnt, dcnt,
                                       bcnt / dcnt, dcnt / bcnt)],
                                     columns=['lvl', 'condi', 'bcnt', 'dcnt', 'bvsd', 'dvsb']))
                    # ?????? ?????? ??????
                    delattr(mod, 'lvl_{}_{}'.format(i, j))
    except Exception as e:
        print(e)
        setattr(mod, 'lvl_{}_{}'.format(i, j),
                [tmpBool, tmpConDf, tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb, tmpCon, tmpRatList, tmpDat,
                 tmpAggrDf])

    return fResult


def makeMatrixPMN(data, aggr_df, conData, leafType, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum, condition, datBool, lvl,
                  ratDf, invYn, treeNm):
    # data = tmp[8]
    # aggr_df = tmp[9]
    # conData = tmp[1]
    # leafType = 'B'
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
    # ratDf = tmp[7]
    # invYn = 'N'
    # treeNm = 'tree1'
    # -----------------------------------------
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
        if len(data) > 0:
            meltDt = data.iloc[:, 2:data.shape[1]].melt(id_vars='pur_gubn5')
            meltSummary = pd.DataFrame(meltDt.value_counts().reset_index(name='counts')).sort_values(
                ['variable', 'value'])
            tmpFinal = meltSummary.pivot(index=['variable', 'value'], columns='pur_gubn5', values='counts')

            tmpFinal.columns = tmpFinal.columns.values
            tmpFinal.reset_index(level=0, inplace=True)
            tmpFinal.reset_index(level=0, inplace=True)

            tmpFinal.columns = ['cal', 'tree', 'dcnt', 'bcnt']
            tmpFinal = tmpFinal.fillna(0.8)
            # tmpFinal[tmpFinal['bcnt'] == 0]['bcnt'] = 0.8
            # tmpFinal[tmpFinal['dcnt'] == 0]['dcnt'] = 0.8

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
            # ???????????? ???????????? ?????? makeMatrixPMN?????? ???????????????
            if treeNm == 'tree1':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet.sort_values('dvsb', ascending=False)

                        conH = midSet.iloc[0]['tree']
                        rConH = midSet.iloc[0]['condi']
                        valH = midSet.iloc[0]['value']
                        calX = midSet.iloc[0]['cal_x']
                        calH = midSet.iloc[0]['cal_y']
                    else:
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.3]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('bcnt')
            elif treeNm == 'tree2':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('dvsb', ascending=False)

                        conH = midSet.iloc[1]['tree']
                        rConH = midSet.iloc[1]['condi']
                        valH = midSet.iloc[1]['value']
                        calX = midSet.iloc[1]['cal_x']
                        calH = midSet.iloc[1]['cal_y']
                    else:
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.22]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet[midSet['dvsb'] > midSet['prevDcnt'] / midSet['prevBcnt']]
                    midSet = midSet.sort_values('dcnt', ascending=False)
            elif treeNm == 'tree3':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.4]
                        midSet = midSet.sort_values('dvsb', ascending=False)

                        conH = midSet.iloc[0]['tree']
                        rConH = midSet.iloc[0]['condi']
                        valH = midSet.iloc[0]['value']
                        calX = midSet.iloc[0]['cal_x']
                        calH = midSet.iloc[0]['cal_y']
                    else:
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.23]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet.sort_values('bcnt', ascending=False)
            elif treeNm == 'tree4':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.4]
                        midSet = midSet.sort_values('dvsb', ascending=False)

                        conH = midSet.iloc[1]['tree']
                        rConH = midSet.iloc[1]['condi']
                        valH = midSet.iloc[1]['value']
                        calX = midSet.iloc[1]['cal_x']
                        calH = midSet.iloc[1]['cal_y']
                    else:
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet.sort_values('bcnt')
            elif treeNm == 'tree5':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet.sort_values('bcnt', ascending=False)

                        conH = midSet.iloc[0]['tree']
                        rConH = midSet.iloc[0]['condi']
                        valH = midSet.iloc[0]['value']
                        calX = midSet.iloc[0]['cal_x']
                        calH = midSet.iloc[0]['cal_y']
                    else:
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.24]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet[midSet['bcnt'] <= (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.25]
                    midSet = midSet.sort_values('dcnt', ascending=False)
            elif treeNm == 'tree6':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet.sort_values('bcnt', ascending=False)

                        conH = midSet.iloc[1]['tree']
                        rConH = midSet.iloc[1]['condi']
                        valH = midSet.iloc[1]['value']
                        calX = midSet.iloc[1]['cal_x']
                        calH = midSet.iloc[1]['cal_y']
                    else:
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.25]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet[midSet['rRat'] > 20]
                    midSetChk = midSet[midSet['rRt'] > midSet['dvsb']]

                    if midSetChk['dvsb'].count() > 0:
                        midSet = midSet.sort_values('rRt', ascending=False)
                    else:
                        midSet = midSet.sort_values('sRt', ascending=False)
            elif treeNm == 'tree7':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet.sort_values('bcnt')

                        conH = midSet.iloc[0]['tree']
                        rConH = midSet.iloc[0]['condi']
                        valH = midSet.iloc[0]['value']
                        calX = midSet.iloc[0]['cal_x']
                        calH = midSet.iloc[0]['cal_y']
                    else:
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.26]
                        midSet = midSet.sort_values('dvsb', ascending=False)
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
                    if lvl == 1:
                        midSet = midSet.sort_values('dcnt', ascending=False)

                        conH = midSet.iloc[0]['tree']
                        rConH = midSet.iloc[0]['condi']
                        valH = midSet.iloc[0]['value']
                        calX = midSet.iloc[0]['cal_x']
                        calH = midSet.iloc[0]['cal_y']
                    else:
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.27]
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

                    midSet = midSet.sort_values('sRt', ascending=False)
            elif treeNm == 'tree9':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet.sort_values('dcnt', ascending=False)

                        conH = midSet.iloc[1]['tree']
                        rConH = midSet.iloc[1]['condi']
                        valH = midSet.iloc[1]['value']
                        calX = midSet.iloc[1]['cal_x']
                        calH = midSet.iloc[1]['cal_y']
                    else:
                        midSet = midSet[
                            midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.28]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet[midSet['bcnt'] <= (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.25]
                    midSet = midSet.sort_values('dcnt', ascending=False)

            if (lvl > 1 and leafType == 'A') or leafType == 'B':
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
            # A,B,C,D ???????????? ??????

            aggr_df = aggr_df[tmpBool]
            data = data[tmpBool]
            tmpRt = midSet.iloc[0]
            conTmp = conData[~((conData['colname'] == midSet.iloc[0]['tree']))]
            data = data.drop(conH, axis=1)

            # ???????????? ????????? ??????
            if leafType == 'B' or leafType == 'C' or leafType == 'D':
                condition, tmpBool = chkDupCond(data, midSet, condition, tmpBool)

        else:
            tmpBool = pd.DataFrame()
            tmpRt = pd.DataFrame([(0.8, 0.8, 0, 0)], columns=['bcnt', 'dcnt', 'bvsd', 'dvsb'])
            conTmp = conData
    except Exception as e:
        tmpBool = pd.DataFrame()
        tmpRt = pd.DataFrame([(0.8, 0.8, 0, 0)], columns=['bcnt', 'dcnt', 'bvsd', 'dvsb'])
        conTmp = conData

    return data, aggr_df, tmpRt, tmpBool, condition, conTmp, midSet


def drawTreePMN(data, level, limitRatio, initConds, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum):
    #####################################################################################
    # ????????? ????????? ??????
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

    trees = pd.DataFrame([('tree1', 2)], columns=('treeNm', 'branch'))
    trees = trees.append(pd.DataFrame([('tree2', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree3', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree4', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree5', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree6', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree7', 2)], columns=('treeNm', 'branch')))
    trees = trees.append(pd.DataFrame([('tree8', 2)], columns=('treeNm', 'branch')))
    # row = 0
    # i = 1 j = 2 row = 0
    try:
        for row in range(0, trees.shape[0]):
            for i in range(0, level + 1):
                leaf = trees.iloc[row].branch ** i

                if i == 0:
                    data['x'] = True
                    datBool = data['x']
                    data = data.drop('x', axis=1)
                    setattr(mod, 'lvl_0_1', [datBool, initConds, 0.8, 0.8, 0.8, 0.8, '', '', data, data.iloc[:, 2]])
                else:
                    for j in range(1, leaf + 1):
                        # -----------------------------------------------------------------------------------------------
                        print("[" + datetime.datetime.today().strftime(
                            "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(j))
                        # -----------------------------------------------------------------------------------------------
                        if j % trees.iloc[row].branch == 1:
                            tmp = getattr(mod, 'lvl_{}_{}'.format(i - 1, j // trees.iloc[row].branch + 1))
                            # tmp = getattr(mod, 'lvl_0_1')
                            try:
                                prevDcnt = tmp[8].iloc[:, 2].value_counts()[0]
                            except Exception as e:
                                prevDcnt = 0.8
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0.8
                                pass

                            print("[" + datetime.datetime.today().strftime(
                                "%Y-%m-%d %H:%M:%S") + "] Lvl_" + str(i) + "_" + str(j) + " - " + trees.iloc[
                                      row].treeNm + " / prevBcnt : " + str(
                                prevBcnt) + " / prevDcnt : " + str(
                                prevDcnt))
                            tmpNxtRat = pd.DataFrame()
                            tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrixPMN(
                                tmp[8], tmp[9], tmp[1], 'A', rRatYn, rRatNum, rRtYn, rRtLvl,
                                rRtNum, tmp[6], tmp[0], i, tmp[7], 'N', trees.iloc[row].treeNm)
                            tmpNxtRat = tmpRatList
                        # -----------------------------------------------------------------------------------------------
                        elif j % trees.iloc[row].branch == 0:
                            tmpDat, tmpAggrDf, tmpRat, tmpBool, tmpCon, tmpConDf, tmpRatList = makeMatrixPMN(
                                tmp[8], tmp[9], tmp[1], 'B', rRatYn, rRatNum, rRtYn, rRtLvl,
                                rRtNum, tmp[6], tmp[0], i, tmpNxtRat, 'N', trees.iloc[row].treeNm)

                        if i >= 3:  # ?????? 1,2,3??????????????? ????????? ?????? ????????? ????????????
                            setattr(mod, 'lvl_{}_{}'.format(i, j),
                                    [pd.DataFrame(), pd.DataFrame(), tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb,
                                     tmpCon,
                                     tmpRatList, pd.DataFrame(), pd.DataFrame()])

                        if i != level:
                            # print("here2!!")
                            setattr(mod, 'lvl_{}_{}'.format(i, j),
                                    [tmpBool, tmpConDf, tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb, tmpCon,
                                     tmpRatList, tmpDat, tmpAggrDf])
                        else:  # ????????? ??????????????? ?????? ?????? ?????? ??????????????? ???????????? ??????
                            # print("here3!!")
                            setattr(mod, 'lvl_{}_{}'.format(i, j),
                                    [pd.DataFrame(), pd.DataFrame(), tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb,
                                     tmpCon,
                                     tmpRatList, pd.DataFrame(), pd.DataFrame()])

            # mod = sys.modules[__name__]
            for i in range(0, level + 1):
                leaf = trees.iloc[row].branch ** i
                for j in range(1, leaf + 1):
                    tmp = getattr(mod, 'lvl_{}_{}'.format(i, j))
                    # print('lvl_'+str(i)+'_'+str(j))
                    # print(tmp[3].value_counts('pur_gubn5')[0])
                    try:
                        bcnt = float(tmp[2])
                        if bcnt == 0:
                            bcnt = 0.8
                    except Exception as e:
                        bcnt = 0.8
                        pass

                    try:
                        dcnt = float(tmp[3])
                        if dcnt == 0:
                            dcnt = 0.8
                    except Exception as e:
                        dcnt = 0.8
                        pass

                    fResult = fResult.append(
                        pd.DataFrame([('lvl_' + str(i) + '_' + str(j), tmp[6], bcnt, dcnt,
                                       bcnt / dcnt, dcnt / bcnt)],
                                     columns=['lvl', 'condi', 'bcnt', 'dcnt', 'bvsd', 'dvsb']))
                    # ?????? ?????? ??????
                    delattr(mod, 'lvl_{}_{}'.format(i, j))
    except Exception as e:
        print(e)
        setattr(mod, 'lvl_{}_{}'.format(i, j),
                [tmpBool, tmpConDf, tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb, tmpCon, tmpRatList, tmpDat,
                 tmpAggrDf])

    return fResult


def checkCondition(data, condition):
    condition = pd.DataFrame(condition.split(' AND '))

    for i in range(0, condition.shape[0]):
        if condition.iloc[i, 0].find('<=') > 0:
            con = condition.iloc[i, 0].split('<=')[0]
            con = str(con).replace(' ', '')

            try:
                val = float(condition.iloc[i, 0].split('<=')[1])
                data = data[data[con] <= val]
            except Exception as e:
                val = condition.iloc[i, 0].split('<=')[1]
                val = str(val).replace(' ', '')
                data = data[data[con] <= data[val]]
                pass

        else:
            con = condition.iloc[i, 0].split('>')[0]
            con = str(con).replace(' ', '')

            try:
                val = float(condition.iloc[i, 0].split('>')[1])
                data = data[data[con] > val]
            except Exception as e:
                val = condition.iloc[i, 0].split('>')[1]
                val = str(val).replace(' ', '')
                data = data[data[con] > data[val]]
                pass

    return print(data.value_counts('pur_gubn5'))


def changeCondition(origin):
    origin = origin.split(' ')
    mod = ''
    for o in range(0, len(origin)):
        if origin[o] == '>':
            origin[o] = '<='
        elif origin[o] == '<=':
            origin[o] = '>'
        elif origin[o] == 'AND':
            origin[o] = 'OR'
        elif origin[o] == 'OR':
            origin[o] = 'AND'

    for x in range(0, len(origin)):
        if x == 0:
            mod = origin[x]
        else:
            mod = mod + ' ' + origin[x]
    return mod


def run(data, condDcnt, condDvsb):
    # data = pd.read_csv('C:/Users/Shine_anal/PycharmProjects/anlaysis/kbuy1_20210927_parallel_all.csv')
    data = data[(data['dvsb'] > condDcnt) & (data['dcnt'] > condDvsb)]
    data['conLevel'] = 0
    data['dupYn'] = 'N'

    data = data.drop_duplicates()
    data = data.reset_index(drop=True)

    for i in range(0, data.shape[0]):
        try:
            data['conLevel'][i] = pd.DataFrame(data['condi'][i].split(' AND ')).shape[0]
        except Exception as e:
            data['conLevel'][i] = 0
            pass

    data = data.sort_values(by=['conLevel', 'condi'], ascending=[True, True])
    data = data.reset_index(drop=True)

    for j in range(0, data.shape[0] - 1):
        for k in range(j + 1, data.shape[0]):
            print('j : ' + str(j) + ' k : ' + str(k))
            try:
                if (data['condi'][j] in data['condi'][k]) and (data['conLevel'][j] < data['conLevel'][k]):
                    # print("j : (" + str(j) + ")" + data['condi'][j] + " / k : (" + str(k) + ")" + data['condi'][k] )
                    data['dupYn'][j] = 'Y'
            except Exception as e:
                pass

    return data[data['dupYn'] == 'N']


def runDtree(data, level, limitRatio, initConds, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum):
    # ?????? ??????
    fResult_md = drawTree(data, level, limitRatio, initConds, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum)
    fResult_pm = drawTreePM(data, level, limitRatio, initConds, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum)
    fResult_pmn = drawTreePMN(data, level, limitRatio, initConds, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum)

    fResult = fResult_md
    fResult = fResult.append(fResult_pm)
    fResult = fResult.append(fResult_pmn)

    fResult = fResult.iloc[:, 1:fResult.shape[1]].drop_duplicates()
    fResult = fResult.sort_values('dvsb', ascending=False)

    return fResult

def similarDel(result, data):
    resultGt = result[(result['cal'] == 'GT')].sort_values(by=["col", "bcnt"],
                                                           ascending=[False, False]).drop_duplicates()
    resultLt = result[(result['cal'] == 'LT')].sort_values(by=["col", "bcnt"],
                                                           ascending=[False, False]).drop_duplicates()

    finalSet = pd.DataFrame()

    for y in data.iloc[:, 14:data.shape[1]].columns:
        # for y in data.iloc[:, 14:50].columns:
        x = resultGt[(resultGt['col'] == y)]
        # x = resultGt[(resultGt['col'] == 'macd')]
        chkNum = 0
        print("[" + datetime.datetime.today().strftime(
            "%Y-%m-%d %H:%M:%S") + "] condi : " + y)
        for j in range(0, x.shape[0] - 1):
            if chkNum > 5 or j == 0:
                chkNum = 0

            chkNum = chkNum + (x.iloc[j].bcnt - x.iloc[j + 1].bcnt) / x.iloc[j].bcnt * 100

            try:
                if chkNum > 5:
                    x['chkYn'][j] = 'Y'
                else:
                    x['chkYn'][j] = 'N'
            except Exception as e:
                x['chkYn'][j] = 'N'

            finalSet = finalSet.append(x.iloc[j])

    for d in data.iloc[:, 14:data.shape[1]].columns:
        # for d in data.iloc[:, 14:50].columns:
        z = resultLt[(resultLt['col'] == d)]
        print("[" + datetime.datetime.today().strftime(
            "%Y-%m-%d %H:%M:%S") + "] condi : " + d)
        chkNum = 0

        for j in range(0, z.shape[0] - 1):
            if chkNum > 5 or j == 0:
                chkNum = 0

            chkNum = chkNum + (z.iloc[j].bcnt - z.iloc[j + 1].bcnt) / z.iloc[j].bcnt * 100

            if chkNum > 5:
                z['chkYn'][j] = 'Y'
            else:
                z['chkYn'][j] = 'N'
            # print((z.iloc[j].bcnt - z.iloc[j+1].bcnt) / z.iloc[j].bcnt * 100)
            # print(z['condi'][j])
            finalSet = finalSet.append(z.iloc[j])

    finalSet = finalSet[finalSet['chkYn'] == 'Y']

    return finalSet

str1="dis20_ang1_dis60_ang2>2 atan_nmind_nmind_sig_gap>-34 cv>0 macd_osc7_close7_rate_ang>0 fast10D_ang3_1bd>0 fast10D_ang1>slow10D_ang1 bbands_up_ang1>stdbal_sls_ang1 aroon_up>20 cv_ang1>0 sona_sig_ang1>0 sona_mom>sona_sig high_low_rate>10 macd_osc>0 macd_sig_ang1>0 momp5_sig4_ang3_1bd>0 macd>macd_sig mfi_sig_ang1>0 mfi>mfi_sig momp_sig_ang3_1bd>0 nmind>nmind_sig high_price_ang2>high_price_ang3 cv_arc1>30 dis10_ang1_dis20_ang2>2 sroc13_ang2>0 vo_ang1>0 bbands_up>stdbal_sls atan_sona_mom_sona_sig_gap>49 Adx>vo atan2_macd_osc_close_rate7_ang>36 fast5D_ang3_1bd>0 stdbal_cl_arc3>-72 rsi_arc7>-52 momp_arc7>55 atan_nmind_nmind_sig_gap>44 sma5__sar>0 stdbal_es__sar>0 momp_sig_ang1>0 stdbal_cl__sar>0 aroon10_up>aroon10_dn dis5_ang1_dis10_ang1>dis5_ang1_dis20_ang1 bbands_mid_ang1>stdbal_lls_ang1 sma20_ang1>stdbal_lls_ang1 dmi_ang1>0 sona_sig>-60 macd_slow3k_ang2>0 aroon10_up>fast5D vr_arc3>-62 sma10__sar>0 aroon10_up>fast3D stdbal_es__stdbal_sls>0 sma5_arc3>-50 macd_ang1>cci_sig_ang1 bbands_up_arc3>-66 atan_dis10_dis20_gap>-85 sma5_ang3>0 stdbal_cl_ang1>stdbal_sls_ang1 aroon10_up>slow10D high_price_ang1>low_price_ang1 bwi_ang1>0 macd_slow3D>20 sma5_arc3>54 sroc_ang2>0 aroon10_up>m14_slow3D aroon10_up>slow5D nmind_arc3>-44 sona_mom_ang1>cci_sig_ang1 bbands_up_arc3>34 rsi_sig_ang2>0 sroc_sig_ang3_1bd>0 sroc13_sig_ang3_1bd>0 rsi_sig_arc3>-49 cci_sig_ang1>0 bbands_mid_ang1>stdbal_sls_ang1 sma20_ang1>stdbal_sls_ang1 usigma_sig_ang3>0 rsi_ang1_rsi_sig_ang1>rsi_ang2_rsi_sig_ang2 vr_arc3>62 aroon10_up>50 cci_arc7>-69 atan_fast5D_slow5D_gap>-44 stdbal_lls_arc3>-81 aroon10_up>fast10D macd_sig_arc1>53 nmind_sig_ang1_nmind_sig_ang2>0 bwi_ang2>0 cci_arc7>-49 sona_mom_arc3>26 momp_ang1>dis5_ang1 aroon10_up>m14_slow3k mfi_arc3>-3 sma10_arc3>-73 m14_slow3k>50 rsi_arc7>41 sona_mom_arc3>61 stdbal_bl__sar>0 low_price_ang2>0 atr_sma>100 stdbal_cl_ang1>stdbal_lls_ang1 nmind_arc3>27 m14_slow3k_arc3>0 macd_osc_arc7>-32 aroon10_up>rsi_sig6 stdbal_cl_arc3>80 stdbal_cl_ang1>0 aroon10_up>macd_slow3D macd>-20 macd_arc7>-63 Adx_ang1>0 DIn_ang1_Adx_ang2>-9 vo>-3 macd_osc_high7d_low7d_rate>-194 cv_arc3>-52 aroon_up>aroon_dn stdbal_cl_arc1>74 mfi_sig>50 macd_osc_ang1>vo_ang1 aroon10_up>macd_slow3k aroon10_up>rsi sroc13_ang3>0 dis10_dis20>-10 fast3D_ang1>macd_slow3D_ang1 sma10_arc3>70"
str2="cci_day5>0 slow5D_ang1_slow10D_ang1>-3 vo>0 macd_slow3k_ang3>0 fast5D_ang1>fast10D_ang1 bbands_up>stdbal_lls sma20_arc3>-69 rsi_sig6_ang3>0 fast3D_ang1>m14_slow3D_ang1 cv_sig_ang1>0 cv>cv_sig m14_slow3k_arc3>52 m14_slow3k>rsi_sig6 fast5D_slow5D_arc1>-26 usigma_sig>0 nmind_sig_ang1>0 fast3D_ang1_fast5D_ang1>0 fast3D_ang1>fast5D_ang1 fast3D_arc3>45 sona_mom_arc7>-66 sma5_arc7>75 vr_arc7>61 atr_sig_arc3>-54 sma20_arc3>-11 atan_cv_cv_sig_gap>47 sma5__sma20>0 sma5>sma20 vo_arc3>0 sma20_ang3>0 m14_slow3k_ang1>rsi_sig6_ang1 fast3D_slow3D_arc3>-11 sona_mom_arc7>-47 slow10D_ang1_slow5D_ang2>-4 sona_mom>macd_sig sona_mom_ang1>macd_ang1 momp_sig_ang2>0 macd>0 dis5_ang3_1bd>3 macd_osc_day10>2 slow10D_ang1_slow10D_ang2>0 vo>vo_sig macd_sig_arc3>-52 sroc_sig_ang1>0 sroc13_sig_ang1>0 fast3D_slow3D_arc3>23 vo_arc1>51 momp>dis5 slow10D_ang1_slow3D_ang2>-9 nmind_sig_ang2_1bd>0 open_high4d_rate>-11 nmind_arc7>-59 m14_slow3k_ang1>m14_slow3D_ang1 rsi_sig_arc3>11 cci>100 cv_sig>0 fast10D_ang3>0 fast10D_ang2_slow10D_ang2>0 nmind_arc7>-38 cv_arc7>-63 fast3D>slow10D aroon10_up>95 aroon_up>60 slow3D_ang1_fast5D_ang2>-3 fast3D>slow5D vr25_150g_day5>0 cci_sig_ang2>0 Adx_ang2_1bd>0 cci_sig>-20 Adx_ang2>0 fast3D_arc7>-56 nmind_ang1_nmind_ang2>nmind_sig_ang1_nmind_sig_ang2 nmind_ang1_nmind_sig_ang1>nmind_ang2_nmind_sig_ang2 stdbal_cl__sma5>0 m14_slow3k_ang1>macd_slow3k_ang1 cv_arc7>4 sma10_arc7>-56 macd_sig_arc3>26 momp_ang1>dis10_ang1 sroc13_ang1_1bd>2 formula_arc_sma7>-13 macd_slow3k>rsi_sig6 sona_sig_arc3>-39 sma10_arc7>45 nmind>vo fast5D_slow5D_arc1>52 momp_sig_momp5>-25 slow5D_fast10D>-10 dis20_dis60>-15 slow3D_ang1>slow10D_ang1 sma5>sma60 sma5__sma60>0 sona_sig_ang3>0 vo_arc3>53 low_low1bd_gt_low1bd_low2bd>0 cci_sig_ang1_cci_sig_ang2>3 momp_sig_ang3>0 m14_slow3D>45 dis5_ang1_dis10_ang2>3 rsi_sig_arc7>-53 slow3D>40 nmind_ang1>vo_ang1 Adx_ang3_1bd>0 slow5D_ang1_slow10D_ang2>-2 stdbal_bl__sma10>0 stdbal_bl>sma10 macd_slow3D_ang3>0 fast5D_slow5D_arc3>-52 sma10_sma20_arc>-70 fast10D_slow10D_arc1>-32 cci_sig>-10 rsi_sig>50 nmind_sig_ang3_1bd>0 stdbal_cl__stdbal_sls>0 stdbal_cl>stdbal_sls sona_sig_arc3>50 atan_fast10D_slow10D_gap>-12 cci_sig_arc3>-57 Adx_ang3>0"
str3="low_high_rate>-15 momp>dis10 fast3D_fast20K>-33 stdbal_bl__stdbal_sls>0 fast5D_ang3>slow5D_ang3 fast3D_fast10D_arc>-54 m14_slow3D_ang3_1bd>0 fast3D_ang1>macd_slow3k_ang1 sma10__sma20>0 sma10>sma20 vo_arc7>5 bwave>2 sma10_sma20_arc>22 macd_osc>vo fast3D_arc7>54 fast10D>slow10D sona_sig>0 vr25>150 vo_arc7>25 m14_slow3k_ang1_m14_slow3D_ang1>m14_slow3k_ang2_m14_slow3D_ang2 dis10_dis60>-20 macd_sig>-10 cci_sig_ang3>0 cl_cl3bd_arc_op_op3bd_arc_hi_hi3bd_arc_lo_lo3bd_arc_gubn>0 macd_sig_arc7>-56 fast10D_slow10D_arc1>-4 fast5D_ang1>macd_slow3D_ang1 macd_slow3k_arc3>7 vo_ang1_vo_ang2>vo_sig_ang1_vo_sig_ang2 vo_ang1_vo_sig_ang1>vo_ang2_vo_sig_ang2 sigma_arc7>49 fast3D_fast10D_arc>-34 vo_sig_ang1>0 fast5D_slow5D_arc3>-6 macd_sig_arc7>-36 sroc_sig_ang2>0 sroc13_sig_ang2>0 momp_ang1_momp5_ang1>momp_sig_ang1_momp5_sig4_ang1 cci_sig_arc3>45 close_3bd_open_high5d_low5d_arc>-55 nmind>25 open_price_ang1>0 dis5_ang2_dis10_ang2>-2 DIp_DIn>25 m14_slow3D_ang1>macd_slow3D_ang1 cci_sig>0 aroon>28 vo>10 aroon10_up_ang1>aroon10_dn_ang1 macd_slow3D>45 sona_sig_arc7>-41 momp_sig_ang1_momp5_ang1>-19 fast3D-fast10D>0 fast3D>fast10D m14_slow3D_ang1>rsi_sig6_ang1 sroc_sig>99 sroc13_sig>99 vo_ang2_1bd>0 momp_sig_arc7>-27 fast5D>slow10D atan_close_ang7__85l_day7>2 dis20_ang1>14 sma10__sma60>0 sma10>sma60 fast10D_slow10D_arc3>-15 DIn_ang3_1bd>-2 slow10D_ang1_fast10D_ang2>-3 slow3D_ang2_fast10D_ang2>-3 vo_ang3_1bd>0 sona_sig_arc7>57 fast3D_ang1_fast5D_ang2>6 slow3D_ang3_1bd>0 lo_2bd_gt_lo2bd_4bd>0 sma5>stdbal_sls slow10D_ang2_1bd>0 vr25_170g_day5>0 momp_ang1>dis20_ang1 slow5D_ang1>0 sigma_arc3>51 stdbal_cl>stdbal_bl fast3D_fast5D_arc>-13 sona_mom>cci_sig nmind_sig_ang2>0 slow5D_ang2_1bd>0 atan_dis5_dis10_gap>-80 open_price_ang1>open_price_ang3 momp>dis20 vo_sig_ang1_vo_sig_ang2>0 atr_sig_arc7>-54 nmind_ang2_nmind_sig_ang2>7 sroc_sig_ang3>0 sroc13_sig_ang3>0 fast5D>50 macd_sig>0 stdbal_cl__stdbal_lls>0 stdbal_cl>stdbal_lls fast3D>fast5D atan_fast10D_slow10D_gap>48 slow3D_ang1>slow5D_ang1 vo_sig_arc1>27 stdbal_bl__stdbal_lls>0 fast5D_ang2_fast10D_ang2>0 momp_sig_arc3>48 fast10D_ang1_slow10D_ang1>2 momp_momp_sig>momp5_momp5_sig4 momp_momp5>momp_sig_momp5_sig4 stdbal_sls_arc1>-27"
str4="slow10D_ang1_fast5D_ang2>-7 momp_sig_ang1_momp5_ang2>-10 atr_sig_arc3>51 cv>25 fast3D>rsi_sig6 momp_ang1_momp_sig_ang1>13 cci_1bd_rate>-10 rsi_sig_arc7>46 stdbal_bl_ang1>0 dis60_ang3>5 dis5_ang1_dis10_ang1>-3 fast10D_slow10D_arc3>50 momp5_sig4_ang1_momp_ang2>-7 DIn_ang2_Adx_ang2>-6 close_3bd_open_high5d_low5d_arc>45 atr_sig_arc7>-7 cci_sig_arc7>-36 sona_mom>40 macd_slow3D>rsi_sig6 bbands_mid>sma60 sma20__sma60>0 macd_slow3k>70 macd_osc>25 high_1bd_low_rate>18 cci_sig_arc7>15 dis5_ang1>11 DIp_ang1_dmi_ang2>0 fast3D_fast5D_arc>52 macd_osc_day5>2 macd_slow3k_80g_day5>0 slow3D_arc3>-47 rsi>68 sma10_arc3_80g_day7>2 dis5_dis60>-20 slow10D_ang3_1bd>0 momp_ang1_momp_sig_ang2>14 macd>cci_sig nmind_sig_ang3>0 dis5_dis10>-5 aroon_ang1>0 sma5>stdbal_lls dmi_ang2>6 macd_slow3k>rsi macd_slow3k_arc1>46 formula_ang1>3 cci_ang3_1bd>28 formula_arc>macd_osc sma10>stdbal_sls m14_slow3D>rsi_sig6 sar_arc1>-51 sar_arc3>-51 momp_sig_arc7>53 dmi_ang3>4 sma10_arc7_80g_day7>2 vhf_ang1_1bd>0 vhf_ang2_1bd>0 fast3D_fast5D>slow3D_slow5D m14_slow3k>rsi vhf_ang3_1bd>0 open_price_ang2>open_price_ang3 vo_sig_ang3>0 vo_sig_ang2_1bd>0 DIp_ang3>4 momp_sig_ang1_momp5_sig4_ang1>-4 mfi>73 vo_ang3>2 dis10_ang1_dis20_ang1>-2 aroon_ang3>3 sona_mom>macd slow5D_ang3_1bd>0 slow3D_slow10D_arc>-45 momp5_sig4_ang1_momp5_ang2>-6 DIp_ang3_1bd>4 aroon10_up_arc3>84 nmind_sig>0 dis5_dis20>-10 momp_ang2_momp_sig_ang2>7 rsi_ang1_rsi_sig_ang1>7 DIp_ang1_DIp_ang2>5 aroon10>40 DIp_ang2_1bd>6 nmind_sig_arc3>36 slow5D_ang1_slow3D_ang2>-4 DIp_dmi>-21 nmind_ang2>12 DIn_ang1>-5 DIn_ang1_dmi_ang1>-26 fast5D_fast10D>0 fast5D>fast10D bwave2_1d5g_day7>2 fast3D_ang1_slow5D_ang2>11 vo_sig_arc1>52 DIp_ang2_Adx_ang2>5 slow3D_arc3>29 nmind_ang2_1bd>9 dis20_ang1_dis60_ang2>6 fast5D_ang1_fast10D_ang1>3 Adx_dmi>-27 low_5bd_high_rate>-2 slow3D_ang1_slow3D_ang2>2 momp_sig_ang1_momp_ang2>-9 slow3D_ang2_fast5D_ang2>-3 DIn_ang1_DIn_ang2>-2 nmind_ang3_1bd>6 fast5D_ang1_fast5D_ang2>3 DIn_ang1_DIp_ang2>-15 nmind_ang1_nmind_ang2>7 tdi_ang1>0 nmind_ang2_nmind_sig_ang2>10 fast3D_ang2_slow5D_ang2>5 dis10_ang1_dis60_ang1>-4 vo_ang2>3 fast5D_ang1>macd_slow3k_ang1 m14_slow3D_ang1>macd_slow3k_ang1 slow3D>slow10D momp_ang1_momp5_sig4_ang1>11"
str5="Adx_ang1_DIp_ang2>-7 slow3D_ang1_fast10D_ang1>0 slow3D_ang1>fast10D_ang1 macd_slow3k_ang3_1bd>0 atan_macd_osc_rate_ang_7_3>43 open_5bd_open_rate>2 vo_ang2_vo_sig_ang2>0 slow3D_ang2_slow5D_ang2>0 momp_ang1_momp5_sig4_ang2>13 rsi_ang1>10 rsi_ang3>4 fast10K_fast5D>27 fast20K_fast10D>28 momp_sig_ang2_momp5_ang2>-10 vr>200 cci_sig_ang2>7 vo_ang1>5 dis10_ang1>13 fast3D_ang2_1bd>6 mfi_ang3_1bd>2 slow5D>46 fast10D_ang1_fast3D_ang2>-8 high_3bd_rate>13 low_1bd_high_rate>-3 nmind_sig_arc7>0 cci_1bd_rate>9 vo_sig_arc3>51 macd_osc_day10>5 sroc>103 fast3D_ang1_slow5D_ang1>11 atan_macd_slow3k_macd_slow3D_gap>46 vo_sig_ang3_1bd>0 slow3D_slow5D_arc>-52 macd_slow3k_arc3>50 slow3D_ang1_slow5D_ang1>2 fast3D_fast10K>-30 slow3D_ang1_fast5D_ang1>-4 high7_low7_rate_ang>72 m14_slow3k_ang1>9 fast5K_fast3D>25 aroon10_up_ang1>slow10D_ang1 m14_slow3k_ang2>5 fast5D_ang3>2 vo_day5>2 slow3D_slow10D_arc>48 vo_sig_arc7>45 DIp>nmind cci_ang1_cci_ang2>40 mfi_ang2_1bd>3 aroon10_up_ang2>0 high_5bd_low_rate>21 sma10>stdbal_lls macd_slow3D>69 close_3bd_open_rate>17 macd_osc_high3d_low3d_rate>0 slow5D_ang1_fast10D_ang1>-3 momp5_ang1_momp_ang2>7 atr_ang2_1bd>8 rsi_sig6_ang2>2 m14_slow3k_ang1_m14_slow3k_ang2>3 fast3D_ang1_slow3D_ang1>8 dis5_ang1_dis60_ang2>3 rsi_ang2>6 slow5D_fast10D>-5 DIp_Adx>0 macd_slow3k_ang2_1bd>0 sma5__sma10>sma10__sma20 nmind_sig_arc7>43 dis10_ang1_dis60_ang2>5 m14_slow3k_ang2_1bd>4 slow3D_ang1>rsi_sig6_ang1 atan_sigma_sigma_sig_gap>53 fast5D_fast20K>-32 momp5_sig4_ang1_momp5_sig4_ang2>2 slow5D_ang3>0 vo_sig>-3 close_5bd_rate>18 momp5>118 mfi_ang2>4 slow3D_fast10K>-42 bbands_mid>stdbal_sls sma20>stdbal_sls DIn_ang1_Adx_ang1>-8 slow3D_ang2>2 cci_ang1_cci_sig_ang2>86 fast3D_ang1>rsi_ang1 low_3bd_high_rate>-2 formula_arc_sma7>13 rsi_sig_ang1_rsi_ang2>-5 slow3D_arc7>52 DIn_ang2>-3 formula_arc__85l_day7>0 macd_slow3k_ang1_macd_slow3D_ang1>0 macd_slow3k_ang1>macd_slow3D_ang1 slow5D_ang1>slow10D_ang1 m14_slow3k_80g_day5>0 slow3D_ang1_fast5D_ang2>0 dis10_ang2_dis60_ang2>-2 fast3D_ang3>5 slow3D_ang1>5 dis20_ang1_dis60_ang1>-2 nmind_ang1_nmind_sig_ang2>20 aroon>45 fast5D_ang1_slow3D_ang2>4 slow10D_ang1_fast3D_ang2>-11 dis5_ang1_dis60_ang1>-6 open_1bd_high_rate>-2"
str6="m14_slow3k_ang3_1bd>3 cci_sig>48 formula_arc_85g_day7>2 cci_ang1_cci_sig_ang1>83 sigma_arc1>53 m14_slow3D_ang2>3 momp5_ang2_1bd>8 macd_osc_high7d_low7d_rate>-59 fast3D_ang1_slow3D_ang2>11 atr_sma_ang1_1bd>14 cci>171 slow3D>slow5D DIn_ang2_1bd>-2 DIp_ang1>vo_ang1 DIp_ang2_dmi_ang2>-5 slow5D_ang2_fast10D_ang2>-2 slow5D>slow10D open_1bd_close_rate>0 mfi_ang1>7 sma5_arc7_80g_day7>3 fast10D_ang2>2 close_5bd_open_rate>18 macd_slow3D_ang3_1bd>0 cci_sig_ang3>6 atr_sig_ema_ang2_1bd>2 vr25_150g_day10>2 momp_ang1>16 nmind_high5d_low5d_rate>-98 m14_slow3D_ang3>2 fast5D_ang2_slow10D_ang2>3 momp_momp_sig>16 DIp_ang2>7 dis5_ang1_dis20_ang1>-4 fast10D_ang1_slow10D_ang2>3 cci_ang2_1bd>50 nmind_ang1_nmind_sig_ang1>20 cv_sig_ang3>2 DIn_Adx>-32 DIp_ang1_Adx_ang2>11 macd_slow3k_90g_day5>0 sona_mom_ang2_1bd>22 macd_slow3D>rsi macd_slow3k_ang2_macd_slow3D_ang2>0 Adx_ang1_dmi_ang1>-16 slow10D>45 dis5-dis10>-4 dis5_ang2_dis60_ang2>-3 dis60_ang1>16 fast3D_ang1>15 m14_slow3k_ang1_m14_slow3D_ang1>4 momp_sig_ang2_momp5_sig4_ang2>-2 rsi_sig6_ang1>4 dmi_ang3_1bd>4 fast5D_ang1>8 fast5D_fast10D>slow5D_slow10D fast5D_slow5D>fast10D_slow10D DIp_ang1>13 close_low3bd_close3bd_arc>11 cci_ang2_cci_sig_ang2>50 macd_slow3D_90g_day10>2 macd_slow3k_90g_day10>2 momp_ang3>6 atr_sma_ang3_1bd>6 fast5D_ang1_slow10D_ang1>7 macd_slow3k_ang1_macd_slow3k_ang2>0 Adx_ang1>3 slow3D_slow10D>4 vo_sig>0 rsi_rsi_sig>12 rsi_sig_ang1>3 bwave2_ang1>0 vo_sig_ang1_vo_ang2>-4 bbands_mid>stdbal_lls sma20>stdbal_lls fast5K_slow10D>45 nmind_ang1_nmind_ang2>10 momp_momp5_sig4>15 formula>105 aroon10_up_ang1>slow5D_ang1 dis10_dis20>-4 rsi_ang1_rsi_ang2>5 slow3D_slow5D_arc>52 slow3D_ang1>macd_slow3D_ang1 dis10_ang1_dis20_ang2>6 dis10_ang3>5 fast3D_slow5D>13 fast3D_ang2_fast10D_ang2>6 nmind_ang3>10 fast3D_slow3D>11 slow10D_ang1_fast3D_ang2>-10 fast10D-fast3D>-15 atan_macd_osc_close_rate7_ang>4 nmind_ang1>25 aroon10_up_ang1>slow3D_ang1 slow3D_ang2_1bd>3 slow5D_ang2_slow10D_ang2>0 fast10D_ang1>macd_slow3D_ang1 momp5_ang3>6 sroc13_ang1>3 fast5D_ang1>m14_slow3D_ang1 fast3D_fast10D>10 dis10_dis60>-10 fast10K_slow5D>39 DIp_ang1_DIn_ang2>16 sma5_arc3_80g_day7>3 slow3D_slow5D>2 dmi>63 macd_slow3D_ang1_macd_slow3k_ang2>0 vo_day10>5"
str7="momp_ang2>9 vo_ang1_vo_ang2>2 DIn_dmi>-57 stdbal_lls__sar>0 aroon10_dn>20 sar_ang3>0 m14_slow3k_ang2_m14_slow3D_ang2>3 DIn_ang3>-2 formula_ang2>2 rsi_ang2_rsi_sig_ang2>5 fast10D_ang1>4 nmind_sig>7 dis5_ang2_dis20_ang2>-2 mfi_ang1_1bd>7 sar_ang1>0 sar_arc1>0 sar_arc3>0 momp5_ang2>9 macd_slow3k_ang1>rsi_sig6_ang1 macd_slow3k_80g_day10>4 stdbal_sls__sar>0 aroon10_up_ang1>fast10D_ang1 formula_arc_ang1>Adx_ang1 fast3D_ang1_fast10D_ang1>12 Adx>nmind formula_arc_ang2>2 stdbal_bl__sma5>0 fast3D_slow10D>18 m14_slow3k_ang1>rsi_ang1 DIp_ang1_Adx_ang1>11 aratio>126 rsi_ang2_1bd>6 slow3D>fast10D aroon10_up_ang1>rsi_sig6_ang1 nmind_nmind_sig>32 slow3D_fast20K>-34 slow3D_ang2_slow10D_ang2>3 aroon10_up_ang1>0 aroon10_up_arc1>0 fast5K_slow3D>41 vo_ang1_vo_sig_ang1>5 formula_ang2_1bd>2 aroon10_up_ang1>macd_slow3D_ang1 fast5D>rsi_sig6 DIp_ang1_DIn_ang1>18 fast3D_fast5D>fast5D_fast10D cci_high5d_low5d_rate>10 macd_slow3D_90g_day5>0 bwave3>7 cci_ang1_cci_ang2>cci_ang2_cci_sig_ang2 m14_slow3D_ang1>7 m14_slow3k_m14_slow3D>7 momp_ang2_momp5_ang2>0 slow3D_ang1_slow10D_ang2>6 fast5K_fast5D>35 vo_sig_ang2>2 sroc_ang1>2 DIp>macd_osc slow10D_ang1_fast5D_ang2>-4 fast3D_ang1_fast3D_ang2>7 fast5D_ang2_slow5D_ang2>4 fast3D_ang1_slow10D_ang1>16 aroon10_up_ang2>5 slow5D_ang1_fast5D_ang2>-3 close_3bd_rate>19 sar__sma60>0 aroon10_up_ang1>m14_slow3D_ang1 fast3D>m14_slow3D bbands_up_ang1>stdbal_cl_ang1 DIn>10 m14_slow3k>macd_slow3D atr_sma_ang2_1bd>11 slow3D_slow5D>slow5D_slow10D fast5D_ang1_slow5D_ang1>7 cv_sig_ang1>7 DIp_ang2_DIn_ang2>11 momp5_sig4_ang1>5 mfi_ang3>4 momp_ang1_momp5_ang1>0 close_low3d_rate>24 aroon10_up_ang1>macd_slow3k_ang1 formula_arc_ang1>vo_ang1 momp5_sig4_ang1_momp_sig_ang2>4 slow3D_ang1_fast3D_ang2>-3 slow3D_fast5D>-4 macd_osc>49 rsi_day5>0 Adx_ang1_DIn_ang2>6 fast10K_slow10D>47 fast10D-slow10D>6 momp5_ang1>17 cv>50 vr>250 macd_sig>cci_sig slow5D_ang1_fast3D_ang2>-7 slow3D_ang1_slow5D_ang2>6 slow10D_ang1_slow5D_ang2>0 fast10D_ang1_fast3D_ang2>-5 vr25_170g_day10>2 stdbal_sls__stdbal_lls>0 stdbal_sls>stdbal_lls fast5D_slow10D>13 aroon_ang2>5 formula_arc_day5>3 aroon10_up_ang1>fast5D_ang1 cci_ang2>69 fast3D_ang1_slow10D_ang2>17 atr_sig_ema_ang3_1bd>2 vr25_ang2>31"
str8="aroon10_up_ang1>fast3D_ang1 m14_slow3k_ang1_m14_slow3k_ang2>6 nmind_sig_ang1_nmind_ang2>-12 dis20_ang1_dis5_ang2>9 dmi_ang1_dmi_ang2>8 fast5D_ang1_fast10D_ang2>7 Adx>macd_osc momp5_sig4_ang2>3 dis5_dis20>-6 fast5D_ang1_fast3D_ang2>0 stdbal_bl>open_price slow3D_ang1>macd_slow3k_ang1 dis20_ang3>6 aratio_ang3>7 slow3D_ang3>3 momp_ang1_momp5_ang2>9 dis20_ang2_1bd>8 sona_mom_3bd_rate>25 fast10D_ang1>macd_slow3k_ang1 aroon10_up_ang1>m14_slow3k_ang1 m14_slow3k-m14_slow3D>8 open_price>sma5 dmi_ang2_1bd>9 m14_slow3D_ang1_m14_slow3k_ang2>0 aroon_up_ang1>0 dis60_ang1_dis10_ang2>10 close_open_high5d_low5d_arc>-53 slow3D_ang1_slow10D_ang1>7 m14_slow3k_ang3>6 fast5D_fast10D>7 DIn_ang1_Adx_ang2>-6 formula_arc_ang1>DIp_ang1 fast3D_ang2_slow5D_ang2>10 dis10_ang1_dis5_ang2>8 close_open_rate>14 fast10D_slow10D>7 fast10D_ang2_1bd>2 fast10D_ang1>rsi_sig6_ang1 cci>216 aroon_ang3_1bd>2 m14_slow3D>rsi dis10_dis60>-6 macd_osc_high3d_low3d_rate>12 dis20_dis60>-3 momp_ang2_momp5_sig4_ang2>8 slow3D_fast10D>4 fast10D_ang1_fast5D_ang2>0 aratio_ang1>19 macd_slow3k_ang1>5 dis5_ang3>5 mfi_sig_ang2>2 formula_arc_ang3>6 slow5D_ang1_fast10D_ang2>0 fast3D_ang2_fast5D_ang2>6 slow3D_ang1_slow5D_ang1>6 macd_slow3D_ang2>2 fast5D_ang2_1bd>5 aroon10_up_ang1>rsi_ang1 nmind_sig_ang1>4 macd_sig_ang1>cci_sig_ang1 dis10_ang3_1bd>5 aratio_ang2>11 fast3D_ang2_slow10D_ang2>11 momp_sig_ang1_momp5_sig4_ang2>0 cci_sig_ang1_cci_ang2>-43 fast3D_ang2_fast10D_ang2>9 momp5_ang1_momp5_ang2>9 dis20_ang2>9 vo_ang2>8 nmind>macd_osc atan_macd_osc_ang7_2>48 dis60_ang2_1bd>9 fast3D>macd_slow3D hi_5bd_cl_5bd_arc_gt_op_5bd_lo_5bd_arc>0 fast5D_ang1_fast10D_ang1>6 aroon_ang1>5 slow5D_ang1_slow3D_ang2>0 aroon_ang3>8 DIp_ang1_dmi_ang1>-4 slow5D_slow10D>7 fast5D_ang2>6 open_5bd_open_rate>7 momp5_momp5_sig4>15 DIp_ang2_dmi_ang2>-2 vo>30 mfi_ang1_1bd>10 dis60_ang2>10 macd_slow3D_ang1>rsi_sig6_ang1 fast20K_slow10D>46 slow5D_ang2>2 fast5K_fast10D>46 momp_ang1_momp_ang2>9 dis60_ang3_1bd>6 DIp_ang1_DIp_ang2>8 dmi_ang3_1bd>7 close_open_high5d_low5d_arc>-10 aroon10_up_ang1>10 aroon10_up_arc1>87 DIn_ang1_dmi_ang2>-11 momp5_ang1_momp_sig_ang2>17 nmind_ang2_1bd>18 cci_cci_sig>160"
str9="atr_sig_ema_ang1_1bd>6 stdbal_sls_arc3>77 formula_ang3>2 Adx>DIp m14_slow3k_80g_day10>3 rsi_ang3_1bd>5 momp5_ang2_momp5_sig4_ang2>8 cci_ang3>58 low_5bd_high_rate>3 m14_slow3k>macd_slow3k aroon_up_ang3>5 slow5D_ang1>3 slow5D>fast10D DIp_ang2_Adx_ang2>8 dis20_ang1_dis20_ang2>8 fast5D_slow5D>10 atr_sma_ang3_1bd>10 stdbal_lls__sma10>0 fast3D_fast5D>11 mfi_ang2>7 cv_sig_ang2>6 sma5_arc3__80l_day7>2 fast5K_slow5D>51 DIp_ang1_dmi_ang2>6 aroon_dn>40 momp5_sig4_ang2_1bd>3 DIn_ang2_dmi_ang2>-9 m14_slow3k_90g_day5>0 vo_ang3>7 sma5_ang1>stdbal_cl_ang1 Adx_ang2_dmi_ang2>-5 m14_slow3D>macd_slow3D vo_ang3_1bd>4 aroon10_up_ang3>13 fast10K_fast10D>44 slow10D_ang1>macd_slow3D_ang1 macd_slow3k_20l_day10>2 close_5bd_open_high_5bd_low_arc>-70 fast5D_ang1_slow10D_ang1>10 slow3D_ang1>m14_slow3D_ang1 vo_sig_ang1>vo_ang2 DIp_Adx>8 vo_ang1_vo_ang2>5 dis5_ang2_1bd>7 mfi_ang2_1bd>6 dis60_ang1_dis20_ang2>10 fast3D_fast20K>-18 atan_macd_osc_rate_ang7>16 formula_arc_ang2>17 sona_mom_ang2_1bd>37 cci_sig_ang2_1bd>9 dmi_ang1_DIp_ang2>14 Adx_ang1_dmi_ang2>-4 slow3D>rsi_sig6 dmi_ang1>21 vr>294 momp5_ang1_momp5_sig4_ang2>16 Adx>45 slow3D_ang2_fast5D_ang2>0 dis60_ang1_dis5_ang2>12 momp_ang2_1bd>10 DIp_ang2_1bd>9 momp_sig_momp5_sig4>0 slow5D_fast20K>-31 dmi_ang1_DIn_ang2>24 sma10_arc7__80l_day7>2 m14_slow3k_ang1_m14_slow3D_ang1>8 hi1bd_5bd_lo1bd_5bd_arc_hi_5bd_lo_5bd_arc>0 hi1bd_5bd_lo1bd_5bd_arc_hi_5bd_lo_5bd_arc_gubn>0 slow3D_fast10D>7 dis10_ang2_1bd>8 slow3D_ang2_slow5D_ang2>5 atr_ang3_1bd>11 vo_ang2_1bd>6 fast_3D_80g_day7>0 dis60_ang1_dis60_ang2>9 cci_sig_ang2>16 hi_lo_op_cl_arc_hi_lo_op_cl_3bd_arc>0 vo_ang2_vo_sig_ang2>7 fast10D_ang1_slow5D_ang2>4 momp_ang2_momp_sig_ang2>10 rsi_sig_ang3>2 aroon10_dn>slow10D vr25_ang3>29 momp5_ang1_momp5_sig4_ang1>14 dis20_ang1_dis10_ang2>9 aroon10_dn>40 momp_momp5>4 aroon10_dn>slow5D rsi_ang2>9 forerate_ang3>0 fast5D_ang1_slow3D_ang2>7 fast3D>rsi slow3D_ang2>6 fast3D_ang3>9 slow3D_ang1>9 aroon_up_ang2>8 dis10_ang2>9 sona_sig>98 formula_arc_ang1>nmind_ang1 bwave2__3d5l_day7>0 low_3bd_high_rate>2 cci_sig>100 cci_sig_ang3_1bd>7 close_high4d_rate>12 slow3D_ang3_1bd>4 slow10D_ang1>macd_slow3k_ang1 fast5D_ang1_slow5D_ang2>10 slow5D_slow10D>10"
str10="nmind_ang1>macd_osc_ang1 forerate_ang2>0 fast5D_ang3>5 nmind_sig_ang3>3 stdbal_sls__sma10>0 dis5_ang1_dis5_ang2>7 slow10D_ang1_slow3D_ang2>0 aroon_ang2_1bd>8 fast3D>macd_slow3k aroon10_dn>fast10D stdbal_lls__sma5>0 dis5_ang2>8 m14_slow3D_ang3_1bd>4 atan_close_ang7_85g_day7>2 momp_ang3_1bd>7 fast5D_ang3_1bd>4 fast5D>macd_slow3D sma10_arc3__80l_day7>2 fast5D>m14_slow3D aroon10_up_ang1>20 mfi_sig_ang3>2 dis10_ang1_dis10_ang2>8 sma10>open_price stdbal_sls_ang1>stdbal_lls_ang1 macd_slow3k_80g_day10>7 momp5_ang3_1bd>7 dis20>dis60 DIn>vo fast5D_ang2_slow10D_ang2>7 stdbal_sls_ang1>0 macd>cci slow5D_ang1>macd_slow3D_ang1 high5d_low5d_close_5bd_open_arc>-54 slow3D>macd_slow3D fast5D_ang1>m14_slow3k_ang1 slow3D_ang1_fast5D_ang1>0 slow3D_ang1>fast5D_ang1 formula_arc_ang1>macd_osc_ang1 vr25_150g_day5>4 fast10D>rsi_sig6 atr_sig_ang3_1bd>2 close_open_close_3bd_open_arc>-43 dis20_ang3_1bd>6 momp5_sig4_ang3>3 fast10D_ang1>m14_slow3D_ang1 m14_slow3k_20l_day10>2 sona_mom_ang1>cci_ang1 sona_mom>cci aroon>75 fast3D>m14_slow3k formula_arc_ang1>86 aroon10_dn>fast5D slow5D_ang1>macd_slow3k_ang1 sma5_arc7__80l_day7>3 slow3D>fast5D slow3D_ang2_fast10D_ang2>5 macd_ang1>cci_ang1 slow3D>m14_slow3D stdbal_sls__sma5>0 aroon10_dn>rsi_sig6 m14_slow3k_90g_day10>2 fast10D>m14_slow3D forerate_ang1>0 aroon10_dn>macd_slow3D dis10>dis60 formula_ang3_1bd>2 vr25_3bd_rate>92 atan_dis5_dis10_gap>-53 slow10D>m14_slow3D aroon10_dn>slow3D sona_mom>230 aroon10_dn>m14_slow3D fast10D>macd_slow3D formula_arc_ang1>147 dis5_ang1_dis10_ang1>dis10_ang1_dis20_ang1 Adx_ang1>vo_ang1 vr25_ang1>104 sar__sma20>0 slow3D_ang1>m14_slow3k_ang1 high5d_low5d_rate>24 DIn_ang2_dmi_ang2>-5 high5d_low5d_close_5bd_open_arc>52 bbands_up>close_price bbands_up>high_price bbands_up>stdbal_es wpr>0 bbands_mid>sma10 dis10>dis20 sma5__sma10>sma5__sma20 slow10D>macd_slow3D fast3D_ang2>15 slow3D_slow5D>14 stdbal_sls_arc1>77 formula_arc_sma_85g_day7>0 macd_sig>cci m14_slow3D>macd_slow3k slow5D_ang1>fast10D_ang1 slow5D>macd_slow3D slow5D>rsi_sig6 aroon10_dn>macd_slow3k dis20_ang2_dis60_ang2>0 fast3D_ang1_fast5D_ang1>14 DIp_ang1>nmind_ang1 aroon_dn>60 dis5>dis60 macd_slow3k_20l_day5>2 nmind_sig_ang1_nmind_ang2>-3"
str11="cci_ang2_cci_sig_ang2>16 fast10D_ang1_slow5D_ang2>0 macd_osc_arc7>56 sma5_sma10_arc>-73 macd_arc7>35 atan_fast5D_slow5D_gap>-4 macd_slow3k_ang1_macd_slow3D_ang1>macd_slow3k_ang2_macd_slow3D_ang2 fast5D>slow5D fast3D_arc3>-22 slow5D_ang1_fast5D_ang2>-6 slow5D_ang1_slow5D_ang2>0 fast3D_ang2>2 stdbal_es__stdbal_lls>0 m14_slow3k_ang1>macd_slow3D_ang1 cv_arc3>40 momp_sig_momp5_sig4>-9 sma5>sma10 sma5__sma10>0 fast3D_fast10D>-7 aroon10_up>75 atan_cv_cv_sig_gap>-41 fast3D_ang3>0 macd_slow3k>40 slow3D_ang1>0 fast5D_ang1>rsi_sig6_ang1 vr_arc7>-63 momp_sig_arc3>-53 sona_mom>0 sma5_sma10_arc>50 sroc_ang3>0 sma5_arc7>-79 mfi_arc3>52 vr_ang1>148 fast5D>macd_slow3k fast5D_ang1>rsi_ang1 slow3D>macd_slow3k atan_dis5_dis20_gap>-45 stdbal_lls_arc3>0 sma10_ang1>stdbal_cl_ang1 atan_dis10_dis20_gap>28 aroon10_dn_ang3>0 aroon10_dn_arc3>0 close_open_close_3bd_open_arc>51 slow5D_ang1>rsi_sig6_ang1 sma5>stdbal_cl slow5D>m14_slow3D bbands_dn_arc3>-60 slow10D>rsi_sig6 aroon10_dn>m14_slow3k bwave10>4 macd_slow3k_ang1>rsi_ang1 slow3D_ang1>rsi_ang1 bbands_dn_arc3>-30 aroon_ang2_1bd>18 fast10D_ang1>m14_slow3k_ang1 fast5K_fast10D>61 momp_ang2_momp5_ang2>3 vr25_1bd_rate>81 aroon10_dn>rsi DIp_ang3>9 aroon10_dn>fast3D DIn>macd_osc bbands_dn_ang1>stdbal_sls_ang1 momp_sig>dis5 fast5K_fast20K>8 dis10_ang2_dis20_ang2>0 bbands_dn_arc1>-54 bbands_mid>sma5 dis5>dis20 aroon_up_ang1>30 slow10D>macd_slow3k vr25_7bd_rate>171 bbands_dn_ang1>stdbal_lls_ang1 fast10D>macd_slow3k stdbal_bl__stdbal_cl>0 stdbal_bl>stdbal_cl slow10D_ang1>m14_slow3D_ang1 slow10D>m14_slow3k bbands_dn_ang1>0 vr25_70l_day5>0 m14_slow3D_ang1>rsi_ang1 fast10D>m14_slow3k macd_slow3k_ang3>10 dis5>dis10 DIn>nmind stdbal_lls>stdbal_es stdbal_lls__close_price>0 slow5D_ang1>m14_slow3D_ang1 slow3D_ang2_slow10D_ang2>10 macd_slow3D_ang1>12 macd_slow3k_macd_slow3D>12 bbands_mid_ang1>stdbal_cl_ang1 sma20_ang1>stdbal_cl_ang1 atan_dis5_dis20_gap>39 fast10K_fast20K>0"
str12="bbands_dn_arc1>52 fast5D>m14_slow3k atan_macd_osc_ang7>-56 m14_slow3D>90 nmind_sig>30 dmi>82 slow5D>macd_slow3k atan_aroon10_dn_aroon10_up_gap>-87 hi_3bd_arc_lo_3bd_arc_cl_3bd_arc_op_3bd_arc>0 aroon10_dn_ang2>0 fast5D>rsi slow5D_ang2_slow10D_ang2>5 fast10K_fast10D>62 aroon10_dn>75 aroon10_dn_ang1>slow5D_ang1 slow5D_ang1_slow10D_ang2>6 atan_macd_osc_ang7>32 macd_slow3k_ang2>14 cci_sig_ang1_cci_ang2>-10 slow5D_ang1>6 DIp_ang1>macd_osc_ang1 slow3D>75 fast5D_ang2_slow5D_ang2>8 slow3D>m14_slow3k rsi_1bd_rate>53 slow5D_ang1_slow10D_ang1>6 DIp_ang1_dmi_ang1>8 momp_sig>dis10 slow10D_ang1>rsi_sig6_ang1 wpr_ang1_1bd>0 vr25_70l_day10>3 stdbal_sls__close_price>0 stdbal_sls>stdbal_es vr25_ang3>74 vr25_7bd_rate>233 sar__sma10>0 slow5D_ang1>m14_slow3k_ang1 m14_slow3D_90g_day10>3 aroon10_dn_ang1>slow3D_ang1 bbands_mid>stdbal_cl sma20>stdbal_cl vo_sig>30 fast10D>rsi atan_dis5_dis10_gap>48 Adx_ang2_dmi_ang2>4 macd_sig_ang1>cci_ang1 fast_10D_80g_day7>0 sar__sma5>0 slow5D>75 slow10D_ang1>m14_slow3k_ang1 slow5D>m14_slow3k m14_slow3D_ang2>12 atan_aroon10_dn_aroon10_up_gap>84 aroon10_dn>aroon10_up bbands_dn_ang1>stdbal_cl_ang1 slow3D>rsi slow10D>rsi"


strX = str1 + ' ' + str2 + ' ' + str3 + ' ' + str4 + ' ' + str5 + ' ' + str6 + ' ' + str7 + ' ' + str8 + ' ' + str9 + ' ' + str10 + ' ' + str11 + ' ' + str12
strSplit = strX.split(' ')

f = open('C:/Users/Shine_anal/PycharmProjects/anlaysis/test.csv', 'w')
for i in range (0, len(strSplit)):
    f.write(strSplit[i] + '\n')
f.close()

def makeMatrix(data, aggr_df, conData, leafType, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum, condition, datBool, lvl, ratDf,
               invYn, treeNm):

# Data Loading
df = pd.read_csv('C:/Users/Shine_anal/PycharmProjects/anlaysis/inobuy4_close_h4_9year.csv')  # dtree?????? ?????? ?????????
dfCon = pd.read_csv('C:/Users/Shine_anal/PycharmProjects/anlaysis/test.csv', header=None)    # str1,2,3 ?????? ???????????? ????????? ??? csv?????? 

# ?????? ?????????????????? del(str)
newDff, dfConF = initCond(df, dfCon)
# ??????
fResult = runDtree(newDff, 7, 4, dfConF, 'Y', 1, 'Y', 3, 1)
# ???????????? ?????? ?????????
run(fResult, 10, 6) # data, condDcnt, condDvsb
# csv?????? ??????
fResult.to_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/kbuy1_20210927_parallel_all.csv")
