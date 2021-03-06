# coding=utf-8
import sys
import pandas as pd
import datetime
import numpy as np


def makeMatrixPMN(data, aggr_df, conData, leafType, rRatYn, rRatNum, rRtYn, rRtLvl, rRtNum, condition, datBool, lvl, ratDf, invYn, treeNm):
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
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.3]
                        midSet = midSet.sort_values('dvsb', ascending=False)
                elif leafType == 'B':
                    midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                    midSet = midSet.sort_values('bcnt')
            elif treeNm == 'tree2':
                if leafType == 'A':
                    if lvl == 1:
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
                        midSet = midSet.sort_values('dvsb', ascending=False)

                        conH = midSet.iloc[1]['tree']
                        rConH = midSet.iloc[1]['condi']
                        valH = midSet.iloc[1]['value']
                        calX = midSet.iloc[1]['cal_x']
                        calH = midSet.iloc[1]['cal_y']
                    else:
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.22]
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
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.23]
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
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.2]
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
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.24]
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
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.25]
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
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.26]
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
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.27]
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
                        midSet = midSet[midSet['bcnt'] + midSet['dcnt'] > (midSet['prevBcnt'] + midSet['prevDcnt']) * 0.28]
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
            tmpRt = pd.DataFrame([(0, 0, 0, 0)], columns=['bcnt', 'dcnt', 'bvsd', 'dvsb'])
            conTmp = conData
    except Exception as e:
        tmpBool = pd.DataFrame()
        tmpRt = pd.DataFrame([(0, 0, 0, 0)], columns=['bcnt', 'dcnt', 'bvsd', 'dvsb'])
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
                    setattr(mod, 'lvl_0_1', [datBool, initConds, '', '', '', '', '', '', data, data.iloc[:, 2]])
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
                                prevDcnt = 0
                                pass

                            try:
                                prevBcnt = tmp[8].iloc[:, 2].value_counts()[1]
                            except Exception as e:
                                prevBcnt = 0
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
                        pd.DataFrame([(trees.iloc[row].treeNm, 'lvl_' + str(i) + '_' + str(j), tmp[6], bcnt, dcnt,
                                       bcnt / dcnt, dcnt / bcnt)],
                                     columns=['treeNm', 'lvl', 'condi', 'bcnt', 'dcnt', 'bvsd', 'dvsb']))
                    # ?????? ?????? ??????
                    # delattr(mod, 'lvl_{}_{}'.format(i, j))
    except Exception as e:
        print(e)
        setattr(mod, 'lvl_{}_{}'.format(i, j),
                [tmpBool, tmpConDf, tmpRat.bcnt, tmpRat.dcnt, tmpRat.bvsd, tmpRat.dvsb, tmpCon, tmpRatList, tmpDat,
                 tmpAggrDf])

    return fResult


# Data Loading
df = pd.read_csv('C:/Users/Shine_anal/Desktop/kbuy1.csv')
dfCon = pd.read_csv('C:/Users/Shine_anal/Desktop/dtreeCon.csv')

# ?????? ??????????????????
newDff, dfConF = initCond(df, dfCon)

# ?????? ??????
fResult_pmn = drawTreePMN(newDff, 7, 4, dfConF, 'Y', 1, 'Y', 3, 1)

fResult_pmn = fResult_pmn.iloc[:, 2:fResult_pmn.shape[1]].drop_duplicates()
fResult_pmn = fResult_pmn.sort_values('bvsd', ascending=False)
fResult_pmn.to_csv("C:/Users/Shine_anal/Desktop/kbuy1_20210906_dtree_pmn.csv")

bbands_up > close_price AND aroon10_up > 95.0 AND Adx_ang1 <= 0.0 AND momp5_ang3_1bd <= 5.0 AND forerate_ang3 > 0.0 AND fast10D_ang1 <= rsi_sig6_ang1 AND sma10 > open_price

df[
    (
            ((df['aroon10_up'] > 95.0)
             & (df['bbands_up'] > df['close_price']))
& (df['fast10D_ang1'] <= df['rsi_sig6_ang1']))
            & (df['Adx_ang1'] <= 0.0)
            & (df['sma10'] > df['open_price'])
            & (df['momp5_ang3_1bd'] <= 5.0)
            & (df['forerate_ang3'] > 0.0)

].value_counts('pur_gubn5')

pur_gubn5
0
14
1
1