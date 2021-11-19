# coding=utf-8
# 라이브러리 불러오기
import pydotplus
from sklearn.tree import export_graphviz
import matplotlib.pyplot as plt
from collections import Counter
import pandas as pd
from sklearn.metrics import accuracy_score
import sklearn.model_selection as ms
import warnings
from sklearn.tree import DecisionTreeClassifier, export_text, export_graphviz
import os
import sklearn
os.environ["PATH"] += os.pathsep + 'C:/Program Files/Graphviz/bin/'

warnings.filterwarnings('ignore')

##############################################################################
# 매수해야하는 timing을 찾아보자
##############################################################################
df = pd.read_csv("C:/Users/Shine_anal/PycharmProjects/anlaysis/SELLCONDFIND.csv")
df = df.fillna(0)
df['result'] = 0

for i in range(0, df.shape[0]):
    if df['RN2'].iloc[i] < 10:
        df['result'].iloc[i] = 1

df = df.dropna()

### 의사결정 트리 시각화
# 종속변수, 입력변수 나누기

# fncbuy3      33083
# fsbuy2       27926
# fkbuy1       20489
# fkosbuy2     11078
# fsbuy3        6686
# fkosbuy3      6215
# fgkosbuy4     1905
# fncbuy2       1280
# fgkosbuy3      750

df = df[df['LABEL'] == 'fncbuy3']

x = df.iloc[:, 9:df.shape[1]-3]
y = df.iloc[:, -1]

x.info()
y.value_counts()



x_train, x_test, y_train, y_test = ms.train_test_split(x, y,
                                                      test_size = 0.2)

Counter(y_train)
Counter(y_test)

# 의사결정트리 만들기 (엔트로피구하기, 트리2단계)
model = DecisionTreeClassifier(criterion="entropy", max_depth=5)

# 모델 학습
model.fit(x_train, y_train)

# test데이터 라벨 예측
y_pred = model.predict(x_test)

# 정확도 계산
accuracy_score(y_test, y_pred)

# 입력변수들의 중요도 확인(확률값)
model.feature_importances_

pd.DataFrame({'feature': x_train.columns, 'importance': model.feature_importances_})

treeMODEL = DecisionTreeClassifier(criterion = 'entropy',
                             max_depth = 10,
                             random_state = 0).fit(x_train, y_train)

plotResult = sklearn.tree.plot_tree(treeMODEL,
                                    feature_names = x_train.columns,
                                   filled = True)

#가지치기

tree_prune = DecisionTreeClassifier(criterion = 'entropy',
                             max_depth = 3,
                             random_state = 0).fit(x_train, y_train)

tree_prune.fit = tree_prune.fit(x_train, y_train)

fig, axes = plt.subplots(nrows = 1, ncols = 1, figsize = (10,5))

plt.figure(figsize = (16,9))
plotResult = sklearn.tree.plot_tree(tree_prune,
                                    feature_names = x_train.columns,
                                   filled = True)

##########################################################################
# fncbuy3
##########################################################################
df.value_counts('result')
0    31965
1     1118

df[(df['RRR_5A_FC'] <= 4.85)
  & (df['THR_OPEN_HIGH4D_RATE'] <= -0.645)
  # & (df['THR_DIS60'] <= 99.575)
].value_counts('result')

x_train[x_train['RRR_5A_FC'] < 3.05]