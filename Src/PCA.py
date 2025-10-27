import pandas as pd
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt

# 读取原始数据
R = pd.read_csv("/Users/sadostrage/Downloads/Data intensive/GroupXX/Data/synthetic_R.csv")

# 读取不同方法的输出结果
method2 = pd.read_csv("/Users/sadostrage/Downloads/Data intensive/GroupXX/Data/results/method2_output.csv/method2_result.csv")
method1 = pd.read_csv("/Users/sadostrage/Downloads/Data intensive/GroupXX/Data/results/method1_output.csv/method1_result.csv")

# 去掉 row_id 列
cols = [c for c in R.columns if c != "row_id"]

# PCA 拟合整个原始数据
pca = PCA(n_components=2)
R_pca = pca.fit_transform(R[cols])

# 将输出结果映射到 PCA 空间
def project(df):
    return pca.transform(df[cols])

m1_pca = project(method1)
m2_pca = project(method2)

# 绘制散点图
plt.figure(figsize=(8,6))
plt.scatter(R_pca[:,0], R_pca[:,1], color='lightgray', s=10, label="Original Data R")
plt.scatter(m1_pca[:,0], m1_pca[:,1], color='red', s=20, label="Method 1")
plt.scatter(m2_pca[:,0], m2_pca[:,1], color='blue', s=20, label="Method 2")
plt.xlabel("Principal Component 1")
plt.ylabel("Principal Component 2")
plt.legend()
plt.title("PCA Projection: Diversity Comparison of Method 1 vs Method 2")
plt.show()
