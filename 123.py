import matplotlib.pyplot as plt
import numpy as np

# 数据
methods = ['Method 1', 'Method 2']
imp_values = [504.93, 568.29]
div_values = [19.74, 22.97]

x = np.arange(len(methods))  # x轴位置
width = 0.35  # 柱宽

fig, ax = plt.subplots(figsize=(6,4))

# 双柱：imp(R') 与 diversity
bar1 = ax.bar(x - width/2, imp_values, width, label="imp(R')")
bar2 = ax.bar(x + width/2, div_values, width, label='Diversity')

# 添加标签
ax.set_ylabel('Score')
ax.set_title('Comparison of imp(R′) and Diversity between Methods')
ax.set_xticks(x)
ax.set_xticklabels(methods)
ax.legend()

# 在柱顶标出数值
for bars in [bar1, bar2]:
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, height + 1,
                f'{height:.1f}', ha='center', va='bottom', fontsize=9)

plt.tight_layout()
plt.show()
