import faulthandler
import sys

# 启用故障处理，将错误输出到 stderr
faulthandler.enable()

# 强制重定向 stderr 到控制台，确保能看到崩溃信息
# (有些 IDE 可能会吞掉 stderr)
import os
os.environ['MPLBACKEND'] = 'Agg' # 双重保险

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

print("Start plotting...")
try:
    plt.plot([1, 2, 3], [4, 5, 6])
    print("Plot created, saving...")
    # 尝试保存
    plt.savefig(r"E:\stock\GP\当日策列\static\test.png")
    print("Done!")
except Exception as e:
    print(f"Python Error: {e}")

# 如果程序在这里之前消失了，faulthandler 应该会打印出 "Fatal Python error: Segmentation fault"