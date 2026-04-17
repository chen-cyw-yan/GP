from datetime import datetime

# 1. 原始时间字符串
time_str = "2026-04-17 00:00:00"

# 2. 转换为 datetime 对象
dt_obj = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")

# 3. 转换为时间戳 (秒)
timestamp = int(dt_obj.timestamp())

print(f"秒级时间戳: {timestamp}")
print(f"毫秒级时间戳: {timestamp * 1000}")