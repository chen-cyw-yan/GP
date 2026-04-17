import json
import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *

def main():
    # 1. 创建 Client
    # 请替换为你自己的 App ID 和 App Secret
    client = lark.Client.builder() \
        .app_id("cli_a9256b2aef7a5cd4") \
        .app_secret("t22QBXS6MVqsXC41GoCDvbxin0tpXyL3") \
        .log_level(lark.LogLevel.DEBUG) \
        .build()

    # 2. 构造查询条件 (Filter)
    # 逻辑：列A >= '2026-04-16' AND 列B = '选项1'
    # 注意：filter_str 需要是一个 JSON 字符串
    filter_json = {
        "conjunction": "and",
        "conditions": [
            {
                "field_name": "触发日期",  # 替换为你的时间列实际名称
                "operator": "isGreaterThanOrEqual", # 时间大于等于
                "value": ["2026-04-17"] # 时间格式通常为 YYYY-MM-DD 或 YYYY-MM-DD HH:mm
            },
            {
                "field_name": "代码",  # 替换为你的单选列实际名称
                "operator": "is",     # 单选等于
                "value": ["sh600552"]    # 替换为你要查询的单选选项值
            }
        ]
    }
    # filter_str = json.dumps(filter_json, ensure_ascii=False)

    # 3. 构造请求对象
    request: SearchAppTableRecordRequest = SearchAppTableRecordRequest.builder() \
        .app_token("FFewwkxf2izEVxkyA7Yc821GnXe") \
        .table_id("tbliSMaFdxeKSM8y") \
        .user_id_type("open_id") \
        .page_size(20) \
        .request_body(SearchAppTableRecordRequestBody.builder()
            .view_id("vewMxw9kIo")                 # 可选：指定视图 ID，留空则查询全表
            .field_names(["触发日期", "代码"]) # 指定返回的字段，提高效率
            .filter(filter_json)          # <--- 关键点：注入过滤器字符串
            .build()) \
        .build()

    # 4. 发起请求
    response: SearchAppTableRecordResponse = client.bitable.v1.app_table_record.search(request)

    # 5. 处理结果
    if not response.success():
        lark.logger.error(
            f"查询失败, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}")
        return

    # 打印查询到的记录
    records = response.data.items
    lark.logger.info(f"查询到 {len(records)} 条记录")
    for record in records:
        # 获取字段值需要通过 fields 属性
        fields = record.fields
        print(f"时间列A: {fields.get('列A')}, 单选列B: {fields.get('列B')}")

if __name__ == "__main__":
    main()