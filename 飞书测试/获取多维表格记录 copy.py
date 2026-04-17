import json

import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *


# SDK 使用说明: https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/server-side-sdk/python--sdk/preparations-before-development
# 以下示例代码默认根据文档示例值填充，如果存在代码问题，请在 API 调试台填上相关必要参数后再复制代码使用
# 复制该 Demo 后, 需要将 "YOUR_APP_ID", "YOUR_APP_SECRET" 替换为自己应用的 APP_ID, APP_SECRET.
def main():
    # 创建client
    client = lark.Client.builder() \
        .app_id("cli_a9256b2aef7a5cd4") \
        .app_secret("t22QBXS6MVqsXC41GoCDvbxin0tpXyL3") \
        .log_level(lark.LogLevel.DEBUG) \
        .build()

    # 构造请求对象
    request: SearchAppTableRecordRequest = SearchAppTableRecordRequest.builder() \
        .app_token("FFewwkxf2izEVxkyA7Yc821GnXe") \
        .table_id("tbliSMaFdxeKSM8y") \
        .page_size(100) \
        .request_body(
            SearchAppTableRecordRequestBody.builder()
            .view_id("vewMxw9kIo")
            .field_names(["触发日期", "代码"])
            .filter({
                "conjunction": "and",
                "conditions": [
                    {
                        "field_name": "触发日期",   # 时间列
                        "operator": "is",  # >=
                        "value": ["ExactDate",'1776355200000']
                    },
                    {
                        "field_name": "代码",   # 单选列
                        "operator": "is",
                        "value": ["sh600552"]                    }
                ]
            })
            .build()
        ) \
        .build()

    # 发起请求
    response: SearchAppTableRecordResponse = client.bitable.v1.app_table_record.search(request)

    # 处理失败返回
    if not response.success():
        lark.logger.error(
            f"client.bitable.v1.app_table_record.search failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
        return

    # 处理业务结果
    lark.logger.info(lark.JSON.marshal(response.data, indent=4))


if __name__ == "__main__":
    main()