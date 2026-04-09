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
    request: CreateAppTableRecordRequest = CreateAppTableRecordRequest.builder() \
        .app_token("FFewwkxf2izEVxkyA7Yc821GnXe") \
        .table_id("tbliSMaFdxeKSM8y") \
        .user_id_type("open_id") \
        .request_body(AppTableRecord.builder()
            .fields({
        "代码": "sz003042",
        "名称": "中农联合",
        "触发日期": 1775723162000,
        "收盘价": 5.8,
        "触动次数":1,
        "异动类型":'',
        "触及所需涨幅":0.1,
        "预警详情":"xxx",
        "计划":'',
        "行业板块":"xx",
        "概念板块":"xx",
        "板块共振得分":0.1
    })
            .build()) \
        .build()

    # 发起请求
    response: CreateAppTableRecordResponse = client.bitable.v1.app_table_record.create(request)

    # 处理失败返回
    if not response.success():
        lark.logger.error(
            f"client.bitable.v1.app_table_record.create failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
        return

    # 处理业务结果
    lark.logger.info(lark.JSON.marshal(response.data, indent=4))


if __name__ == "__main__":
    main()