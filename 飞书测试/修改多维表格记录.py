import json

import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *


# SDK 使用说明: https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/server-side-sdk/python--sdk/preparations-before-development
# 以下示例代码默认根据文档示例值填充，如果存在代码问题，请在 API 调试台填上相关必要参数后再复制代码使用
def main():
    # 创建client
    # 使用 user_access_token 需开启 token 配置, 并在 request_option 中配置 token
    client = lark.Client.builder() \
        .enable_set_token(True) \
        .log_level(lark.LogLevel.DEBUG) \
        .build()

    # 构造请求对象
    request: UpdateAppTableRecordRequest = UpdateAppTableRecordRequest.builder() \
        .app_token("FFewwkxf2izEVxkyA7Yc821GnXe") \
        .table_id("tbliSMaFdxeKSM8y") \
        .record_id("recvh3BO6ThwK9") \
        .request_body(AppTableRecord.builder()
            .fields({"附件":[{"file_token":"V84nbWyvEoL0DYxh1REcq9Nunqc"}]})
            .build()) \
        .build()

    # 发起请求
    option = lark.RequestOption.builder().user_access_token("u-fi6731_3x5PpttR2Wtv47w15jczx0loXp8EamMo02CCH").build()
    response: UpdateAppTableRecordResponse = client.bitable.v1.app_table_record.update(request, option)

    # 处理失败返回
    if not response.success():
        lark.logger.error(
            f"client.bitable.v1.app_table_record.update failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
        return

    # 处理业务结果
    lark.logger.info(lark.JSON.marshal(response.data, indent=4))


if __name__ == "__main__":
    main()
