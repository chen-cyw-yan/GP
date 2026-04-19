import json

import lark_oapi as lark
from lark_oapi.api.drive.v1 import *
import os

# SDK 使用说明: https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/server-side-sdk/python--sdk/preparations-before-development
# 以下示例代码默认根据文档示例值填充，如果存在代码问题，请在 API 调试台填上相关必要参数后再复制代码使用
# 复制该 Demo 后, 需要将 "YOUR_APP_ID", "YOUR_APP_SECRET" 替换为自己应用的 APP_ID, APP_SECRET.

# 通过获取文档节点，使用获取到的obj_token作为parent_node  "file_token": "V84nbWyvEoL0DYxh1REcq9Nunqc"


def main():
    # 创建client
    client = lark.Client.builder() \
        .app_id("cli_a9256b2aef7a5cd4") \
        .app_secret("t22QBXS6MVqsXC41GoCDvbxin0tpXyL3") \
        .log_level(lark.LogLevel.DEBUG) \
        .build()

    # 构造请求对象
    file_path=r"E:\stock\GP\当日策列\static\test.png"
    file = open(file_path, "rb")
    request: UploadAllMediaRequest = UploadAllMediaRequest.builder() \
        .request_body(UploadAllMediaRequestBody.builder()
            .file_name("demo.png")
            .parent_type("bitable_image")
            .size(str(os.path.getsize(file_path)))
            .parent_node('AYw3buqaVaGuv1sLtI8chGJLn0v')
            .file(file)
            .build()) \
        .build()

    # 发起请求
    response: UploadAllMediaResponse = client.drive.v1.media.upload_all(request)

    # 处理失败返回
    if not response.success():
        lark.logger.error(
            f"client.drive.v1.media.upload_all failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
        return

    # 处理业务结果
    lark.logger.info(lark.JSON.marshal(response.data, indent=4))


if __name__ == "__main__":
    main()