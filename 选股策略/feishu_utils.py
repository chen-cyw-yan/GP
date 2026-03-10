#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2026/3/10 17:12
# @Author : chenyanwen
# @email:1183445504@qq.com
import json
import uuid
import lark_oapi as lark
from lark_oapi.api.im.v1 import *
class feishu_utils:
    def __init__(self,app_id,app_secret):
        self.app_id = app_id
        self.app_secret = app_secret
        self.client = lark.Client.builder() \
            .app_id(app_id) \
            .app_secret(app_secret) \
            .log_level(lark.LogLevel.DEBUG) \
            .build()
    def set_message_for_text(self,receive_id_type,receive_id,context):
        # 构造请求对象
        request: CreateMessageRequest = CreateMessageRequest.builder() \
            .receive_id_type(receive_id_type) \
            .request_body(CreateMessageRequestBody.builder()
                          .receive_id(receive_id)
                          .msg_type("text")
                          .content(context)
                          .uuid(json.dumps(str(uuid.uuid4())))
                          .build()) \
            .build()

        # 发起请求
        response: CreateMessageResponse = self.client.im.v1.message.create(request)

        # 处理失败返回
        if not response.success():
            lark.logger.error(
                f"client.im.v1.message.create failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}")
            return 'send text error!!'

        # 处理业务结果
        lark.logger.info(lark.JSON.marshal(response.data, indent=4))
        return 'send text success!!'

    def set_message_for_imge(self, receive_id_type, receive_id, image_path):
        # 1. 读取图片文件 (建议使用 with 自动关闭)
        try:
            with open(image_path, "rb") as file:
                # 2. 构造上传图片请求
                request = CreateImageRequest.builder() \
                    .request_body(CreateImageRequestBody.builder()
                                  .image_type("message")
                                  .image(file)
                                  .build()) \
                    .build()

                # 3. 发起上传图片请求
                response_img: CreateImageResponse = self.client.im.v1.image.create(request)

                # 4. 处理上传失败
                if not response_img.success():
                    error_msg = f"client.im.v1.image.create failed, code: {response_img.code}, msg: {response_img.msg}"
                    lark.logger.error(error_msg)
                    return 'upload image error!!'

                # 【关键修复点】：从 response_img.data 中获取 image_key
                # 注意：不同版本SDK可能是 response_img.data.image_key 或 response_img.data.get('image_key')
                # 通常 SDK 生成的 data 是一个对象，直接访问属性
                image_key = response_img.data.image_key

                lark.logger.info(f"Image uploaded successfully, key: {image_key}")

        except FileNotFoundError:
            lark.logger.error(f"File not found: {image_path}")
            return 'file not found!!'
        except Exception as e:
            lark.logger.error(f"Error reading image: {str(e)}")
            return 'read image error!!'

        # 5. 构造发送消息请求
        # 图片消息的 content 必须是 JSON 字符串：{"image_key": "xxx"}
        content_json = json.dumps({"image_key": image_key}, ensure_ascii=False)

        # 生成真实的 UUID
        msg_uuid = str(uuid.uuid4())

        request_msg = CreateMessageRequest.builder() \
            .receive_id_type(receive_id_type) \
            .request_body(CreateMessageRequestBody.builder()
                          .receive_id(receive_id)
                          .msg_type("image")
                          .content(content_json)  # 传入 JSON 字符串
                          .uuid(msg_uuid)  # 填入生成的 UUID
                          .build()) \
            .build()

        # 6. 发起发送消息请求
        response: CreateMessageResponse = self.client.im.v1.message.create(request_msg)

        # 7. 处理发送失败
        if not response.success():
            lark.logger.error(
                f"client.im.v1.message.create failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}")
            return 'send image error!!'

        # 8. 成功
        lark.logger.info(f"Message sent successfully! LogID: {response.get_log_id()}")
        return 'send img success!!'


if __name__ == '__main__':
    feishu_utils=feishu_utils('cli_a9256b2aef7a5cd4','t22QBXS6MVqsXC41GoCDvbxin0tpXyL3')
    # feishu_utils.set_message_for_text('chat_id','oc_cd642a7fec1dcd847e91b2e1775809d2',"{\"text\":\"test1111 content\"}")
    feishu_utils.set_message_for_imge('chat_id', 'oc_cd642a7fec1dcd847e91b2e1775809d2',
                                      r"C:\Users\cyw\Desktop\jupyternotebook\git-python\GP\charts\sh600468.png")