#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2026/3/10 17:12
# @Author : chenyanwen
# @email:1183445504@qq.com
import json
import uuid
import os
import lark_oapi as lark
from lark_oapi.api.im.v1 import *

class FeishuUtils:  # 【建议】类名首字母大写，符合 Python 规范
    def __init__(self, app_id, app_secret):
        self.app_id = app_id
        self.app_secret = app_secret
        self.client = lark.Client.builder() \
            .app_id(app_id) \
            .app_secret(app_secret) \
            .log_level(lark.LogLevel.DEBUG) \
            .build()

    def set_message_for_text(self, receive_id_type, receive_id, text_content):
        """
        发送文本消息
        :param text_content: 直接传入文本字符串，函数内部构造 JSON
        """
        # 【修复】content 必须是 JSON 字符串 {"text": "..."}
        if not text_content.startswith("{"):
            content_json = json.dumps({"text": text_content}, ensure_ascii=False)
        else:
            content_json = text_content

        request: CreateMessageRequest = CreateMessageRequest.builder() \
            .receive_id_type(receive_id_type) \
            .request_body(CreateMessageRequestBody.builder()
                          .receive_id(receive_id)
                          .msg_type("text")
                          .content(content_json)
                          # 【修复】uuid 直接传字符串，不要 json.dumps
                          .uuid(str(uuid.uuid4())) 
                          .build()) \
            .build()

        response: CreateMessageResponse = self.client.im.v1.message.create(request)

        if not response.success():
            lark.logger.error(
                f"send text failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}")
            return 'send text error!!'

        lark.logger.info("send text success!!")
        return 'send text success!!'

    def set_message_for_image(self, receive_id_type, receive_id, image_path):
        # 1. 读取图片文件
        if not os.path.exists(image_path):
            lark.logger.error(f"File not found: {image_path}")
            return 'file not found!!'

        try:
            with open(image_path, "rb") as file:
                # 2. 上传图片
                request_img = CreateImageRequest.builder() \
                    .request_body(CreateImageRequestBody.builder()
                                  .image_type("message")
                                  .image(file)
                                  .build()) \
                    .build()

                response_img: CreateImageResponse = self.client.im.v1.image.create(request_img)

                if not response_img.success():
                    lark.logger.error(f"upload image failed: {response_img.code} - {response_img.msg}")
                    return 'upload image error!!'

                image_key = response_img.data.image_key
                lark.logger.info(f"Image uploaded, key: {image_key}")

        except Exception as e:
            lark.logger.error(f"Error reading image: {str(e)}")
            return 'read image error!!'

        # 3. 发送图片消息
        content_json = json.dumps({"image_key": image_key}, ensure_ascii=False)
        
        request_msg = CreateMessageRequest.builder() \
            .receive_id_type(receive_id_type) \
            .request_body(CreateMessageRequestBody.builder()
                          .receive_id(receive_id)
                          .msg_type("image")
                          .content(content_json)
                          .uuid(str(uuid.uuid4()))
                          .build()) \
            .build()

        response: CreateMessageResponse = self.client.im.v1.message.create(request_msg)

        if not response.success():
            lark.logger.error(f"send image failed: {response.code} - {response.msg}")
            return 'send image error!!'

        return 'send img success!!'
    
    def set_message_for_file(self, receive_id_type, receive_id, file_path, file_name):
            if not os.path.exists(file_path):
                lark.logger.error(f"File not found: {file_path}")
                return 'file not found!!'

            try:
                with open(file_path, "rb") as f:
                    # ⚠️ 注意：文件上传 API 在 drive.v1，不是 file.v1！
                    request = CreateFileRequest.builder() \
                        .request_body(CreateFileRequestBody.builder()
                            .file_type("xlsx")  # 或 "xls", "pdf" 等
                            .file_name(file_name)
                            .file(f)
                            .build()) \
                        .build()

                    response: CreateFileResponse = self.client.im.v1.file.create(request)  # ← 关键：drive.v1.file

                    if not response.success():
                        lark.logger.error(f"Upload file failed: {response.code} - {response.msg}")
                        return 'upload file error!!'

                    file_key = response.data.file_key
                    lark.logger.info(f"File uploaded, key: {file_key}")

            except Exception as e:
                lark.logger.error(f"Error reading file: {str(e)}")
                return 'read file error!!'

            # 发送文件消息
            content = json.dumps({"file_key": file_key}, ensure_ascii=False)
            msg_request = CreateMessageRequest.builder() \
                .receive_id_type(receive_id_type) \
                .request_body(CreateMessageRequestBody.builder()
                    .receive_id(receive_id)
                    .msg_type("file")
                    .content(content)
                    .uuid(str(uuid.uuid4()))
                    .build()) \
                .build()

            resp = self.client.im.v1.message.create(msg_request)
            if not resp.success():
                lark.logger.error(f"Send file message failed: {resp.code} - {resp.msg}")
                return 'send file error!!'

            return 'send file success!!'



if __name__ == '__main__':
    APP_ID = 'cli_a9256b2aef7a5cd4'
    APP_SECRET = 't22QBXS6MVqsXC41GoCDvbxin0tpXyL3'
    
    # 【核心修复】变量名改为 fs_client，不要和类名 FeishuUtils 重复
    fs_client = FeishuUtils(APP_ID, APP_SECRET)
    
    # 测试文本 (注意 content 格式)
    # res = fs_client.set_message_for_text('chat_id', 'oc_cd642a7fec1dcd847e91b2e1775809d2', "Hello World from Python!")
    
    # 测试图片
    # img_path = r"C:\Users\cyw\Desktop\jupyternotebook\git-python\GP\charts\sh600468.png"
    
    # # 检查文件是否存在再运行，避免报错
    # if os.path.exists(img_path):
    #     res = fs_client.set_message_for_image('chat_id', 'oc_cd642a7fec1dcd847e91b2e1775809d2', img_path)
    #     print(res)
    # else:
    #     print(f"测试文件不存在: {img_path}")

    # 测试文件
    file_path = r"C:\Users\cyw\Desktop\jupyternotebook\git-python\GP\prod_online\script\result.xlsx"
    
    # 检查文件是否存在再运行，避免报错
    if os.path.exists(file_path):
        res = fs_client.set_message_for_file('chat_id', 'oc_cd642a7fec1dcd847e91b2e1775809d2', file_path,'result.xlsx')
        print(res)
    else:
        print(f"测试文件不存在: {file_path}")