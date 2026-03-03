#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2026/2/26 17:52
# @Author : chenyanwen
# @email:1183445504@qq.com
import requests
import threading
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

class GlobalRequestPatcher:

    _lock = threading.Lock()
    _patched = False

    def __init__(self, cookies=None, headers=None):
        self.cookies = cookies or {}
        self.headers = headers or {}
        self.original_get = None
        self.original_post = None
        self.session = self._build_session()

    # ==================================================
    # 构建带重试的 session
    # ==================================================
    def _build_session(self):
        session = requests.Session()

        session.cookies.update(self.cookies)
        session.headers.update(self.headers)

        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[403, 429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    # ==================================================
    # 开始 patch
    # ==================================================
    def patch(self):

        with self._lock:

            if GlobalRequestPatcher._patched:
                logger.info("requests 已经被 patch，跳过")
                return

            self.original_get = requests.get
            self.original_post = requests.post

            def patched_get(url, **kwargs):
                kwargs.setdefault("timeout", (10, 30))
                return self.session.get(url, **kwargs)

            def patched_post(url, **kwargs):
                kwargs.setdefault("timeout", (10, 30))
                return self.session.post(url, **kwargs)

            requests.get = patched_get
            requests.post = patched_post

            GlobalRequestPatcher._patched = True
            logger.info("✓ requests 全局接管成功")

    # ==================================================
    # 恢复
    # ==================================================
    def restore(self):

        with self._lock:

            if not GlobalRequestPatcher._patched:
                return

            requests.get = self.original_get
            requests.post = self.original_post

            GlobalRequestPatcher._patched = False
            logger.info("✓ requests 已恢复")