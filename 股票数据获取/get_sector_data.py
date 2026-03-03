#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2026/2/26 17:53
# @Author : chenyanwen
# @email:1183445504@qq.com
import akshare as ak
from GlobalRequestPatcher import GlobalRequestPatcher

patcher = GlobalRequestPatcher(
    cookies={"Cookie": 'qgqp_b_id=c8f8164529ce240b8d7a6db25718d9cd; st_nvi=qSlN8OYk0yCR5UOXmlxsw5bae; st_si=18540620399655; websitepoptg_api_time=1772154282558; nid18=09c78fbd1ebdd3f7a9ca9e364b111321; nid18_create_time=1772154283171; gviem=s1L4I1JSsv417WQZqKMnSb898; gviem_create_time=1772154283171; p_origin=https%3A%2F%2Fpassport2.eastmoney.com; mtp=1; ct=qJittJ4_fvn8sLhnddpFBAueVDi6KiegN9uByDiyGaK6LgJPiqPWyEiRrsIMYCRJ0ir83QAZUddp6hcNtYi689K-FM3vknOWy0gjP-7BmSkZxcZaPkros0ldk7AgciiwelZKxBFbpuYzjiBgg_9sUuZYSjsTM4aKv57fc8gJ8ho; ut=FobyicMgeV67Opd1lR0vsl0qZAwxFl1dhivAJm2E7PxP5E-N8emcO0yDZU_8r5WDffovjdu2hJYmoXkIAWuKxU6pdWKdH3ombyUTRO3wYghcW0AJGKx_lasNF4LO73tnhrt2LTpklv-pwVTatZy58Nse2pjnRoeXP4oX5eX4IB7fi7EQJ0fN8lcHVEikq-CDPViAVXWGo-JFvjeaqrFaV39XMqTf1wNXi41rzfom5GS8dKWyuCWuh6I_392p7BPs_S8rXWHl90RVq7WQIgQxe_YMFP7ikLjudqN1YTCzO9-oNOi6gsV0JFe1jh2T-v3emHp-quFNRjq1GfPYFvWqaIXaG8bzwbpZDOGe0CcJsgvXJ9uVDQW2J7ar9hMVMIn-YxBzpCBexWUrKDxzhuAJhkF4KKGUx9sRuOOvczJHV_cTRJIGNg9mNa4GeLTOISnSZV8X1XzYW8zAiMy3TqW2Ol1FxN2zYRAGFkkoIwq5wLvXUSS7Bchezg; pi=8462047422670334%3Ba8462047422670334%3B%E8%82%A1%E5%8F%8B69O7871o35%3BRYWmx5I1p7oYudV0U74wAmN%2FyfS9NWmlNWFcQ14JUgjkkbAXZdBMmzi%2BM%2FK8Rdsh9zRb5mf9PYfgXdcR5HA7T7DZhvIkVNuwiv7ESlWdCJcDg7loq3FzHJGyTba5qv6YQ3erro6va48xrW0kUp%2FCUpDQuPQsJ9SCV%2Fk4b7J3lWgpnR2EAPyrFCk0dWU1Pz5YTMxB%2B4oZ%3B81t0tLS0GEgq%2B7oDZuc0jQuwvDT7YlrcKuoUVTrKxvTDVL0ioWAVFQuKBmPUoURpjmBwfiINYkczoIIEV4qOkwaLdqN9r5pHKc0ehsoBlG7I%2B%2FEfUxUCGD3ZyFB5bgUMJjtDGnlaWskYnmgs8Vu5jtdpwpahvA%3D%3D; uidal=8462047422670334%e8%82%a1%e5%8f%8b69O7871o35; sid=; vtpst=|; fullscreengg=1; fullscreengg2=1; st_pvi=41935840362909; st_sp=2025-03-13%2009%3A33%3A37; st_inirUrl=http%3A%2F%2Fquote.eastmoney.com%2Fcenter%2Fgridlist.html; st_sn=7; st_psi=20260227090619461-113200301201-9143523869; st_asi=delete'},
    headers={
        "Accept": 'text/html, */*; q=0.01',
        "Accept-Encoding": 'gzip, deflate, br, zstd',
        "Accept-Language": 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        "Connection": 'keep-alive',
        "Cookie": 'qgqp_b_id=c8f8164529ce240b8d7a6db25718d9cd; st_nvi=qSlN8OYk0yCR5UOXmlxsw5bae; st_si=18540620399655; websitepoptg_api_time=1772154282558; nid18=09c78fbd1ebdd3f7a9ca9e364b111321; nid18_create_time=1772154283171; gviem=s1L4I1JSsv417WQZqKMnSb898; gviem_create_time=1772154283171; p_origin=https%3A%2F%2Fpassport2.eastmoney.com; mtp=1; ct=qJittJ4_fvn8sLhnddpFBAueVDi6KiegN9uByDiyGaK6LgJPiqPWyEiRrsIMYCRJ0ir83QAZUddp6hcNtYi689K-FM3vknOWy0gjP-7BmSkZxcZaPkros0ldk7AgciiwelZKxBFbpuYzjiBgg_9sUuZYSjsTM4aKv57fc8gJ8ho; ut=FobyicMgeV67Opd1lR0vsl0qZAwxFl1dhivAJm2E7PxP5E-N8emcO0yDZU_8r5WDffovjdu2hJYmoXkIAWuKxU6pdWKdH3ombyUTRO3wYghcW0AJGKx_lasNF4LO73tnhrt2LTpklv-pwVTatZy58Nse2pjnRoeXP4oX5eX4IB7fi7EQJ0fN8lcHVEikq-CDPViAVXWGo-JFvjeaqrFaV39XMqTf1wNXi41rzfom5GS8dKWyuCWuh6I_392p7BPs_S8rXWHl90RVq7WQIgQxe_YMFP7ikLjudqN1YTCzO9-oNOi6gsV0JFe1jh2T-v3emHp-quFNRjq1GfPYFvWqaIXaG8bzwbpZDOGe0CcJsgvXJ9uVDQW2J7ar9hMVMIn-YxBzpCBexWUrKDxzhuAJhkF4KKGUx9sRuOOvczJHV_cTRJIGNg9mNa4GeLTOISnSZV8X1XzYW8zAiMy3TqW2Ol1FxN2zYRAGFkkoIwq5wLvXUSS7Bchezg; pi=8462047422670334%3Ba8462047422670334%3B%E8%82%A1%E5%8F%8B69O7871o35%3BRYWmx5I1p7oYudV0U74wAmN%2FyfS9NWmlNWFcQ14JUgjkkbAXZdBMmzi%2BM%2FK8Rdsh9zRb5mf9PYfgXdcR5HA7T7DZhvIkVNuwiv7ESlWdCJcDg7loq3FzHJGyTba5qv6YQ3erro6va48xrW0kUp%2FCUpDQuPQsJ9SCV%2Fk4b7J3lWgpnR2EAPyrFCk0dWU1Pz5YTMxB%2B4oZ%3B81t0tLS0GEgq%2B7oDZuc0jQuwvDT7YlrcKuoUVTrKxvTDVL0ioWAVFQuKBmPUoURpjmBwfiINYkczoIIEV4qOkwaLdqN9r5pHKc0ehsoBlG7I%2B%2FEfUxUCGD3ZyFB5bgUMJjtDGnlaWskYnmgs8Vu5jtdpwpahvA%3D%3D; uidal=8462047422670334%e8%82%a1%e5%8f%8b69O7871o35; sid=; vtpst=|; fullscreengg=1; fullscreengg2=1; st_pvi=41935840362909; st_sp=2025-03-13%2009%3A33%3A37; st_inirUrl=http%3A%2F%2Fquote.eastmoney.com%2Fcenter%2Fgridlist.html; st_sn=7; st_psi=20260227090619461-113200301201-9143523869; st_asi=delete',
        "Host": 'quote.eastmoney.com',
        "Referer": 'https://quote.eastmoney.com',
        "Sec-Fetch-Dest": 'empty',
        "Sec-Fetch-Mode": 'cors',
        "Sec-Fetch-Site": 'same-origin',
        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0',
        "X-Requested-With": 'XMLHttpRequest',
        "sec-ch-ua": '"Not:A-Brand";v="99", "Microsoft Edge";v="145", "Chromium";v="145"',
        "sec-ch-ua-mobile": '?0',
        "sec-ch-ua-platform": '"Windows"',
    }
)

patcher.patch()

# 之后所有 akshare 请求都会走你的 session
# stock_board_concept_hist_em_df = ak.stock_board_concept_hist_em(symbol="绿色电力", period="daily", start_date="20260101", end_date="20260226", adjust="")
# print(stock_board_concept_hist_em_df)



stock_board_concept_cons_em_df = ak.stock_board_concept_cons_em(symbol="融资融券")
print(stock_board_concept_cons_em_df)
# 如果需要恢复
patcher.restore()