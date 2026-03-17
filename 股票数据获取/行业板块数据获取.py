from selenium import webdriver
from datetime import datetime
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import pymysql
import time
import pandas as pd
import logging
conn = pymysql.connect(
            host='127.0.0.1',
            user='root',
            password='chen',
            database='gp',
            # use_unicode=args.encoding,
        )
cursor = conn.cursor()
def toSql(sql: str, rows: list):
    """
        连接数据库
    """
    # print(sql,rows)
    try:

        cursor.executemany(sql, rows)
        conn.commit()
    except Exception as e:
        raise ConnectionError("[ERROR] 连接数据库失败，具体原因是：" + str(e))
# print(stock_zh_a_spot_em_df)
# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_driver(headless=True):
    """初始化 Chrome WebDriver"""
    # driver_path='C:\App\chromedriver-win64\chromedriver-win64\chromedriver.exe'
    driver_path='E:/Program Files/chromedriver-win64/chromedriver-win64/chromedriver.exe'
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")  # 无头模式
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    driver_path='E:\Program Files\chromedriver-win64\chromedriver-win64\chromedriver.exe'
    service = Service(executable_path=driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # 绕过 Selenium 检测
    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': '''
            delete navigator.__proto__.webdriver;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
            );
        '''
    })
    return driver

def scrape_thshy_page(driver, page_num):
    """爬取指定页码的行业数据"""
    url = f"https://q.10jqka.com.cn/thshy/index/field/199112/order/desc/page/{page_num}/ajax/1/"
    
    logger.info(f"正在加载第 {page_num} 页: {url}")
    driver.get(url)
    
    try:
        # 等待表格加载完成（关键：等待 tbody 出现）
        WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "tbody tr"))
            )
        time.sleep(2)  # 额外缓冲
            
            # 获取页面源码并解析
        soup = BeautifulSoup(driver.page_source, 'html.parser')
            # print(soup.text)
        table = soup.select_one('.m-table.m-pager-table')
            # print(table)
        if not table:
            logger.warning(f"第 {page_num} 页未找到数据表")
            return []
            
        rows = []
        for tr in table.select('tr'):
            cols = tr.select('td')
            if len(cols) < 5:
                continue
            row = {
                    '板块名称': cols[1].get_text(strip=True),
                    '板块链接': cols[1].find('a').get('href',''),
                    '板块编号': cols[1].find('a').get('href','').split('/')[-2],
                    '涨跌幅': cols[2].get_text(strip=True),
                    '总成交量':cols[3].get_text(strip=True),
                    '总成交额':cols[4].get_text(strip=True),
                    '主力净流入': cols[5].get_text(strip=True),
                    '上涨家数': cols[6].get_text(strip=True),
                    '下跌家数': cols[7].get_text(strip=True),
                    '均价':cols[8].get_text(strip=True),
                    '领涨股':cols[9].get_text(strip=True),
                    '领涨股最新价':cols[10].get_text(strip=True),
                    '领涨股涨跌幅':cols[11].get_text(strip=True),
            }
            rows.append(row)
        return rows
    
    except Exception as e:
        logger.error(f"第 {page_num} 页加载失败: {str(e)}")
        return []

def main():
    driver = init_driver(headless=False)
    all_data = []
    
    try:
        # 先访问首页获取合法会话
        driver.get("https://q.10jqka.com.cn/thshy/index")
        time.sleep(3)
        
        # 获取总页数（可选）
        total_pages = 5  # 同花顺行业共约5页，可动态获取
        
        for page in range(1, total_pages + 1):
            data = scrape_thshy_page(driver, page)
            if data:
                all_data.extend(data)
                logger.info(f"成功获取第 {page} 页，共 {len(data)} 条记录")
            else:
                logger.warning(f"第 {page} 页无数据，停止爬取")
                break
            
            # 控制频率，避免被限
            time.sleep(2)
    
    finally:
        driver.quit()
    
    # 保存结果
    if all_data:
        df = pd.DataFrame(all_data)
        # df.to_csv("thshy_industries.csv", index=False, encoding='utf-8-sig')
        columns_name={
            "板块名称":"plate_name",
            "板块链接":"plate_url",
            "板块编号":"plate_code",
            "涨跌幅":"change_pct",
            '总成交量':'volume_count',
            '总成交额':'amount_sum',
            "主力净流入":"main_net_inflow",
            "领涨股":"leading_stock",
            "上涨家数":"rising_count",
            "下跌家数":"falling_count",
            "均价":"avg_price",
            "领涨股":"leading_stock_name",
            "领涨股最新价":"leading_stock_price",
            "领涨股涨跌幅":"leading_stock_change"
        }
        df=df.rename(columns=columns_name)
        today_dt = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        today_str = today_dt.strftime("%Y-%m-%d")
        # today_str ='2026-03-06'
        logger.info(f"当前任务日期: {today_str}")
        df['trade_date']=today_str
        sql = f"REPLACE INTO gp.thshy_industry(`{'`,`'.join(df.columns)}`) VALUES ({','.join(['%s' for _ in range(df.shape[1])])})"
        toSql(sql=sql, rows=df.values.tolist())
        logger.info(f"✅ 数据已保存至 thshy_industries.csv，共 {len(df)} 条记录")
    else:
        logger.error("❌ 未获取到任何数据")

if __name__ == "__main__":
    main()