import logging
import fetch_stock_data_for_multithread as fetch_stock
import stock_selector_for_myself as selector
import stock_deep_report as deep_report
import block_analysis
import get_now_stock_data as now_stock
# ==============================
# 日志配置
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)
def main():
    logger.info("更新日线数据...")
    fetch_stock.main()
    # now_stock.main()
    logger.info("更新日线数据完成..")
    
    logger.info("进行计算策略..")
    selector.main()
    logger.info("计算策略完成..")

    logger.info("计算所属板块..")
    block_analysis.main()
    logger.info("计算所属板块完成..")

    logger.info("个股深度分析..")
    # deep_report.main()
    logger.info("个股深度分析完成..")
if __name__ == '__main__':
    main()