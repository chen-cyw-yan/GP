import logging
import fetch_stock_data_for_multithread as fetch_stock
import prod_online.script.stock_selector_at_home as selector
import stock_deep_report as deep_report
import block_analysis
import get_now_stock_data as now_stock
import fetch_threshold as threshold
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
    logger.info("="*30 + " 开始更新日线数据任务 " + "="*30)
    # fetch_stock.main()
    now_stock.main()
    
    logger.info("="*30 + " 开始计算选股策列任务 " + "="*30)
    selector.main()

    logger.info("="*30 + " 开始执行板块分析任务 " + "="*30)
    block_analysis.main()

    logger.info("="*30 + " 开始执行本日大小单阈值分析任务 " + "="*30)
    # threshold.main()


    logger.info("="*30 + " 开始执行股票深度分析任务 " + "="*30)
    deep_report.main()
    logger.info("="*30 + " 执行完成 " + "="*30)
if __name__ == '__main__':
    main()