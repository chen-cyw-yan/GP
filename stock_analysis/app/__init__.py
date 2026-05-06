from flask import Flask

from .config import Config


def _warm_strategy_cache(app: Flask) -> None:
    """启动时拉库并计算因子，填充内存缓存，避免首次打开 dashboard 长时间等待。"""
    from .services.strategy_data import get_strategy_frame

    uri = app.config["DATABASE_URI"]
    app.logger.info("试盘策略：开始预热缓存（拉取 stock 并计算信号）…")
    get_strategy_frame(uri)
    app.logger.info("试盘策略：缓存预热完成")


def create_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_object(Config)
    if test_config:
        app.config.from_mapping(test_config)

    from . import routes
    from .strategy_blueprint import strategy_bp

    app.register_blueprint(routes.bp)
    app.register_blueprint(strategy_bp)

    if app.config.get("WARM_STRATEGY_ON_STARTUP") and not app.testing:
        with app.app_context():
            try:
                _warm_strategy_cache(app)
            except Exception as e:
                app.logger.warning(
                    "试盘策略：启动预热失败，首次请求将自动重试。原因: %s", e, exc_info=app.debug
                )

    return app
