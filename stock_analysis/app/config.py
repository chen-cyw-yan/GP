import os


def _env_bool(key: str, default: str = "1") -> bool:
    return os.environ.get(key, default).strip().lower() not in ("0", "false", "no", "")


class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY", "dev-change-in-production")
    DATABASE_URI = os.environ.get(
        "DATABASE_URI",
        "mysql+pymysql://root:chen@127.0.0.1:3306/gp",
    )
    # 启动时预加载试盘策略全表缓存（关闭：环境变量 WARM_STRATEGY_ON_STARTUP=0）
    WARM_STRATEGY_ON_STARTUP = _env_bool("WARM_STRATEGY_ON_STARTUP", "1")
