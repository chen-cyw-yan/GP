from flask import Blueprint, current_app, jsonify, request, render_template

from .services.strategy_data import (
    build_kline_payload,
    get_strategy_frame,
    invalidate_strategy_cache,
    list_buy_signals_recent,
    list_stock_analysis_startup,
    strategy_search_stocks,
)

strategy_bp = Blueprint("strategy", __name__)


@strategy_bp.route("/dashboard")
def kline_dashboard():
    return render_template("kline_dashboard.html", title="K线 · 股票分析系统")


def _parse_page_args():
    page = request.args.get("page", default=1, type=int)
    page_size = request.args.get("page_size", default=20, type=int)
    page = max(1, page)
    page_size = max(1, min(page_size, 100))
    return page, page_size


@strategy_bp.route("/api/strategy/buy-signals")
def api_buy_signals():
    # 默认不限换手：库里常见当日 turnover=0 或小数尺度不一致，避免列表被滤空
    turnover_min = request.args.get("turnover_min", default=0.0, type=float)
    recent_days = request.args.get("recent_days", default=120, type=int)
    q = request.args.get("q", default="", type=str).strip()
    page, page_size = _parse_page_args()
    try:
        df = get_strategy_frame(current_app.config["DATABASE_URI"])
    except RuntimeError as e:
        return jsonify({"ok": False, "error": str(e)}), 503
    items, total = list_buy_signals_recent(
        df,
        turnover_min=turnover_min,
        recent_trading_days=recent_days,
        query=q or None,
        page=page,
        page_size=page_size,
    )
    return jsonify(
        {
            "ok": True,
            "items": items,
            "page": page,
            "page_size": page_size,
            "total": total,
        }
    )


@strategy_bp.route("/api/strategy/startup-list")
def api_startup_list():
    """启动策列：stock_analysis.need_to_analysis = '1'"""
    page, page_size = _parse_page_args()
    try:
        items, total = list_stock_analysis_startup(
            current_app.config["DATABASE_URI"],
            page=page,
            page_size=page_size,
        )
    except RuntimeError as e:
        return jsonify({"ok": False, "error": str(e)}), 503
    return jsonify(
        {
            "ok": True,
            "items": items,
            "page": page,
            "page_size": page_size,
            "total": total,
        }
    )


@strategy_bp.route("/api/strategy/search")
def api_search():
    q = request.args.get("q", default="", type=str)
    page, page_size = _parse_page_args()
    try:
        df = get_strategy_frame(current_app.config["DATABASE_URI"])
    except RuntimeError as e:
        return jsonify({"ok": False, "error": str(e)}), 503
    items, total = strategy_search_stocks(df, q, page=page, page_size=page_size)
    return jsonify(
        {
            "ok": True,
            "items": items,
            "page": page,
            "page_size": page_size,
            "total": total,
        }
    )


@strategy_bp.route("/api/strategy/kline/<path:code>")
def api_kline(code):
    tail = request.args.get("bars", default=600, type=int)
    tail = max(60, min(tail, 5000))
    try:
        df = get_strategy_frame(current_app.config["DATABASE_URI"])
    except RuntimeError as e:
        return jsonify({"ok": False, "error": str(e)}), 503
    payload = build_kline_payload(df, code.strip(), tail=tail)
    if payload.get("error"):
        return jsonify({"ok": False, "error": payload["error"]}), 404
    return jsonify({"ok": True, "data": payload})


@strategy_bp.route("/api/strategy/refresh-cache", methods=["POST"])
def api_refresh_cache():
    invalidate_strategy_cache()
    return jsonify({"ok": True})
