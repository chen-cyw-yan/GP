from flask import Blueprint, render_template

bp = Blueprint("main", __name__)


@bp.route("/")
def index():
    return render_template("index.html", title="首页 · 股票分析系统")


@bp.route("/health")
def health():
    return {"status": "ok"}, 200
