"""生产环境可由 gunicorn 等加载： gunicorn -w 4 'wsgi:app' """
from app import create_app

app = create_app()
