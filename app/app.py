from flask import Flask
from app.routes import bp


def create_app():
    app = Flask(__name__)
    app.register_blueprint(bp, url_prefix="/")
    return app

# WSGI app used both for local run and by lambda adapter
app = create_app()

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
