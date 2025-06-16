from flask import Flask
from .controllers import main_bp


def create_app(db_path: str = 'ecoulement.db') -> Flask:
    """Factory to create Flask app."""
    app = Flask(__name__)
    app.config['DATABASE'] = db_path

    # Register blueprint
    app.register_blueprint(main_bp)
    return app
