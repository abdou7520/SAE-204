"""Flask controllers / routes."""

from flask import Blueprint, current_app, render_template, abort

from .models import Database

main_bp = Blueprint('main', __name__)


def get_db() -> Database:
    return Database(current_app.config['DATABASE'])


@main_bp.route('/')
def index():
    db = get_db()
    regions = db.get_regions()
    return render_template('index.html', regions=regions)


@main_bp.route('/region/<code>')
def region(code):
    db = get_db()
    deps = db.get_departements_by_region(code)
    if not deps:
        abort(404)
    return render_template('region.html', departements=deps, code=code)


@main_bp.route('/departement/<code>')
def departement(code):
    db = get_db()
    communes = db.get_communes_by_departement(code)
    if not communes:
        abort(404)
    return render_template('departement.html', communes=communes, code=code)
