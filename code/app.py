import requests
from datetime import datetime, timedelta

from flask import Flask, render_template, request
from models.database import DatabaseManager

app = Flask(__name__)
db_manager = DatabaseManager("ecoulement.db")

@app.route('/')
def home():
    """
    Gère la page d'accueil de l'application.
    Récupère toutes les stations et applique les filtres de région, département et recherche.
    """
    region = request.args.get("region")
    departement = request.args.get("departement")
    search = request.args.get("search", "").lower().strip()

    # Récupérer toutes les stations depuis la base de données locale
    stations = db_manager.get_all_stations()

    # Appliquer le filtrage basé sur les paramètres de l'URL
    if region:
        stations = [s for s in stations if s["libelle_region"] == region]
    if departement:
        stations = [s for s in stations if s["libelle_departement"] == departement]
    if search:
        stations = [
            s for s in stations
            if search in s["libelle_station"].lower() or search in s["code_station"].lower()
        ]

    # Pour afficher les options uniques dans les filtres du formulaire
    regions = sorted(set(s["libelle_region"] for s in db_manager.get_all_stations()))
    departements = sorted(set(s["libelle_departement"] for s in db_manager.get_all_stations()))

    return render_template("index.html", stations=stations, regions=regions, departements=departements)

@app.route('/station/<code_station>')
def station_detail(code_station):
    """
    Affiche les détails d'une station spécifique, y compris ses observations
    et campagnes récentes depuis l'API Hub'eau.
    """
    # Nettoyer le code de la station pour l'utiliser avec la base de données et l'API
    code_station_cleaned = code_station.strip().replace(" ", "")
    station = db_manager.get_station_by_code(code_station_cleaned)

    observations_data = []
    campagnes_data = []

    if station:
        # --- Récupération des données d'observations (ecoulement/observations) ---
        # Cette API fournit des données d'observations visuelles d'écoulement.
        observations_api_url = "https://hubeau.eaufrance.fr/api/v1/ecoulement/observations"
        observations_params = {
            "code_station": code_station_cleaned,
            "size": 200,  # Limite le nombre d'observations récupérées
            "sort": "date_observation_desc" # Trie par date d'observation descendante (du plus récent au plus ancien)
        }
        try:
            obs_response = requests.get(observations_api_url, params=observations_params)
            obs_response.raise_for_status() # Lève une exception pour les codes d'état HTTP 4xx/5xx
            obs_api_data = obs_response.json()
            if obs_api_data and obs_api_data.get('data'):
                observations_data = obs_api_data['data']
        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de la récupération des observations pour {code_station_cleaned}: {e}")
            # En cas d'erreur, observations_data restera vide, ce qui sera géré par le template
            pass

        # --- Récupération des données de campagnes (ecoulement/campagnes) ---
        # Cette API fournit des informations sur les campagnes d'observations effectuées.
        campagnes_api_url = "https://hubeau.eaufrance.fr/api/v1/ecoulement/campagnes"
        campagnes_params = {
            "code_station": code_station_cleaned,
            "size": 50, # Limite le nombre de campagnes récupérées
            "sort": "date_campagne_desc" # Trie par date de campagne descendante
        }
        try:
            camp_response = requests.get(campagnes_api_url, params=campagnes_params)
            camp_response.raise_for_status()
            camp_api_data = camp_response.json()
            if camp_api_data and camp_api_data.get('data'):
                campagnes_data = camp_api_data['data']
        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de la récupération des campagnes pour {code_station_cleaned}: {e}")
            # En cas d'erreur, campagnes_data restera vide
            pass

    if not station:
        return "Station introuvable", 404

    return render_template(
        "station.html",
        station=station,
        observations_data=observations_data,
        campagnes_data=campagnes_data
    )

# Nouvelle route pour la page graphique globale (pour toutes les stations)
@app.route('/graphique_global')
def graphique_global():
    """
    Affiche un graphique de distribution des types d'écoulement pour toutes les stations,
    utilisant les données de l'API Hub'eau Écoulement des cours d'eau.
    """
    global_ecoulement_data = []

    # Récupération des données d'observations d'écoulement pour toutes les stations
    # sur une période récente (ex: les 90 derniers jours)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90) # Récupère les données des 90 derniers jours

    observations_api_url = "https://hubeau.eaufrance.fr/api/v1/ecoulement/observations"
    observations_params = {
        "date_observation_min": start_date.strftime("%Y-%m-%d"),
        "date_observation_max": end_date.strftime("%Y-%m-%d"),
        "size": 20000, # Taille max pour essayer de récupérer un maximum de données
        "sort": "date_observation_desc"
    }

    try:
        obs_response = requests.get(observations_api_url, params=observations_params)
        obs_response.raise_for_status()
        api_data = obs_response.json()

        if api_data and api_data.get('data'):
            # Pour ce graphique, nous avons besoin du libellé de l'écoulement.
            global_ecoulement_data = [
                {"libelle": obs["libelle_ecoulement"]} # Seul le libellé est nécessaire pour la distribution
                for obs in api_data["data"]
            ]

    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la récupération des données globales d'écoulement pour le graphique: {e}")
        pass

    # Note: station est null ici car c'est un graphique global, pas lié à une station spécifique
    return render_template(
        "graphique.html", # Utilise le même template graphique.html
        station={"libelle_station": "Toutes les stations", "code_station": "GLOBAL"}, # Données factices pour le titre
        ecoulement_graph_data=global_ecoulement_data
    )


if __name__ == '__main__':
    app.run(debug=True)
