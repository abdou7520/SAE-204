#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script d’import des données statiques “écoulement/stations” depuis l’API Hub’Eau
→ SQLite (ecoulement.db) en respectant le schéma ER normalisé :
    • region(code_region PK, libelle_region)
    • departement(code_departement PK, libelle_departement, code_region FK)
    • commune(code_commune PK, libelle_commune, code_departement FK)
    • bassin(code_bassin PK, libelle_bassin)
    • cours_eau(code_cours_eau PK, libelle_cours_eau, uri_cours_eau, code_bassin FK)
    • station(code_station PK, libelle_station, uri_station, etat_station,
              date_maj_station, latitude DOUBLE, longitude DOUBLE,
              code_commune FK, code_cours_eau FK)

Le script :
  1. Appele l’endpoint Hub’Eau « api-ecoulement/station » page par page.
  2. Remplit d’abord les tables référentielles (region → departement → commune,
     bassin → cours_eau) puis la table station.
  3. Ignore toute station dont un identifiant clé est manquant ou lat/lon hors plage.

L’exécution part du principe que la base SQLite ecoulement.db est déjà créée
avec le DDL exact ci‑dessus et que PRAGMA foreign_keys = ON est activé.
"""

import json
import time
import requests
import sqlite3
from pathlib import Path

###############################################################################
# Paramètres                                                                ###
###############################################################################

DB_PATH = Path("ecoulement.db")
API_URL = (
    "https://hubeau.eaufrance.fr/api/v1/ecoulement/station"
    "?size=500&fields={fields}&sort=code_station&format=json"
)

FIELDS = [
    # référentiels géographiques
    "code_region", "libelle_region",
    "code_departement", "libelle_departement",
    "code_commune", "libelle_commune",
    # référentiels hydro
    "code_bassin", "libelle_bassin",
    "code_cours_eau", "libelle_cours_eau", "uri_cours_eau",
    # station
    "code_station", "libelle_station", "uri_station",
    "etat_station", "date_maj_station",
    "latitude", "longitude",
]
FIELDS_PARAM = ",".join(FIELDS)

###############################################################################
# Connexion SQLite                                                          ###
###############################################################################

conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA foreign_keys = ON")
cur = conn.cursor()

###############################################################################
# Fonctions INSERT référentielles                                          ###
###############################################################################

def insert_region(code_region: str, libelle_region: str):
    if not code_region or not libelle_region:
        return
    cur.execute(
        "INSERT OR IGNORE INTO region (code_region, libelle_region) VALUES (?, ?)",
        (code_region, libelle_region),
    )

def insert_departement(code_departement: str, libelle_departement: str, code_region: str):
    if not code_departement or not libelle_departement or not code_region:
        return
    cur.execute(
        """
        INSERT OR IGNORE INTO departement (code_departement, libelle_departement, code_region)
        VALUES (?, ?, ?)
        """,
        (code_departement, libelle_departement, code_region),
    )

def insert_commune(code_commune: str, libelle_commune: str, code_departement: str):
    if not code_commune or not libelle_commune or not code_departement:
        return
    cur.execute(
        """
        INSERT OR IGNORE INTO commune (code_commune, libelle_commune, code_departement)
        VALUES (?, ?, ?)
        """,
        (code_commune, libelle_commune, code_departement),
    )

def insert_bassin(code_bassin: str, libelle_bassin: str):
    if not code_bassin or not libelle_bassin:
        return
    cur.execute(
        "INSERT OR IGNORE INTO bassin (code_bassin, libelle_bassin) VALUES (?, ?)",
        (code_bassin, libelle_bassin),
    )

def insert_cours_eau(code_cours_eau: str, libelle_cours_eau: str, uri_cours_eau: str, code_bassin: str):
    if not code_cours_eau or not libelle_cours_eau or not code_bassin:
        return
    cur.execute(
        """
        INSERT OR IGNORE INTO cours_eau (code_cours_eau, libelle_cours_eau, uri_cours_eau, code_bassin)
        VALUES (?, ?, ?, ?)
        """,
        (code_cours_eau, libelle_cours_eau, uri_cours_eau, code_bassin),
    )

###############################################################################
# Fonction INSERT station (corrigée)                                       ###
###############################################################################

def insert_station(
    code_station: str,
    libelle_station: str,
    uri_station: str,
    etat_station: str,
    date_maj_station: str,
    latitude,
    longitude,
    code_commune: str,
    code_cours_eau: str
):
    """Insère ou ignore une station.
    Les colonnes redondantes (coordonnee_x/y, code_region, code_departement, code_bassin)
    ont été supprimées du schéma normalisé.
    """
    # Vérification des clés étrangères indispensables
    if not code_commune or not code_cours_eau:
        print(f"⚠ IGNORE station ‘{code_station}’ : commune ou cours d’eau manquant(e)")
        return

    # Vérification simple des coordonnées (facultatif)
    if latitude is None or longitude is None or not (-90.0 <= latitude <= 90.0) or not (-180.0 <= longitude <= 180.0):
        print(f"⚠ IGNORE station ‘{code_station}’ : latitude/longitude hors plage")
        return

    try:
        cur.execute(
            """INSERT OR IGNORE INTO station (
                    code_station, libelle_station, uri_station,
                    etat_station, date_maj_station,
                    latitude, longitude,
                    code_commune, code_cours_eau
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                code_station, libelle_station, uri_station,
                etat_station, date_maj_station,
                latitude, longitude,
                code_commune, code_cours_eau
            ),
        )
    except sqlite3.IntegrityError as e:
        print(f"Erreur d’intégrité pour station ‘{code_station}’ : {e}")

###############################################################################
# Boucle principale d’import                                                ###
###############################################################################

def fetch_and_load():
    page = 1
    total_processed = 0
    while True:
        url = API_URL.format(fields=FIELDS_PARAM) + f"&page={page}"
        print(f"→ GET {url}")
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data", [])
        if not data:
            break

        for rec in data:
            # Nettoyage des champs ("" → None)
            code_region         = rec.get("code_region")        or None
            libelle_region      = rec.get("libelle_region")     or None
            code_departement    = rec.get("code_departement")   or None
            libelle_departement = rec.get("libelle_departement")or None
            code_commune        = rec.get("code_commune")       or None
            libelle_commune     = rec.get("libelle_commune")    or None
            code_bassin         = rec.get("code_bassin")        or None
            libelle_bassin      = rec.get("libelle_bassin")     or None
            code_cours_eau      = rec.get("code_cours_eau")     or None
            libelle_cours_eau   = rec.get("libelle_cours_eau")  or None
            uri_cours_eau       = rec.get("uri_cours_eau")      or None

            code_station        = rec.get("code_station")       or None
            libelle_station     = rec.get("libelle_station")    or None
            uri_station         = rec.get("uri_station")        or None
            etat_station        = rec.get("etat_station")       or None
            date_maj_station    = rec.get("date_maj_station")   or None

            latitude            = rec.get("latitude")
            longitude           = rec.get("longitude")

            # 1) référentiels géographiques
            insert_region(code_region, libelle_region)
            insert_departement(code_departement, libelle_departement, code_region)
            insert_commune(code_commune, libelle_commune, code_departement)

            # 2) référentiels hydrologiques
            insert_bassin(code_bassin, libelle_bassin)
            insert_cours_eau(code_cours_eau, libelle_cours_eau, uri_cours_eau, code_bassin)

            # 3) station elle-même
            insert_station(
                code_station, libelle_station, uri_station,
                etat_station, date_maj_station,
                latitude, longitude,
                code_commune,
                code_cours_eau
            )
            total_processed += 1

        conn.commit()
        print(f"Page {page} : {len(data)} lignes traitées (cumul : {total_processed})")
        page += 1
        time.sleep(0.8)  # petites pauses pour respecter l’API

    print("Import terminé ! Total :", total_processed)

###############################################################################
# Point d’entrée                                                             ###
###############################################################################

if __name__ == "__main__":
    try:
        fetch_and_load()
    finally:
        conn.close()
