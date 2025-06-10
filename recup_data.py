#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script d’import des données statiques “écoulement/stations” depuis l’API Hub’Eau
→ SQLite (database.db) en respectant le schéma ER : region, departement, commune,
  bassin, cours_eau, station.
Prérequis :
  - database.db contient déjà les tables créées selon le modèle ER défini :
      • region(code_region PK, libelle_region)
      • departement(code_departement PK, libelle_departement, code_region FK)
      • commune(code_commune PK, libelle_commune, code_departement FK)
      • bassin(code_bassin PK, libelle_bassin)
      • cours_eau(code_cours_eau PK, libelle_cours_eau, uri_cours_eau)
      • station(
            id_station     INTEGER PK AUTOINCREMENT,
            code_station   TEXT UNIQUE NOT NULL,
            libelle_station TEXT NOT NULL,
            uri_station    TEXT,
            etat_station   TEXT,
            date_maj_station TEXT,
            latitude       REAL,
            longitude      REAL,
            coordonnee_x   REAL,
            coordonnee_y   REAL,
            code_region       TEXT NOT NULL REFERENCES region(code_region),
            code_departement  TEXT NOT NULL REFERENCES departement(code_departement),
            code_commune      TEXT NOT NULL REFERENCES commune(code_commune),
            code_bassin       TEXT NOT NULL REFERENCES bassin(code_bassin),
            code_cours_eau    TEXT NOT NULL REFERENCES cours_eau(code_cours_eau)
        )
"""

import requests
import sqlite3
import time
import sys

# -------------------------------------------------------------------
# 1. Configuration de l’API et du chemin de la base SQLite
# -------------------------------------------------------------------
BASE_URL    = "https://hubeau.eaufrance.fr/api/v1/ecoulement/stations"
PAGE_SIZE   = 500  # nombre d’enregistrements renvoyés par page (max 500)
DB_PATH     = "database.db"

# Champs à récupérer depuis l’API (seulement les champs dont on a besoin)
FIELDS = [
    "code_region", "libelle_region",
    "code_departement", "libelle_departement",
    "code_commune", "libelle_commune",
    "code_bassin", "libelle_bassin",
    "code_cours_eau", "libelle_cours_eau", "uri_cours_eau",
    "code_station", "libelle_station", "uri_station",
    "etat_station", "date_maj_station",
    "latitude", "longitude",
    "coordonnee_x", "coordonnee_y"
]
FIELDS_PARAM = ",".join(FIELDS)

# -------------------------------------------------------------------
# 2. Connexion SQLite
# -------------------------------------------------------------------
try:
    conn = sqlite3.connect(DB_PATH)
except sqlite3.Error as e:
    print(f"Impossible d’ouvrir la base SQLite {DB_PATH} : {e}")
    sys.exit(1)

# Activer la vérification des clés étrangères (SQLite)
conn.execute("PRAGMA foreign_keys = ON;")
cur = conn.cursor()

# -------------------------------------------------------------------
# 3. Fonctions d’insertion “INSERT OR IGNORE” pour chaque table
# -------------------------------------------------------------------
def insert_region(code_region: str, libelle_region: str):
    """Insère ou ignore une région."""
    if not code_region or not libelle_region:
        return
    cur.execute("""
        INSERT OR IGNORE INTO region (code_region, libelle_region)
        VALUES (?, ?)
    """, (code_region, libelle_region))


def insert_departement(code_departement: str, libelle_departement: str, code_region: str):
    """Insère ou ignore un département."""
    if not code_departement or not code_region:
        return
    cur.execute("""
        INSERT OR IGNORE INTO departement (code_departement, libelle_departement, code_region)
        VALUES (?, ?, ?)
    """, (code_departement, libelle_departement, code_region))


def insert_commune(code_commune: str, libelle_commune: str, code_departement: str):
    """Insère ou ignore une commune."""
    if not code_commune or not code_departement:
        return
    cur.execute("""
        INSERT OR IGNORE INTO commune (code_commune, libelle_commune, code_departement)
        VALUES (?, ?, ?)
    """, (code_commune, libelle_commune, code_departement))


def insert_bassin(code_bassin: str, libelle_bassin: str):
    """Insère ou ignore un bassin."""
    if not code_bassin or not libelle_bassin:
        return
    cur.execute("""
        INSERT OR IGNORE INTO bassin (code_bassin, libelle_bassin)
        VALUES (?, ?)
    """, (code_bassin, libelle_bassin))


def insert_cours_eau(code_cours_eau: str, libelle_cours_eau: str, uri_cours_eau: str):
    """Insère ou ignore un cours d’eau."""
    if not code_cours_eau or not libelle_cours_eau:
        return
    cur.execute("""
        INSERT OR IGNORE INTO cours_eau (code_cours_eau, libelle_cours_eau, uri_cours_eau)
        VALUES (?, ?, ?)
    """, (code_cours_eau, libelle_cours_eau, uri_cours_eau))


def insert_station(
    code_station: str,
    libelle_station: str,
    uri_station: str,
    etat_station: str,
    date_maj_station: str,
    latitude,
    longitude,
    coordonnee_x,
    coordonnee_y,
    code_region: str,
    code_departement: str,
    code_commune: str,
    code_bassin: str,
    code_cours_eau: str
):
    """Insère ou ignore une station, après avoir vérifié toutes les clés étrangères."""
    # Vérifier la présence des clés étrangères
    missing = []
    if not code_region:
        missing.append("code_region")
    if not code_departement:
        missing.append("code_departement")
    if not code_commune:
        missing.append("code_commune")
    if not code_bassin:
        missing.append("code_bassin")
    if not code_cours_eau:
        missing.append("code_cours_eau")

    if missing:
        print(f"⚠ IGNORE station « {code_station} »: clés manquantes {missing}")
        return

    try:
        cur.execute("""
            INSERT OR IGNORE INTO station (
                code_station, libelle_station, uri_station,
                etat_station, date_maj_station,
                latitude, longitude, coordonnee_x, coordonnee_y,
                code_region, code_departement, code_commune,
                code_bassin, code_cours_eau
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            code_station, libelle_station, uri_station,
            etat_station, date_maj_station,
            latitude, longitude, coordonnee_x, coordonnee_y,
            code_region, code_departement, code_commune,
            code_bassin, code_cours_eau
        ))
    except sqlite3.IntegrityError as e:
        print(f" Erreur d’intégrité pour station « {code_station} »: {e}")


# -------------------------------------------------------------------
# 4. Boucle de pagination et import
# -------------------------------------------------------------------
page = 1
total_processed = 0

while True:
    params = {
        "size": PAGE_SIZE,
        "page": page,
        "fields": FIELDS_PARAM
    }
    print(f"> Appel API Hub’Eau : page {page} …", end=" ")
    try:
        resp = requests.get(BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"\nErreur HTTP : {e}")
        break

    payload = resp.json()
    data = payload.get("data", [])
    if not data:
        print("Aucune donnée, fin de la pagination.")
        break

    print(f"{len(data)} enregistrements reçus.")
    for rec in data:
        # Extraire et nettoyer les champs (remplacer "" par None)
        code_region        = rec.get("code_region")        or None
        libelle_region     = rec.get("libelle_region")     or None
        code_departement   = rec.get("code_departement")   or None
        libelle_departement= rec.get("libelle_departement")or None
        code_commune       = rec.get("code_commune")       or None
        libelle_commune    = rec.get("libelle_commune")    or None
        code_bassin        = rec.get("code_bassin")        or None
        libelle_bassin     = rec.get("libelle_bassin")     or None
        code_cours_eau     = rec.get("code_cours_eau")     or None
        libelle_cours_eau  = rec.get("libelle_cours_eau")  or None
        uri_cours_eau      = rec.get("uri_cours_eau")      or None

        code_station       = rec.get("code_station")       or None
        libelle_station    = rec.get("libelle_station")    or None
        uri_station        = rec.get("uri_station")        or None
        etat_station       = rec.get("etat_station")       or None
        date_maj_station   = rec.get("date_maj_station")   or None

        latitude           = rec.get("latitude")
        longitude          = rec.get("longitude")
        coordonnee_x       = rec.get("coordonnee_x")
        coordonnee_y       = rec.get("coordonnee_y")

        # 1) Insérer les tables référentielles (parent → enfant)
        insert_region(code_region, libelle_region)
        insert_departement(code_departement, libelle_departement, code_region)
        insert_commune(code_commune, libelle_commune, code_departement)
        insert_bassin(code_bassin, libelle_bassin)
        insert_cours_eau(code_cours_eau, libelle_cours_eau, uri_cours_eau)

        # 2) Commit intermédiaire pour garantir l’existence des FK
        conn.commit()

        # 3) Debug : afficher la station avant insertion
        print(f"  • Station {code_station} "
              f"(région={code_region}, dept={code_departement}, comm={code_commune}, "
              f"bassin={code_bassin}, cours_eau={code_cours_eau})")

        # 4) Insérer la station
        insert_station(
            code_station, libelle_station, uri_station,
            etat_station, date_maj_station,
            latitude, longitude, coordonnee_x, coordonnee_y,
            code_region, code_departement, code_commune,
            code_bassin, code_cours_eau
        )
        total_processed += 1

    # Commit de fin de page
    conn.commit()
    print(f"→ Page {page} complétée : {len(data)} enregistrements traités (cumul : {total_processed}).")

    # Si on a reçu moins que PAGE_SIZE, c’est qu’on est à la dernière page
    if len(data) < PAGE_SIZE:
        print("→ Dernière page atteinte, arrêt.")
        break

    page += 1
    time.sleep(0.2)  # pause courte pour ne pas surcharger l’API

# -------------------------------------------------------------------
# 5. Clôture de la connexion
# -------------------------------------------------------------------
print(f"Import terminé : {total_processed} stations traitées.")
conn.close()
