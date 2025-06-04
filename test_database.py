#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_database.py

Vérifie l’intégrité d’une base SQLite (database.db) :
- PRAGMA integrity_check
- PRAGMA foreign_key_check
"""

import sqlite3
import sys

DB_PATH = "database.db"

# 1. Ouvrir la connexion à la base SQLite
try:
    conn = sqlite3.connect(DB_PATH)
except sqlite3.Error as e:
    print(f"Erreur : impossible d’ouvrir la base '{DB_PATH}' → {e}")
    sys.exit(1)

# 2. Activer la vérification des clés étrangères (utile si PRAGMA foreign_key_check)
conn.execute("PRAGMA foreign_keys = ON;")
cur = conn.cursor()

# 3. Vérification d’intégrité générale
cur.execute("PRAGMA integrity_check;")
result_integrity = cur.fetchone()[0]  # ‘ok’ si tout est bon
print("integrity_check:", result_integrity)

# 4. Vérification des clés étrangères (aucun résultat attendu si tout est cohérent)
cur.execute("PRAGMA foreign_key_check;")
fk_errors = cur.fetchall()  # liste vide si pas d’erreur
print("foreign_key_check:", fk_errors)

# 5. (Optionnel) Comptage des lignes pour s’assurer des volumes
tables = ["region", "departement", "commune", "bassin", "cours_eau", "station"]
for tbl in tables:
    try:
        cur.execute(f"SELECT COUNT(*) FROM {tbl};")
        count = cur.fetchone()[0]
        print(f"{tbl}: {count} lignes")
    except sqlite3.Error:
        print(f"{tbl}: table introuvable ou autre erreur")

# 6. Fermer la connexion
conn.close()
