#!/usr/bin/env python3
"""test_sa204.py — Tests rapides pour valider la base statique SAE 2.04

Usage :
    python test_sa204.py [chemin_vers_ecoulement.db]

Le script vérifie :
  1. Activation des clés étrangères.
  2. `PRAGMA foreign_key_check` ne retourne aucune violation.
  3. Chaque table de référence contient au moins 1 ligne.
  4. Exemple de jointure région → station fonctionne.

Il sort avec code 0 si tout est OK, sinon 1.
"""

import sqlite3
import sys
from pathlib import Path

MIN_ROWS = {
    "region": 1,
    "departement": 1,
    "commune": 1,
    "bassin": 1,
    "cours_eau": 1,
    "station": 1,
}

DB_PATH = Path(sys.argv[1] if len(sys.argv) > 1 else "ecoulement.db")
if not DB_PATH.exists():
    sys.stderr.write(f"❌ Fichier {DB_PATH} introuvable\n")
    sys.exit(1)

conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA foreign_keys = ON;")
cur = conn.cursor()

print("→ Clés étrangères ACTIVÉES (PRAGMA foreign_keys = ON)")

# 1) Vérifier l’intégrité référentielle
fk_violations = list(cur.execute("PRAGMA foreign_key_check;").fetchall())
if fk_violations:
    sys.stderr.write("❌ Violations de clés étrangères détectées :\n")
    for row in fk_violations:
        sys.stderr.write(f"  {row}\n")
    sys.exit(1)
print("✓ Aucune violation de FK")

# 2) Vérifier qu’il y a des données dans chaque table
for table, min_rows in MIN_ROWS.items():
    count = cur.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
    if count < min_rows:
        sys.stderr.write(f"❌ Table {table} contient {count} ligne(s), attendu ≥ {min_rows}\n")
        sys.exit(1)
    print(f"✓ {table}: {count} ligne(s)")

# 3) Exemple de requête : nombre de stations par région
print("→ Exemple : 5 premiers décomptes stations par région")
for code_reg, nb in cur.execute(
    """
    SELECT d.code_region, COUNT(*)
      FROM station s
      JOIN commune c ON s.code_commune = c.code_commune
      JOIN departement d ON c.code_departement = d.code_departement
     GROUP BY d.code_region
     ORDER BY COUNT(*) DESC
     LIMIT 5;
    """
):
    print(f"  Région {code_reg}: {nb} station(s)")

print("\n🎉  Tous les tests SAE 2.04 sont PASSÉS")
sys.exit(0)
