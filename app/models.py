"""Data access layer using SQLite."""

import sqlite3
from typing import List, Tuple, Dict


class Database:
    def __init__(self, path: str):
        self.path = path

    def query(self, sql: str, params: Tuple = ()) -> List[Tuple]:
        with sqlite3.connect(self.path) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.execute(sql, params)
            return cur.fetchall()

    def get_regions(self) -> List[sqlite3.Row]:
        return self.query("SELECT code_region, libelle_region FROM region ORDER BY libelle_region")

    def get_departements_by_region(self, code_region: str) -> List[sqlite3.Row]:
        return self.query(
            "SELECT code_departement, libelle_departement FROM departement WHERE code_region=? ORDER BY libelle_departement",
            (code_region,),
        )

    def get_communes_by_departement(self, code_dep: str, limit: int = 20) -> List[sqlite3.Row]:
        return self.query(
            "SELECT code_commune, libelle_commune FROM commune WHERE code_departement=? ORDER BY libelle_commune LIMIT ?",
            (code_dep, limit),
        )
    def get_stations_by_commune(self, code_commune: str, limit: int = 20) -> List[sqlite3.Row]:
        return self.query(
            """
            SELECT code_station, libelle_station
              FROM station
             WHERE code_commune=?
             ORDER BY libelle_station
             LIMIT ?
            """,
            (code_commune, limit),
        )

    def get_station(self, code_station: str) -> sqlite3.Row | None:
        rows = self.query(
            """
            SELECT s.code_station, s.libelle_station, s.uri_station,
                   s.etat_station, s.latitude, s.longitude,
                   c.libelle_commune
              FROM station s
              JOIN commune c ON s.code_commune = c.code_commune
             WHERE s.code_station=?
            """,
            (code_station,),
        )
        return rows[0] if rows else None
