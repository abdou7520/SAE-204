import sqlite3

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path

    def get_all_stations(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        query = """
        SELECT
            s.code_station,
            s.libelle_station,
            s.latitude,
            s.longitude,
            c.libelle_commune,
            d.libelle_departement,
            r.libelle_region,
            ce.libelle_cours_eau
        FROM station s
        JOIN commune c ON s.code_commune = c.code_commune
        JOIN departement d ON c.code_departement = d.code_departement
        JOIN region r ON d.code_region = r.code_region
        JOIN cours_eau ce ON s.code_cours_eau = ce.code_cours_eau
        """
        cursor.execute(query)

        stations = []
        for row in cursor.fetchall():
            r = dict(row)
            r["code_station"] = r["code_station"].replace(" ", "")  # nettoyage
            stations.append(r)

        conn.close()
        return stations

    def get_station_by_code(self, code_station):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        query = """
        SELECT
            s.code_station,
            s.libelle_station,
            s.latitude,
            s.longitude,
            c.libelle_commune,
            d.libelle_departement,
            r.libelle_region,
            ce.libelle_cours_eau
        FROM station s
        JOIN commune c ON s.code_commune = c.code_commune
        JOIN departement d ON c.code_departement = d.code_departement
        JOIN region r ON d.code_region = r.code_region
        JOIN cours_eau ce ON s.code_cours_eau = ce.code_cours_eau
        WHERE REPLACE(s.code_station, ' ', '') = ?
        """
        cursor.execute(query, (code_station.replace(" ", ""),))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
