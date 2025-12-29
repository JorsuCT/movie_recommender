import pandas as pd
import ast
from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "TSCD2025")
PATH_CSV = "./data/" 

class MovieGraphLoader:
    def __init__(self):
        self.driver = GraphDatabase.driver(URI, auth=AUTH)

    def close(self):
        self.driver.close()

    def clean_json_field(self, x, key_name):
        
        """Convierte strings "[{'name': 'X'}]" a listas Python ['X']"""

        try:
            if pd.isna(x): return []
            eval_x = ast.literal_eval(x)
        
            if isinstance(eval_x, list):
                return [i[key_name] for i in eval_x]
            return []
        
        except:
            return []

    def get_director(self, x):
        
        """Extrae solo el director del JSON de crew"""
        
        try:
            if pd.isna(x): return None
        
            for i in ast.literal_eval(x):
                if i['job'] == 'Director':
                    return i['name']
            return None
        
        except:
            return None

    def crear_indices(self):
        
        """Crea índices para que la inserción no sea eterna"""
        
        print("Creando índices y constraints")
        
        queries = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (m:Pelicula) REQUIRE m.movieId IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (u:Usuario) REQUIRE u.userId IS UNIQUE",
            "CREATE INDEX IF NOT EXISTS FOR (d:Director) ON (d.nombre)",
            "CREATE INDEX IF NOT EXISTS FOR (g:Genero) ON (g.nombre)",
            "CREATE INDEX IF NOT EXISTS FOR (k:Keyword) ON (k.nombre)"
        ]
        
        with self.driver.session() as session:
            for q in queries:
                session.run(q)

    def cargar_peliculas_metadata(self):
        
        print("--- Procesando Metadata, Credits y Keywords ---")
        
        meta = pd.read_csv(f'{PATH_CSV}movies_metadata.csv', low_memory = False, on_bad_lines = 'skip')
        links = pd.read_csv(f'{PATH_CSV}links.csv')
        credits = pd.read_csv(f'{PATH_CSV}credits.csv')
        keywords = pd.read_csv(f'{PATH_CSV}keywords.csv')

        meta = meta[pd.to_numeric(meta['id'], errors = 'coerce').notnull()]
        meta['id'] = meta['id'].astype(int)
        
        links = links.dropna(subset = ['tmdbId', 'movieId'])
        links['tmdbId'] = links['tmdbId'].astype(int)
        links['movieId'] = links['movieId'].astype(int)

        df = pd.merge(links, meta[['id', 'title', 'genres', 'vote_average']], left_on = 'tmdbId', right_on = 'id')
        
        credits['id'] = pd.to_numeric(credits['id'], errors = 'coerce')
        keywords['id'] = pd.to_numeric(keywords['id'], errors = 'coerce')
        
        df = df.merge(credits[['id', 'crew']], on = 'id', how = 'left')
        df = df.merge(keywords[['id', 'keywords']], on = 'id', how = 'left')

        print(f"Total películas a cargar: {len(df)}")

        query_movie = """
        MERGE (m:Pelicula {movieId: $movieId})
        SET m.tmdbId = $tmdbId, 
            m.titulo = $titulo,
            m.rating_promedio = $vote_average
        """
        
        query_details = """
        MATCH (m:Pelicula {movieId: $movieId})
        
        // Director
        FOREACH (dirName IN CASE WHEN $director IS NOT NULL THEN [$director] ELSE [] END |
            MERGE (d:Director {nombre: dirName})
            MERGE (m)-[:DIRIGIDA_POR]->(d)
        )
        
        // Géneros
        FOREACH (genName IN $genres |
            MERGE (g:Genero {nombre: genName})
            MERGE (m)-[:PERTENECE_A]->(g)
        )

        // Keywords (Opcional: Limitar a 5 para no saturar)
        FOREACH (keyName IN $keywords |
            MERGE (k:Keyword {nombre: keyName})
            MERGE (m)-[:TIENE_TEMA]->(k)
        )
        """

        with self.driver.session() as session:
            count = 0

            for _, row in df.iterrows():
                director = self.get_director(row['crew'])
                generos = self.clean_json_field(row['genres'], 'name')
                keys = self.clean_json_field(row['keywords'], 'name')

                session.run(query_movie, 
                            movieId = row['movieId'], 
                            tmdbId = row['tmdbId'], 
                            titulo = row['title'],
                            vote_average = row['vote_average'])
                
                session.run(query_details, 
                            movieId = row['movieId'], 
                            director = director,
                            genres = generos,
                            keywords = keys)
                
                count += 1
                if count % 100 == 0: print(f"Procesadas {count} películas")

    def cargar_ratings(self):

        print("--- Procesando Ratings (Usuarios) ---")
        ratings = pd.read_csv(f'{PATH_CSV}ratings_small.csv')

        print(f"Total ratings a cargar: {len(ratings)}")
        
        query = """
        MATCH (m:Pelicula {movieId: $movieId})
        MERGE (u:Usuario {userId: $userId})
        MERGE (u)-[r:VIO]->(m)
        SET r.rating = $rating, r.timestamp = $timestamp
        """
        
        with self.driver.session() as session:
            count = 0
            for _, row in ratings.iterrows():
                session.run(query, 
                            userId = int(row['userId']), 
                            movieId = int(row['movieId']), 
                            rating = float(row['rating']),
                            timestamp = int(row['timestamp']))
                count += 1
                if count % 1000 == 0: print(f"Cargados {count} ratings")

if __name__ == "__main__":
    loader = MovieGraphLoader()
    try:
        loader.crear_indices()
        loader.cargar_peliculas_metadata()
        loader.cargar_ratings()
        print("El grafo está listo.")
    finally:
        loader.close()
