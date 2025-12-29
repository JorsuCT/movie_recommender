from neo4j import GraphDatabase

URI = "bolt://127.0.0.1:7687"
AUTH = ("neo4j", "TSCD2025")

driver = GraphDatabase.driver(URI, auth = AUTH, encrypted = False)

def generar_recomendacion(user_id, movie_id_origen):
    
    """
    Algoritmo: "Usuarios que vieron X, también vieron Y"
    """
    
    print(f"\nAnalizando recomendaciones para Usuario {user_id} basado en la película {movie_id_origen}...")

    query = """
    // 1. Encontrar la película origen
    MATCH (peli_origen:Pelicula {movieId: $movieId})
    
    // 2. Encontrar otros usuarios que la vieron y les gustó (Rating > 3)
    MATCH (peli_origen)<-[r1:VIO]-(otro_usuario:Usuario)
    WHERE r1.rating >= 3.0
    
    // 3. Ver qué OTRAS películas vieron esos usuarios (y les gustaron)
    MATCH (otro_usuario)-[r2:VIO]->(peli_recomendada:Pelicula)
    WHERE peli_recomendada <> peli_origen AND r2.rating >= 3.0
    
    // 4. Contar y ordenar por popularidad
    RETURN peli_recomendada.titulo AS Titulo, 
           peli_recomendada.movieId AS ID, 
           count(otro_usuario) AS Frecuencia,
           avg(r2.rating) AS RatingPromedio
    ORDER BY Frecuencia DESC, RatingPromedio DESC
    LIMIT 5
    """
    
    with driver.session() as session:
        result = session.run(query, movieId = movie_id_origen)
        recomendaciones = list(result)
        
        if not recomendaciones:
            print("No hay suficientes datos aún para recomendar.")
            return

        print(f"RECOMENDACIONES TOP 5:")
        for idx, record in enumerate(recomendaciones, 1):
            titulo = record["Titulo"] or "Sin Título"
            print(f"{idx}. {titulo} (Coincidencias: {record['Frecuencia']})")

if __name__ == "__main__":
    try:
        generar_recomendacion(user_id=123, movie_id_origen=1)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        driver.close()