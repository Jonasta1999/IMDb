from sqlalchemy import create_engine, text
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import random
from dotenv import load_dotenv
import os
from webscrape.webscrape import subscription_services_from_summary
load_dotenv()

# python -m uvicorn api.api_app:app --reload --port 8000

# Load database URL from environment variable
DATABASE_URL = os.getenv("DATABASE_URL")

# Create database engine
engine = create_engine(DATABASE_URL)

# Create FastAPI app
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Define API endpoint to get movies with filters
@app.get("/movies")
def get_title(
    tconst: str = None,
    primary_title: str = None,
    start_year: int = None,
    end_year: int = None,
    runtime_minutes: int = None,
    genres: str = None,
    apply_all_genres: bool = False,
    average_rating_min: float = None,
    average_rating_max: float = None,
    num_votes: int = None,
    limit: int = None,
):
    # Prepare genre filtering
    genres = genres.split(",") if genres else []
    genre_filter = ""
    params = {
        "tconst": tconst,
        "primary_title": f"%{primary_title}%",
        "start_year": start_year,
        "end_year": end_year,
        "runtime_minutes": runtime_minutes,
        "average_rating_min": average_rating_min,
        "average_rating_max": average_rating_max,
        "num_votes": num_votes,
        "limit": limit,
    }
    # Build genre filter clause
    if genres:
        genre_clauses = []
        for i, g in enumerate(genres):
            key = f"genre{i}"
            genre_clauses.append(f"genres ILIKE :{key}")
            params[key] = f"%{g}%"
        if apply_all_genres:
            genre_filter = "AND " + " AND ".join(genre_clauses)
        else:
            genre_filter = " AND (" + " OR ".join(genre_clauses) + ")"

    # Execute query
    with engine.connect() as connection:       
        sql = f"""
        SELECT 
            tconst,
            primary_title,
            start_year,
            runtime_minutes,
            genres,
            average_rating,
            num_votes
        FROM movies
        WHERE 1=1
        {"AND tconst = :tconst" if tconst else ""}
        {"AND primary_title ILIKE :primary_title" if primary_title else ""}
        {"AND start_year >= :start_year" if start_year else ""}
        {"AND start_year <= :end_year" if end_year else ""}
        {"AND runtime_minutes = :runtime_minutes" if runtime_minutes else ""}
        {genre_filter}
        {"AND average_rating >= :average_rating_min" if average_rating_min else ""}
        {"AND average_rating <= :average_rating_max" if average_rating_max else ""}
        {"AND num_votes >= :num_votes" if num_votes else ""}
        ORDER BY average_rating DESC
        """
        
        sql = text(sql)
        result = connection.execute(sql, params).mappings().all()
        movies = [dict(row) for row in result]

        # Get limit random num between 0 and len(movies)-1
        if len(movies) > limit:
            indices = random.sample(range(len(movies)), limit)
            indices = sorted(indices)
        else:
            indices = range(len(movies))
        movies = [movies[i] for i in indices]

        return {"count": len(movies), "movies": movies}


def normalize_genre(g: str) -> str:
    g = g.strip().strip('"').strip("'")
    if not g: return ""
    # Consistent title-case like IMDb’s canonical names
    return g[0].upper() + g[1:].lower()

# Define API endpoint to get distinct genres
@app.get("/genres")
def get_genres():
    sql = text("""
        SELECT DISTINCT TRIM(BOTH ' ' FROM unnest(string_to_array(genres, ','))) AS genre
        FROM movies
        WHERE genres IS NOT NULL AND genres <> ''
        ORDER BY genre
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).all()
    return [r[0] for r in rows if r[0]]

@app.get("/streaming")
def get_streaming_services(title: str, country: str = "dk"):
    country = country.lower()
    services = subscription_services_from_summary(title, country)
    return {"services": services}
