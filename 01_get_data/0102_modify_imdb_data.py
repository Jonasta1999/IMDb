import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv()

# Data file paths
paths = (
    "01_get_data/data/title_basics.tsv.gz",
    "01_get_data/data/title_ratings.tsv.gz",
)

# Load database URL from environment variable
DATABASE_URL = os.getenv("DATABASE_URL")

# Create database engine
engine = create_engine(DATABASE_URL)

# Load data
df = pd.read_csv(paths[0], sep="\t", compression="gzip", na_values="\\N")
df2 = pd.read_csv(paths[1], sep="\t", compression="gzip", na_values="\\N")

# Select relevant columns
df = df[
    ["tconst", "titleType", "primaryTitle", "startYear", "runtimeMinutes", "genres"]
]

# Filter for movies and tvMovies only
df = df[df["titleType"].isin(["movie", "tvMovie"])]

# Rename columns
df = df.rename(
    columns={
        "tconst": "tconst",
        "titleType": "title_type",
        "primaryTitle": "primary_title",
        "startYear": "start_year",
        "runtimeMinutes": "runtime_minutes",
        "genres": "genres",
    }
)
df2 = df2.rename(
    columns={
        "tconst": "tconst",
        "averageRating": "average_rating",
        "numVotes": "num_votes",
    }
)

# Inner join dataframes on 'tconst'
df = pd.merge(df, df2, on="tconst", how="inner")

# Add run_time column
df["run_time"] = datetime.now()

# Store data in the database
df.to_sql("movies", engine, if_exists="replace", index=False)
