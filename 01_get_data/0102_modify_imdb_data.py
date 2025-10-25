import pandas as pd
from sqlalchemy import create_engine

# Data file paths
paths = (
    "01_get_data/data/title_basics.tsv.gz",
    "01_get_data/data/title_ratings.tsv.gz",
)

# Read DATABASE URL from credentials file
with open("credentials/neon.txt", "r") as f:
    DATABASE_URL = f.read()

# Create database engine
engine = create_engine(DATABASE_URL)

# Load data
df = pd.read_csv(paths[0], sep="\t", compression="gzip", na_values="\\N")
df2 = pd.read_csv(paths[1], sep="\t", compression="gzip", na_values="\\N")

# Select relevant columns
df = df[["tconst", "titleType", "primaryTitle", "startYear", "runtimeMinutes", "genres"]]

# Filter for movies and tvMovies only
df = df[df["titleType"].isin(["movie", "tvMovie"])]

# Rename columns
df = df.rename(columns={
    "tconst": "tconst",
    "titleType": "title_type",
    "primaryTitle": "primary_title",
    "startYear": "start_year",
    "runtimeMinutes": "runtime_minutes",
    "genres": "genres",
})
df2 = df2.rename(columns={
    "tconst": "tconst",
    "averageRating": "average_rating",
    "numVotes": "num_votes",
})

#print len of both dataframes
print(f"Title Basics DataFrame length: {len(df)}")
print(f"Title Ratings DataFrame length: {len(df2)}")

# Inner join dataframes on 'tconst'
df = pd.merge(df, df2, on="tconst", how="inner")

# Store data in the database
df.to_sql("movies", engine, if_exists="replace", index=False)


