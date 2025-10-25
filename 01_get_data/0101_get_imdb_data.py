import requests

# URLs
title_basics_url = "https://datasets.imdbws.com/title.basics.tsv.gz"
title_ratings_url = "https://datasets.imdbws.com/title.ratings.tsv.gz"
urls = (title_basics_url, title_ratings_url)

# Output paths
title_basics_output_path = "01_get_data/data/title_basics.tsv.gz"
title_ratings_output_path = "01_get_data/data/title_ratings.tsv.gz"
output_paths = (title_basics_output_path, title_ratings_output_path)

for url, output_path in zip(urls, output_paths):
    response = requests.get(url)
    with open(output_path, "wb") as f:
        f.write(response.content)
