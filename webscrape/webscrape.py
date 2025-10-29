import re, requests
from bs4 import BeautifulSoup

def subscription_services_from_summary(title: str, country: str = "dk") -> list[str]:

    clean_title = re.sub(r"[^a-zA-Z0-9\s-]", "", title).strip().replace(" ", "-").lower()
    url = f"https://www.justwatch.com/{country}/movie/{clean_title}"
    r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    text = soup.get_text(" ", strip=True)

    # e.g. Currently you are able to watch "Inception" streaming on Sky Go, Now TV Cinema.
    m = re.search(r'Currently you are able to watch\s+"[^"]+"\s+streaming on\s+(.+?)\.', text, flags=re.I)
    if not m:
        return []

    # split on commas and "and"
    parts = [p.strip() for p in re.split(r",| and ", m.group(1))]
    # basic cleanup
    return [p for p in parts if p and p.lower() not in {"subscription", "free", "rent", "buy", "all"}]

#print(subscription_services_from_summary("inception", "us"))
# -> ['Sky Go', 'Now TV Cinema']
