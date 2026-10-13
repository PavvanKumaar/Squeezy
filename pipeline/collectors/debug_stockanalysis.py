
import requests
from bs4 import BeautifulSoup
import re

url = "https://stockanalysis.com/stocks/gme/statistics/"
headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

try:
    response = requests.get(url, headers=headers, timeout=20)

    print("Status:", response.status_code)
    print("Final URL:", response.url)
    print("Content-Type:", response.headers.get("Content-Type"))
    print("HTML length:", len(response.text))

    soup = BeautifulSoup(response.text, "html.parser")
    print("Page title:", soup.title.get_text(strip=True) if soup.title else None)

    # Check whether the response contains relevant terms.
    for term in ("short interest", "short ratio", "shortPercentOfFloat"):
        matches = list(re.finditer(term, response.text, re.I))
        print(f"\n{term}: {len(matches)} occurrence(s)")

        for match in matches[:3]:
            start = max(0, match.start() - 150)
            end = min(len(response.text), match.end() + 250)
            print(response.text[start:end].replace("\n", " ")[:500])

    # Check for embedded JSON data.
    print("\nJSON script tags:")
    for script in soup.find_all("script", type="application/ld+json"):
        print(script.get_text()[:500])

except requests.RequestException as exc:
    print("Request failed:", repr(exc))
