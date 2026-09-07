"""
Extracts one book's data from a <article class="product_pod"> element.

Rating is stored as a CSS class, not text -- e.g. class="star-rating Three".
That's a real thing you'll hit constantly in scraping: the data you want is
sometimes encoded in an attribute, not the visible text.
"""
from bs4 import BeautifulSoup

RATING_WORDS = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}


def parse_book(article) -> dict:
    title = article.h3.a["title"]
    price_text = article.find("p", class_="price_color").get_text()  # e.g. "£51.77"
    availability = article.find("p", class_="instock availability").get_text(strip=True)
    rating_classes = article.find("p", class_="star-rating")["class"]  # e.g. ["star-rating", "Three"]
    rating_word = rating_classes[1]

    return {
        "title": title,
        "price_raw": price_text,
        "availability": availability,
        "rating": RATING_WORDS.get(rating_word),  # int, or None if unrecognized
    }


def parse_page(html: str) -> list:
    soup = BeautifulSoup(html, "html.parser")
    return [parse_book(article) for article in soup.find_all("article", class_="product_pod")]


if __name__ == "__main__":
    with open("sample_page.html") as f:
        html = f.read()

    books = parse_page(html)
    print(f"Parsed {len(books)} books from the sample:\n")
    for b in books:
        print(b)
