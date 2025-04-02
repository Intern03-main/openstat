from utils import is_internet_available
from scraper import scrape_all


if __name__ == "__main__":
    if is_internet_available():
        scrape_all()
    else:
        print("No internet connection found.\nExiting...")