import functools
import socket
import requests
import time
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError


def wait_for_selector_with_retry(page, selector, timeout=30000, retry_interval=5):
    """Retries waiting for a selector with automatic internet checking and infinite wait."""
    while True:
        try:
            return page.wait_for_selector(selector, timeout=timeout)
        except PlaywrightTimeoutError:
            print(f"Timeout waiting for selector: {selector}")
            if not is_internet_available():
                print("Internet lost. Waiting to reconnect before retrying selector...")
                wait_for_internet()
            else:
                print(f"Selector not found. Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)


def is_internet_available(retries=3, initial_timeout=3, max_timeout=15):
    """Checks if internet is available under all conditions with retries and fallback."""
    hosts = ["8.8.8.8", "1.1.1.1"]  # Google and Cloudflare DNS
    port = 53  # DNS port
    timeout = initial_timeout

    for attempt in range(retries):
        for host in hosts:
            try:
                # Set dynamic timeout that increases with retries
                socket.setdefaulttimeout(timeout)
                # Attempt connection to the DNS server
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((host, port))
                sock.close()
                return True
            except (socket.timeout, socket.error):
                continue

        # Fallback to HTTP request if DNS fails after retries
        try:
            response = requests.get("https://www.google.com", timeout=timeout)
            if response.status_code == 200:
                return True
        except (requests.ConnectionError, requests.Timeout):
            pass

        # Increase timeout progressively if connection is slow
        timeout = min(timeout * 2, max_timeout)
        time.sleep(2)  # Pause briefly before retrying

    return False


def wait_for_internet(retry_interval=5):
    """Waits for internet connection."""
    while not is_internet_available():
        print(f"Internet lost! Retrying in {retry_interval} seconds...")
        time.sleep(retry_interval)
    print("Network found!")


def require_internet(func):
    """Decorator to check internet before running a function."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Wait until internet connection is restored before proceeding
        wait_for_internet()
        # Run the function after connection is restored
        return func(*args, **kwargs)

    return wrapper
