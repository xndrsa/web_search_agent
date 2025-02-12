import requests
from bs4 import BeautifulSoup
import re


def get_proxy_list():
    url = "https://free-proxy-list.net/"


    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')

    table = soup.find('table', {'class': 'table table-striped table-bordered'})

    if not table:
        raise ValueError("Not found.")

    rows = table.find_all('tr')[1:]

    filtered_proxies = []

    for row in rows[:10]:
        cols = row.find_all('td')
        if len(cols) >= 7:
            ip = cols[0].text.strip()
            port = int(cols[1].text.strip())
            country = cols[3].text.strip()

            filtered_proxies.append({
                        'ip': ip,
                        'port': port,
                        'country': country
                    })

    return filtered_proxies

