import requests
from bs4 import BeautifulSoup
import pandas as pd 
# install these library using terminal
# python3.11 -m pip install pandas
# python3.11 -m pip install bs4

res = requests.get('https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films')
# print(res.text)

soup = BeautifulSoup(res.text, 'html.parser')

elements = soup.select('#mw-content-text > div > div > div.mw-parser-output > table:nth-child(5) > tbody > tr')
# print(elements)

# ######################################
# td_tags = elements[0].find_all('td')
# # print(td_tags)

# rank = td_tags[0].get_text(strip=True)
# film = td_tags[1].get_text(strip=True)
# year = td_tags[2].get_text(strip=True)

# ######################################

elements_50 = elements[:51]

temp = []

for elements in elements_50:
    td_tags = elements.find_all('td')
    if len(td_tags) >= 8:
        rank = td_tags[0].get_text(strip=True)
        film = td_tags[1].get_text(strip=True)
        year = td_tags[2].get_text(strip=True)

        temp.append([rank, film, year])
    else:
        continue

df = pd.DataFrame(temp, columns=['Rank', 'Title', 'Year'])

print(df)
