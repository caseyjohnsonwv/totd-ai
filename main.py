import calendar
from datetime import datetime
import requests


def main():
    data_rows = []

    for months_back in range(14):
        url = f"https://trackmania.io/api/totd/{months_back}"
        headers = {'User-Agent' : 'TOTD-AI-Style-Prediction'}
        resp = requests.get(url, headers=headers)
        j = resp.json()

        month, year = j['month'], j['year']
        for totd in j['days']:
            uploaded_ts = datetime.fromisoformat(totd['map']['timestamp'])
            # csv data below
            totd_year = year
            totd_month = month
            totd_day = totd['monthday']
            totd_day_of_week = calendar.day_name[totd['weekday']]
            map_id = totd['map']['mapId']
            author_name = totd['map']['authorplayer']['name']
            author_id = totd['map']['authorplayer']['id']
            author_time = totd['map']['authorScore'] / 1000
            gold_time = totd['map']['goldScore'] / 1000
            silver_time = totd['map']['silverScore'] / 1000
            bronze_time = totd['map']['bronzeScore'] / 1000
            submitter_name = totd['map']['submitterplayer']['name']
            submitter_id = totd['map']['submitterplayer']['id']
            uploaded_year = uploaded_ts.year
            uploaded_month = uploaded_ts.month
            uploaded_day = uploaded_ts.day
            uploaded_day_of_week = calendar.day_name[uploaded_ts.weekday()]
            exchange_id = totd['map']['exchangeid']
            data_rows.append([
                totd_year, totd_month, totd_day, totd_day_of_week,
                map_id, author_name, author_id,
                author_time, gold_time, silver_time, bronze_time,
                submitter_name, submitter_id,
                uploaded_year, uploaded_month, uploaded_day, uploaded_day_of_week,
                exchange_id
            ])
            print(data_rows[-1])


if __name__ == '__main__':
    main()