import scrapy
from datetime import datetime
from pathlib import Path


class CovidStateData(scrapy.Spider):
    name = 'CovidStateDate'

    def start_requests(self):
        with open('full_url.csv') as f_file:
            for line in f_file:
                if not line.strip():
                    continue
                yield scrapy.Request(url=line, callback=self.parse)

    def parse(self, response):
        date_extraction_string = response.xpath('//*[@id="site-dashboard"]//h2/span/text()').get()
        extract = date_extraction_string.split()

        _date = int(extract[3])
        _month = extract[4]
        _year = int(extract[5][0:4])
        _time = extract[6]
        _timezone = extract[7]

        date_string = f'{_date} {_month} {_year}'

        _datetime = str(datetime.strptime(date_string, '%d %B %Y'))
        download_loc = Path.cwd() / 'data/'
        Path(download_loc).mkdir(parents=True, exist_ok=True)

        table_rows = response.xpath('//*[@id="state-data"]//table//tr')

        for row in table_rows[1:]:
            yield {
                'date': _datetime,
                'state': row.xpath('td[2]//text()').get(),
                'active_cases': row.xpath('td[3]//text()').get(),
                # 'total_confirmed_cases': row.xpath('td[3]//text()').get(),
                'cured_discharged_migrated': row.xpath('td[4]//text()').get(),
                'deaths': row.xpath('td[5]//text()').get(),
                'total_confirmed_cases': row.xpath('td[6]//text()').get(),
            }

    custom_settings = {
        'FEED_URI': 'data/state_data_v2.csv',
        'FEED_FORMAT': 'csv',
        'FEED_EXPORTERS': {
            'csv': 'scrapy.exporters.CsvItemExporter',
        },
        'FEED_EXPORT_ENCODING': 'utf-8',
    }
