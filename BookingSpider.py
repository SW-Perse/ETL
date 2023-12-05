import os
import logging
import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd

df = pd.read_csv(filepath_or_buffer='top_10_temp.csv')

os.chdir("/content")
os.getcwd()

class BookingSpider(scrapy.Spider):
    name = 'booking'
    start_urls = ['https://www.booking.com']
    cities = df['city'].unique()

    def start_requests(self):# allows spider to navigate between cities' URL
        for city in self.cities:
            self.city = city
            search_url = f'https://www.booking.com/search.html?ss={self.city}'

            yield scrapy.Request(url=search_url, callback=self.parse_search_results, cb_kwargs={'city': self.city})

    def parse_search_results(self, response, city):
        hotel_link = response.xpath('//*[@class="a78ca197d0"]/@href')
        items = {}
        
        for url in hotel_link.extract():
            items['city_to_visit'] = city
            yield scrapy.Request(url, callback=self.parse_review, cb_kwargs={'items': items})
    
    def parse_review(self, response, items):
        hotel_name_xpath = response.xpath('//*[@class="d2fee87262 pp-header__title"]/text()')
        hotel_address_xpath = response.xpath('//span[contains(@class, "hp_address_subtitle")]/text()')
        lat_lon_booking_hotel_xpath = response.xpath('//a[@id="hotel_header"]/@data-atlas-latlng')
        hotel_general_review = response.xpath('//*[@class="a3b8729ab1 e6208ee469 cb2cbb3ccb"]/text()')
        hotel_rating = response.xpath('//*[@class="a3b8729ab1 d86cee9b25"]/text()')
        numbers_of_reviews = response.xpath('//*[@class="abf093bdfe f45d8e4c32 d935416c47"]/text()')
        hotel_facilities_xpath = response.xpath('//*[@class="a5a5a75131"]/text()')
        hotel_description_xpath = response.xpath('//*[@class="a53cbfa6de b3efd73f69"]/text()')

        items['hotel_name'] = hotel_name_xpath.get()
        items['hotel_address'] = hotel_address_xpath.get()
        items['lat_lon_hotel'] = lat_lon_booking_hotel_xpath.get()
        items['hotel_general_review'] = hotel_general_review.get()
        items['hotel_rating'] = hotel_rating.get()
        items['numbers_of_reviews'] = numbers_of_reviews.get()
        items['hotel_facilities'] = hotel_facilities_xpath.getall()
        items['hotel_description'] = hotel_description_xpath.get()
        items['url_booking_hotel'] = response.url

        yield items

# Name of the file where the results will be saved
filename = "hotels_info.json"

# If file already exists, delete it before crawling (because Scrapy will 
# concatenate the last and new results otherwise)
if filename in os.listdir('/content/'):
        os.remove('/content/' + filename)

# Declare a new CrawlerProcess with some settings
## USER_AGENT => Simulates a browser on an OS
## LOG_LEVEL => Minimal Level of Log 
## FEEDS => Where the file will be stored 
process = CrawlerProcess(settings = {
    'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 OPR/99.0.0.0',
    'LOG_LEVEL': logging.INFO,
    "FEEDS": {
        '/content/' + filename : {"format": "json"},
    }
})

# Start the crawling using the spider you defined above
process.crawl(BookingSpider)
process.start()