#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from abc import ABC
from builtins import str
import json
import re
import urllib.request
import urllib.parse
import urllib.error
import urllib.parse
import abc
from albert.item.amazonitems import PrecommandesLivresAmazonItem
import scrapy.http.request

# from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


def get_url_parameters(url):
    try:
        split_url = urllib.parse.urlsplit(urllib.parse.unquote_plus(url))
    except AttributeError:
        return

    d = {}
    for query_parameter in split_url.query.split("&"):
        try:
            query_parameter_key, query_parameter_value = query_parameter.split("=")
        except ValueError:
            continue

        if query_parameter_key == "rh":
            for x in query_parameter_value.split(","):
                if x not in ["n:301061", "p_6:A1X6FK5RDHNB96"]:
                    try:
                        k, v = x.split(":")
                    except ValueError:
                        continue

                    d[k] = v
        elif query_parameter_key == "pickerToList" and query_parameter_value == "lbr_books_authors_browse-bin":
            d["pickerToList"] = "lbr_books_authors_browse-bin"

    return d


class AmazonPrecommandesLivreSpider(scrapy.Spider, ABC, metaclass=abc.ABCMeta):
    allowed_domains = ["amazon.fr"]
    download_delay = 0.5
    name = "amazon_precommandes_livre"
    handle_httpstatus_list = [404]
    crawlera_enabled = True
    crawlera_apikey = 'ca8e16eeca8e4c9da8dc39383670b2da'
    custom_settings = {
        'CONCURRENT_REQUESTS': 8,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_crawlera.CrawleraMiddleware': 610,
        },
        'DEFAULT_REQUEST_HEADERS': {
            'X-Crawlera-Profile': 'mobile',
        }
    }

    def __init__(self):
        super(AmazonPrecommandesLivreSpider, self).__init__()

        self.browsed_url_parameters = []
        self.number_of_requested_detailed_offer_urls = 0
        self.base_url = 'https://www.amazon.fr'
        self.asin_in_detailed_offer_url_re = re.compile(r'(?<=/dp/)[A-Z0-9]{10}', re.UNICODE)
        self.publication_date_re = re.compile(r'(?:\b\d\d? )?\b\w{3,9} \d{4}\b', re.UNICODE)
        self.format_re = re.compile(r'[()]', re.UNICODE)
        self.html_comment_re = re.compile(r'<!--(.*)-->', re.DOTALL | re.UNICODE)
        # options = webdriver.ChromeOptions()
        # options.add_argument("--disable-extensions")
        # options.add_argument("--headless")
        # options.add_argument("--disable-gpu")
        # options.add_argument("--no-sandbox")
        # self.driver = webdriver.Chrome(chrome_options=options, executable_path='/usr/bin/chromedriver')
        # self.driver.implicitly_wait(60)
        # self.driver = webdriver.Chrome()

    def closed(self, reason):
        self.logger.debug(
            f'number_of_requested_detailed_offer_urls: {str(self.number_of_requested_detailed_offer_urls)}')
        self.logger.debug(len(self.browsed_url_parameters))
        self.logger.debug(self.browsed_url_parameters)

    def construct_url(self, extension):
        return f'{self.base_url}/{extension}'

    def start_requests(self):
        # start_url = "/s?rh=n:301061,p_6:A1X6FK5RDHNB96,p_n_publication_date:183198031&sort=date-desc-rank&unfiltered=1"
        start_url = '/s?rh=n:301061,p_6:A1X6FK5RDHNB96,p_n_publication_date:183198031'
        start_url_parameters = get_url_parameters(start_url)
        self.logger.debug(start_url_parameters)
        if start_url_parameters is not None:
            self.browsed_url_parameters.append(start_url_parameters)
        self.logger.debug(self.browsed_url_parameters)
        url = self.construct_url(start_url)
        yield scrapy.http.request.Request(url, callback=self.parse_root_node)

    def parse_root_node(self, response):
        # Browse the genre nodes
        # selected_urls = response.xpath(
        #     '//span[text()="Livres"]/../../../..//following-sibling::ul//a/@href').getall()[:5]
        # self.logger.debug(len(selected_urls))
        selected_urls = [
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A689215031%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_1',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A4237308031%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_2',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A301144%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_3',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A301133%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_4',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A293136011%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_5',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A8434456031%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_6',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A302050%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_7',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A301147%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_8',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A301985%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_9',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A301135%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_10',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A573312%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_11',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A3961911%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_12',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A301142%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_13',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A301138%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_14',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A689214031%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_15',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A302009%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_16',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A301131%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_17',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A301137%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_18',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A355635011%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_19',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A302004%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_20',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A355636011%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_21',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A301997%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_22',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A302042%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_23',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A301132%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_24',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A301134%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_25',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A12641896031%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_26',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A1381962031%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_27',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A301141%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_28',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A301139%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_29',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A301146%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_30',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A302049%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_31',
            'https://www.amazon.fr/s?i=stripbooks&bbn=301130&rh=n%3A301061%2Cn%3A302051%2Cp_6%3AA1X6FK5RDHNB96%2Cp_n_publication_date%3A183198031&dc&qid=1594244961&rnid=301130&ref=sr_nr_n_32'
        ]
        for selected_url in selected_urls[:5]:
            # url = self.construct_url(selected_url)
            url = selected_url
            yield scrapy.http.request.Request(url, callback=self.parse_genre_node)

    def parse_genre_node(self, response):
        # self.driver.get(response.url)
        # timeout = 5
        # try:
        #     element_present = EC.presence_of_element_located((By.CSS_SELECTOR, 'li.a-last'))
        #     WebDriverWait(self.driver, timeout).until(element_present)
        # except Exception as err:
        #     self.logger.error('Timed out waiting for page to load : {}'.format(err))
        # yield {'driver_step1': 'OK'}
        # Scrape the genre
        # genre = response.xpath(
        #     'normalize-space(//li[span/a/span[normalize-space()="Livres"]]/following-sibling::li/span/span)').get()

        # for result_selector in response.xpath(
        #         '//div[contains(@class, "s-result-list")]/div'):
        #     # Scrape the genre rank
        #     # try:
        #     #     result_id = int(result_selector.xpath('@data-index').get())
        #     #     genre_featured_rank = result_id
        #     # except Exception as e:
        #     #     self.logger.info("Got Exception in result_id conversion" + e.message)
        #     #     genre_featured_rank = 0
        #     # else:
        #     genre_featured_rank = 1
        #
        #     detailed_offer_urls = result_selector.xpath(
        #         './/a[(normalize-space(.)="Broché") or (normalize-space(.)="Relié") or (normalize-space(.)="Tankobon broché") or (normalize-space(.)="Poche")]/@href').getall()
        #     for detailed_offer_url in detailed_offer_urls:
        #         # Dismiss if the ASIN has already been scraped
        #         try:
        #             asin = re.search(self.asin_in_detailed_offer_url_re, detailed_offer_url).group()
        #         except AttributeError:
        #             continue
        #
        #         yield scrapy.http.request.Request(self.construct_url(f'/dp/{asin}'),
        #                                           callback=self.parse_detailed_offer,
        #                                           meta={'genre': genre,
        #                                                 'genre_featured_rank': genre_featured_rank})
        # link = self.driver.find_element_by_link_text('Suivant').get_attribute('href')
        link = response.xpath('//li[@class="a-last"]//a/@href').get()
        if link is None:
            yield {'link': link,
                   'request': response.url}
                   # 'html': response.text}
        else:
            yield scrapy.Request(self.construct_url(link[1:]),
                                 callback=self.parse_genre_node)
        # try:
        #     next_page_url = self.driver.find_elements_by_css_selector('li.a-last').get_attribute('href')
        #     yield {'next page :' : next_page_url}
        #     yield scrapy.Request(next_page_url,
        #                          callback=self.parse_genre_node)
        # except Exception as err:
        #     self.logger.error(err)
        #     self.logger.error('Selenium not working - xpath :')
        #     self.logger.debug('trying substitute to response :')
        #     response.replace(body=self.driver.find_element_by_tag_name("body").get_attribute("innerText"))
        #     yield {
        #         'response': response.xpath('//a[contains(text(), "Suivant")]/@href').get()
        #         'driver': response.body.strip()[:1000]
        #     }
        #     next_page_url = response.xpath('//a[contains(text(), "Suivant")]/@href').get()
        #     yield scrapy.Request(self.construct_url(next_page_url),
        #                          callback=self.parse_genre_node)
        # self.logger.debug(next_page_url.text)
        # self.logger.debug(next_page_url)
        # yield {'driver': self.driver.page_source}
        # if next_page_url is not None:
        #     yield scrapy.Request(self.construct_url(next_page_url),
        #                          callback=self.parse_genre_node)
        # else:
        #     print(f'retrying request {response.request.url}')
        #     yield scrapy.http.request.Request(response.request.url,
        #                                       callback=self.parse_genre_node,
        #                                       meta={'try_number': '1'},
        #                                       dont_filter=True)
        # else:
        #     yield {'body_not_working': response.text,
        #            'genre': genre,
                   # 'url': response.request.url}

    def parse_detailed_offer(self, response):
        self.number_of_requested_detailed_offer_urls += 1

        genre = response.meta["genre"]
        genre_featured_rank = response.meta["genre_featured_rank"]

        item = PrecommandesLivresAmazonItem()

        # Scrape the ASIN
        try:
            asin = re.search(self.asin_in_detailed_offer_url_re, response.request.url).group()
        except AttributeError:
            return
        item["sku"] = asin

        # Scrape title
        title = response.xpath('normalize-space(//span[@id="productTitle"])').get()
        if title:
            item["title"] = title

        # Scrape the authors
        authors = response.xpath('normalize-space(//span[@class="author notFaded"]/a)').get()
        if authors:
            item["authors"] = authors

        title_other_fields = response.xpath(
            '//h1[@id="title"]/span[@class="a-size-medium a-color-secondary a-text-normal"]/text()[1]').getall()

        # Scrape the format
        for title_other_field in title_other_fields:
            if re.search(self.format_re, title_other_field) is None and re.search(self.publication_date_re,
                                                                                  title_other_field) is None:
                item["format"] = title_other_field.strip()
                break

        # Scrape the availability
        availability = response.xpath('normalize-space(//div[@id="availability"]/span)').get()
        if availability:
            item["availability"] = availability

        # Scrape the cart
        cart = response.xpath('//input[@id="add-to-cart-button"][@type="submit"]/@value').get()
        if cart is not None:
            item["cart"] = cart

        # Scrape image URL
        try:
            data_dynamic_image_dict = json.loads(
                response.xpath('//div[@id="imageBlockContainer"]//@data-a-dynamic-image').get())
        except (TypeError, ValueError):
            pass
        else:
            image_urls = list(data_dynamic_image_dict.keys())
            if image_urls:
                for image_url in image_urls:
                    split_image_url = urllib.parse.urlsplit(image_url)
                    split_image_url_path = split_image_url.path
                    if split_image_url_path.count(".") < 2:
                        new_image_url = image_url
                        break
                else:
                    image_url = image_urls[0]
                    split_image_url = urllib.parse.urlsplit(image_url)
                    split_image_url_path = split_image_url.path
                    image_url_path_parts = split_image_url_path.split(".")
                    if len(image_url_path_parts) > 1:
                        image_url_path_parts.pop(-2)
                    image_url_new_path = ".".join(image_url_path_parts)
                    if image_url_new_path not in ["/images/G/08/x-site/icons/no-img-lg.gif",
                                                  "/images/G/08/x-site/icons/no-img-sm.gif"]:
                        new_image_url = urllib.parse.urlunsplit(
                            urllib.parse.SplitResult(split_image_url.scheme, split_image_url.netloc, image_url_new_path,
                                                     split_image_url.query, split_image_url.fragment))

                try:
                    item["image_url"] = new_image_url
                except UnboundLocalError:
                    pass

        if genre:
            item["genre"] = genre

        item["genre_featured_rank"] = genre_featured_rank

        yield item


if __name__ == "__main__":
    pass
