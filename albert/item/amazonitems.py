import scrapy.item


class PrecommandesLivresAmazonItem(scrapy.item.Item):
    sku = scrapy.item.Field()
    title = scrapy.item.Field()
    authors = scrapy.item.Field()
    format = scrapy.item.Field()
    availability = scrapy.item.Field()
    cart = scrapy.item.Field()
    image_url = scrapy.item.Field()
    genre = scrapy.item.Field()
    genre_featured_rank = scrapy.item.Field()
