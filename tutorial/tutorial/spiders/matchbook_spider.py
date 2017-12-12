# -*- coding: utf-8 -*-
import os
import scrapy
import pdb

# 一场比赛Item
class matchItem(scrapy.Item):
    eventId = scrapy.Field()    # 比赛唯一ID
    host = scrapy.Field()       # 主队名称
    guest = scrapy.Field()      # 客队名称
    startTime = scrapy.Field()  # 开始时间
    volume = scrapy.Field() # 成交量
    ifRunning = scrapy.Field()  # 是否已开赛
    markets = scrapy.Field()    # 保存多个市场的列表
# 一场比赛的三个市场
# 1.Match odds 胜平负
# 2 Handicap 亚盘
# 3 Total 总进球数
# 因为有可能三个盘口会有重复，所以只记录三个盘口不区分类型
class marketItem(scrapy.Item):
    # For single match market
    market_name = scrapy.Field()
    market_content = scrapy.Field()

class runner_oddsItem(scrapy.Item):
    # For single odds runner market
    host = scrapy.Field()
    guest = scrapy.Field()
    draw = scrapy.Field()

class runner_handicapItem(scrapy.Item):
    # For single handicap runner market
    name = scrapy.Field()
    host = scrapy.Field()
    guest = scrapy.Field()

class runner_totalItem(scrapy.Item):
    # For single total runner market
    name = scrapy.Field()
    over = scrapy.Field()
    under = scrapy.Field()

class priceItem(scrapy.Item):
    # For single price
    side = scrapy.Field()
    odds = scrapy.Field()
    amount = scrapy.Field()

class SoccerSpider(scrapy.Spider):
    name = 'soccer'
    allowed_domains = ['www.matchbook.com']

    def __init__(self, category=None, *args, **kwargs):
        super(SoccerSpider, self).__init__(*args, **kwargs)
        self.start_urls = ['https://www.matchbook.com/edge/rest/events?language=en&currency=USD&exchange-type=back-lay&odds-type=DECIMAL&price-depth=6&price-order=price desc&include-event-participants=true&offset=0&per-page=18&tag-url-names=soccer,%s' %category]

    def parse(self, response):
        
        for eventId in response.xpath('//event'):
            matchId = eventId.extract()  #这场比赛的ID
            match_item = matchItem()
            match_item['eventId'] = eventId.xpath('id/text()').extract()[0]
            name = eventId.xpath('name/text()').extract()[0]
            is_vs_market = 'vs' in name
            if not is_vs_market:
                continue
            match_item['host'] = name.split('vs')[0].strip()
            match_item['guest'] = name.split('vs')[1].strip()
            match_item['startTime'] = eventId.xpath('start/text()').extract()[0]
            match_item['volume'] = float(eventId.xpath('volume/text()').extract()[0])
            match_item['ifRunning'] = eventId.xpath('in-running-flag/text()').extract()[0]
            match_array = []
            market_count = 0
            # 只记录3个市场
            for market in eventId.css('markets market'):
                if market_count >= 3:
                    break
                # pdb.set_trace()
                market_item = marketItem()
                # 保存market 下多个runner的列表
                markets_array = []
                market_name = market.css('market-type::text').extract()[0]  #当前遍历到的market名称
                market_item['market_name'] = market_name
                # 遍历一个市场下的多个选择项
                count = 0 # runner循环计数
                for runner in market.css('runners'):
                    # 遍历一个选择项下的多个交易数据
                    # 不同市场使用不同runner
                    if(market_name == 'one_x_two'):
                        runner_item = runner_oddsItem()
                    elif (market_name == 'handicap'):
                        runner_item = runner_handicapItem()
                        handicap_text = market.css('handicap::text').extract()
                        if len(handicap_text) != 0:
                            runner_item['name'] = handicap_text[0]
                        else:
                            runner_item['name'] = market.css('asian-handicap::text').extract()[0]
                    else:
                        runner_item = runner_totalItem()
                        asian_handicap_text = market.css('asian-handicap::text').extract()
                        if len(asian_handicap_text) != 0:
                            runner_item['name'] = asian_handicap_text[0]
                        else:
                            runner_item['name'] = market.css('handicap::text').extract()[0]

                    # 用数组保存price
                    price_arr = []
                    # 只保存6个价格
                    price_count = 0
                    for price in runner.css('prices'):
                        if price_count >= 6:
                            break
                        price_side = price.css('side::text').extract()[0]
                        if len(price_arr) == 3 and price_side == price_arr[2]['side']:
                            continue
                        price_item = priceItem()
                        price_item['side'] = price_side
                        price_item['odds'] = price.css('odds::text').extract()[0]
                        price_item['amount'] = price.css('available-amount::text').extract()[0]
                        price_arr.append(price_item)
                        price_count += 1

                    if market_name == 'one_x_two':
                        if count == 0:
                            runner_item['host'] = price_arr
                        elif count == 1:
                            runner_item['guest'] = price_arr
                        else:
                            runner_item['draw'] = price_arr
                    elif market_name == 'handicap':
                        if count == 0:
                            runner_item['host'] = price_arr
                        elif count == 1:
                            runner_item['guest'] = price_arr
                    else:
                        if count == 0:
                            runner_item['over'] = price_arr
                        elif count == 1:
                            runner_item['under'] = price_arr
                    count += 1
                    markets_array.append(runner_item)
                market_item['market_content'] = markets_array
                match_array.append(market_item)
                market_count += 1
            match_item['markets'] = match_array
            yield match_item


