# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymysql.cursors
import datetime
import logging
import pdb

now_time = datetime.datetime.now().strftime('%Y%m%d_%H%M')

class TutorialPipeline(object):

    def process_item(self, item, spider):
        # pdb.set_trace()  # 运行到这里会自动暂停
        # 获取当前联赛名称
        current_league = spider.start_urls[0].split(',' )[-1]

        # 处理数据
        # 保存host_win_back_price
        host_win_back_price_list = [0.0,0.0,0.0]
        # 保存host_win_back_price_amount
        host_win_back_price_amount_list = [0.0,0.0,0.0]
        # 保存host_win_lay_price
        host_win_lay_price_list = [0.0,0.0,0.0]
        # 保存host_win_lay_price_amount
        host_win_lay_price_amount_list = [0.0,0.0,0.0]
        # 保存guest_win_back_price
        guest_win_back_price_list = [0.0,0.0,0.0]
        # 保存guest_win_back_price_amount
        guest_win_back_price_amount_list = [0.0,0.0,0.0]
        # 保存guest_win_lay_price
        guest_win_lay_price_list = [0.0,0.0,0.0]
        # 保存guest_win_lay_price_amount
        guest_win_lay_price_amount_list = [0.0,0.0,0.0]
        # 保存draw_back_price
        draw_back_price_list = [0.0,0.0,0.0]
        # 保存draw_back_price_amount
        draw_back_price_amount_list = [0.0,0.0,0.0]
        # 保存draw_lay_price
        draw_lay_price_list = [0.0,0.0,0.0]
        # 保存draw_lay_price_amount
        draw_lay_price_amount_list = [0.0,0.0,0.0]
        # ——————————————————————————
        # 保存handicap name
        handicap_name = ''
        # 保存handicap_host_win_back_price
        handicap_host_win_back_price_list = [0.0,0.0,0.0]
        # 保存handicap_host_win_back_price_amount
        handicap_host_win_back_price_amount_list = [0.0,0.0,0.0]
        # 保存handicap_host_win_lay_price
        handicap_host_win_lay_price_list = [0.0,0.0,0.0]
        # 保存handicap_host_win_lay_price_amount
        handicap_host_win_lay_price_amount_list = [0.0,0.0,0.0]
        # 保存handicap_guest_win_back_price
        handicap_guest_win_back_price_list = [0.0,0.0,0.0]
        # 保存handicap_guest_win_back_price_amount
        handicap_guest_win_back_price_amount_list = [0.0,0.0,0.0]
        # 保存handicap_guest_win_lay_price
        handicap_guest_win_lay_price_list = [0.0,0.0,0.0]
        # 保存handicap_guest_win_lay_price_amount
        handicap_guest_win_lay_price_amount_list = [0.0,0.0,0.0]
        # ——————————————————————————
        # 保存total name
        total_name = ''
        # 保存over_win_back_price
        over_win_back_price_list = [0.0,0.0,0.0]
        # 保存over_win_back_price_amount
        over_win_back_price_amount_list = [0.0,0.0,0.0]
        # 保存over_win_lay_price
        over_win_lay_price_list = [0.0,0.0,0.0]
        # 保存over_win_lay_price_amount
        over_win_lay_price_amount_list = [0.0,0.0,0.0]
        # 保存under_win_back_price
        under_win_back_price_list = [0.0,0.0,0.0]
        # 保存under_win_back_price_amount
        under_win_back_price_amount_list = [0.0,0.0,0.0]
        # 保存under_win_lay_price
        under_win_lay_price_list = [0.0,0.0,0.0]
        # 保存under_win_lay_price_amount
        under_win_lay_price_amount_list = [0.0,0.0,0.0]
        for market in item['markets']:
            if market['market_name'] == 'one_x_two':
                for runner in market['market_content']:
                    if 'host' in runner.keys():
                        # host price
                        price_count = 0
                        for price in runner['host']:
                            # 如果是back类型
                            if (price['side'] == 'back'):
                                if price_count < 3:
                                    host_win_back_price_list[price_count] = price['odds']
                                    host_win_back_price_amount_list[price_count] = price['amount']
                            # 如果是lay类型
                            else:
                                sub_price_count = 0
                                if price_count >= 3:
                                    sub_price_count = price_count-3
                                host_win_lay_price_list[sub_price_count] = price['odds']
                                host_win_lay_price_amount_list[sub_price_count] = price['amount']
                            price_count += 1
                    elif 'guest' in runner.keys():
                        # guest price
                        price_count = 0
                        for price in runner['guest']:
                            # 如果是back类型
                            if (price['side'] == 'back'):
                                if price_count < 3:
                                    guest_win_back_price_list[price_count] = price['odds']
                                    guest_win_back_price_amount_list[price_count] = price['amount']
                            # 如果是lay类型
                            else:
                                sub_price_count = 0
                                if price_count >= 3:
                                    sub_price_count = price_count - 3
                                guest_win_lay_price_list[sub_price_count] = price['odds']
                                guest_win_lay_price_amount_list[sub_price_count] = price['amount']
                            price_count += 1
                    elif 'draw' in runner.keys():
                        # draw price
                        price_count = 0
                        for price in runner['draw']:
                            # 如果是back类型
                            if (price['side'] == 'back'):
                                if price_count < 3:
                                    draw_back_price_list[price_count] = price['odds']
                                    draw_back_price_amount_list[price_count] = price['amount']
                            # 如果是lay类型
                            else:
                                sub_price_count = 0
                                if price_count >= 3:
                                    sub_price_count = price_count - 3
                                draw_lay_price_list[sub_price_count] = price['odds']
                                draw_lay_price_amount_list[sub_price_count] = price['amount']
                            price_count += 1
            elif market['market_name'] == 'handicap':
                count = 0
                for runner in market['market_content']:
                    if count == 0:
                        # 以主队handicap 为名字
                        handicap_name = runner['name']
                    if 'host' in runner.keys():
                        # host price
                        price_count = 0
                        for price in runner['host']:
                            # 如果是back类型
                            if (price['side'] == 'back'):
                                if price_count < 3:
                                    handicap_host_win_back_price_list[price_count] = price['odds']
                                    handicap_host_win_back_price_amount_list[price_count] = price['amount']
                            # 如果是lay类型
                            else:
                                sub_price_count = 0
                                if price_count >= 3:
                                    sub_price_count = price_count - 3
                                handicap_host_win_lay_price_list[sub_price_count] = price['odds']
                                handicap_host_win_lay_price_amount_list[sub_price_count] = price['amount']
                            price_count += 1
                    elif 'guest' in runner.keys():
                        # guest price
                        price_count = 0
                        for price in runner['guest']:
                            # 如果是back类型
                            if (price['side'] == 'back'):
                                if price_count < 3:
                                    handicap_guest_win_back_price_list[price_count] = price['odds']
                                    handicap_guest_win_back_price_amount_list[price_count] = price['amount']
                            # 如果是lay类型
                            else:
                                sub_price_count = 0
                                if price_count >= 3:
                                    sub_price_count = price_count - 3
                                handicap_guest_win_lay_price_list[sub_price_count] = price['odds']
                                handicap_guest_win_lay_price_amount_list[sub_price_count] = price['amount']
                            price_count += 1
                    count += 1
            else:
                count = 0
                for runner in market['market_content']:
                    if count == 0:
                        # 以over total 为名字
                        total_name = runner['name']
                    if 'over' in runner.keys():
                        # over price
                        price_count = 0
                        for price in runner['over']:
                            # 如果是back类型
                            if (price['side'] == 'back'):
                                if price_count < 3:
                                    over_win_back_price_list[price_count] = price['odds']
                                    over_win_back_price_amount_list[price_count] = price['amount']
                            # 如果是lay类型
                            else:
                                sub_price_count = 0
                                if price_count >= 3:
                                    sub_price_count = price_count - 3
                                over_win_lay_price_list[sub_price_count] = price['odds']
                                over_win_lay_price_amount_list[sub_price_count] = price['amount']
                            price_count += 1
                    elif 'under' in runner.keys():
                        # under price
                        price_count = 0
                        for price in runner['under']:
                            # 如果是back类型
                            if (price['side'] == 'back'):
                                if price_count < 3:
                                    under_win_back_price_list[price_count] = price['odds']
                                    under_win_back_price_amount_list[price_count] = price['amount']
                            # 如果是lay类型
                            else:
                                sub_price_count = 0
                                if price_count >= 3:
                                    sub_price_count = price_count - 3
                                under_win_lay_price_list[sub_price_count] = price['odds']
                                under_win_lay_price_amount_list[sub_price_count] = price['amount']
                            price_count += 1
                    count += 1
        # Connect to the database
        config = {
            'host' : 'localhost',
            'user' : 'root',
            'password' : '',
            'db' : 'match_france',
            'charset' : 'utf8mb4',
            'cursorclass' : pymysql.cursors.DictCursor
        }
        connection = pymysql.connect(**config)
        try:
            with connection.cursor() as cursor:
                # 设置当前数据库名称
                db_name = 'match_'+current_league
                # 设置当前表名
                tableName = current_league + '_match_' + now_time
                # use 对应数据库
                cursor.execute('use '+db_name)
                # 建表
                build_table = (
                    "CREATE TABLE IF NOT EXISTS "' %s '""
                    "(event_id VARCHAR(20) NOT NULL PRIMARY KEY,"
                    "host_name VARCHAR(20) NOT NULL,"
                    "guest_name VARCHAR(20) NOT NULL,"
                    "start_time VARCHAR(30) NOT NULL,"
                    "volume FLOAT NOT NULL,"
                    "if_running BOOLEAN NOT NULL,"
                    "host_win_back_price1 FLOAT,"
                    "host_win_back_price1_amount FLOAT,"
                    "host_win_back_price2 FLOAT,"
                    "host_win_back_price2_amount FLOAT,"
                    "host_win_back_price3 FLOAT,"
                    "host_win_back_price3_amount FLOAT,"
                    "host_win_lay_price1 FLOAT,"
                    "host_win_lay_price1_amount FLOAT,"
                    "host_win_lay_price2 FLOAT,"
                    "host_win_lay_price2_amount FLOAT,"
                    "host_win_lay_price3 FLOAT,"
                    "host_win_lay_price3_amount FLOAT,"
                    "guest_win_back_price1 FLOAT,"
                    "guest_win_back_price1_amount FLOAT,"
                    "guest_win_back_price2 FLOAT,"
                    "guest_win_back_price2_amount FLOAT,"
                    "guest_win_back_price3 FLOAT,"
                    "guest_win_back_price3_amount FLOAT,"
                    "guest_win_lay_price1 FLOAT,"
                    "guest_win_lay_price1_amount FLOAT,"
                    "guest_win_lay_price2 FLOAT,"
                    "guest_win_lay_price2_amount FLOAT,"
                    "guest_win_lay_price3 FLOAT,"
                    "guest_win_lay_price3_amount FLOAT,"
                    "draw_back_price1 FLOAT,"
                    "draw_back_price1_amount FLOAT,"
                    "draw_back_price2 FLOAT,"
                    "draw_back_price2_amount FLOAT,"
                    "draw_back_price3 FLOAT,"
                    "draw_back_price3_amount FLOAT,"
                    "draw_lay_price1 FLOAT,"
                    "draw_lay_price1_amount FLOAT,"
                    "draw_lay_price2 FLOAT,"
                    "draw_lay_price2_amount FLOAT,"
                    "draw_lay_price3 FLOAT,"
                    "draw_lay_price3_amount FLOAT,"
                    "hancicap_name VARCHAR(20),"
                    "hancicap_host_win_back_price1 FLOAT,"
                    "hancicap_host_win_back_price1_amount FLOAT,"
                    "hancicap_host_win_back_price2 FLOAT,"
                    "hancicap_host_win_back_price2_amount FLOAT,"
                    "hancicap_host_win_back_price3 FLOAT,"
                    "hancicap_host_win_back_price3_amount FLOAT,"
                    "hancicap_host_win_lay_price1 FLOAT,"
                    "hancicap_host_win_lay_price1_amount FLOAT,"
                    "hancicap_host_win_lay_price2 FLOAT,"
                    "hancicap_host_win_lay_price2_amount FLOAT,"
                    "hancicap_host_win_lay_price3 FLOAT,"
                    "hancicap_host_win_lay_price3_amount FLOAT,"
                    "hancicap_guest_win_back_price1 FLOAT,"
                    "hancicap_guest_win_back_price1_amount FLOAT,"
                    "hancicap_guest_win_back_price2 FLOAT,"
                    "hancicap_guest_win_back_price2_amount FLOAT,"
                    "hancicap_guest_win_back_price3 FLOAT,"
                    "hancicap_guest_win_back_price3_amount FLOAT,"
                    "hancicap_guest_win_lay_price1 FLOAT,"
                    "hancicap_guest_win_lay_price1_amount FLOAT,"
                    "hancicap_guest_win_lay_price2 FLOAT,"
                    "hancicap_guest_win_lay_price2_amount FLOAT,"
                    "hancicap_guest_win_lay_price3 FLOAT,"
                    "hancicap_guest_win_lay_price3_amount FLOAT,"
                    "total_name VARCHAR(20),"
                    "over_win_back_price1 FLOAT,"
                    "over_win_back_price1_amount FLOAT,"
                    "over_win_back_price2 FLOAT,"
                    "over_win_back_price2_amount FLOAT,"
                    "over_win_back_price3 FLOAT,"
                    "over_win_back_price3_amount FLOAT,"
                    "over_win_lay_price1 FLOAT,"
                    "over_win_lay_price1_amount FLOAT,"
                    "over_win_lay_price2 FLOAT,"
                    "over_win_lay_price2_amount FLOAT,"
                    "over_win_lay_price3 FLOAT,"
                    "over_win_lay_price3_amount FLOAT,"
                    "under_win_back_price1 FLOAT,"
                    "under_win_back_price1_amount FLOAT,"
                    "under_win_back_price2 FLOAT,"
                    "under_win_back_price2_amount FLOAT,"
                    "under_win_back_price3 FLOAT,"
                    "under_win_back_price3_amount FLOAT,"
                    "under_win_lay_price1 FLOAT,"
                    "under_win_lay_price1_amount FLOAT,"
                    "under_win_lay_price2 FLOAT,"
                    "under_win_lay_price2_amount FLOAT,"
                    "under_win_lay_price3 FLOAT,"
                    "under_win_lay_price3_amount FLOAT)"
                )
                cursor.execute(build_table % tableName)

                sql = (
                    "INSERT INTO "+tableName+" VALUES "
                    "(%s, '%s', '%s', '%s', %f, %d, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f,"
                    "%f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, '%s', %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f,"            
                    "'%s', %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f)"
                )
                cursor.execute(sql % (item['eventId'], item['host'], item['guest'], item['startTime'], item['volume'],item['ifRunning'] == 'true',
                float(host_win_back_price_list[0]),float(host_win_back_price_amount_list[0]),
                float(host_win_back_price_list[1]), float(host_win_back_price_amount_list[1]),
                float(host_win_back_price_list[2]), float(host_win_back_price_amount_list[2]),
                float(host_win_lay_price_list[0]),float(host_win_lay_price_amount_list[0]),
                float(host_win_lay_price_list[1]),float(host_win_lay_price_amount_list[1]),
                float(host_win_lay_price_list[2]),float(host_win_lay_price_amount_list[2]),
                float(guest_win_back_price_list[0]), float(guest_win_back_price_amount_list[0]),
                float(guest_win_back_price_list[1]), float(guest_win_back_price_amount_list[1]),
                float(guest_win_back_price_list[2]), float(guest_win_back_price_amount_list[2]),
                float(guest_win_lay_price_list[0]), float(guest_win_lay_price_amount_list[0]),
                float(guest_win_lay_price_list[1]), float(guest_win_lay_price_amount_list[1]),
                float(guest_win_lay_price_list[2]), float(guest_win_lay_price_amount_list[2]),
                float(draw_back_price_list[0]), float(draw_back_price_amount_list[0]),
                float(draw_back_price_list[1]), float(draw_back_price_amount_list[1]),
                float(draw_back_price_list[2]), float(draw_back_price_amount_list[2]),
                float(draw_lay_price_list[0]), float(draw_lay_price_amount_list[0]),
                float(draw_lay_price_list[1]), float(draw_lay_price_amount_list[1]),
                float(draw_lay_price_list[2]), float(draw_lay_price_amount_list[2]),
                handicap_name,
                float(handicap_host_win_back_price_list[0]),float(handicap_host_win_back_price_amount_list[0]),
                float(handicap_host_win_back_price_list[1]),float(handicap_host_win_back_price_amount_list[1]),
                float(handicap_host_win_back_price_list[2]),float(handicap_host_win_back_price_amount_list[2]),
                float(handicap_host_win_lay_price_list[0]), float(handicap_host_win_lay_price_amount_list[0]),
                float(handicap_host_win_lay_price_list[1]), float(handicap_host_win_lay_price_amount_list[1]),
                float(handicap_host_win_lay_price_list[2]), float(handicap_host_win_lay_price_amount_list[2]),
                float(handicap_guest_win_back_price_list[0]),
                float(handicap_guest_win_back_price_amount_list[0]),
                float(handicap_guest_win_back_price_list[1]),
                float(handicap_guest_win_back_price_amount_list[1]),
                float(handicap_guest_win_back_price_list[2]),
                float(handicap_guest_win_back_price_amount_list[2]),
                float(handicap_guest_win_lay_price_list[0]), float(handicap_guest_win_lay_price_amount_list[0]),
                float(handicap_guest_win_lay_price_list[1]), float(handicap_guest_win_lay_price_amount_list[1]),
                float(handicap_guest_win_lay_price_list[2]), float(handicap_guest_win_lay_price_amount_list[2]),
                total_name,
                float(over_win_back_price_list[0]),float(over_win_back_price_amount_list[0]),
                float(over_win_back_price_list[1]),float(over_win_back_price_amount_list[1]),
                float(over_win_back_price_list[2]),float(over_win_back_price_amount_list[2]),
                float(over_win_lay_price_list[0]), float(over_win_lay_price_amount_list[0]),
                float(over_win_lay_price_list[1]), float(over_win_lay_price_amount_list[1]),
                float(over_win_lay_price_list[2]), float(over_win_lay_price_amount_list[2]),
                float(under_win_back_price_list[0]), float(under_win_back_price_amount_list[0]),
                float(under_win_back_price_list[1]), float(under_win_back_price_amount_list[1]),
                float(under_win_back_price_list[2]), float(under_win_back_price_amount_list[2]),
                float(under_win_lay_price_list[0]), float(under_win_lay_price_amount_list[0]),
                float(under_win_lay_price_list[1]), float(under_win_lay_price_amount_list[1]),
                float(under_win_lay_price_list[2]), float(under_win_lay_price_amount_list[2])
             ))
            # connection is not autocommit by default. So you must commit to save your changes.
            connection.commit()

        finally:
            connection.close()

        return item
