# -*- coding: utf-8 -*-

from notifiers.telegramBot import telegramBot as tBot
import logging
from credentials import credentials_telegram, credentials_mysql
from utils.DBtools import DB

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    logger = logging.getLogger(__name__)   
    
    db = DB(host=credentials_mysql['host'],
            port=credentials_mysql['port'],
            user=credentials_mysql['user'],
            password=credentials_mysql['password'],
            logger=logger,
            db='binance')
    
    tBot(credentials_telegram['token_telegram'],
         credentials_telegram['channel_id'],
         DB=db,
         logger=logger)