# -*- coding: utf-8 -*-
"""
First Try using a Telegram Bot
Using this tutorial
https://www.freecodecamp.org/news/learn-to-build-your-first-bot-in-telegram-with-python-4c99526765e4/

"""

from telegram.ext import Updater, CommandHandler, CallbackContext
import requests
import re
import logging
from utils import indicadores
import urllib, json
from datetime import datetime, timedelta, time

#Emojis
rocket = u'\U0001F680'
down_arrow = u'\U00002B07'
up_arrow = u'\U00002B06'

class telegramBot():
    
    def __init__(self, token, channel_id, DB, logger=None):
    #testing channel
    #channel_id = '-1001361357182'
    #BTCInfo channel
        self.channel_id = channel_id
        self.token = token
        if not logger:
            logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
            self.logger = logging.getLogger(__name__)   
        else:
            self.logger = logger
        
        #Store the DataBase class.
        self.db = DB
        self.db.connect()
        
        self.main()

    def get_url(self):
        contents = requests.get('https://random.dog/woof.json', proxies=self.proxies).json()    
        url = contents['url']
        return url
    
    def get_image_url(self):
        allowed_extension = ['jpg','jpeg','png']
        file_extension = ''
        while file_extension not in allowed_extension:
            url = self.get_url()
            file_extension = re.search("([^.]*)$",url).group(1).lower()
        return url
    
    
    def dog(self, update, context):
        self.logger.info('Wow, {} pidio la foto de un Perro!, este sabe!'.format(str(update.message.from_user.username)))
        url = self.get_image_url()
        chat_id = update.message.chat_id
        context.bot.send_photo(chat_id=chat_id, photo=url)
    
    
    def start(self, update, context):
        self.logger.info('Wow!, alguien me inicio, y su nickname es {}'.format(str(update.message.from_user.username)))
        context.bot.send_message(
            chat_id=update.message.chat_id,
            text="Hola {}!, bienvenido al bot que nos hará ricos (?)\n\n Para saber que puedo hacer escribe /help".format(str(update.message.from_user.username))
        )
        
    def help_callback(self, update, context):
        """Send a message when the command /help is issued."""
    
        update.message.reply_text(
            'Estos son todos los comandos disponibles\n' +
            '/help: Muestra este menu de opciones\n ' +
            '/dog: Te devuelvo la foto de un perro hermoso!\n' +
            '/cat: Te gustan los gatos?. Tengo fotos de gatos!\n ' +
            '/precioBTC: Te digo cual es el último precio de BTC que tengo (Binance)\n'
        )
        self.logger.info("Alguien no sabe donde esta parado y pregunto /help")
    
    def precioBTC(self, update, context):
        self.logger.info("{} quiere saber el precio del btc...".format(str(update.message.from_user.username)))
        data = self.db.read_last_row()
        context.bot.send_message(
            chat_id=update.message.chat_id,
            text="El último precio que tengo del BTC es: {} con fecha {}".format(data['price'],data['trade_time'])
        )
    
    def callback_daily(self, context: CallbackContext):
        self.logger.info("It's time for the daily price change!")
        start_date = (datetime.now()-timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S.%f')
        data = self.db.read_table(start_date=start_date)
        data.set_index('event_time', inplace=True)
        ohlc = indicadores.getOHLC(data, period='1D')
        #print(ohlc)
        close_ytd = ohlc.loc[ohlc.index.day == (datetime.today()-timedelta(days=1)).day, 'close'].values
        current_price = self.db.read_last_price()
    
        text = (
                'Actualización del precio para el día de hoy:\n' +
                'Anoche el BTC/USDT cerró en el precio de ${} '.format(close_ytd) +
                'y el precio actual es de ${}.\n'.format(current_price)
                )
        text2 = 'Esto nos da un cambio de {:.2f}% '.format((1-close_ytd/current_price)*100)
        if current_price > close_ytd:
            if (1-close_ytd/current_price)*100 > 1:
                text2 = text2 + rocket
            else:
                text2 = text2 + up_arrow
        else:
            text2 = text2 + down_arrow
                
        context.bot.send_message(chat_id=self.channel_id, 
                                 text=text+text2)
    
    
    def cat(self, update, context):
        self.logger.info('Wow, {} pidio la foto de un gato!'.format(str(update.message.from_user.username)))
        img_url = json.loads(urllib.request.urlopen('http://aws.random.cat/meow').read().decode())['file']
        context.bot.send_photo(chat_id=update.message.chat_id, photo=img_url)
    
    
    def main(self):
        updater = Updater(self.token, use_context=True)
        dp = updater.dispatcher
        dp.add_handler(CommandHandler('dog',self.dog))
        dp.add_handler(CommandHandler('cat',self.cat))
        dp.add_handler(CommandHandler('start',self.help_callback))
        dp.add_handler(CommandHandler('help',self.help_callback))
        dp.add_handler(CommandHandler('precioBTC',self.precioBTC))
        
        #Add the daily message
        j = updater.job_queue
        j.run_daily(self.callback_daily, time=time(12, 0, 0))
    
        updater.start_polling()
        updater.idle()
    
