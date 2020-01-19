# -*- coding: utf-8 -*-

from utils import createLogger, DBtools
import time, sys
from datetime import datetime
from binance.client import Client
from binance.websockets import BinanceSocketManager
from credentials import credentials_binance, credentials_mysql

class BTCreader():
    def __init__(self):
        #create the logger
        self.logger = createLogger.createLogger()
        
        self.ticker = 'BTCUSDT'
        #read the credentials from the file
        self.__api_key = credentials_binance['key']
        self.__api_secret = credentials_binance['secret']
        self.__mysql_host = credentials_mysql['host']
        self.__mysql_port = credentials_mysql['port']
        self.__mysql_user = credentials_mysql['user']
        self.__mysql_password = credentials_mysql['password']
        self.reconnect_count = 0
        
        #set no proxies, for the case that the proxy is configured in the system
        self.proxies = {
            'http': '',
            'https': ''
        }

        #define the Client
        self.client = Client(self.__api_key, self.__api_secret,{'proxies':self.proxies})
        
        self.exchange_info = self.client.get_exchange_info()
        self.logger.info("Client initialized")
    
        #%% Websocket to get the MD
        self.bm = BinanceSocketManager(self.client, user_timeout=60)

        #open DB class handle
        self.db = DBtools.DB(host = self.__mysql_host,
                        port=self.__mysql_port,
                        user=self.__mysql_user,
                        password=self.__mysql_password,
                        db = 'crypto',
                        logger = self.logger)

        # start any sockets here, i.e a trade socket
        self.createWS()
        # then start the socket manager
        self.startWS()

        try:
            while True:
                if not self.bm.isAlive():
                    #if the connection is lost, wait for a second and reconnect.
                    self.logger.error("The connection was lost, maybe from the server side")
                    self.logger.debug("trying to reconnect")
                    time.sleep(1)
                    self.bm.start()
                    if self.bm.isAlive():
                        self.logger.debug("connection re-established")
                    else:
                        self.logger.error("Could not reconnect, sleeping for 1 minute and retry")
                        time.sleep(60)
        except KeyboardInterrupt:
            self.logger.info("BTCReader closed by user")
            self.closeWS()
            self.db.close()
        except Exception:
            self.logger.exception("BTCReader closed by error")
            self.logger.debug("Clossing connection to the websocket and database")
            self.closeWS()
            self.db.close()
            sys.exit()

    def startWS(self):
        """
        Start the websocket communication
        """
        self.bm.start()
        
    def closeWS(self):
        """
        Close the websocket communication
        """
        self.bm.close()
        
    def createWS(self):
        """
        Create the websocket and get the conn_key
        """
        self.conn_key = self.bm.start_trade_socket(self.ticker, self.process_message)
        
    def process_message(self, msg):
        """"
        The method to process the msg received in the WebSocket
        """
        if msg['e'] == 'error':
            if msg['m'] == 'Max reconnect retries reached':
                self.logger.error("Max recconect retries reached")
                time.sleep(60)
                self.logger.debug("Trying to generate again the WS")
                # sleep gor 1 minute
                self.createWS()
                self.startWS()
                if self.bm.isAlive():
                    self.logger.debug("And it's alive again!")
                else:
                    self.logger.error("Ups, no this time budy")
            else:
                self.logger.error("Error in the message. The message said:{}".format(msg))
                self.logger.debug("Sleeping for 1 sec to prevent more errors.")
                time.sleep(1)
        else:
            # process message normally
            msg['E'] = datetime.fromtimestamp(int(msg['E'])/1000).strftime('%Y-%m-%d %H:%M:%S.%f')
            msg['T'] = datetime.fromtimestamp(int(msg['T'])/1000).strftime('%Y-%m-%d %H:%M:%S.%f')
            data = {'event_time':msg['E'],
                    'trade_id':msg['t'],
                    'price':msg['p'],
                    'quantity':msg['q'],
                    'bid_id':msg['b'],
                    'ask_id':msg['a'],
                    'trade_time':msg['T'],
                    'maker':msg['m']}
            #the ignore field will be ignored.
            if not self.db.exist_table(msg['s']):
                self.db.create_ticker_table(msg['s'])
            self.db.appendMD(table=msg['s'],MD=data)
            self.logger.debug("Trade stored in the database")
        
        
        
if __name__ == '__main__':
    btcreader = BTCreader()