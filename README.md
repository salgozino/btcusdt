# btcusd
Bitcoin (Binance) Bot.

Read with a websocket the bitcoin trades, and store to a database.

In the near future, it will analyze the trend and send trade recomendations.

Use by your own risk

To install in your pc, need a MySQL database (or use google cloud).

first you neeed to install the requirements
  pip install -r requierements

go to credentials and change the Binance API credentials, and the database host, port, user and password.

then run the BTCreader, and automatically will be saving all the BTCUSDT trades to the database.

if you run also runTbot, you can get a telegram bot to ask the prices and get the daily changes in the bitcoin price, for that, you need to setup a telegram bot first, and store the credentials in the credentials file.
