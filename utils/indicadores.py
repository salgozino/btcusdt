# -*- coding: utf-8 -*-
"""
Indicadores Técnicos para series temporales.

En general, el input tiene que ser un dataframe, donde index es tiempo, y se 
debe especificar el nombre de la columna a considerar (precio por ejemplo).
"""
import pandas as pd
import numpy as np

def MACD(df,columna,short_window=12,long_window=26,smooth_signal=9):
    """
    MACD indicator.
    input:
        * df -> dataframe de pandas con columna de precio de cierre.
        * columna -> nombre de la columna de precio
        * short_window = 12 -> longitud de la media movil rapida
        * long_window = 26 -> longitud de la media movil lenta
        * smooth_signal = 9 -> longitud de la media movil de suavizado
    outputs dentro del mismo dataframe:
        * MACD -> DataFrame with MACD; MACD_signal and MACD_diff columns.
    """
    df2 = df.copy()
    MACD = pd.DataFrame()
    df2['EMA{}'.format(short_window)] = df[columna].ewm(span=short_window).mean()
    df2['EMA{}'.format(short_window)].fillna(df2['EMA{}'.format(short_window)], inplace=True)
    df2['EMA{}'.format(long_window)] = df[columna].ewm(span=long_window).mean()
    df2['EMA{}'.format(long_window)].fillna(df2['EMA{}'.format(long_window)], inplace=True)
    MACD['MACD'] = df2['EMA{}'.format(short_window)] - df2['EMA{}'.format(long_window)]
    MACD['MACD_SIGNAL'] = MACD['MACD'].rolling(window=smooth_signal,min_periods=1).mean()
    MACD['MACD_HIST'] = MACD['MACD'] - MACD['MACD_SIGNAL']
    return MACD

def RSI(df,columna,window_length=14):
    """
    Compute the RSI indicator (relative strenght index) from the column
    specified. window_length recommended = 14
    input:
        * df -> dataframe de pandas con columna de precio de cierre.
        * columna -> nombre de la columna de precio
        * window_length -> longitud de la media movil.
    output:
        * RSI -> dataframe with the indicator.
    """
    RSI = pd.DataFrame(index = df.index)
    # Get rid of the first row, which is NaN since it did not have a previous 
    # row to calculate the differences
    delta = df[columna].diff()
    delta = delta[1:] 
    # Make the positive gains (up) and negative gains (down) Series
    up, down = delta.copy(), delta.copy()
    up[up < 0] = 0
    down[down > 0] = 0

    roll_up1 = up.ewm(window_length,min_periods=window_length).mean()
    roll_down1 = down.abs().ewm(window_length,min_periods=window_length).mean()
    
    # Calculate the RSI based on EWMA
    RSI['RSI_EWMA'] = 100.0 - (100.0 / (1.0 + roll_up1 / roll_down1))
    # Calculate the SMA
    roll_up2 = up.rolling(window=window_length,min_periods=window_length).mean()
    roll_down2 = down.abs().rolling(window=window_length,min_periods=window_length).mean()
    # Calculate the RSI based on SMA
    RSI['RSI_SMA'] = 100.0 - (100.0 / (1.0 + roll_up2 / roll_down2))
    # Completo los valores NaN con el primer valor
    RSI.fillna(method='ffill', inplace=True)
    return RSI

def PP(df, close_col = 'close', high_col = 'high', low_col = 'low'):
    """
    P = (H + L + C)/3
    Nivel de Pivot = (Anterior High + Anterior Low + Anterior Close) / 3
    El dataframe ingresado debe tener columnas Close, High y Close 
    """
    puntos_pivot = (df[close_col].shift(1) + df[low_col].shift(1) + df[high_col].shift(1)) / 3 
    return puntos_pivot

def VWAP(df,column_price, column_volume): 
    """
    Volumen ponderado por precio
    Si existe un NaN lo completo con el valor siguiente
    inputs:
        * df -> DataFrame con los datos de precio y volumen
        * column_price -> nombre de la columna precio
        * column_volume -> nombre de la columna volumen
    outputs
        * new datafrma with
    """    
    df.fillna(method='ffill', inplace=True)
    q = df[column_volume].values
    p = df[column_price].values
    vwap = (p * q).cumsum() / q.cumsum()
    return pd.DataFrame({'VWAP':vwap}, index=df.index)
    
def ROC(df, n=2, column_name='Close'):
    """
    :param df: pandas.DataFrame
    :param n: cant. de offset
    :return: pandas.DataFrame
    """
    roc = pd.DataFrame(np.gradient(df[column_name]), index=df.index, columns=['ROC_{}'.format(n)])
    return roc

def STOCHASTIC(df, column_name = 'Close',window=14, smoothk=1,smoothd=3):
    """
    The stochastic oscillator is calculated using the following formula:
    %K = 100(C - L14)/(H14 - L14)
    Where:
    C = the most recent closing price
    L14 = the low of the 14 previous trading sessions
    H14 = the highest price traded during the same 14-day period
    %K= the current market rate for the currency pair
    %D = 3-period moving average of %K
        inputs:
            * df = dataframe with close price
    """
    df.fillna(method='ffill', inplace=True)
    stoch = pd.DataFrame(index=df.index)
    max, min = df[column_name].rolling(window=window).max(), df[column_name].rolling(window=window).min()
    stoch['STO_K'] = 100*(df[column_name] - min)/(max-min)
    stoch['STO_K'] = stoch.STO_K.rolling(window=smoothk).mean()
    stoch['STO_D'] = stoch.STO_K.rolling(window=smoothd).mean()
    return stoch

def BB(df, column_price='Close', window_length = 20, n_std = 2):
    """
    Bollinger Bands
    df -> dataframe with the price information
    column_price -> name of the column with the price
    window_length -> length for the sma (average)
    n_std -> number of std to define the upper and lower limit
    """
    bb = pd.DataFrame(index=df.index)
    bb['BB_m'] = df[column_price].rolling(window=window_length).mean()
    rolling_std  = df[column_price].rolling(window=window_length).std()
    bb['BB_u'] = bb['BB_m'] + (rolling_std*n_std)
    bb['BB_d'] = bb['BB_m'] - (rolling_std*n_std)
    return bb

def ADX(df, col_close='Close', col_high='High', col_low='Low', window_length=14):
    """
    
    """
    moveup = (df[col_high] - df[col_high].shift(-1)).to_frame().rename(columns={'High':'MoveUp'})
    movedown = (df[col_low].shift(-1) - df[col_low]).to_frame().rename(columns={'Low':'MoveDown'})
    
    #direction movements
    dm = pd.concat([moveup, movedown], axis=1)
    dm['pdm'] = 0
    dm['ndm'] = 0
    
    condition = (moveup['MoveUp']>0) & (moveup['MoveUp']>movedown['MoveDown'])
    dm.loc[condition, 'pdm'] = moveup.loc[condition, 'MoveUp']
    condition = (movedown['MoveDown']>0) & (movedown['MoveDown']>moveup['MoveUp'])
    dm.loc[condition, 'ndm'] = movedown.loc[condition, 'MoveDown']
    
    atr = ATR(df, col_close=col_close, col_high=col_high, col_low=col_low, window_length=window_length)
    
    adx = pd.DataFrame(index=df.index)
    adx['PDI'] = 100*dm.pdm.ewm(span=window_length).mean()/atr['ATR']
    adx['NDI'] = 100*dm.ndm.ewm(span=window_length).mean()/atr['ATR']
    adx['ADX'] = 100*np.abs(adx.PDI - adx.NDI)/(adx.PDI + adx.NDI)
    return adx

def ATR(df, col_close='Close', col_high='High', col_low='Low', window_length=14):
    """
    Average True Range
    df -> dataframe with open high low close
    """
    atr = pd.DataFrame([df[col_high]-df[col_low], np.abs(df[col_high]-df[col_close].shift(-1)), np.abs(df[col_low]-df[col_close].shift(-1))]).max(axis=0).rolling(window=window_length).mean()
    atr = pd.DataFrame(atr, index = df.index, columns = ['ATR'])
    return atr
    
def PCT_CHANGE(df, col_name):
    """
    Determino el cambio porcentual de la columna especificada en df
    """
    return pd.DataFrame(df[col_name].pct_change()).rename(columns={col_name:'PCT_CHANGE'})

def EASYMOVEMENT(df, col_high='High', col_low='Low', col_vol = 'Volume', window_length=14,  x=100000000):
    """
    https://www.daytrading.com/ease-of-movement
    """
    distance = (df[col_high] + df[col_low])/2 - (df[col_high].shift(-1) + df[col_low].shift(-1))/2
    box_ratio = (df[col_vol]/x)/(df[col_high]-df[col_low])
    spem = distance/box_ratio
    em = pd.DataFrame(spem, columns=['EM_1'])
    em['EM_'+str(window_length)] = spem.rolling(window=window_length).mean()
    return em
    
def CCI(df, col_close='Close', col_high = 'High', col_low='Low', window_length =20, constant = .015):
    """
    Commoditty Channel index
    https://school.stockcharts.com/doku.php?id=technical_indicators:commodity_channel_index_cci
    """
    typical_price = (df[col_high] + df[col_low] + df[col_low])/3
    mean_deviation = typical_price.rolling(window=window_length).std()
    CCI = pd.DataFrame((typical_price -  typical_price.rolling(window=window_length).mean()) / (constant*mean_deviation), columns = ['CCI'])
    return CCI


#%% Generate Indicators
def get_indicators(df):
    macd = MACD(df, columna='Close')
    rsi = RSI(df, columna='Close')
    vwap = VWAP(df, column_price='Close', column_volume='Volume')
    roc5 = ROC(df, n=5, column_name='Close')
    stoch = STOCHASTIC(df, column_name='Close')
    bb = BB(df, column_price='Close')
    adx = ADX(df, col_close='Close', col_low='Low', col_high='High', window_length=14)
    pct_change = PCT_CHANGE(df, col_name='Close');
    em = EASYMOVEMENT(df)
    cci = CCI(df)
    return pd.concat([macd, rsi, vwap, roc5, stoch, bb, adx, pct_change, em, cci], axis=1)


def drop_high_corr_indicators(indicators, condition = 0.7):
    import matplotlib.pyplot as plt
    corr_matrix = indicators.corr().abs()
    upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(np.bool))

    # Find index of feature columns with correlation greater than 0.95
    to_drop = [column for column in upper.columns if any(upper[column] > condition)]
    
    plt.matshow(corr_matrix)
    plt.show()
    indicators.drop(indicators[to_drop], axis=1, inplace=True)
    plt.matshow(indicators.corr().abs())
    plt.show()
    
    return indicators

def getOHLC(df,column_name_price = 'price', column_name_size = 'quantity',period='1D'):
    try:
        df.drop_duplicates(subset='date_LA', keep='first', inplace=True)
    except:
        pass
    if 'date_LA' in df.columns.names:
        df.set_index('date_LA', inplace=True)
        df.index.names = ['date']
    
    df.index = pd.to_datetime(df.index)
    ohlc = df[column_name_price].resample(period).ohlc()
    try:
        vol  = df[column_name_size].resample(period).sum()

        ohlcv = pd.concat([ohlc, vol], axis=1, join_axes=[ohlc.index])
        ohlcv.rename(columns={column_name_size:'volume'}, inplace=True)
        return ohlcv
    except:
        return ohlc