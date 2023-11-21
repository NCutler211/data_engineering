"""
__author__ = "Nate Cutler"
__email__ = "ncutler211@gmail.com"
"""

import pandas as pd
import numpy as np
import requests
import matplotlib.pyplot as plt

def load_price_data():
    """
    Loads price data from a CSV file hosted online.
    
    Returns:
    pd.DataFrame: DataFrame containing price data.
    """
    price = pd.read_csv("https://docs.google.com/spreadsheets/d/1lCkFZhz-NGTuE1ZilzJA_ZYBZsbCSDpS2MllyGZWjX4/gviz/tq?tqx=out:csv")
    return price

price = load_price_data()

def min_daily_volume_after_2010():
    """
    Calculates the minimum daily volume after the year 2010.

    Returns:
    float: Minimum daily volume after 2010.
    """
    price['Date'] = pd.to_datetime(price['Date'])
    df_2010 = price.set_index(['Date'])
    df_2010 = df_2010.loc['2010-01-01':]
    return df_2010['Volume'].min()

print(min_daily_volume_after_2010())

def number_of_green_days():
    """
    Counts the number of days where the closing price is higher than the opening price.

    Returns:
    int: Number of days with a higher closing price than the opening price.
    """
    price["up_binom"] = price['Close'] - price['Open']
    price['up_binom'] = np.where(price['up_binom'] >= 0, 1, 0)
    return int(price['up_binom'].sum())

print(number_of_green_days())

def plot_closing_price_after_2020():
    """
    Plots closing stock prices after the year 2020.
    """
    df_2020 = price.set_index(['Date'])
    df_2020 = df_2020.loc['2020-01-02':]

    x_axis = df_2020.index
    y_axis = df_2020['Close']

    plt.plot(x_axis, y_axis)
    plt.title('Closing stock price after 2020')
    plt.xlabel('Day')
    plt.ylabel('Closing Price')
    plt.show()

plot_closing_price_after_2020()

def market_value(buy_date, sell_date):
    """
    Calculates the market value between two given dates based on buy and sell prices.

    Args:
    buy_date (str): Date of purchase in 'YYYY-MM-DD' format.
    sell_date (str): Date of sale in 'YYYY-MM-DD' format.

    Returns:
    float: Market value between the buy and sell dates.
    """
    buy_date = pd.to_datetime(buy_date)
    stock_count = 1000 / price.loc[buy_date]['Open']
    sell_price = stock_count * price.loc[sell_date]['Close']
    return sell_price

print(market_value('1999-12-31', '2019-12-31'))

url = 'http://api.tvmaze.com/singlesearch/shows?q=Silicon Valley&embed=episodes'
sv_json_obj = requests.get(url)
sv_json = sv_json_obj.json()

def get_show_premiered():
    """
    Gets the premiere date of the first episode of 'Silicon Valley'.

    Returns:
    str: Premiere date of the first episode.
    """
    for episode in sv_json['_embedded']['episodes']:
        if episode['season'] == 1 and episode['number'] == 1:
            return episode['airdate']

print(get_show_premiered())

def get_summary(season, episode):
    """
    Gets the summary of a specific episode of 'Silicon Valley'.

    Args:
    season (int): Season number of the episode.
    episode (int): Episode number within the season.

    Returns:
    str: Summary of the specified episode.
    """
    for ep in sv_json['_embedded']['episodes']:
        if ep['season'] == season and ep['number'] == episode:
            return ep['summary']

print(get_summary(2, 5))
