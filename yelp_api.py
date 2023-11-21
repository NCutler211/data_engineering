"""
__author__ = "Nate Cutler"
__email__ = "ncutler211@gmail.com"
"""

#!pip install --target=$nb_path yelpapi


from yelpapi import YelpAPI
from pandas.io.json import json_normalize
import pandas as pd
import numpy as np
import time

def setup_yelp_api(api_key):
    """
    Sets up Yelp API with the provided API key.
    
    Args:
    api_key (str): Yelp API key.
    
    Returns:
    YelpAPI: Yelp API object.
    """
    yelp_api = YelpAPI(api_key)
    return yelp_api

def get_taco_shops_data(yelp_api, location='Tucson, AZ', term='taco'):
    """
    Retrieves taco shop data based on location and term.
    
    Args:
    yelp_api (YelpAPI): Yelp API object.
    location (str): Location to search for taco shops (default is 'Tucson, AZ').
    term (str): Search term (default is 'taco').
    
    Returns:
    pd.DataFrame: DataFrame containing taco shop data.
    """
    taco_shops = yelp_api.search_query(term=term, location=location)
    taco_shops_df = pd.json_normalize(taco_shops['businesses'])
    return taco_shops_df[['id', 'alias', 'name', 'review_count', 'rating']]

def get_taco_shop_reviews(yelp_api, taco_shops_df):
    """
    Retrieves and aggregates reviews for each taco shop.
    
    Args:
    yelp_api (YelpAPI): Yelp API object.
    taco_shops_df (pd.DataFrame): DataFrame containing taco shop data.
    
    Returns:
    pd.DataFrame: DataFrame containing taco shop reviews.
    """
    taco_shops_reviews_df = pd.DataFrame()
    
    for i in range(len(taco_shops_df)):
        reviews = yelp_api.reviews_query(taco_shops_df['id'][i])
        time.sleep(.5)
        reviews_df = pd.json_normalize(reviews['reviews'])
        reviews_df['location_id'] = taco_shops_df['id'][i]
        taco_shops_reviews_df = taco_shops_reviews_df.append(reviews_df)
    
    return taco_shops_reviews_df[['id', 'text', 'rating', 'time_created', 'location_id']]

def analyze_taco_shops(taco_shops_df, taco_shops_reviews_df):
    """
    Analyzes taco shop ratings and aggregates mean ratings.
    
    Args:
    taco_shops_df (pd.DataFrame): DataFrame containing taco shop data.
    taco_shops_reviews_df (pd.DataFrame): DataFrame containing taco shop reviews.
    
    Returns:
    pd.DataFrame: DataFrame containing analyzed taco shop comparison.
    """
    latest_reviews_agg = taco_shops_reviews_df.groupby('location_id', as_index=False).agg({'mean'}).reset_index()
    latest_reviews_agg.columns = ['location_id', 'mean_rating']
    
    taco_shops_comp = latest_reviews_agg.set_index('location_id').join(taco_shops_df.set_index('id'))
    taco_shops_comp['still_good'] = np.where(taco_shops_comp['mean_rating'] >= taco_shops_comp['rating'], 'yes', 'no')
    
    return taco_shops_comp

def main():
    api_key = 'R1UO3E1KhyfI1Qqa7xvNtfYmqIgOEtvSD_yb6o1a4_RqJh2TrXxYdipxX8heUywJ7u0SAkRAXM5hhjKLvFBSUZMjL-7RgiCDAW7LhguEH1rkaW_KMNG1lGTGNbqwZHYx'
    yelp_api = setup_yelp_api(api_key)

    taco_shops_df = get_taco_shops_data(yelp_api)
    taco_shops_reviews_df = get_taco_shop_reviews(yelp_api, taco_shops_df)
    
    taco_shops_comp = analyze_taco_shops(taco_shops_df, taco_shops_reviews_df)
    print(taco_shops_comp)  # Displaying the analyzed taco shop comparison
    
if __name__ == "__main__":
    main()
