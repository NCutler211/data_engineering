"""
__author__ = "Nate Cutler"
__credits__ = data source from MovieLens.com
__email__ = "ncutler211@gmail.com"
"""

import pandas as pd
from datetime import datetime
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

def load_data():
    """
    Loads movie and ratings data from the provided URLs.
    
    Returns:
    pd.DataFrame: DataFrames for movies and ratings.
    """
    movies = pd.read_csv('https://itsa322.s3.us-east-2.amazonaws.com/movies.csv')
    ratings = pd.read_csv('https://itsa322.s3.us-east-2.amazonaws.com/ratings.csv')
    return movies, ratings

def preprocess_movies_data(movies_df):
    """
    Preprocesses movies data by displaying shape, head, tail, and checking NaN values.
    
    Args:
    movies_df (pd.DataFrame): DataFrame containing movies data.
    """
    print("Movies shape:", movies_df.shape)
    print("Head of movies:")
    print(movies_df.head())
    print("Tail of movies:")
    print(movies_df.tail())
    print("Number of NaNs in movies:")
    print(movies_df.isna().sum())

def preprocess_ratings_data(ratings_df):
    """
    Preprocesses ratings data by displaying shape, head, tail, and checking NaN values.
    Converts timestamps to a readable format and adds a 'review_dt' column.
    
    Args:
    ratings_df (pd.DataFrame): DataFrame containing ratings data.
    """
    print("Ratings shape:", ratings_df.shape)
    print("Head of ratings:")
    print(ratings_df.head())
    print("Tail of ratings:")
    print(ratings_df.tail())
    print("Number of NaNs in ratings:")
    print(ratings_df.isna().sum())

    test_vec = pd.to_datetime(ratings_df['timestamp'], unit='s')
    ratings_df['review_dt'] = test_vec

    print("Min review date:", ratings_df['review_dt'].min())
    print("Max review date:", ratings_df['review_dt'].max())

def analyze_movies_ratings(movies_df, ratings_df):
    """
    Analyzes movies and ratings data by joining them, performing statistical analysis,
    and visualizing insights.
    
    Args:
    movies_df (pd.DataFrame): DataFrame containing movies data.
    ratings_df (pd.DataFrame): DataFrame containing ratings data.
    """
    ratings_by_movie = ratings_df.groupby('movieId')['rating'].agg(['mean', 'std', 'count']).reset_index()
    ratings_by_movie.columns = ['movieId', 'rating_mean', 'rating_std', 'rating_count']

    movies_with_ratings = movies_df.set_index('movieId').join(ratings_by_movie.set_index('movieId'))

    movies_with_ratings = movies_with_ratings[movies_with_ratings['rating_count'] >= 10]

    movies_with_ratings['rating_group'] = pd.cut(movies_with_ratings['rating_mean'], 3, labels=['bad', 'fine', 'good'])
    movies_with_ratings['rating_agreement'] = pd.cut(movies_with_ratings['rating_std'], 3, labels=['agreement', 'average', 'controversial'])

    # Example of filtering by specific criteria
    controversial_movies = movies_with_ratings[
        (movies_with_ratings['rating_group'] == 'bad') &
        (movies_with_ratings['rating_agreement'] == 'controversial') &
        (movies_with_ratings['rating_count'] >= 100)
    ]

    # Additional analysis or visualizations can be added here

    # Example: Finding the movie with the highest rating count
    max_rating_count_movie_id = movies_with_ratings['movieId'][movies_with_ratings['rating_count'] == movies_with_ratings['rating_count'].max()].values[0]
    most_viewed_df = ratings_df[ratings_df['movieId'] == max_rating_count_movie_id]

    sns.lineplot(data=most_viewed_df, x='review_dt', y='rating')

    # Example: Finding a movie with specific title and visualizing its ratings over the years
    dredd_id = movies_with_ratings['movieId'][movies_with_ratings['title'].str.contains('^Dredd')].values[0]
    dredd_df = ratings_df[ratings_df['movieId'] == dredd_id]

    sns.lineplot(x='review_dt', y='rating', data=dredd_df)

def main():
    movies, ratings = load_data()

    preprocess_movies_data(movies)
    preprocess_ratings_data(ratings)

    analyze_movies_ratings(movies, ratings)

if __name__ == "__main__":
    main()
