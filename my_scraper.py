# Import the MLB_Scrape class from the module
from api_scraper import MLB_Scrape
import polars as pl
pl.Config.set_tbl_cols(-1)

# Initialize the scraper
scraper = MLB_Scrape()

# Call the get_sport_id method
sport_ids = scraper.get_sport_id()
print(sport_ids)

df_player = scraper.get_players(sport_id=1,season=2024,game_type=['R'])

import polars as pl
# Bryce Player Id
player_id = 682243
season = 2024

# Get Game IDs for Bryce Miler
player_games = scraper.get_player_games_list(player_id=player_id, season=season, game_type=['R'], pitching=True)

# Get Data for Bryce Miler
data = scraper.get_data(game_list_input=player_games)
df = scraper.get_data_df(data_list=data)
# Print the data
#print(df) 

grouped_df = (
    df.filter(pl.col("pitcher_id") == player_id)
    .group_by(['pitcher_id', 'pitch_type'])
    .agg([
        pl.col('is_pitch').drop_nans().count().alias('pitches'),
        pl.col('start_speed').drop_nans().mean().round(1).alias('start_speed'),
        pl.col('ivb').drop_nans().mean().round(1).alias('ivb'),
        pl.col('hb').drop_nans().mean().round(1).alias('hb'),
        pl.col('spin_rate').drop_nans().mean().round(0).alias('spin_rate'),
    ])
    .with_columns(
        (pl.col('pitches') / pl.col('pitches').sum().over('pitcher_id')).round(3).alias('proportion')
    )
    ).sort('proportion', descending=True)

# Display the grouped DataFrame
#print(grouped_df)

def scrape(name, season): 
    df_player = scraper.get_players(sport_id=1,season=season,game_type=['R'])
    player_id = df_player.row(by_predicate=(pl.col("name") == name))[0]
    player_games = scraper.get_player_games_list(player_id=player_id, season=season, game_type=['R'], pitching=True)
    data = scraper.get_data(game_list_input=player_games)
    df = scraper.get_data_df(data_list=data)
    grouped_df = (
    df.filter(pl.col("pitcher_id") == player_id)
    .group_by(['pitcher_id', 'pitch_type'])
    .agg([
        pl.col('is_pitch').drop_nans().count().alias('pitches'),
        pl.col('start_speed').drop_nans().mean().round(1).alias('start_speed'),
        pl.col('ivb').drop_nans().mean().round(1).alias('ivb'),
        pl.col('hb').drop_nans().mean().round(1).alias('hb'),
        pl.col('spin_rate').drop_nans().mean().round(0).alias('spin_rate'),
    ])
    .with_columns(
        (pl.col('pitches') / pl.col('pitches').sum().over('pitcher_id')).round(3).alias('proportion')
    )
    ).sort('proportion', descending=True)

    print(grouped_df)

def AAA_scrape(name, season): 
    df_player = scraper.get_players(sport_id=11,season=season,game_type=['R'])
    player_id = df_player.row(by_predicate=(pl.col("name") == name))[0]
    player_games = scraper.get_player_games_list(player_id=player_id, season=season, game_type=['R'], pitching=True, sport_id=11)
    data = scraper.get_data(game_list_input=player_games)
    df = scraper.get_data_df(data_list=data)
    grouped_df = (
    df.filter(pl.col("pitcher_id") == player_id)
    .group_by(['pitcher_id', 'pitch_type'])
    .agg([
        pl.col('is_pitch').drop_nans().count().alias('pitches'),
        pl.col('start_speed').drop_nans().mean().round(1).alias('start_speed'),
        pl.col('ivb').drop_nans().mean().round(1).alias('ivb'),
        pl.col('hb').drop_nans().mean().round(1).alias('hb'),
        pl.col('spin_rate').drop_nans().mean().round(0).alias('spin_rate'),
        pl.col('launch_angle').drop_nans().mean().round(0).alias('launch_angle'),
        (pl.col('is_whiff').count() / pl.col('is_swing').count()).alias('whiff rate'),
    ])
    .with_columns(
        (pl.col('pitches') / pl.col('pitches').sum().over('pitcher_id')).round(3).alias('proportion')
    )
    ).sort('proportion', descending=True)

    print(grouped_df)

AAA_scrape("Bobby Miller", 2024)
scrape("Bobby Miller", 2024)

AAA_scrape("Bubba Chandler", 2025)