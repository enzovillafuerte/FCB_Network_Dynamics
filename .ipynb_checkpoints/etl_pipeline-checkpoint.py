

# Importing needed variables
import pandas as pd 
import numpy as np 
from statsbombpy import sb 
from datetime import datetime

# Accessing interested league games (store them as dictionary/list)

# We are interested in the 2010-2011 season
season_df = sb.matches(competition_id=11, season_id=22)

# Initalizing empty dictionary to store game metadata to use for loop
matches_dict = {}

# Loop through the DataFrame and populate the dictionary
for i, row in season_df.iterrows():
    
    match_id = row['match_id']
    
    # Store the desired columns as values for each match_id
    matches_dict[match_id] = {
        'match_date': row['match_date'],
        'competition': row['competition'],
        'season': row['season'],
        'home_team': row['home_team'],
        'away_team': row['away_team'],
    }


# Framework/Function with Champions League Final game 2010/2011
def etl_pipeline(match_id):

    game_df = sb.events(match_id=match_id)

    # Retrieving data only for passes and shots
    # Pass is the main interest, however, we will get the xA from the xG related to shots
    # Shots data will be also stored in a different table in the warehouse for the final aggregations
    game_df =  game_df[(game_df.pass_end_location.isnull()==False) | (game_df.shot_statsbomb_xg.isnull()==False)]
                            
    game_df = game_df.sort_values(by=['period', 'timestamp'])
    game_df = game_df.reset_index(drop=True) 

    # /////////////////////////////////////////////////////////////
    # ---------------------- EXPECTED THREAT ----------------------
    # /////////////////////////////////////////////////////////////

    # New variables on location
    # Extracting coordinates from original data

    # The data within coordinates is reported as [x, y], we need to create separate variables here, using str.split
    # Two variables to look for: location  pass_end_location

    for i, row in game_df.iterrows():
            
            # Using loc to dynamically create and assign values to new variable 
            try:
                # Passer coordinates
                game_df.loc[i, 'x'] = row['location'][0]
                game_df.loc[i, 'y'] = row['location'][1]
                
                # Receiver coordinates
                # Not all of these values will work since we also have shots that have no 'pass_end_location'
                game_df.loc[i, 'end_x'] = row['pass_end_location'][0]
                game_df.loc[i, 'end_y'] = row['pass_end_location'][1]
                
            
            # For some reason some records are all null and going to the end, we handle them by using the exception
            # They will include the same value as the last possible record
            # we won't need them anyways
            # They dont appear once I re-run the game_df variable assigment. Some issues with merge before probably
            except Exception as e:
                print(f"Row {i} : {e}")


    # xT Grid
    xT = np.array([
        [0.006383, 0.007796, 0.008449, 0.009777, 0.011263, 0.012483, 0.014736, 0.017451, 0.021221, 0.027563, 0.034851, 0.037926],
        [0.007501, 0.008786, 0.009424, 0.010595, 0.012147, 0.013845, 0.016118, 0.018703, 0.024015, 0.029533, 0.040670, 0.046477],
        [0.008880, 0.009777, 0.010013, 0.011105, 0.012692, 0.014291, 0.016856, 0.019351, 0.024122, 0.028552, 0.054911, 0.064426],
        [0.009411, 0.010827, 0.010165, 0.011324, 0.012626, 0.014846, 0.016895, 0.019971, 0.023851, 0.035113, 0.108051, 0.257454],
        [0.009411, 0.010827, 0.010165, 0.011324, 0.012626, 0.014846, 0.016895, 0.019971, 0.023851, 0.035113, 0.108051, 0.257454],
        [0.008880, 0.009777, 0.010013, 0.011105, 0.012692, 0.014291, 0.016856, 0.019351, 0.024122, 0.028552, 0.054911, 0.064426],
        [0.007501, 0.008786, 0.009424, 0.010595, 0.012147, 0.013845, 0.016118, 0.018703, 0.024015, 0.029533, 0.040670, 0.046477],
        [0.006383, 0.007796, 0.008449, 0.009777, 0.011263, 0.012483, 0.014736, 0.017451, 0.021221, 0.027563, 0.034851, 0.037926]
    ])

    xT_rows, xT_cols = xT.shape

    # categorize each record in a bin for starting point and ending point
    game_df['x1_bin'] = pd.cut(game_df['x'], bins = xT_cols, labels=False)
    game_df['y1_bin'] = pd.cut(game_df['y'], bins = xT_rows, labels=False)

    game_df['x2_bin'] = pd.cut(game_df['end_x'], bins = xT_cols, labels=False)
    game_df['y2_bin'] = pd.cut(game_df['end_y'], bins = xT_rows, labels=False)

    # Filling those records with NaN in the end_X or end_y .- These records are shot records, so they will not be included in pass analysis anyways!
    game_df[['x2_bin', 'y2_bin']] = game_df[['x2_bin', 'y2_bin']].fillna(0)

    # change their types to integers for the xT bin assignment to work
    game_df['x2_bin'] = game_df['x2_bin'].astype(int)
    game_df['y2_bin'] = game_df['y2_bin'].astype(int)


    # define the start zone and end zone values of passes (kinda like x,y coordinates in a map plot)
    game_df['start_zone_value'] = game_df[['x1_bin', 'y1_bin']].apply(lambda x: xT[x[1]][x[0]],axis=1)
    game_df['end_zone_value'] = game_df[['x2_bin', 'y2_bin']].apply(lambda x: xT[x[1]][x[0]],axis=1)

    # the difference of end_zone and start_zone is the expected threat value for the action (pass) - not accounting for dribble xT here
    # value can be negative or positive (progressive)
    game_df['pass_xT'] = game_df['end_zone_value'] - game_df['start_zone_value']

    # progressive xT measuring progressive passes
    # Will be interesting to contrast with xGChain
    game_df['progressive_xT'] = ''

    # iterate and fill values for Progressive xT
    counter = 0 

    while counter < len(game_df):
        if game_df['pass_xT'][counter] > 0:
            game_df['progressive_xT'][counter] = game_df['pass_xT'][counter]
        else:
            game_df['progressive_xT'][counter] = 0.00
        counter += 1

    
    # /////////////////////////////////////////////////////////////
    # ---------------------- EXPECTED ASSISTS ----------------------
    # /////////////////////////////////////////////////////////////

    # Literature for Advanced Unconventional Metrics:
    # Quantifying Successful Possession (xPG): https://www.americansocceranalysis.com/home/2018/7/10/ra32uzf18ma2viefm74yjsph8ywywk
    # Possession Value (PV): https://www.statsperform.com/resource/introducing-a-possession-value-framework/
    # xGChain and xGBuildup: https://statsbomb.com/articles/soccer/introducing-xgchain-and-xgbuildup/ ~ THIS ONE COULD BE EXTREMELY USEFUL

    # Validate that the shot record is right after the pas_shot_assist record = CORRECT
    assist_shot = game_df[(game_df.pass_shot_assist.isnull()==False) | (game_df.shot_statsbomb_xg.isnull()==False) ]

    # Next step: add the shot_statsbomb_xg value to the previous record as xA (See Renato Tapia Code)

    # Applying the shift only when the 'type' of the record is 'Pass'. Helps to avoid misassigning xA for shots without previous passes
    # assist_shot.loc[] -> conditionally select rows where the Type is Pass
    # 'Expected Assists -> new column created
    assist_shot.loc[assist_shot['type'] == 'Pass', 'expected_assists_xA'] = assist_shot['shot_statsbomb_xg'].shift(-1)

    # Perform quality chech
    # Output: GOOD

    # Keeping only the xA column since it is what we're interesed in keeping
    # also keep columns that will be in the merge/join
    # merge vs join? determine the best one for the case
    assist_shot = assist_shot[['id', 'pass_assisted_shot_id', 'expected_assists_xA' ]]

    # dropping rows with null values
    assist_shot = assist_shot.dropna()

    # merge on id and pass_assisted_shot_id to the original frame
    # we need to do a left join to the game_df based on 'id'and maybe on 'pass_assisted_shot_id'
    game_df_xA = pd.merge(game_df, assist_shot, how='left', on=['id', 'pass_assisted_shot_id'])


    # /////////////////////////////////////////////////////////////
    # ---------------------- xGChain & xGBuildUp ----------------------
    # /////////////////////////////////////////////////////////////

    '''
    it’s dead simple:

    1. Find all the possessions each player is involved in.
    2. Find all the shots within those possessions.
    3. Sum their xG (you might take the highest xG per possession, or you might treat the shots as dependent events, whatever floats your boat).
    Assign that sum to each player, however involved they were.
    '''


    # step-1: identify chain of passes
    xg_chain = game_df_xA

    # Step 2 - Creating a new variable for cumulative xG

    def xg_cumulative(df):
        
        # Filtering out rows where there is no xG value
        df_filtered = df[df['shot_statsbomb_xg'].notna()]

        # Cumulative sum by posession
        df_filtered['shot_statsbomb_xg_cum'] = df_filtered.groupby('possession')['shot_statsbomb_xg'].cumsum()

        # Merging the cumulative data back to the main df
        df = df.merge(df_filtered[['shot_statsbomb_xg', 'possession', 'shot_statsbomb_xg_cum']], 
                    on=['shot_statsbomb_xg', 'possession'], 
                    how='left')

        return df

    xg_chain = xg_cumulative(xg_chain)

    # Step 3 : 

    def xg_chain_maker(df):
        
        # Identify the shot records where `shot_statsbomb_xg` is not null
        shot_records = df[df['shot_statsbomb_xg_cum'].notnull()]

        # Step 2: Create a mapping from `possession` to `shot_statsbomb_xg`
        xg_mapping = {}
        
        for _, shot in shot_records.iterrows():
            # For each shot record, map its xG to earlier records in the same possession
            before_shot = df[(df['possession'] == shot['possession']) & (df['timestamp'] <= shot['timestamp'])]
            xg_mapping.update({row['id']: shot['shot_statsbomb_xg_cum'] for _, row in before_shot.iterrows()})

        # Step 3: Assign `xGChain` to all records based on `possession` before the shot
        df['xg_chain'] = df['id'].map(xg_mapping)

        return df

    xg_chain = xg_chain_maker(xg_chain)

    # Step 4: xG Build Up
    # Exclude the shooter and the assister for build up

    # Getting rid of the records where there is a xG value
    possession_build = xg_chain[xg_chain.shot_statsbomb_xg.isnull()==True]

    # Getting rid of the shot assissters
    possession_build = possession_build[possession_build.pass_shot_assist.isnull()==True]

    # Create new variable based on xG variable
    possession_build['xg_buildup'] = possession_build['xg_chain']


    # Keeping needed columns for the merge
    possession_build = possession_build[['id', 'xg_buildup']]

    # Dropping null values for efficiency in processing
    possession_build = possession_build.dropna()

    # Merging into the xg_chain dataframe 
    xg_chain = pd.merge(xg_chain, possession_build , how='left', on=['id'])

    # Keeping needeed variables only, to avoid duplicates in the merge
    xg_chain = xg_chain[['id', 'shot_statsbomb_xg_cum', 'xg_chain', 'xg_buildup']]


    # /////////////////////////////////////////////////////////////
    # ------------------------ FINAL MERGE ------------------------
    # /////////////////////////////////////////////////////////////

    final_game_df = pd.merge(game_df_xA, xg_chain, how='left', on=['id'])

    # Adding last_updated column

    # Adding extraction date
    today = datetime.today()
    final_game_df['Extraction Date'] = today

    # Adding match_id, match_date, and home_vs_away


    return final_game_df

# Creating Supabase Schema and doing Historical Load

project_url = 'https://djhafhplvwcwmnmaivue.supabase.co'
supabase_password = 'tznufBzxGccsrg1r'
api_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRqaGFmaHBsdndjd21ubWFpdnVlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MjU3MzM5NzMsImV4cCI6MjA0MTMwOTk3M30.om1taIniduMidTbHSpldcMvmToLJHLWC89Ix27EOxm4'

# Output
for match_id, match_data in matches_dict.items():

    try:

        # Running the ETL function
        data = etl_pipeline(match_id)

        # Add data with match details
        data['match_id'] = match_id
        data['match_date'] = match_data['match_date']
        data['season'] = match_data['season']
        data['competition'] = match_data['competition']
        data['h2h'] = match_data['home_team'] + ' vs ' + match_data['away_team']

        # ['ball_recovery_offensive', 'block_save_block', 'pass_backheel']
        # ['bad_behaviour_card', 'ball_recovery_offensive', 'block_save_block', 'pass_backheel']
        # ['ball_recovery_offensive', 'pass_backheel']
        # ['bad_behaviour_card', 'block_save_block', 'pass_backheel', 'pass_goal_assist']

        
        # STILL NEED TO CLEAN DATA READY FOR INGESTION BUT FOR NOW WE'ER GOOD

        # Potential problems:
            # 1. Ordering of Columns
            # 2. Not all games have the same number of columns
                # - will need to see a workaround for when creating the schema in supabase.
                # - I think it is better to know what I'm looking for and then only keep needed variables and only then do the ingestion. 
                # -  maybe an option for allowing schema evolution could help?
        # Re-ordering the columns
        # getting the last 5 columns
        # cols_to_move = data.columns[-4:]

        # Get the remaining columns
        # remaining_cols = data.columns[:-4]

        # Reorder the DataFrame by placing the last 5 columns at the front
        # data = data[list(cols_to_move) + list(remaining_cols)]

        '''
        data[['match_id', 'match_date', 'season', 'competition', 'h2h', 
        'bad_behaviour_card', 'ball_receipt_outcome', 'ball_recovery_offensive',
       'ball_recovery_recovery_failure', 'block_save_block',
       'carry_end_location', 'clearance_aerial_won', 'counterpress',
       'dribble_outcome', 'dribble_overrun', 'duel_outcome', 'duel_type',
       'duration', 'foul_committed_advantage', 'foul_committed_card',
       'foul_committed_type', 'foul_won_advantage', 'foul_won_defensive',
       'goalkeeper_body_part', 'goalkeeper_end_location', 'goalkeeper_outcome',
       'goalkeeper_position', 'goalkeeper_technique', 'goalkeeper_type', 'id',
       'index', 'interception_outcome', 'location', 'match_id', 'minute',
       'off_camera', 'pass_aerial_won', 'pass_angle', 'pass_assisted_shot_id',
       'pass_backheel', 'pass_body_part', 'pass_cross', 'pass_end_location',
       'pass_goal_assist', 'pass_height', 'pass_length', 'pass_outcome',
       'pass_recipient', 'pass_shot_assist', 'pass_switch', 'pass_type',
       'period', 'play_pattern', 'player', 'player_id', 'position',
       'possession', 'possession_team', 'possession_team_id', 'related_events',
       'second', 'shot_body_part', 'shot_end_location', 'shot_first_time',
       'shot_freeze_frame', 'shot_key_pass_id', 'shot_outcome',
       'shot_statsbomb_xg', 'shot_technique', 'shot_type',
       'substitution_outcome', 'substitution_replacement', 'tactics', 'team',
       'team_id', 'timestamp', 'type', 'under_pressure', 'x', 'y', 'end_x',
       'end_y', 'x1_bin', 'y1_bin', 'x2_bin', 'y2_bin', 'start_zone_value',
       'end_zone_value', 'pass_xT', 'progressive_xT', 'expected_assists_xA',
       'shot_statsbomb_xg_cum', 'xg_chain', 'xg_buildup']]

       '''

        print(f'Success load for {match_id}')


        data.to_csv(f"{match_id}.csv", index=False)

    except Exception as e:
        print(f'Error ocurred for {match_id}: {e}')

    # sleep 



# 69276
# a = etl_pipeline(18236)
a = etl_pipeline(69276)

a.to_excel('QualityCheck_Df_2_Test.xlsx', index=False)

print('All working')

## Ingest into Supabase















