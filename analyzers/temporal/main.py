import polars as pl
import numpy as np
import pandas as pd
import plotly.express as px
import plotext as plt

CSV__INPUT = 'reddit_vm'

def load_csv(CSV__INPUT: str):
    """
    Given user selection in the CLI of a CSV to analyze, the CSV is read in as a Polars dataframe and an attempt at auto-detection of
    the datetime column is made.

    Parameters:
    - CSV__INPUT: the name of the csv file in string format

    Returns:
    - df: a Polars dataframe made from the loaded CSV with a chosen datetime column formatted.
    - datetime_column_name: a string value of the chosen datetime column name. 

    # TODO: List out all possible datetime formats that Polars can automatically detect for transparency to non-technical users. We can potentially code out other options that Polars does not cover. 
    """

    df = pl.read_csv(f'../../sample_data/{CSV__INPUT}.csv', try_parse_dates = True)
    dtype_dict = dict(zip(df.columns, df.dtypes))

    datetime_cols = [col for col, dtype in dtype_dict.items() if isinstance(dtype, pl.Datetime)]
    if len(datetime_cols) == 0:
        raise TypeError('No datetime columns were found.')
    elif len(datetime_cols) > 1:
        print(f'More than one datetime column found. Please choose one of the following to analyze: {datetime_cols}')
        # TODO: Create CLI interaction where user must choose which datetime column to analyze and relabel dataset.
        raise TypeError('Multiple datetime columns were found. Choosing a column on CLI is not yet supported, please remove other datetime columns for the time being.')
    else:
        print(f'Datetime column found: {datetime_cols[0]}')
        datetime_col_name = datetime_cols[0]

    print(df.head(5))
    return df, datetime_col_name

def process_datetime_feature_engineering(df: pl.DataFrame, datetime_col_name: str):
    """
    Based off of a datetime column, create useful datetime-related columns such as hour, minute, and minute of day. 

    Parameters:
    - df: a Polars dataframe containing a column with the datetime datatype.
    - datetime_col_name: a chosen column name (str) resembling the datetime data

    Returns:
    - df: a Polars dataframe with additional columns extracted from datetime_col_name of df

    """
    
    df = df.with_columns(
            pl.col(datetime_col_name).dt.cast_time_unit("ms").dt.replace_time_zone(None)
        ).with_columns(
            (pl.col(datetime_col_name).dt.hour()).cast(int).alias("hour"), # extract hour from timestamp and ensure that the integer type has a large enough bit-size (defaults to i8 without casting)
            (pl.col(datetime_col_name).dt.minute().cast(int).alias("minute")),  # extract minute from timestamp and ensure that the integer type has a large enough bit-size (defaults to i8 without casting)
        ).with_columns(
            (pl.col("hour")*60 + pl.col("minute")).alias("minute_of_day") # get the minute marker in the day
        )
    return df

def create_time_interval_label_dict(TIME__INTERVAL__LENGTH: int):
    """
    A helper function to generate a dictionary assigned time period index to time period labels
    """
    # extract the hour of the day and minute of the hour for the starting time of each period
    start_time_array = np.array([str(i//60).zfill(2) + ":" + str(int(np.round(((i/60)%1)*60))).zfill(2) for i in np.arange(0, 1440, TIME__INTERVAL__LENGTH)])
    
    time_interval_dict = {}
    for i in range(len(start_time_array)):
        if i == len(start_time_array) - 1:
            time_interval_dict[i] = start_time_array[i] + "-" + "00:00"
        else:
            time_interval_dict[i] = start_time_array[i] + "-" + start_time_array[i+1]
    return time_interval_dict

def analyze_time_of_day(df, TIME__INTERVAL__LENGTH=60):
    """
    Based on the provided Polars dataframe and a specified number of minutes to divide a day up into, group the data 
    into TIME__INTERVAL__LENGTH-long periods. After that, convert it into a Pandas dataframe and display a bar graph using
    Plotly Express detailing the record frequency over time. 

    Parameters:
    - df: a Polars dataframe containing a column with the datetime datatype.
    - datetime_col_name: a chosen column name (str) resembling the datetime data

    Returns:
    - df: a Polars dataframe with additional columns extracted from datetime_col_name of df

    """
    if (1440/TIME__INTERVAL__LENGTH % 1) != 0:
        print(f"Warning: The number of minutes you provided ({TIME__INTERVAL__LENGTH}) do not divide evenly so the created time periods will not be equal.\
        \nWe recommend numbers that are factors of 1440 such as 10, 15, 30, 45, 60, 120, etc.")
        # TODO: Add a CLI of 'Would you like to proceed?'

    # Group the dataframe based on TIME__INTERVAL__LENGTH parameter
    grouped_df = df.with_columns(
                    (pl.col("minute_of_day") // TIME__INTERVAL__LENGTH).alias("time_interval")
                ).group_by("time_interval").agg([
                    pl.col("minute_of_day").count().alias("count")
                ]).sort("time_interval")

    # Relabel time_interval values to time periods.
    time_interval_label_dict = create_time_interval_label_dict(TIME__INTERVAL__LENGTH)
    grouped_df = grouped_df.with_columns(
                    grouped_df['time_interval'].replace_strict(time_interval_label_dict).alias("time_interval")
                )

    return grouped_df

def plot_time_of_day_to_terminal(grouped_df: pl.DataFrame, TIME__INTERVAL__LENGTH: int):
    """
    Plot the grouped Polars dataframe on the CLI.
    """
    grouped_pd_df = grouped_df.to_pandas()
    plt.bar(grouped_df['time_interval'], grouped_df['count'])
    plt.title(f'Count of Records by Time of Day ({TIME__INTERVAL__LENGTH}-min intervals)')
    plt.xlabel(f'{TIME__INTERVAL__LENGTH}-Minute Interval Label')
    plt.ylabel('Count')
    plt.xticks(np.arange(0, len(grouped_df['time_interval']) + 1, 5), np.array(grouped_df['time_interval'][::5])) # ! struggles to plot first x-tick when TIME__INTERVAL__LENGTH = 60 (bug with plotext?)
    plt.yticks(np.arange(0, grouped_df['count'].max()+5, 5), np.arange(0, grouped_df['count'].max()+5, 5))
    plt.show()

def plot_time_of_day_to_plotly(grouped_df: pl.DataFrame, TIME__INTERVAL__LENGTH: int, save_fig=False, save_method='html', filename=f'frequency_bar_graph'):
    """ 
    Plot the grouped Polars dataframe on Plotly with the option to export as HTML, PNG, etc. 
    """
    
    # Create the Plotly bar graph
    fig = px.bar(grouped_df.to_pandas(), 
                x='time_interval', 
                y='count', 
                orientation='v',
                title=f'Count of Records by Time of Day ({TIME__INTERVAL__LENGTH}-min intervals)', 
                labels={'time_interval': f'{TIME__INTERVAL__LENGTH}-Minute Interval Label', 'count': 'Count'},
            )

    # Show the plot
    # fig.show() # TODO: Allow for graph export and/or upload to HTML

    if save_fig:
        if save_method == 'html':
            fig.write_html(f"{filename}.html")
        if save_method == 'png':
            fig.write_image(f"{filename}.png")

def save_df_to_csv(df: pl.DataFrame, filename: str):
    """
    Save the Polars dataframe to a CSV with name filename.
    """
    df.to_pandas().to_csv(f'{filename}.csv')

if __name__ == "__main__":
    CSV__INPUT = 'reddit_vm'    
    df, datetime_col_name = load_csv(CSV__INPUT)
    df = process_datetime_feature_engineering(df, datetime_col_name)

    TIME__INTERVAL__LENGTH = 60
    grouped_df = analyze_time_of_day(df, TIME__INTERVAL__LENGTH)
    plot_time_of_day_to_terminal(grouped_df, TIME__INTERVAL__LENGTH)
    plot_time_of_day_to_plotly(grouped_df, TIME__INTERVAL__LENGTH)
    # save_df_to_csv(grouped_df, 'time_interval_analysis') # TODO: if user elects to export to CSV