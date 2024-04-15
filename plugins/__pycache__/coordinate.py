import pandas as pd
import datetime

def clean_df_coordinate(df_coordinate):
    df_coordinate= df_coordinate[df_coordinate["latitude"].notnull()]
    df_coordinate= df_coordinate[df_coordinate["longitude"].notnull()]

    df_coordinate=df_coordinate[df_coordinate.longitude.apply(type) != datetime.datetime]
    df_coordinate=df_coordinate[df_coordinate.latitude.apply(type) != datetime.datetime]
    df_coordinate=df_coordinate[df_coordinate.longitude.apply(type) != str]
    df_coordinate=df_coordinate[df_coordinate.latitude.apply(type) != str]

    df_coordinate=df_coordinate.sort_values(['commune_code','code'],ascending=True)
    
    return df_coordinate