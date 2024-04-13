
import datetime

def clean_df_coordinate(df_coordinate):
    cleaned_df_coordinate = df_coordinate[df_coordinate["longitude"].notnull()]
    
    cleaned_df_coordinate=cleaned_df_coordinate[df_coordinate.commune_code.apply(type) != datetime.datetime]
    cleaned_df_coordinate=cleaned_df_coordinate[df_coordinate.longitude.apply(type) != datetime.datetime]
    cleaned_df_coordinate=cleaned_df_coordinate[df_coordinate.latitude.apply(type) != datetime.datetime]
    cleaned_df_coordinate=cleaned_df_coordinate[df_coordinate.code.apply(type) != datetime.datetime]
    
    cleaned_df_coordinate=cleaned_df_coordinate[df_coordinate.commune_code.apply(type) != str]
    cleaned_df_coordinate=cleaned_df_coordinate[df_coordinate.longitude.apply(type) != str]
    cleaned_df_coordinate=cleaned_df_coordinate[df_coordinate.latitude.apply(type) != str]
    cleaned_df_coordinate=cleaned_df_coordinate[df_coordinate.code.apply(type) != str]
    
    return cleaned_df_coordinate