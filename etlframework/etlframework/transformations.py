'''
Module to define transformations on dataframes
'''
import pyspark.sql.functions as F


def transform_data(input_df):
    ''' This function takes a dataframe as input and returns a transformed
    dataframe'''
    transformed_df = (input_df
                      .groupBy('Location',)
                      .agg(F.sum('ItemCount').alias('TotalItemCount')))
    return transformed_df
