#!/usr/bin/env python
# -*- coding: UTF-8 -*-

__author__ = "Joe McCarthy"
__credits__ = ["Joe McCarthy"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Joe McCarthy"
__email__ = "joe@interrelativity.com"

"""
This module contains code to support the PyData Seattle 2017 workshop on
Unevenly spaced time series analysis of The Simpsons in Pandas"\
https://pydata.org/seattle2017/schedule/presentation/104/
"""


def clean_simpsons_script_lines(df):
    """
    Returns a version of the DataFrame with consistent data types in each column
    """

    # Boolean field: speaking_line

    df.loc[:, 'speaking_line'] = df.speaking_line.apply(
        lambda x: isinstance(x, bool) and x is True 
            or isinstance(x, str) and x == 'true')

    # int fields: timestamp_in_ms, character_id, location_id, word_count

    for column_label in ['timestamp_in_ms', 'character_id', 'location_id', 'word_count']:
        # drop rows with non-numeric strings that cannot be converted to integers
        df = df[df[column_label].apply(lambda x: not isinstance(x, str) or x.isdigit())] 
        df.loc[:, column_label] = df.loc[:, column_label].fillna(0).astype('int64')

    # str fields: raw_character_text, raw_location_text, normalized_text

    for column_label in ['raw_character_text', 'raw_location_text', 'spoken_words', 'normalized_text']:
        df.loc[:, column_label] = df.loc[:, column_label].fillna('')

    # additional cleaning for normalized text values containing only punctuation

    df.loc[(df.normalized_text == '') & (df.spoken_words != ''), 'spoken_words'] = ''

    # remove rows with word_count>=1000 (probably misplaced timestamp_in_ms values)
    # and sort the rows by 'id'
    
    return df[df.word_count<1000].sort_values('id').reset_index(drop=True)
    
    
def create_simpsons_characters_dataframe(df):
    df_characters = df.groupby(
        ['character_id', 'raw_character_text'])['id'].nunique().reset_index().rename(
        columns={'id': 'num_lines'})
    for column_label in ['episode_id', 'season']:
        df_characters = df_characters.merge(
            df.groupby('raw_character_text')[column_label].nunique().reset_index().rename(
                columns={column_label: 'num_{}s'.format(column_label.replace('_id', ''))}),
            on='raw_character_text',
            how='left')
        df_characters = df_characters.merge(
            df.groupby('raw_character_text')[column_label].agg(
                ['first', 'last']).reset_index().rename(
                columns={'first': 'first_{}'.format(column_label), 
                         'last': 'last_{}'.format(column_label)}),
            on='raw_character_text',
            how='left')
    return df_characters