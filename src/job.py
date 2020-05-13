import pandas as pd
from hashlib import md5
import datetime


def run():
    data = pd.read_csv("input_data/weather.20160201.csv")
    pd_geo_location_dim = get_geo_location(data)
    save_geo_dimension(pd_geo_location_dim)
    pd_site_info_dim = get_site_info(data)
    save_site_dimension(pd_site_info_dim)
    pd_weather_fact = get_weather_fact(data)


def get_geo_location(data):
    geo_location = data[['Region', 'Country']].copy()
    geo_location = geo_location.drop_duplicates(subset=['Region', 'Country'])
    geo_location = geo_location[pd.notnull(geo_location['Country'])]
    geo_location['key_column'] = geo_location.apply(lambda row: row.Region + row.Country, axis=1)
    geo_location['location_id'] = (geo_location
                                   .apply(lambda row: str(int(md5(row.key_column.encode('utf-8')).hexdigest(), 16)),
                                          axis=1)
                                   )
    geo_location = geo_location.drop(columns='key_column').reset_index(drop=True)
    return geo_location


def get_site_info(data):
    site_info = data[['ForecastSiteCode', 'SiteName', 'Latitude', 'Longitude']].copy()
    site_info = site_info.drop_duplicates(subset=['ForecastSiteCode', 'SiteName', 'Latitude', 'Longitude'])
    site_info['SiteName'] = site_info.apply(lambda row: row.SiteName[:-1 * (len(str(row.ForecastSiteCode)) + 2)],
                                            axis=1)
    site_info['site_id'] = (site_info
                            .apply(lambda row: str(int(md5(str(row.ForecastSiteCode)
                                                       .encode('utf-8')).hexdigest(), 16)), axis=1)
                            )
    site_info = site_info.sort_values('ForecastSiteCode').reset_index(drop=True)
    return site_info


def time_conversion(t):
    return datetime.datetime.strptime(str(t), '%H').strftime("%H:%M")


def fill_values(series):
    values_counted = series.value_counts()
    if values_counted.empty:
        return series
    most_frequent = values_counted.index[0]
    new_country = series.fillna(most_frequent)
    return new_country


def get_weather_fact(data):
    weather_fact = data.copy()
    weather_fact['ObservationDate'] = weather_fact.ObservationDate.str[:-9]
    weather_fact['ObservationTime'] = weather_fact['ObservationTime'].apply(lambda x: time_conversion(x))
    weather_fact['SiteName'] = weather_fact.apply(lambda row: row.SiteName[:-1 * (len(str(row.ForecastSiteCode)) + 2)],
                                                  axis=1)
    group_country = weather_fact.groupby('Region')['Country']
    weather_fact.loc[:, 'Country'] = group_country.transform(fill_values)
    return weather_fact


def save_geo_dimension(data):
    try:
        geo_dim = pd.read_parquet("output_data/geo_dimension.parquet", engine="pyarrow")
    except OSError:
        data.to_parquet("output_data/geo_dimension.parquet", engine="pyarrow")
        return
    pd_geo_dim = pd.merge(geo_dim, data, on='location_id', how='outer')
    pd_geo_dim.to_parquet("output_data/geo_dimension.parquet", engine="pyarrow")
    return


def save_site_dimension(data):
    try:
        site_dim = pd.read_parquet("output_data/site_dimension.parquet", engine="pyarrow")
    except OSError:
        data.to_parquet("output_data/site_dimension.parquet", engine="pyarrow")
        return
    pd_site_dim = pd.merge(site_dim, data, on='site_id', how='outer')
    pd_site_dim.to_parquet("output_data/site_dimension.parquet", engine="pyarrow")
    return
