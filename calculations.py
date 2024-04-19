import os
import pandas as pd
import datetime
import numpy as np

class Calculations:
    def __init__(self, files):
        self.trips = self.produce_trips_table(files)
        print("Trips initialized:", isinstance(self.trips, pd.DataFrame))
        self.daily_counts = self.calculate_daily_counts(self.get_trips())
        self.monthly_counts = self.calculate_monthly_counts(self.get_trips())
        
    
    def get_trips(self):
        return self.trips
        
    
    def get_daily_counts(self):
        return self.daily_counts

    def get_monthly_counts(self):
        return self.monthly_counts

    def produce_trips_table(self, files):
        # DataFrame must have at least the 'Bikeid', 'Starttime', 'Trip id', 'From station id', 'To station id' columns
        dataframes = [pd.read_csv(file) for file in files]
        trips = pd.concat(dataframes)
        trips = trips[['Bikeid', 'starttime', 'From station id', 'To Station id']]
        trips['Starttime'] = pd.to_datetime(trips['Starttime'])
        return trips
    
    def calculate_daily_counts(self, trips):
        trips['date'] = trips['Starttime'].dt.date
        from_cnt = trips.groupby(['date', 'From station id']).size().reset_index(name='fromCNT')
        to_cnt = trips.groupby(['date', 'To station id']).size().reset_index(name='toCNT')
        daily_counts = pd.merge(from_cnt, to_cnt, how='outer', left_on=['date', 'From station id'], right_on=['date', 'To station id'])
        daily_counts['station_id'] = daily_counts['From station id'].fillna(daily_counts['To station id']).astype(int)
        daily_counts = daily_counts.drop(columns=['From station id', 'To station id'])
        daily_counts[['fromCNT', 'toCNT']] = daily_counts[['fromCNT', 'toCNT']].fillna(0).astype(int)
        daily_counts['date'] = daily_counts['date'].apply(lambda x: x.strftime('%m/%d/%Y'))
        return daily_counts.sort_values(['date', 'station_id'])

    def calculate_monthly_counts(self, trips):
        trips['month'] = trips['Starttime'].dt.to_period('M')
        from_cnt = trips.groupby(['month', 'From station id']).size().reset_index(name='fromCNT')
        to_cnt = trips.groupby(['month', 'To station id']).size().reset_index(name='toCNT')
        monthly_counts = pd.merge(from_cnt, to_cnt, how='outer', left_on=['month', 'From station id'], right_on=['month', 'To station id'])
        monthly_counts['station_id'] = monthly_counts['From station id'].fillna(monthly_counts['To station id']).astype(int)
        monthly_counts = monthly_counts.drop(columns=['From station id', 'To station id'])
        monthly_counts[['fromCNT', 'toCNT']] = monthly_counts[['fromCNT', 'toCNT']].fillna(0).astype(int)
        monthly_counts['month'] = monthly_counts['month'].apply(lambda x: x.strftime('%m/%Y'))
        return monthly_counts.sort_values(['month', 'station_id'])
    
            
if __name__ == "__main__":
    files = ['HealthyRideRentals2021-Q1.csv', 'HealthyRideRentals2021-Q2.csv', 'HealthyRideRentals2021-Q3.csv']
    calculations = Calculations(files)
    print("-------------- Trips Table ---------------")
    print(calculations.get_trips().head(10))
    print()
    print("-------------- Daily Counts ---------------")
    print(calculations.get_daily_counts().head(10))
    print()
    print("------------- Monthly Counts---------------")
    print(calculations.get_monthly_counts().head(10))
    print()
