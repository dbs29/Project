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
            
           
    

        trips = trips[['Bikeid', 'Starttime', 'From station id', 'To station id']]
        trips['Starttime'] = pd.to_datetime(trips['Starttime'])
        
        return trips
    
    def calculate_daily_counts(self, trips):
        trips['Starttime'] = pd.to_datetime(trips['Starttime'])
       
        
        daily_counts = trips.groupby(['Starttime', 'From station id'])['Bikeid'].size().reset_index(name='fromCNT')
        daily_counts['toCNT'] = trips.groupby(['Starttime', 'To station id'])['Bikeid'].count().reset_index(name='toCNT')['toCNT']
        daily_counts['rebalCNT'] = daily_counts['fromCNT'] - daily_counts['toCNT']
        
        
        
        daily_counts = daily_counts.rename(columns={'Starttime': 'day', 'From station id': 'station_id'})
        daily_counts['day'] = daily_counts['day'].dt.strftime('%m/%d/%Y').astype(str)
        daily_counts['station_id'] = daily_counts['station_id'].astype(int)
        daily_counts['fromCNT'] = daily_counts['fromCNT'].fillna(0)
        daily_counts['toCNT'] = daily_counts['toCNT'].fillna(0)
        daily_counts['rebalCNT'] = daily_counts['rebalCNT'].fillna(0)
        daily_counts['fromCNT'] = daily_counts['fromCNT'].astype(int)
        daily_counts['toCNT'] = daily_counts['toCNT'].astype(int)
        daily_counts['rebalCNT'] = daily_counts['rebalCNT'].astype(int)
        daily_counts = daily_counts.sort_values(by='rebalCNT', ascending=False)
        return daily_counts
        
    
       
        
        

    def calculate_monthly_counts(self, trips):
        monthly_counts = trips.groupby([trips['Starttime'].dt.to_period('M'), 'From station id'])['Bikeid'].count().reset_index(name='fromCNT')
        monthly_counts['toCNT'] = trips.groupby([trips['Starttime'].dt.to_period('M'), 'To station id'])['Bikeid'].count().reset_index(name='toCNT')['toCNT']
        monthly_counts['rebalCNT'] = abs(monthly_counts['fromCNT'] - monthly_counts['toCNT'])
        monthly_counts = monthly_counts.rename(columns={'Starttime': 'month', 'From station id': 'station_id'})
        monthly_counts['month'] = monthly_counts['month'].dt.strftime('%m/%Y')
        monthly_counts['station_id'] = monthly_counts['station_id'].astype(int)
        monthly_counts = monthly_counts.sort_values(by='rebalCNT', ascending=False)
        return monthly_counts
    
            
if __name__ == "__main__":
    calculations = Calculations(['HealthyRideRentals2021-Q1.csv', 'HealthyRideRentals2021-Q2.csv', 'HealthyRideRentals2021-Q3.csv'])
    print("-------------- Trips Table ---------------")
    print(calculations.get_trips().head(10))
    print()
    print("-------------- Daily Counts ---------------")
    print(calculations.get_daily_counts().head(10))
    print()
    print("------------- Monthly Counts---------------")
    print(calculations.get_monthly_counts().head(10))
    print()