import pandas as pd

class Calculations:
    def __init__(self, files):
        self.trips = self.produce_trips_table(files)
        print("Trips initialized:", isinstance(self.trips, pd.DataFrame))
        self.daily_counts = self.calculate_daily_counts(self.trips)
        self.monthly_counts = self.calculate_monthly_counts(self.trips)
        
    def get_trips(self):
        return self.trips
    
    def get_daily_counts(self):
        return self.daily_counts

    def get_monthly_counts(self):
        return self.monthly_counts

    def produce_trips_table(self, files):
        dataframes = [pd.read_csv(file) for file in files]
        trips = pd.concat(dataframes)
        # Ensure column names match the CSV files
        trips = trips[['Bikeid', 'Starttime', 'From station id', 'To station id']]
        trips['Starttime'] = pd.to_datetime(trips['Starttime'])
        return trips
    
    def calculate_daily_counts(self, trips):
        trips['date'] = trips['Starttime'].dt.date
        from_cnt = trips.groupby(['date', 'From station id']).size().reset_index(name='fromCNT')
        to_cnt = trips.groupby(['date', 'To station id']).size().reset_index(name='toCNT')
        rebalanced = self.calculate_rebalancing(trips)
        # Merge the counts and then rebalancing data
        daily_counts = from_cnt.merge(to_cnt, left_on=['date', 'From station id'], right_on=['date', 'To station id'], how='outer')
        daily_counts = daily_counts.merge(rebalanced, on=['date', 'station_id'], how='left')
        daily_counts['fromCNT'] = daily_counts['fromCNT'].fillna(0).astype(int)
        daily_counts['toCNT'] = daily_counts['toCNT'].fillna(0).astype(int)
        daily_counts['rebalCNT'] = daily_counts['rebalCNT'].fillna(0).astype(int)
        daily_counts['date'] = daily_counts['date'].apply(lambda x: x.strftime('%m/%d/%Y'))
        daily_counts['station_id'] = daily_counts['From station id'].fillna(daily_counts['To station id']).astype(int)
        daily_counts = daily_counts.drop(columns=['From station id', 'To station id'])
        return daily_counts.sort_values(['date', 'station_id'])
    
    def calculate_monthly_counts(self, trips):
        trips['month'] = trips['Starttime'].dt.to_period('M').dt.strftime('%m/%Y')
        from_cnt = trips.groupby(['month', 'From station id']).size().reset_index(name='fromCNT')
        to_cnt = trips.groupby(['month', 'To station id']).size().reset_index(name='toCNT')
        rebalanced = self.calculate_rebalancing(trips, 'monthly')
        # Merge the counts and then rebalancing data
        monthly_counts = from_cnt.merge(to_cnt, left_on=['month', 'From station id'], right_on=['month', 'To station id'], how='outer')
        monthly_counts = monthly_counts.merge(rebalanced, on=['month', 'station_id'], how='left')
        monthly_counts['fromCNT'] = monthly_counts['fromCNT'].fillna(0).astype(int)
        monthly_counts['toCNT'] = monthly_counts['toCNT'].fillna(0).astype(int)
        monthly_counts['rebalCNT'] = monthly_counts['rebalCNT'].fillna(0).astype(int)
        monthly_counts['station_id'] = monthly_counts['From station id'].fillna(monthly_counts['To station id']).astype(int)
        monthly_counts = monthly_counts.drop(columns=['From station id', 'To station id'])
        return monthly_counts.sort_values(['month', 'station_id'])
    
    def calculate_rebalancing(self, trips, period='daily'):
        # Sort the trips by Bikeid and Starttime to find rebalancing events
        trips_sorted = trips.sort_values(['Bikeid', 'Starttime'])
        trips_sorted['next_from_station'] = trips_sorted['From station id'].shift(-1)
        trips_sorted['next_bikeid'] = trips_sorted['Bikeid'].shift(-1)
        trips_sorted['next_starttime'] = trips_sorted['Starttime'].shift(-1)
        rebalanced = trips_sorted[
            (trips_sorted['To station id'] != trips_sorted['next_from_station']) &
            (trips_sorted['Bikeid'] == trips_sorted['next_bikeid']) &
            (trips_sorted['next_starttime'] > trips_sorted['Starttime'])
        ]
        # Count the rebalancing events by day or month
        if period == 'daily':
            rebal_cnt = rebalanced.groupby(['date', 'To station id']).size().reset_index(name='rebalCNT')
            rebal_cnt.rename(columns={'To station id': 'station_id'}, inplace=True)
        else:  # monthly
            rebal_cnt = rebalanced.groupby([trips_sorted['Starttime'].dt.to_period('M').dt.strftime('%m/%Y'), 'To station id']).size().reset_index(name='rebalCNT')
            rebal_cnt.rename(columns={'To station id': 'station_id'}, inplace=True)
        return rebal_cnt

if __name__ == "__main__":
    files = ['HealthyRideRentals2021-Q1.csv', 'HealthyRideRentals2021-Q2.csv', 'HealthyRideRentals2021-Q3.csv']
    calculations = Calculations(files)
    print("-------------- Trips Table ---------------")
    print(calculations.get_trips().head(10))
    print("\n-------------- Daily Counts ---------------")
    print(calculations.get_daily_counts().head(10))
    print("\n------------- Monthly Counts---------------")
    print(calculations.get_monthly_counts().head(10))
    print()

