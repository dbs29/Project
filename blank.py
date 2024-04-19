trips['Starttime'] = pd.to_datetime(trips['Starttime'])
     # Ensure Starttime is parsed as a datetime
        trips['Starttime'] = pd.to_datetime(trips['Starttime'])
        
        # Group by day and station_id to calculate counts
        daily_counts = trips.groupby([trips['Starttime'].dt.strftime('%m/%d/%Y'), 'From station id']).size().reset_index(name='fromCNT')
        
        #daily_counts = daily_counts.groupby(['Starttime', 'From station id'])['fromCNT'].sum().reset_index()
        daily_counts = daily_counts.sort_values(by='fromCNT', ascending=False)
        # Calculate toCNT and rebalCNT
        daily_counts['toCNT'] = trips.groupby([trips['Starttime'].dt.strftime('%m/%d/%Y'), 'To station id']).size().reset_index(name='toCNT')['toCNT']
        daily_counts['rebalCNT'] = np.where(daily_counts['fromCNT'] + daily_counts['toCNT'] == 0, 1, 0)
        
        # Rename columns and format date
        daily_counts = daily_counts.rename(columns={'Starttime': 'day', 'From station id': 'station_id'})
        daily_counts['station_id'] = daily_counts['station_id'].astype(int)
        print(daily_counts.head(100250))
        return daily_counts






daily_counts = trips.groupby(['Starttime', 'From station id'])['Bikeid'].count().reset_index(name='fromCNT')
        daily_counts['toCNT'] = trips.groupby(['Starttime', 'To station id'])['Bikeid'].count().reset_index(name='toCNT')['toCNT']
        daily_counts['rebalCNT'] = daily_counts['fromCNT'] + daily_counts['toCNT']
        daily_counts = daily_counts.rename(columns={'Starttime': 'day', 'From station id': 'station_id'})
        daily_counts['day'] = daily_counts['day'].dt.strftime('%m/%d/%Y')
        daily_counts['station_id'] = daily_counts['station_id'].astype(int)
        return daily_counts