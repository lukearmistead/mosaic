import datetime
import pandas as pd
import prettytable

strava_data_path = "data/strava/activities.csv"
ski_activities = ["AlpineSki", "BackcountrySki"]
weekly_ski_day_goal = 5


def monday_last_week():
    today = datetime.date.today()
    days_since_monday = today.weekday()  # Monday returns 0, Tuesday returns 1
    monday = today - datetime.timedelta(days=days_since_monday + 7)
    return monday

def meters_to_feet(meter):
    return meter * 3.280839895

class Strava:
    def __init__(strava_data_path):
        df = pd.read_csv(strava_data_path)
        df = self.process(df)

        self.ski = df['type'].isin(["AlpineSki", "BackcountrySki"])

        ski_mask = self.df['type'].isin(["AlpineSki", "BackcountrySki"])

    def process_df(self, df):
        for date_col in ['start_date', 'start_date_local']:
            df[date_col] = pd.to_datetime(df[date_col])
        df['date'] = df['start_date_local'].dt.date

        for measure in ['distance', 'total_elevation_gain']
            for activity_type in df['type'].unique():
                m = df['type'] == activity_type
                avg = df.loc[m, measure].mean()
                df.loc[m, measure]n= df.loc[m, measure].replace({0.0: avg})
        return df

    def get_total_ski_days(self):
        total_ski_days = len(self.df.loc[ski_mask, 'date'].unique())



def main():
    pass


if __name__ == '__main__':
    strava = pd.read_csv(strava_data_path)
    ski_mask = strava['type'].isin(ski_activities)
    ski = strava.loc[ski_mask,]
    ski['date'] = pd.to_datetime(ski['start_date_local']).dt.date
    ski['total_elevation_gain'] = ski['total_elevation_gain'].fillna('mean') 

    total_ski_days = len(ski['date'].unique())
    total_ski_vert = meters_to_feet(ski['total_elevation_gain'].sum())
    print('total_ski_days', total_ski_days)
    print('total_ski_vert', total_ski_vert)
    last_monday = monday_last_week()
    print('last_monday', last_monday)
    last_sunday = last_monday + datetime.timedelta(days=6)
    print('last_sunday', last_sunday)
    last_week_mask = ski['date'].between(last_monday, last_sunday)
    last_week_ski_days = len(ski.loc[last_week_mask, 'date'].unique())
    last_week_ski_vert = meters_to_feet(ski.loc[last_week_mask, 'total_elevation_gain'].sum())
    print(ski.loc[last_week_mask, ['total_elevation_gain', 'date']])
    print('last_week_ski_days', last_week_ski_days)
    print('last_week_ski_vert', last_week_ski_vert)
