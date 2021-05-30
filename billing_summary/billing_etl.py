import pandas as pd

#Import Billing Data
def run():

    # Load raw data from resources
    raw_billing_data = pd.read_csv(r"./billing_summary/resources/billings_europe.csv",header=None)
    #raw_billing_data = pd.read_csv(r"./resources/billings_europe.csv",header=None)
    
    # Transform raw inputs to starndardize MDB (master database)
    raw_billing_data = raw_billing_data.iloc[4:,:]
    raw_billing_data = raw_billing_data.transpose()
    raw_billing_data.iloc[0,0] = "Segment - Period"
    raw_billing_data.iloc[0,1] = "Type"
    raw_billing_data.iloc[0,2] = "Subtype"
    raw_billing_data.columns = raw_billing_data.iloc[0]
    raw_billing_data.drop([0,1],axis=0,inplace=True)
    raw_billing_data = pd.melt(raw_billing_data, id_vars=['Segment - Period', 'Type', 'Subtype'], value_name='Value')
    raw_billing_data[['Segment', 'Period']] =  raw_billing_data['Segment - Period'].str.split(" - ",expand=True)
    raw_billing_data.drop('Segment - Period', axis=1,inplace=True)
    raw_billing_data = raw_billing_data.rename(columns={raw_billing_data.columns[2]:"Date"})
    billing_data = raw_billing_data[['Date','Segment', 'Period', 'Type', 'Subtype', 'Value']]
    billing_data["Type"].fillna(method='ffill', inplace=True)
    billing_data = billing_data.astype({'Value': float, 'Date': 'datetime64[ns]'})
    billing_data = billing_data[billing_data.Value.notnull()==True]

    # Calculate sum of billings by country
    sum_of_billings_country = billing_data[billing_data['Subtype'].str.contains(pat = r'\b[A-Z][A-Z]\b', regex = True) == True]
    sum_of_billings_country = sum_of_billings_country.groupby(['Subtype']).Value.sum().reset_index()
    sum_of_billings_country = sum_of_billings_country.rename(columns={'Subtype':'Countries', 'Value':'Billings'})

    # Calculate sum of billings by period 
    sum_of_billings_period = billing_data[billing_data['Type'].str.contains(pat = r'Market', regex = False) == True]
    sum_of_billings_period = sum_of_billings_period[sum_of_billings_period['Date']>='2016-01-01']
    sum_of_billings_period = sum_of_billings_period.groupby(['Period']).Value.sum().reset_index()
    sum_of_billings_period = sum_of_billings_period.rename(columns={'Value':'Billings'})

    # Calculate summary statistics by segment
    summary_statistics_measure_of_spread = billing_data.groupby("Segment").agg(
        {
            "Value": ["size","var","sem","skew"]
        }
    )
    summary_statistics_kurtosis = billing_data.groupby("Segment").apply(pd.DataFrame.kurt)
    summary_statistics_measure_of_location = billing_data.groupby("Segment").describe()
    summary_statistics = pd.merge(summary_statistics_measure_of_spread,summary_statistics_measure_of_location,how='left',on='Segment')
    summary_statistics = pd.merge(summary_statistics,summary_statistics_kurtosis,how='left',on='Segment')

    print(summary_statistics)
    summary_statistics.info()

    return None

run()