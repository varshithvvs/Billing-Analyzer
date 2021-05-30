import pandas as pd
from loguru import logger


class Variables:
    """Placeholder for resued strings across model
    """
    SEGMENT_PERIOD = 'Segment - Period'
    TYPE = 'Type'
    SUBTYPE = 'Subtype'


class EuropeBilling:
    def __init__(self, source_path, destination_path):
        """Europe billing model to generate required metrics

        :param source_path: Source for raw input file
        :type source_path: str
        :param destination_path: Destination path to generate .xlsx report
        :type destination_path: str
        """
        self.source_path = source_path
        self.destination_path = destination_path

    def workflow(self):
        """General workfloe to process required metrics and generate the report 
        """
        raw_billing_data = self.load_csv()
        billing_data = self.generate_mdb(raw_billing_data)
        sum_of_billings_country = self.calc_sum_of_billings_country(
            billing_data)
        sum_of_billings_period = self.calc_sum_of_billings_period(billing_data)
        summary_statistics = self.calc_summary_statistics(billing_data)
        self.generate_report(sum_of_billings_country, sum_of_billings_period,
                             summary_statistics)

    def load_csv(self):
        """Load raw data from resources

        :return: Returning loaded raw data as pd.DataFrame
        :rtype: pd.DataFrame
        """
        logger.debug(f'Loading raw inputs from {self.source_path}')
        raw_billing_data = pd.read_csv(self.source_path, header=None)
        logger.success(f'Loaded raw inputs sucessfully')
        return raw_billing_data

    def generate_mdb(self, raw_billing_data):
        """Transform raw inputs to starndardized MDB (master database)

        :param raw_billing_data: Raw billing data read from soruce path
        :type raw_billing_data: pd.DataFrame
        :return: Processed billing data in with standard structure 
        :rtype: pd.DataFrame
        """
        logger.debug(f'Transforming input from: {self.source_path}')
        raw_billing_data = raw_billing_data.iloc[4:, :]
        raw_billing_data = raw_billing_data.transpose()
        raw_billing_data.iloc[0, 0] = Variables.SEGMENT_PERIOD
        raw_billing_data.iloc[0, 1] = Variables.TYPE
        raw_billing_data.iloc[0, 2] = Variables.SUBTYPE
        raw_billing_data.columns = raw_billing_data.iloc[0]
        raw_billing_data.drop([0, 1], axis=0, inplace=True)
        raw_billing_data = pd.melt(raw_billing_data,
                                   id_vars=[
                                       Variables.SEGMENT_PERIOD,
                                       Variables.TYPE, Variables.SUBTYPE
                                   ],
                                   value_name='Value')
        raw_billing_data[[
            'Segment', 'Period'
        ]] = raw_billing_data[Variables.SEGMENT_PERIOD].str.split(' - ',
                                                                  expand=True)
        raw_billing_data.drop(Variables.SEGMENT_PERIOD, axis=1, inplace=True)
        raw_billing_data = raw_billing_data.rename(
            columns={raw_billing_data.columns[2]: 'Date'})
        billing_data = raw_billing_data[[
            'Date', 'Segment', 'Period', Variables.TYPE, Variables.SUBTYPE,
            'Value'
        ]]
        billing_data[Variables.TYPE].fillna(method='ffill', inplace=True)
        billing_data = billing_data.astype({
            'Value': float,
            'Date': 'datetime64[ns]'
        })
        billing_data = billing_data[billing_data.Value.notnull() == True]
        logger.success('Master database generated')
        return billing_data

    def calc_sum_of_billings_country(self, billing_data):
        """Calculates sum of billings by country using billing data

        :param billing_data: Billing data MDB
        :type billing_data: pd.DataFrame
        :return: Sum of billings by country
        :rtype: pd.DataFrame
        """
        logger.debug('Calculating the sum of billings by country')
        sum_of_billings_country = billing_data[
            billing_data[Variables.SUBTYPE].str.contains(pat=r'\b[A-Z][A-Z]\b',
                                                         regex=True) == True]
        sum_of_billings_country = sum_of_billings_country.groupby(
            [Variables.SUBTYPE]).Value.sum().reset_index()
        sum_of_billings_country = sum_of_billings_country.rename(
            columns={
                Variables.SUBTYPE: 'Countries',
                'Value': 'Billings'
            })
        logger.success('Sum of billings by country calculated')
        return sum_of_billings_country

    def calc_sum_of_billings_period(self, billing_data):
        """Calculate sum of billings by period using billing data

        :param billing_data: Billing data MDB
        :type billing_data: pd.DataFrame
        :return: Sum of billings by period with required conditions
        :rtype: pd.DataFrame
        """
        logger.debug(
            'Calculating the sum of billings by period where Type=="Market and Date>="2016-01-01"'
        )
        sum_of_billings_period = billing_data[billing_data[
            Variables.TYPE].str.contains(pat=r'Market', regex=False) == True]
        sum_of_billings_period = sum_of_billings_period[
            sum_of_billings_period['Date'] >= '2016-01-01']
        sum_of_billings_period = sum_of_billings_period.groupby(
            ['Period']).Value.sum().reset_index()
        sum_of_billings_period = sum_of_billings_period.rename(
            columns={'Value': 'Billings'})
        logger.success('Sum of billings by period calculated')
        return sum_of_billings_period

    def calc_summary_statistics(self, billing_data):
        """Calculate summary statistics by segment using billing data

        :param billing_data: Billing data MDB
        :type billing_data: pd.DataFrame
        :return: Summary statistics by segment
        :rtype: pd.DataFrame
        """
        logger.debug('Calculating summary statistics by segment')
        summary_statistics_measure_of_spread = billing_data.groupby(
            'Segment').agg({'Value': ['var', 'sem', 'skew']})
        summary_statistics_measure_of_spread.columns = summary_statistics_measure_of_spread.columns.droplevel(
            0)
        summary_statistics_kurtosis = billing_data.groupby('Segment').apply(
            pd.DataFrame.kurt)
        summary_statistics_measure_of_location = billing_data.groupby(
            'Segment').describe()
        summary_statistics_measure_of_location.columns = summary_statistics_measure_of_location.columns.droplevel(
            0)
        summary_statistics = pd.merge(summary_statistics_measure_of_spread,
                                      summary_statistics_measure_of_location,
                                      how='left',
                                      on='Segment')
        summary_statistics = pd.merge(summary_statistics,
                                      summary_statistics_kurtosis,
                                      how='left',
                                      on='Segment')
        summary_statistics = summary_statistics.rename(
            columns={'Value': 'kurt'})
        summary_statistics = summary_statistics.reindex(columns=[
            'count', 'mean', 'min', '25%', '50%', '75%', 'max', 'sem', 'var',
            'std', 'skew', 'kurt'
        ])
        logger.debug('Summary statistics by segment calculated')
        return summary_statistics

    def generate_report(self, sum_of_billings_country, sum_of_billings_period,
                        summary_statistics):
        """Generate excel report in the destination path using calculated metrics

        :param sum_of_billings_country: Sum of billings by country
        :type sum_of_billings_country: pd.DataFrame
        :param sum_of_billings_period: Sum of billings by period with requried conditions
        :type sum_of_billings_period: pd.DataFrame
        :param summary_statistics: Summary statistics by segment
        :type summary_statistics: pd.DataFrame
        """
        #Generate .xlsx file
        logger.debug('Generating excel reports with calculated metrics')
        writer = pd.ExcelWriter(self.destination_path, engine='xlsxwriter')
        currency_format = writer.book.add_format(
            {'num_format': '[$€-nl-NL] #,##0.00'})
        sum_of_billings_country.to_excel(writer,
                                         sheet_name='Output',
                                         startrow=3,
                                         startcol=0,
                                         index=False)
        sum_of_billings_period.to_excel(writer,
                                        sheet_name='Output',
                                        startrow=3,
                                        startcol=4,
                                        index=False)
        summary_statistics.to_excel(writer,
                                    sheet_name='Segment Summary Stats',
                                    startrow=0,
                                    startcol=0,
                                    index=True)
        writer.sheets['Output'].conditional_format(
            'B5:B20', {
                'type': 'cell',
                'criteria': 'greater than',
                'value': -99999999999999999,
                'format': currency_format
            })
        writer.sheets['Output'].conditional_format(
            'F5:F9', {
                'type': 'cell',
                'criteria': 'greater than',
                'value': -99999999999999999,
                'format': currency_format
            })
        writer.sheets['Output'].set_column(1, 1, 15)
        writer.sheets['Output'].set_column(4, 5, 15)
        writer.sheets['Segment Summary Stats'].set_column(0, 0, 45)
        writer.save()
        logger.success('Excel output generated')


if __name__ == '__main__':
    source_path = r'./src/resources/billings_europe.csv'
    destination_path = r'./src/resources/Python Exerscise Output.xlsx'
    europe_billing = EuropeBilling(source_path, destination_path)
    europe_billing.workflow()