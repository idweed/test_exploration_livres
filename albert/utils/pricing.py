import albert.config.ftp_wrapper as ftp_wrapper
from albert.config.config import fnac_deposit_ftp_access


# from datetime import datetime


class PricingClass:
    def __init__(self):
        # self.today_ymd = datetime.now().strftime('%Y-%m-%d')
        self.today_ymd = '2021-01-11'
        self.destination = '/tmp/ExtractPricing_PT'.format(self.today_ymd)
        self.remote_filename = 'ExtractPricing_PT'
        self.df = ftp_wrapper.download_today_file_by_filename(fnac_deposit_ftp_access,
                                                              self.remote_filename,
                                                              self.destination,
                                                              self.today_ymd,
                                                              debug=True)
