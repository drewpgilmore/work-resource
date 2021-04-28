# DEFINES MONTHLY BILLING CLASS 
# Structure for downloading / processing / generating invoice data
from ref import ref, billing_folder
import os
import datetime as dt
import numpy as np
import pandas as pd

class MonthlyBilling():
    
    def __init__(self, airline, year, month):
        # PATH TO BILLING FOLDER
        # TODO rename file paths
        file_path = billing_folder
        self.billing_files = {}
        self.archive_files = {}
        
        self.airline                 = airline                                        # takes input from init                                                        
        self.abbr                    = ref[airline]['abbr'].upper()                   # 3 letter abbreviation for airline used in file naming structure
        self.netcracker              = ref[airline]['netcracker']                     # BOOL whether airline is billing through Netcracker currently
        #self.nc_prefix               = f'{self.abbr.lower()}_monthly/{self.abbr}'     # airline directory within Netcracker AWS bucket
        self.year                    = year                                           # 
        self.month                   = month                                          #  
        self.mo                      = str(month).zfill(2)                            #  month format for reading files out of AWS "##" or "0#"
        self.date                    = dt.datetime(year, month, 1)                    #  first day of billing month
        self.month_string            = dt.datetime.strftime(self.date,"%B")           #  full name of month for printing later
        self.data_path               = f"{file_path}/Data/{airline}/{year}-{month}"   #  specific folder in which data will be pulled from
        self.output_path             = f"{file_path}/Output/{airline}/{year}-{month}" #  specific folder in which data will be pulled from
        
        reference_prefix             = f"{self.data_path}/{self.abbr}_Monthly_{year}_{self.mo}"
        self.rbo_file                = f"{reference_prefix}_rbo_data.csv"             # this file is only accessible for smaller airlines due to size
        self.sla_file                = f"{reference_prefix}_sla_data.csv"             # this file is only accessible for smaller airlines due to size
        self.whitelist_data_file     = f"{reference_prefix}_whitelist_data.csv"       # listed in bytes grouped by tail id and NPI code
        self.active_aircraft_file    = f"{reference_prefix}_active_aircraft.csv"      # * this will be merged w/ pertailcharges
        self.tail_exclusions_file    = f"{reference_prefix}_tail_exclusions.csv"      # 
        self.route_exclusions_file   = f"{reference_prefix}_route_exclusions.csv"     # 
        self.flight_exclusions_file  = f"{reference_prefix}_flight_exclusions.csv"    # 
        self.slaconfig_data_file     = f"{reference_prefix}_slaconfig_data.csv"       # list of SLA rule ids and their remedy threshholds
        self.slarebuffer_data_file   = f"{reference_prefix}_slarebuffer_data.csv"     # 
        
        # INVOICE GENERATION VARIABLES AND CONFIG
        self.billing_files[f"{self.abbr}_active_aircraft"] = self.active_aircraft_file
        self.output_file = f"{self.output_path}/{self.airline.upper()}_{self.month_string}_{self.year}_Invoice_Support.xlsx"
        
    def folder_check(self):
        # make folders if they don't already exist
        if not os.path.isdir(self.data_path):                                 
            os.mkdir(self.data_path)
            print(f"Made data folder for {self.month_string} {self.year}")
        else:
            print(f"Folder already exists...")

        if not os.path.isdir(self.output_path):                              
            os.mkdir(self.output_path)
            print(f"Made output folder for {self.month_string} {self.year}")
        else:
            print(f"Folder already exists...")
        
    def download_netcracker(self):
        from run_aws import s3, netcracker_bucket
        nc_prefix = f'{self.abbr.lower()}_monthly/{self.abbr}'   

        if self.netcracker:
            for key in s3.list_objects(Bucket=netcracker_bucket, Prefix=nc_prefix)['Contents']:
                if f"{self.year}{self.mo}" in key['Key'] and 'INVAL' not in key['Key']:
                    name = key['Key']
                    drop = f"_{name.split('_')[-2]}_{name.split('_')[-1]}"
                    file_name = name.replace(drop,'').split('/')[-1]
                    #file_name   = key['Key'][:-13].split('/')[1]
                    output_file = f"{self.data_path}/{file_name}_{self.year}_{self.mo}.csv"
                    self.billing_files[file_name] = output_file
                    s3.download_file(netcracker_bucket, key['Key'], output_file)
                    print(f"DOWNLOADED: {file_name}")

    def download_archive(self):
        from run_aws import s3, archive_bucket
        archive_prefix = f"{self.abbr}_monthly_{self.year}_{self.mo}"
        for key in s3.list_objects(Bucket=archive_bucket, Prefix=archive_prefix)['Contents']:
            if key['Size'] < 500000000:
                file_name   = f"{key['Key'].split('_')[4]}_{key['Key'].split('_')[5]}"
                output_file = f"{self.data_path}/{self.abbr}_monthly_{self.year}_{self.mo}_{file_name}.csv"
                self.archive_files[file_name] = output_file
                if not os.path.isfile(output_file):
                    s3.download_file(archive_bucket, key['Key'], output_file)
                    print(f"DOWNLOADED {file_name}")    
                else:
                    print(f"{file_name} already downloaded.")

    def clean_data(self):
        # TODO store data somewhere
        #output_file = f"{self.output_path}/{self.airline.upper()}_{self.month_string}_{self.year}_Invoice_Support.xlsx"
        
        with pd.ExcelWriter(self.output_file) as writer:
            for name, file in self.billing_files.items():
                raw = pd.read_csv(file)
                clean = raw.loc[:, ~raw.columns.str.contains('^Unnamed')]
                clean.columns = [col.strip().upper() for col in clean.columns] 
                clean = clean.applymap(lambda x: x.lstrip() if isinstance(x, str) else x).replace(-1, 0)
                clean.to_excel(writer, sheet_name=name, index=False)
                clean.to_csv(file, index=False)


    def usage_summary(self):
        #output_file = f"{self.output_path}/{self.airline.upper()}_{self.month_string}_{self.year}_Usage_Summary.xlsx"
        
        raw = pd.read_excel(self.output_file, sheet_name=f"{self.abbr}_Session_usage")
        col = [col for col in raw.columns if 'NPI' in col]
        npi_codes = {}
        for c in col:
            npi_codes[c] = raw.loc[0,c]
        
        raw.set_index('TAIL_ID', inplace=True)
        new = pd.DataFrame({})
        new.index.name = 'TAIL_ID'
        
        for i in range(1,len(col)+1):
            name = npi_codes[f"NPI_CODE_{i}"]
            #category = raw[f"USG_CAT_{i}"][0]
            #print(category)
            for t in raw.index:
                new.loc[t, f"{name} Usage (MB)"] = raw.loc[t, f"USAGE_MB_{i}"]

        output_file = f"{self.data_path}/{self.abbr}_Usage_Summary_{self.year}_{self.mo}.csv"
        new.to_csv(output_file)
        return new
            
    def sla_summary(self):
        from functions import get_monthly_sla_scores
        df = get_monthly_sla_scores(self.airline, self.year, self.month)
        output_file = f"{self.data_path}/{self.abbr}_Monthly_SLA_Scores_{self.year}_{self.mo}.csv"
        df.to_csv(output_file)
        
    # Excel file generation 
    def process_invoice(self):
        self.folder_check()
        if self.netcracker:
            self.download_netcracker()
            print('Netcracker files downloaded')
            self.donwload_archive()
            print('Archive files downloaded')
        else:
            self.download_archive()
            print('Archive files downloaded')

        #clean_data()

