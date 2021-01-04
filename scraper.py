from sec_edgar_downloader import Downloader
import os
from bs4 import BeautifulSoup
import datetime
import pandas as pd
from q_end import get_qend
from sqlalchemy import create_engine


#download data
def get_files(filing, CIK, number):
    save_path = r"C:\Users\smore\Documents\13F"
    dl = Downloader(save_path)
    dl.get(filing, CIK,number)
    CIK = CIK.lstrip("0")
    print(CIK)
    files = os.listdir(f"C:/Users/smore/Documents/13F/sec_edgar_filings/{CIK}/{filing}")
    data = [parse(file, CIK) for file in sorted(files)]
    try:
        print(pd.concat(data))
        return pd.concat(data)
    except ValueError:
        print("All Values are None")
        return None
    


#parse data
def parse(file, CIK):
    xml = open(f"C:/Users/smore/Documents/13F/sec_edgar_filings/{CIK}/13F-HR/{file}").read()
    soup = BeautifulSoup(xml,'lxml')

    try:
        date = datetime.datetime.strptime(soup.find('signaturedate').contents[0], '%m-%d-%Y')
        print(date)
        name = soup.find('name').contents[0]
    except:
        return
    
    cols = ['nameOfIssuer','cusip','value','sshPrnamt', 'sshPrnamtType', 'putCall']
    data = []
    colData = []
    print("Processing " + name + " (" + CIK + ") for date " + str(date))
    for infotable in soup.find_all(['ns1:infotable','infotable']):
        row = []
        for col in cols:
            colData = infotable.find([col.lower(), 'ns1:' + col.lower()])
            row.append(colData.text.strip() if colData else 'NaN')
            colData = []
        row.append(date)
        row.append(CIK)
        row.append(name)
        data.append(row)
    df = pd.DataFrame(data)
    cols.append('date')
    cols.append('fund_cik')
    cols.append('fund')
    try:
        df.columns = cols
        return df
    except:
        return





form = '13F-HR'
ciks = ['0000102909']

#storing data
i = 1
for cik in ciks:
    #provide feedback
    print("-----" +str(i) + " of " + str(len(ciks)) + "------")
    i += 1

    #Download files
    df = get_files(form,cik, 1)


    if df is not None:
        #Perform cleaning
        df['value'] = df['value'].astype(float)
        df['sshPrnamt'] = df['sshPrnamt'].astype(int)
        df['date'] = df['date'].apply(get_qend)
        
        #upload to Database
        engine = create_engine("postgresql://postgres:postgres@localhost/thirteenf")
        try:
            print("Uploading holding to database")
            df.to_sql('holdings',con=engine, if_exists='append',chunksize=1000,index=False)
        except:
            print('Did not upload correctly')
            pass
    
            

        



    

