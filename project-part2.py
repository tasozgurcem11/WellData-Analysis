import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import numpy as np
from datetime import *


header_file = "Data/header3.csv"
production_file = "Data/production3.csv"


df_header = pd.read_csv(header_file)

df_header['Grid4'] = df_header['Grid4'].astype(str)
df_header['Grid3'] = df_header['Grid3'].astype(str)
df_header['STR'] = df_header[['Grid4', 'Grid2', 'Grid3']].agg('-'.join, axis=1) #creates STR

df_header1 = df_header.copy()

df_header['API'] = df_header['API'].astype(str) 
df_header = df_header[~df_header.API.str.contains("0001")]
df_header = df_header[~df_header.API.str.contains("0002")]
df_header = df_header[~df_header.API.str.contains("0003")]
df_header = df_header[~df_header.API.str.contains("0004")]

print("Shape Header: " + str(df_header.shape))

print("Number of different API's in Header file: "+ str(len(df_header["API"].unique())))
print("Number of different STR's in Header file: "+ str(len(df_header["STR"].unique())))


#Edit production
#Edit production
df_production = pd.read_csv(production_file)
df_production['ReportMonth'] = df_production['ReportMonth'].astype(str)
df_production['ReportYear'] = df_production['ReportYear'].astype(str)
df_production['PRODMMYYYY'] = df_production[['ReportMonth', 'ReportYear']].agg('/'.join, axis=1)

df_production['API'] = df_production['API'].astype(str) 
df_production = df_production[~df_production.API.str.contains("0001")]
df_production = df_production[~df_production.API.str.contains("0002")]
df_production = df_production[~df_production.API.str.contains("0003")]
df_production = df_production[~df_production.API.str.contains("0004")]

print("Shape Production: " + str(df_production.shape))
print("Number of different API's in Production file: "+ str(len(df_production["API"].unique())))


class Schedule():
    def __init__(self, first_m, first_y, last_m,  last_y):
        self.first_m = first_m
        self.last_m = last_m
        self.first_y = first_y
        self.last_y = last_y
        
    def create_schedule(self):
        list1 = []
        setter = 0
        for year in range(self.first_y, self.last_y+1):
            if year == self.first_y:
                for i in range(self.first_m, 13):
                    temp_list = ["", "", ""]
                    temp_list[0] = i
                    temp_list[1] = int(self.first_y)
                    temp_list[2] = 0
                    list1.append(temp_list)
                    
            elif year != self.first_y and year != self.last_y:
                for i in range(1, 13):
                    temp_list = ["", "", ""]
                    temp_list[0] = i
                    temp_list[1] = int(year)
                    temp_list[2] = 0
                    list1.append(temp_list)
                    
            elif year == self.last_y:
                for i in range(1, self.last_m+1):
                    temp_list = ["", "", ""]
                    temp_list[0] = i
                    temp_list[1] = int(self.last_y)
                    temp_list[2] = 0
                    list1.append(temp_list)        
                    
        return list1
    
    def reset(self):
        self.first_m = 0
        self.last_m = 0
        self.first_y = 0
        self.last_y = 0
        self.list1 = []
    


cem3 = Schedule(12, 1990, 12, 2020)
test_list = cem3.create_schedule()

cem3 = Schedule(12, 1990, 12, 2020)
test_list = cem3.create_schedule()


#12 MONTHS OF SUM FUNCTIONS

def Extract(lst):
        return [item[2] for item in lst]

def create_sum_list(entered_list):
    
    total_sum_list = []

    #print(general_list[0])
    
    sum_list = []
    values_of_list = Extract(entered_list)
    a = np.array(values_of_list)
    b = a.cumsum()
    b[12:] = b[12:] - b[:-12]

    for i in range(len(b)):
        temp_v = []
        temp_v = entered_list[i].insert(4, b[i])
        total_sum_list.append(temp_v)
        #print(entered_list[i])
    
    return entered_list

# Function to find STR from header file
    


API = df_production.API.unique()
df_list = []
lookup_list = []
highlighted_list = []
general_list = []
total_list = []
counter = 0
general_df = pd.DataFrame
new_list = []


print(len(API))
print(df_production.API.value_counts().sum())




API = df_production.API.unique()
STR = df_header.STR.unique()
print(len(STR))

df_list = []
lookup_list = []
highlighted_list = []
general_list = []
total_list = []
counter = 0
general_df = pd.DataFrame
new_list = []
sum = 0
counter = 1
for id in API:
    print(counter)
    counter += 1
    new_df = df_production.loc[df_production['API'] == id]
    header_df0 = df_header.loc[df_header['API'] == id]
    
    STR1 = header_df0.loc[header_df0['API'] == id, 'STR'].item()
    WellName = header_df0.loc[header_df0['API'] == id, 'WellName'].item()
    WellboreProfile = header_df0.loc[header_df0['API'] == id, 'WellBoreProfile'].item()
    WellStatus = header_df0.loc[header_df0['API'] == id, 'WellStatus'].item()
    CurrentOperator = header_df0.loc[header_df0['API'] == id, 'CurrentOperator'].item()
    CompletionDate = header_df0.loc[header_df0['API'] == id, 'CompletionDate'].item()
    FirstProd = header_df0.loc[header_df0['API'] == id, 'FirstProdDate'].item()
    SpudDate = header_df0.loc[header_df0['API'] == id, 'SpudDate'].item()


    
    #Creating new columns for highlight_df
    
    new_df['STR'] = STR1
    new_df['WellName'] = WellName
    new_df['WellboreProfile'] = WellboreProfile
    new_df['WellStatus'] = WellStatus
    new_df['CurrentOperator'] = CurrentOperator
    new_df['CompletionDate'] = CompletionDate
    new_df['FirstProd'] = FirstProd
    new_df['SpudDate'] = SpudDate

    
    
    
    
    #Dropping values with NaN
    new_df.dropna(subset=['TotalOil', 'TotalGas'], inplace=True)
    
    #Dropping values before 2017
    old_rows = new_df[ (new_df["ReportYear"].astype(int) < 2017)  ].index
    new_df.drop(old_rows, inplace = True)
    
    #Sort by Month and Year
    new_df['ReportYear'] = new_df['ReportYear'].astype(int)
    new_df['ReportMonth'] = new_df['ReportMonth'].astype(int)
    new_df['TotalGas'] = new_df['TotalGas'].astype(float)
    new_df['TotalOil'] = new_df['TotalOil'].astype(float)
    new_df = new_df.sort_values(['ReportYear', 'ReportMonth', 'TotalGas', 'TotalOil'], ascending=[False, False, False, False])

    #new_df = new_df.sort_values(by=["ReportYear", "ReportMonth"], ascending=False)
    
    #Dropping values with both 0
    #new_df['TotalOil'] = new_df['TotalOil'].astype(int)
    #new_df['TotalGas'] = new_df['TotalGas'].astype(int)
    
    index_names = new_df[((new_df['TotalOil'] == 0) & (new_df["TotalGas"] == 0))  ].index
    #new_df.drop(index_names, inplace = True)
    
    #new_df = new_df.duplicated(subset=['PRODMMYYYY'])
    
    new_list.append(new_df)

pre_process_df = pd.concat(new_list)
print("After cleaning data, length: " + str(pd.concat(new_list).shape))



#MAIN 1
df0_list = []
for df0 in new_list:

    df0["oilpro"] = df0["TotalOil"].astype(str) + df0["PRODMMYYYY"].astype(str)
    
    df0.drop_duplicates('oilpro', keep='first', inplace = True)
    
    df0 = df0.groupby(['PRODMMYYYY']).agg(**{"TotalOil": ('TotalOil', 'max'), "ReportYear": ('ReportYear', 'max'), "TotalGas": ('TotalGas', 'max'), "API": ('API', 'max'), "ReportMonth": ('ReportMonth', 'max'), "ReportYear": ('ReportYear', 'max'), "WellName": ('WellName', 'max'), "STR": ('STR', 'max'),  "FirstProd": ('FirstProd', 'max') , "CompletionDate": ('CompletionDate', 'max') , "WellboreProfile": ('WellboreProfile', 'max'),"CurrentOperator": ('CurrentOperator', 'max'), "WellStatus": ('WellStatus', 'max'), "SpudDate": ('SpudDate', 'max')}).reset_index()
    df0 = df0.sort_values(by=["ReportYear", "ReportMonth"], ascending=[False, False])
    
    if df0.shape[0] >= 4:
        df0_list.append(df0)
        





#MAIN 2
counter = 0
df1_list = []
general_list2 = []
general_df = pd.DataFrame

for df1 in df0_list:
    counter += 1
    general_df = df1.copy()
    last_year = df1["ReportYear"].iloc[0]
    first_year = df1["ReportYear"].iloc[-1]
    
    
    last_month = df1["ReportMonth"].iloc[0]
    first_month = df1["ReportMonth"].iloc[-1]
    
    test_list= []
    
    index_names = df1[((df1['TotalOil'] == 0) & (df1["TotalGas"] == 0))  ].index
    df1.drop(index_names, inplace = True)
    
    index_names2 = df1[ (df1["TotalOil"] == 0)  ].index
    df1.drop(index_names2, inplace = True)
    
    df1.drop_duplicates(subset ="PRODMMYYYY",keep = False, inplace = True)
    
    each_id = Schedule(first_month, first_year, last_month, last_year)
    test_list = each_id.create_schedule()

    c1 =5
    for index, row in df1.iterrows():
        if c1 == 5:
            before_month1 = row["ReportMonth"] 
            before_year1 = row["ReportYear"]
            before_month2 = ""
            before_year2 = ""
            update_index = test_list.index([row["ReportMonth"], row['ReportYear'], 0])
            test_list[update_index][2] = row['TotalOil']
            c1 = 1
        elif c1 == 1 and (before_month2 != row["ReportMonth"] or before_year2 != row["ReportYear"]):
            before_month1 = row["ReportMonth"] 
            before_year1 = row["ReportYear"]
            update_index = test_list.index([row["ReportMonth"], row['ReportYear'], 0])
            test_list[update_index][2] = row['TotalOil']
            c1 = 0
        elif c1 == 0 and (before_month1 != row["ReportMonth"] or before_year1 != row["ReportYear"]):
            before_month2 = row["ReportMonth"] 
            before_year2 = row["ReportYear"]
            update_index = test_list.index([row["ReportMonth"], row['ReportYear'], 0])
            test_list[update_index][2] = row['TotalOil']
            c1 = 1
        
    general_list.append(test_list)
    
    each_id.reset()
    
    sum_list = create_sum_list(test_list)
    
    total_list.append(sum_list)
    
    general_df['TotalOilSum'] = "0"
    
    for i in range(len(sum_list)):
        temp_str = str(sum_list[i][0]) + "/" + str(sum_list[i][1])
        general_df.loc[(general_df.PRODMMYYYY == temp_str),'TotalOilSum']= sum_list[i][3]
    
    general_list2.append(general_df)
    


#Main 3 
general_list3 = []
for df2 in general_list2:
    
    #Creating Status 
    df2['GasStatus'] = "False"
    df2['OilStatus'] = "False"
    df2.loc[(df2["TotalGas"] < 500.0) & (df2["TotalGas"] != 0),'GasStatus']= "True"
    df2.loc[( df2["TotalOilSum"] < 365.0) & ( df2["TotalOilSum"] != 0),'OilStatus']= "True"
    general_list3.append(df2)



#Main 4
general_list4 = []
str_highlights = []
api_highlights = []
print(len(general_list3))

general_df4 = pd.concat(general_list3)
print(general_df4.shape)
print(len(general_df4.STR.unique()))

for df4 in general_list3:
    if df4['GasStatus'].str.contains('True').any() & df4['OilStatus'].str.contains('True').any() :
        #print("yes")
        api_highlights.append(df4.loc[0, 'API'])
    
print(len(api_highlights)) 


#Main 5
general_df5 = general_df4.groupby(['STR', 'API']).agg({col: ['max'] for col in ['STR', 'API']})
general_df5.columns = ['_'.join(multi_index) for multi_index in general_df5.columns.ravel()]
general_df5 = general_df5.reset_index()
general_df5


#Main 6
str_df_list = []
for api3 in api_highlights:
    str_df = general_df5.loc[general_df5['API'] == api3]
    str_df_list.append(str_df)
    
str_highlights = pd.concat(str_df_list)
str_list = str_highlights["STR"].unique()
print((str_list))
ban_str_list = list(set(str_list).symmetric_difference(set(STR)))



#Main 7 - Final
before_final_list = []
for str10 in str_list:
    before_final_df = general_df4.loc[general_df4['STR'] == str10]
    before_final_list.append(before_final_df)
    print(before_final_df.head())

final_df1 = pd.concat(str_df_list)
print(final_df1.shape)
final_df = general_df4.loc[~(general_df4['STR'].isin(ban_str_list))]

updated_cols = ['STR', 'API', 'PRODMMYYYY', 'TotalOil', 'TotalOilSum', 'TotalGas','ReportYear', 'GasStatus', 'OilStatus', 'WellName', 'WellboreProfile', 'WellStatus','CurrentOperator', 'CompletionDate', 'FirstProd', 'SpudDate'] 
final_df2 = final_df[updated_cols]
final_df2.sort_values(['STR', 'API'], ascending=[True, True], inplace=True)




final_header_df1 = df_header1.loc[df_header1['WellStatus'] == "Permit - New Drill"]
final_header_df2 = df_header1.loc[df_header1['WellStatus'] == "Completed - Not Active"]
final_header_df3 = df_header1.loc[df_header1['WellStatus'] == "DUC - Drilled Uncompleted"]
final_header_df = pd.concat([final_header_df1, final_header_df2, final_header_df3])


final_header_df["PRODMMYYYY"] = "0"
final_header_df["TotalOil"] = "0"
final_header_df["TotalOilSum"] = "0"
final_header_df["TotalGas"] = "0"
final_header_df["GasStatus"] = "0"
final_header_df["OilStatus"] = "0"
final_header_df["FirstProd"] = ""
final_header_df["WellboreProfile"] = ""
final_header_df["ReportYear"] = "0"




updated_cols = ['STR', 'API', 'PRODMMYYYY', 'TotalOil', 'TotalOilSum', 'TotalGas', 'ReportYear', 'GasStatus', 'OilStatus', 'WellName', 'WellboreProfile', 'WellStatus','CurrentOperator', 'CompletionDate', 'FirstProd', 'SpudDate'] 
final_header = final_header_df[updated_cols]
final_header.sort_values(['STR', 'API'], ascending=[True, True], inplace=True)
print(list(final_header.columns.values))

print(final_header_df.shape)
with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    print(final_header[["API", "WellStatus"]])

final_df3 = pd.concat([final_df2, final_header])
print(final_df3.head())



updated_cols2 = ['STR', 'API', 'PRODMMYYYY', 'TotalOil', 'TotalOilSum', 'TotalGas', 'GasStatus', 'OilStatus', 'WellName', 'WellboreProfile', 'WellStatus','CurrentOperator', 'CompletionDate', 'FirstProd', 'SpudDate', 'ReportYear'] 
final_df4 = final_df3[updated_cols2]
final_df4.sort_values(['STR', 'API'], ascending=[True, True], inplace=True)

#print(final_df4["SpudDate"].str.split(' ').str[0])
final_df4["SpudDate"] = final_df4["SpudDate"].str.split(' ').str[0]
final_df4["SpudDate"] = pd.to_datetime(final_df4["SpudDate"])
final_df4["SpudDateAfter"] = "False"


print(final_df4.shape)
print(final_df4['SpudDate'])
list_API = final_df4["API"].unique()
list_STR = final_df4["STR"].unique()
#print(list_STR)


df_list5 = []
for STR2 in list_STR:
    test_df = final_df4.loc[final_df4['STR'] == STR2]
    
    
    test_df.sort_values(['SpudDate', 'STR', 'API'], ascending=[False,True, True], inplace=True)
    #Pinned Date:
    print(STR2)
    string0='True'
        
    if test_df.shape[0] >= 3 and test_df.shape[1] >= 3:
        if test_df["GasStatus"].str.contains(string0,regex=False).any():
            pin = test_df[(test_df.GasStatus == 'True') | (test_df.OilStatus == 'True') ].index[0]
            print(pin)
            pinned_date = test_df.iloc[pin]
            print(pinned_date["SpudDate"])
            pinned_real_date = pinned_date["SpudDate"]
    
            min_date_limit = date(2018, 1, 1)
            test_df.loc[(test_df.SpudDate >= pd.to_datetime('2018-01-01')) , "SpudDateAfter"] = "True"
    
    api_entered = "35017226030000"

    rslt_df = test_df[test_df['API'] == api_entered]
    print('\nResult dataframe :\n', rslt_df)
    
    #test_df = test_df[~(test_df['ReportYear'].astype(int) < 2018)]  

    
    
    
    df_list5.append(test_df)

#print(df_list5[7])


final_df6 = pd.concat(df_list5)


updated_cols2 = ['STR', 'API', 'PRODMMYYYY', 'TotalOil', 'TotalOilSum', 'TotalGas', 'GasStatus', 'OilStatus', 'WellName', 'WellboreProfile', 'WellStatus','CurrentOperator', 'CompletionDate', 'FirstProd', 'SpudDate', 'SpudDateAfter'] 
final_df7 = final_df6[updated_cols2]
final_df7.sort_values(['STR', 'API'], ascending=[True, True], inplace=True)
final_df7.to_csv(r'final-2test.csv', index = False)

