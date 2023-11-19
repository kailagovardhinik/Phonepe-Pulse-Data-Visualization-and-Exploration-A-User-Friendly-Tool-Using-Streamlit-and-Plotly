import os
import json
import pandas as pd
import mysql.connector
import plotly.express as px
import plotly.graph_objects as go

#aggregrated transaction details from phonepe data
path01="G:/Project/pulse/data/aggregated/transaction/country/india/state"
aggr_transc_path=os.listdir(path01)

columns1={"State":[],"Year":[],"Quarter":[],"Transaction Name":[],"Transaction Count":[],"Transaction Amount":[]}
for state in aggr_transc_path:
    aggr_transc_states=path01+"/"+state
    aggr_transc_year_list=os.listdir(aggr_transc_states)
    
    for year in aggr_transc_year_list:
        aggr_transc_cur_year=aggr_transc_states+"/"+year
        aggr_transc_year_list=os.listdir(aggr_transc_cur_year)
        
        for file in aggr_transc_year_list:
            aggr_transc_cur_file=aggr_transc_cur_year+"/"+file
            aggr_transc_data=open(aggr_transc_cur_file,"r")
            aggr_transc_loaddata=json.load(aggr_transc_data)
            
            for i in aggr_transc_loaddata['data']['transactionData']:
                name=i['name']
                count=i['paymentInstruments'][0]["count"]
                amount=i['paymentInstruments'][0]["amount"]
                columns1["Transaction Name"].append(name)
                columns1["Transaction Count"].append(count)
                columns1["Transaction Amount"].append(amount)
                columns1["State"].append(state)
                columns1["Year"].append(year)
                columns1["Quarter"].append(file.strip('.json'))
aggr_transc_df=pd.DataFrame(columns1)
aggr_transc_df["State"] = aggr_transc_df["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
aggr_transc_df["State"] = aggr_transc_df["State"].str.replace("-"," ")
aggr_transc_df["State"] = aggr_transc_df["State"].str.title()
aggr_transc_df["State"] = aggr_transc_df["State"].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Dama")


#aggregrated user details from phonepe data
path02="G:/Project/pulse/data/aggregated/user/country/india/state"
aggr_user_path=os.listdir(path02)

columns2={"State":[],"Year":[],"Quarter":[],"Brand Name":[],"Brand Count":[],"Brand Percentage":[]}

for state in aggr_user_path:
    aggr_user_states=path02+"/"+state
    aggr_user_year_list=os.listdir(aggr_user_states)
    
    for year in aggr_user_year_list:
        aggr_user_cur_year=aggr_user_states+"/"+year
        aggr_user_year_list=os.listdir(aggr_user_cur_year)
        
        for file in aggr_user_year_list:
            aggr_user_cur_file=aggr_user_cur_year+"/"+file
            aggr_user_data=open(aggr_user_cur_file,"r")
            aggr_user_loaddata=json.load(aggr_user_data)
            
            if aggr_user_loaddata['data']['usersByDevice'] is not None:
                for i in aggr_user_loaddata['data']['usersByDevice']:
                    brand=i['brand']
                    count=i["count"]
                    percentage=i["percentage"]
                    columns2["Brand Name"].append(name)
                    columns2["Brand Count"].append(count)
                    columns2["Brand Percentage"].append(percentage)
                    columns2["State"].append(state)
                    columns2["Year"].append(year)
                    columns2["Quarter"].append(file.strip('.json'))
                    
aggr_user_df=pd.DataFrame(columns2)
aggr_user_df["State"] = aggr_user_df["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
aggr_user_df["State"] = aggr_user_df["State"].str.replace("-"," ")
aggr_user_df["State"] = aggr_user_df["State"].str.title()
aggr_user_df["State"] = aggr_user_df["State"].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Dama")


#map transaction details from phonepe data
path03="G:/Project/pulse/data/map/transaction/hover/country/india/state"
map_transc_path=os.listdir(path03)

columns3={"State":[],"Year":[],"Quarter":[],"District Name":[],"District Count":[],"District Amount":[]}

for state in map_transc_path:
    map_transc_states=path03+"/"+state
    map_transc_year_list=os.listdir(map_transc_states)
    
    for year in map_transc_year_list:
        map_transc_cur_year=map_transc_states+"/"+year
        map_transc_year_list=os.listdir(map_transc_cur_year)
        
        for file in map_transc_year_list:
            map_transc_cur_file=map_transc_cur_year+"/"+file
            map_transc_data=open(map_transc_cur_file,"r")
            map_transc_loaddata=json.load(map_transc_data)
            
            for i in map_transc_loaddata['data']['hoverDataList']:
                    name=i['name']
                    count=i['metric'][0]["count"]
                    amount=i['metric'][0]["amount"]
                    columns3["District Name"].append(name)
                    columns3["District Count"].append(count)
                    columns3["District Amount"].append(amount)
                    columns3["State"].append(state)
                    columns3["Year"].append(year)
                    columns3["Quarter"].append(file.strip('.json'))

map_transc_df=pd.DataFrame(columns3)
map_transc_df["State"] = map_transc_df["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
map_transc_df["State"] = map_transc_df["State"].str.replace("-"," ")
map_transc_df["State"] = map_transc_df["State"].str.title()
map_transc_df["State"] = map_transc_df["State"].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Dama")


#map user details from phonepe data
path04="G:/Project/pulse/data/map/user/hover/country/india/state"
map_user_path=os.listdir(path04)

columns4={"State":[],"Year":[],"Quarter":[],"District":[],"Registered Users":[],"No. of Apps Open":[]}

for state in map_user_path:
    map_user_states=path04+"/"+state
    map_user_year_list=os.listdir(map_user_states)
    
    for year in map_user_year_list:
        map_user_cur_year=map_user_states+"/"+year
        map_user_year_list=os.listdir(map_user_cur_year)
        
        for file in map_user_year_list:
            map_user_cur_file=map_user_cur_year+"/"+file
            map_user_data=open(map_user_cur_file,"r")
            map_user_loaddata=json.load(map_user_data)

            for i in map_user_loaddata['data']['hoverData'].items():
                name=i[0] 
                users_count=i[1]['registeredUsers']
                app_count=i[1]['appOpens']
                columns4['District'].append(name)
                columns4['Registered Users'].append(users_count)
                columns4['No. of Apps Open'].append(app_count)
                columns4['State'].append(state)
                columns4['Year'].append(year)
                columns4['Quarter'].append(file.strip('.json'))

map_user_df=pd.DataFrame(columns4)
map_user_df["State"] = map_user_df["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
map_user_df["State"] = map_user_df["State"].str.replace("-"," ")
map_user_df["State"] = map_user_df["State"].str.title()
map_user_df["State"] = map_user_df["State"].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Dama")


#top transaction details from phonepe data
path05="G:/Project/pulse/data/top/transaction/country/india/state"
top_transc_path=os.listdir(path05)

columns5={"State":[],"Year":[],"Quarter":[],"Pincodes":[],"Transaction Count":[],"Transaction Amount":[]}

for state in top_transc_path:
    top_transc_states=path05+"/"+state
    top_transc_year_list=os.listdir(top_transc_states)
    
    for year in top_transc_year_list:
        top_transc_cur_year=top_transc_states+"/"+year
        top_transc_year_list=os.listdir(top_transc_cur_year)
        
        for file in top_transc_year_list:
            top_transc_cur_file=top_transc_cur_year+"/"+file
            top_transc_data=open(top_transc_cur_file,"r")
            top_transc_loaddata=json.load(top_transc_data)
            
            for i in top_transc_loaddata["data"]["pincodes"]:
                pincode=i["entityName"]
                count=i["metric"]["count"]
                amount=i["metric"]["amount"]
                columns5["Pincodes"].append(pincode)
                columns5["Transaction Count"].append(count)
                columns5["Transaction Amount"].append(amount)
                columns5['State'].append(state)
                columns5['Year'].append(year)
                columns5['Quarter'].append(file.strip('.json'))

top_transc_df=pd.DataFrame(columns5)
top_transc_df["State"] = top_transc_df["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_transc_df["State"] = top_transc_df["State"].str.replace("-"," ")
top_transc_df["State"] = top_transc_df["State"].str.title()
top_transc_df["State"] = top_transc_df["State"].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Dama")


#top user details from phonepe data
path06="G:/Project/pulse/data/top/user/country/india/state"
top_user_path=os.listdir(path06)

columns6={"State":[],"Year":[],"Quarter":[],"Pincode":[],"No. of Users":[]}

for state in top_user_path:
    top_user_states=path06+"/"+state
    top_user_year_list=os.listdir(top_user_states)
    
    for year in top_user_year_list:
        top_user_cur_year=top_user_states+"/"+year
        top_user_year_list=os.listdir(top_user_cur_year)
        
        for file in top_user_year_list:
            top_user_cur_file=top_user_cur_year+"/"+file
            top_user_data=open(top_user_cur_file,"r")
            top_user_loaddata=json.load(top_user_data)
            
            for i in top_user_loaddata['data']['pincodes']:
                pincode=i['name']
                user_count=i['registeredUsers']
                columns6["Pincode"].append(pincode)
                columns6["No. of Users"].append(user_count)
                columns6['State'].append(state)
                columns6['Year'].append(year)
                columns6['Quarter'].append(file.strip('.json'))
                
top_user_df=pd.DataFrame(columns6)
top_user_df["State"] = top_user_df["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_user_df["State"] = top_user_df["State"].str.replace("-"," ")
top_user_df["State"] = top_user_df["State"].str.title()
top_user_df["State"] = top_user_df["State"].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Dama")

#converting the datarframes to csv file to update to Mysql
aggr_transc_df.to_csv("aggr_transc_file.csv",index=False)
aggr_user_df.to_csv("aggr_users_file.csv",index=False)
map_transc_df.to_csv("map_transc_file.csv",index=False)
map_user_df.to_csv("map_user_file.csv",index=False)
top_transc_df.to_csv("top_transc_file.csv",index=False)
top_user_df.to_csv("top_user_file.csv",index=False)

#connecting to SQl
mydb=mysql.connector.connect(
    host="localhost",
    user="root",
    password="risehigh07",
    database="phonepe_data"
    )
mycursor=mydb.cursor()

#updating the dataframe to Mysql database from python
#aggregated transaction sql table
create_query1 = '''CREATE TABLE if not exists aggregated_transaction (
                                                                States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                Transaction_Name varchar(50),
                                                                Transaction_Count bigint,
                                                                Transaction_Amount bigint
                                                                        )'''
mycursor.execute(create_query1)
mydb.commit()

for index,row in aggr_transc_df.iterrows():
        insert_query1 = '''INSERT INTO aggregated_transaction (States,
                                                Years, 
                                                Quarter,
                                                Transaction_Name,
                                                Transaction_Count,
                                                Transaction_Amount)
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''
        values = (row["State"],
                row["Year"],
                row["Quarter"],
                row["Transaction Name"],
                row["Transaction Count"],
                row["Transaction Amount"]
                )
        mycursor.execute(insert_query1,values)
        mydb.commit()
        

#aggregated user sql table
create_query2 = '''CREATE TABLE if not exists aggregated_user (
                                                                States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                Brand_Name varchar(50),
                                                                Brand_Count bigint,
                                                                Brand_Percentage bigint
                                                                )'''
mycursor.execute(create_query2)
mydb.commit()

for index,row in aggr_user_df.iterrows():
        insert_query2 = '''INSERT INTO aggregated_user (States,
                                                                Years,
                                                                Quarter,
                                                                Brand_Name,
                                                                Brand_Count ,
                                                                Brand_Percentage)
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''
        values = (row["State"],
                row["Year"],
                row["Quarter"],
                row["Brand Name"],
                row["Brand Count"],
                row["Brand Percentage"]
                )
        mycursor.execute(insert_query2,values)
        mydb.commit()
        

#map transaction sql table
create_query3 = '''CREATE TABLE if not exists map_transaction (
                                                                States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                District_Name varchar(50),
                                                                District_Count bigint,
                                                                District_Amount float
                                                                )'''
mycursor.execute(create_query3)
mydb.commit()

for index,row in map_transc_df.iterrows():
        insert_query3 = '''INSERT INTO map_transaction (States,
                                                        Years,
                                                        Quarter,
                                                        District_Name,
                                                        District_Count,
                                                        District_Amount)
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''
        values = (row["State"],
                row["Year"],
                row["Quarter"],
                row["District Name"],
                row["District Count"],
                row["District Amount"]
                )
        mycursor.execute(insert_query3,values)
        mydb.commit()
        
#map user sql table
create_query4 = '''CREATE TABLE if not exists map_user (
                                                                States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                District_Name varchar(50),
                                                                Registerd_Users bigint,
                                                                Apps_Opened bigint
                                                                )'''
mycursor.execute(create_query4)
mydb.commit()

for index,row in map_user_df.iterrows():
        insert_query4 = '''INSERT INTO map_user (States,
                                                        Years,
                                                        Quarter,
                                                        District_Name,
                                                        Registerd_Users,
                                                        Apps_Opened)
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''
        values = (row["State"],
                row["Year"],
                row["Quarter"],
                row["District"],
                row["Registered Users"],
                row["No. of Apps Open"]
                )
        mycursor.execute(insert_query4,values)
        mydb.commit()
        

#top transaction sql table
create_query5 = '''CREATE TABLE if not exists top_transaction (
                                                                States varchar(50),
                                                                Years int,
                                                                Quarter int,
                                                                Pincodes bigint,
                                                                Transaction_Count bigint,
                                                                Transaction_Amount bigint
                                                                );'''
mycursor.execute(create_query5)
mydb.commit()

for index,row in top_transc_df.iterrows():
        insert_query5 = '''INSERT INTO top_transaction (States,
                                                        Years,
                                                        Quarter,
                                                        Pincodes,
                                                        Transaction_Count,
                                                        Transaction_Amount)
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''
        values = (row["State"],
                row["Year"],
                row["Quarter"],
                row["Pincodes"],
                row["Transaction Count"],
                row["Transaction Amount"]
                )
        mycursor.execute(insert_query5,values)
        mydb.commit()
        
#top user sql table
create_query6 = '''CREATE TABLE if not exists top_user (
                                                        States varchar(50),
                                                        Years int,
                                                        Quarter int,
                                                        Pincodes bigint,
                                                        No_of_Users bigint
                                                        )'''
                                                        
mycursor.execute(create_query6)
mydb.commit()

for index,row in top_user_df.iterrows():
        insert_query6 = '''INSERT INTO top_user (States,
                                                Years,
                                                Quarter,
                                                Pincodes,
                                                No_of_Users
                                                )
                                                values(%s,%s,%s,%s,%s)'''
        values = (row["State"],
                row["Year"],
                row["Quarter"],
                row["Pincode"],
                row["No. of Users"]
                )
        mycursor.execute(insert_query6,values)
        mydb.commit()
