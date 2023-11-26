import os
import requests
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_option_menu import option_menu


#connecting to SQl to retrive data
mydb=psycopg2.connect(
    host="localhost",
    user="postgres",
    password="risehigh07",
    database="phonepe_data"
    )
mycursor=mydb.cursor()


#aggreagated transaction
query1="select * from aggregated_transaction"
mycursor.execute(query1)
table1=mycursor.fetchall()
Aggregated_transaction = pd.DataFrame(table1,columns=("States","Years","Quarter","Transaction_Name","Transaction_Count","Transaction_Amount"))


#aggregated user
query2="select * from aggregated_user"
mycursor.execute(query2)
table2=mycursor.fetchall()
Aggregated_user = pd.DataFrame(table2,columns=("States","Years","Quarter","Brand_Name","Brand_Count","Brand_Percentage"))


#map transaction
query3="select * from map_transaction"
mycursor.execute(query2)
table3=mycursor.fetchall()
Map_transaction = pd.DataFrame(table3,columns=("States","Years","Quarter","District_Name","District_Count","District_Amount"))


#map user
query4="select * from map_user"
mycursor.execute(query4)
table4=mycursor.fetchall()
Map_user=pd.DataFrame(table4,columns=("States","Years","Quarter","District_Name","Registered_Users","Apps_Opened")) 


#top transaction
query5="select * from map_transaction"
mycursor.execute(query5)
table5=mycursor.fetchall()
Top_user=pd.DataFrame(table5,columns=("States","Years","Quarter","Pincodes","Transaction_Count","Transaction_Amount"))


#top user
query6="select * from top_user"
mycursor.execute(query6)
table6=mycursor.fetchall()
Top_user=pd.DataFrame(table6,columns=("States","Years","Quarter","Pincodes","No_of_Users"))


#Transaction amount in states for all years
def transaction_locations_animation_all(Transaction_year):
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response =requests.get(url)
        data1 = json.loads(response.content)
        state_names_transc = [feature["properties"]["ST_NM"] for feature in data1["features"]]
        state_names_transc.sort()
        Transc_states_df = pd.DataFrame({"States":state_names_transc})
        frames = []
        for year in Map_user["Years"].unique():
                for quarter in Aggregated_transaction["Quarter"].unique():
                        transc = Aggregated_transaction[(Aggregated_transaction["Years"]==year)&(Aggregated_transaction["Quarter"]==quarter)]
                        transc1 = transc[["States","Transaction_Amount"]]
                        transc1 = transc1.sort_values(by="States")
                        transc1["Years"]=year
                        transc1["Quarter"]=quarter
                        frames.append(transc1)
        merged_df = pd.concat(frames)
        transaction= px.choropleth(merged_df, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
        transaction.update_geos(fitbounds= "locations", visible =False)
        transaction.update_layout(width =600, height= 700)
        transaction.update_layout(title_font= {"size":25})
                return st.plotly_chart(transaction)


def transaction_locations_animation_year(Transaction_year,year):
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response =requests.get(url)
        data1 = json.loads(response.content)
        state_names_transc = [feature["properties"]["ST_NM"] for feature in data1["features"]]
        state_names_tra.sort()
        Transc_states_df = pd.DataFrame({"States":state_names_transc})
        frames = []
        for year in Map_user["Years"].unique():
                for quarter in Aggregated_transaction["Quarter"].unique():
                        transc = Aggregated_transaction[(Aggregated_transaction["Years"]==year)&(Aggregated_transaction["Quarter"]==quarter)]
                        transc1 = transc[["States","Transaction_Amount"]]
                        transc1 = transc1.sort_values(by="States")
                        transc1["Years"]=year
                        transc1["Quarter"]=quarter
                        frames.append(transc1)
        merged_df = pd.concat(frames)
        
        if Transaction_year=="2018":
                year_2018=merged_df.loc[merged_df['Years'] == 2018]
                if year=="All":
                        transaction_2018= px.choropleth(year_2018, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction_2018.update_geos(fitbounds= "locations", visible =False)
                        transaction_2018.update_layout(width =600, height= 700)
                        transaction_2018.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction_2018)
                elif year=="Q1(January-March)":
                        year_q1=merged_df.loc[merged_df['Quarter'] == 1]
                        transaction1= px.choropleth(year_q1, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction1.update_geos(fitbounds= "locations", visible =False)
                        transaction1.update_layout(width =600, height= 700)
                        transaction1.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction1)
                elif year=="Q2(April-June)":
                        year_q2=merged_df.loc[merged_df['Quarter'] == 2]
                        transaction2= px.choropleth(year_q2, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction2.update_geos(fitbounds= "locations", visible =False)
                        transaction2.update_layout(width =600, height= 700)
                        transaction2.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction2)
                elif year=="Q3(July-September)":
                        year_q3=merged_df.loc[merged_df['Quarter'] == 3]
                        transaction3= px.choropleth(year_q3, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction3.update_geos(fitbounds= "locations", visible =False)
                        transaction3.update_layout(width =600, height= 700)
                        transaction3.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction3)
                elif year=="Q4(October-December)":
                        year_q4=merged_df.loc[merged_df['Quarter'] == 4]
                        transaction4= px.choropleth(year_q4, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction4.update_geos(fitbounds= "locations", visible =False)
                        transaction4.update_layout(width =600, height= 700)
                        transaction4.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction4)
        elif Transaction_year=="2019":
                year_2019=merged_df.loc[merged_df['Years'] == 2019]
                if year=="All":
                        transaction_2019= px.choropleth(year_2019, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                        color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                        animation_frame="Years", animation_group="Quarter")
                        transaction_2019.update_geos(fitbounds= "locations", visible =False)
                        transaction_2019.update_layout(width =600, height= 700)
                        transaction_2019.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction_2019)       

                elif year=="Q1(January-March)":
                        year_q1=merged_df.loc[merged_df['Quarter'] == 1]
                        transaction1= px.choropleth(year_q1, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction1.update_geos(fitbounds= "locations", visible =False)
                        transaction1.update_layout(width =600, height= 700)
                        transaction1.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction1)   
                
                elif year=="Q2(April-June)":
                        year_q2=merged_df.loc[merged_df['Quarter'] == 2]
                        transaction2= px.choropleth(year_q2, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction2.update_geos(fitbounds= "locations", visible =False)
                        transaction2.update_layout(width =600, height= 700)
                        transaction2.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction2)   
                
                elif year=="Q3(July-September)":
                        year_q3=merged_df.loc[merged_df['Quarter'] == 3]
                        transaction3= px.choropleth(year_q3, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction3.update_geos(fitbounds= "locations", visible =False)
                        transaction3.update_layout(width =600, height= 700)
                        transaction3.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction3)   
                
                elif year=="Q4(October-December)":
                        year_q4=merged_df.loc[merged_df['Quarter'] == 4]
                        transaction4= px.choropleth(year_q4, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction4.update_geos(fitbounds= "locations", visible =False)
                        transaction4.update_layout(width =600, height= 700)
                        transaction4.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction4)
        elif Transaction_year=="2020":
                year_2020=merged_df.loc[merged_df['Years'] == 2020]
                if year=="All":
                        transaction_2020= px.choropleth(year_2020, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                        color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                        animation_frame="Years", animation_group="Quarter")
                        transaction_2020.update_geos(fitbounds= "locations", visible =False)
                        transaction_2020.update_layout(width =600, height= 700)
                        transaction_2020.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction_2020)   
                
                elif year=="Q1(January-March)":
                        year_q1=merged_df.loc[merged_df['Quarter'] == 1]
                        transaction1= px.choropleth(year_q1, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction1.update_geos(fitbounds= "locations", visible =False)
                        transaction1.update_layout(width =600, height= 700)
                        transaction1.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction1)   
        
                elif year=="Q2(April-June)":
                        year_q2=merged_df.loc[merged_df['Quarter'] == 2]
                        transaction2= px.choropleth(year_q2, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction2.update_geos(fitbounds= "locations", visible =False)
                        transaction2.update_layout(width =600, height= 700)
                        transaction2.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction2)   
        
                elif year=="Q3(July-September)":
                        year_q3=merged_df.loc[merged_df['Quarter'] == 3]
                        transaction3= px.choropleth(year_q3, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction3.update_geos(fitbounds= "locations", visible =False)
                        transaction3.update_layout(width =600, height= 700)
                        transaction3.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction3)  
        
                elif year=="Q4(October-December)":
                        year_q4=merged_df.loc[merged_df['Quarter'] == 4]
                        transaction4= px.choropleth(year_q4, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction4.update_geos(fitbounds= "locations", visible =False)
                        transaction4.update_layout(width =600, height= 700)
                        transaction4.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction4)
        
        elif Transaction_year=="2021":
                year_2021=merged_df.loc[merged_df['Years'] == 2021]
                if year=="All":
                        transaction_2021= px.choropleth(year_2021, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                        color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                        animation_frame="Years", animation_group="Quarter")
                        transaction_2021.update_geos(fitbounds= "locations", visible =False)
                        transaction_2021.update_layout(width =600, height= 700)
                        transaction_2021.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction_2021)  
                
                elif year=="Q1(January-March)":
                        year_q1=merged_df.loc[merged_df['Quarter'] == 1]
                        transaction1= px.choropleth(year_q1, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction1.update_geos(fitbounds= "locations", visible =False)
                        transaction1.update_layout(width =600, height= 700)
                        transaction1.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction1)   
                
                elif year=="Q2(April-June)":
                        year_q2=merged_df.loc[merged_df['Quarter'] == 2]
                        transaction2= px.choropleth(year_q2, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction2.update_geos(fitbounds= "locations", visible =False)
                        transaction2.update_layout(width =600, height= 700)
                        transaction2.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction2)   
                
                elif year=="Q3(July-September)":
                        year_q3=merged_df.loc[merged_df['Quarter'] == 3]
                        transaction3= px.choropleth(year_q3, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction3.update_geos(fitbounds= "locations", visible =False)
                        transaction3.update_layout(width =600, height= 700)
                        transaction3.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction3)  
                
                elif year=="Q4(October-December)":
                        year_q4=merged_df.loc[merged_df['Quarter'] == 4]
                        transaction4= px.choropleth(year_q4, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction4.update_geos(fitbounds= "locations", visible =False)
                        transaction4.update_layout(width =600, height= 700)
                        transaction4.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction4)
        elif Transaction_year=="2022":
                year_2022=merged_df.loc[merged_df['Years'] == 2022]
                if year=="All":
                        transaction_2022= px.choropleth(year_2022, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                        color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                        animation_frame="Years", animation_group="Quarter")
                        transaction_2022.update_geos(fitbounds= "locations", visible =False)
                        transaction_2022.update_layout(width =600, height= 700)
                        transaction_2022.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction_2021)  
        
                elif year=="Q1(January-March)":
                        year_q1=merged_df.loc[merged_df['Quarter'] == 1]
                        transaction1= px.choropleth(year_q1, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction1.update_geos(fitbounds= "locations", visible =False)
                        transaction1.update_layout(width =600, height= 700)
                        transaction1.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction1)   
                
                elif year=="Q2(April-June)":
                        year_q2=merged_df.loc[merged_df['Quarter'] == 2]
                        transaction2= px.choropleth(year_q2, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction2.update_geos(fitbounds= "locations", visible =False)
                        transaction2.update_layout(width =600, height= 700)
                        transaction2.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction2)
        
                elif year=="Q3(July-September)":
                        year_q3=merged_df.loc[merged_df['Quarter'] == 3]
                        transaction3= px.choropleth(year_q3, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction3.update_geos(fitbounds= "locations", visible =False)
                        transaction3.update_layout(width =600, height= 700)
                        transaction3.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction3)  
        
                elif year=="Q4(October-December)":
                        year_q4=merged_df.loc[merged_df['Quarter'] == 4]
                        transaction4= px.choropleth(year_q4, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction4.update_geos(fitbounds= "locations", visible =False)
                        transaction4.update_layout(width =600, height= 700)
                        transaction4.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction4)
