import requests
import pandas as pd
import json
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
mycursor.execute(query3)
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
def transaction_locations_animation_all():
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response =requests.get(url)
        data1 = json.loads(response.content)
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

def transaction_locations_animation_year(Transaction_year,years):
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response =requests.get(url)
        data1 = json.loads(response.content)
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
                if years=="All":
                        transaction_2018= px.choropleth(year_2018, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction_2018.update_geos(fitbounds= "locations", visible =False)
                        transaction_2018.update_layout(width =600, height= 700)
                        transaction_2018.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction_2018)
                elif years=="Q1(January-March)":
                        year_q1=year_2018.loc[year_2018['Quarter'] == 1]
                        transaction1= px.choropleth(year_q1, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction1.update_geos(fitbounds= "locations", visible =False)
                        transaction1.update_layout(width =600, height= 700)
                        transaction1.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction1)
                elif years=="Q2(April-June)":
                        year_q2=year_2018.loc[year_2018['Quarter'] == 2]
                        transaction2= px.choropleth(year_q2, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction2.update_geos(fitbounds= "locations", visible =False)
                        transaction2.update_layout(width =600, height= 700)
                        transaction2.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction2)
                elif years=="Q3(July-September)":
                        year_q3=year_2018.loc[year_2018['Quarter'] == 3]
                        transaction3= px.choropleth(year_q3, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction3.update_geos(fitbounds= "locations", visible =False)
                        transaction3.update_layout(width =600, height= 700)
                        transaction3.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction3)
                elif years=="Q4(October-December)":
                        year_q4=year_2018.loc[year_2018['Quarter'] == 4]
                        transaction4= px.choropleth(year_q4, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction4.update_geos(fitbounds= "locations", visible =False)
                        transaction4.update_layout(width =600, height= 700)
                        transaction4.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction4)
        elif Transaction_year=="2019":
                year_2019=merged_df.loc[merged_df['Years'] == 2019]
                if years=="All":
                        transaction_2019= px.choropleth(year_2019, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                        color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                        animation_frame="Years", animation_group="Quarter")
                        transaction_2019.update_geos(fitbounds= "locations", visible =False)
                        transaction_2019.update_layout(width =600, height= 700)
                        transaction_2019.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction_2019)       

                elif years=="Q1(January-March)":
                        year_q1=year_2019.loc[year_2019['Quarter'] == 1]
                        transaction1= px.choropleth(year_q1, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction1.update_geos(fitbounds= "locations", visible =False)
                        transaction1.update_layout(width =600, height= 700)
                        transaction1.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction1)   
                
                elif years=="Q2(April-June)":
                        year_q2=year_2019.loc[year_2019['Quarter'] == 2]
                        transaction2= px.choropleth(year_q2, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction2.update_geos(fitbounds= "locations", visible =False)
                        transaction2.update_layout(width =600, height= 700)
                        transaction2.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction2)   
                
                elif years=="Q3(July-September)":
                        year_q3=year_2019.loc[year_2019['Quarter'] == 3]
                        transaction3= px.choropleth(year_q3, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction3.update_geos(fitbounds= "locations", visible =False)
                        transaction3.update_layout(width =600, height= 700)
                        transaction3.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction3)   
                
                elif years=="Q4(October-December)":
                        year_q4=year_2019.loc[year_2019['Quarter'] == 4]
                        transaction4= px.choropleth(year_q4, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction4.update_geos(fitbounds= "locations", visible =False)
                        transaction4.update_layout(width =600, height= 700)
                        transaction4.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction4)
        elif Transaction_year=="2020":
                year_2020=merged_df.loc[merged_df['Years'] == 2020]
                if years=="All":
                        transaction_2020= px.choropleth(year_2020, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                        color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                        animation_frame="Years", animation_group="Quarter")
                        transaction_2020.update_geos(fitbounds= "locations", visible =False)
                        transaction_2020.update_layout(width =600, height= 700)
                        transaction_2020.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction_2020)   
                
                elif years=="Q1(January-March)":
                        year_q1=year_2020.loc[year_2020['Quarter'] == 1]
                        transaction1= px.choropleth(year_q1, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction1.update_geos(fitbounds= "locations", visible =False)
                        transaction1.update_layout(width =600, height= 700)
                        transaction1.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction1)   
        
                elif years=="Q2(April-June)":
                        year_q2=year_2020.loc[year_2020['Quarter'] == 2]
                        transaction2= px.choropleth(year_q2, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction2.update_geos(fitbounds= "locations", visible =False)
                        transaction2.update_layout(width =600, height= 700)
                        transaction2.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction2)   
        
                elif years=="Q3(July-September)":
                        year_q3=year_2020.loc[year_2020['Quarter'] == 3]
                        transaction3= px.choropleth(year_q3, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction3.update_geos(fitbounds= "locations", visible =False)
                        transaction3.update_layout(width =600, height= 700)
                        transaction3.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction3)  
        
                elif years=="Q4(October-December)":
                        year_q4=year_2020.loc[year_2020['Quarter'] == 4]
                        transaction4= px.choropleth(year_q4, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction4.update_geos(fitbounds= "locations", visible =False)
                        transaction4.update_layout(width =600, height= 700)
                        transaction4.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction4)
        
        elif Transaction_year=="2021":
                year_2021=merged_df.loc[merged_df['Years'] == 2021]
                if years=="All":
                        transaction_2021= px.choropleth(year_2021, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                        color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                        animation_frame="Years", animation_group="Quarter")
                        transaction_2021.update_geos(fitbounds= "locations", visible =False)
                        transaction_2021.update_layout(width =600, height= 700)
                        transaction_2021.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction_2021)  
                
                elif years=="Q1(January-March)":
                        year_q1=year_2021.loc[year_2021['Quarter'] == 1]
                        transaction1= px.choropleth(year_q1, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction1.update_geos(fitbounds= "locations", visible =False)
                        transaction1.update_layout(width =600, height= 700)
                        transaction1.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction1)   
                
                elif years=="Q2(April-June)":
                        year_q2=year_2021.loc[year_2021['Quarter'] == 2]
                        transaction2= px.choropleth(year_q2, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction2.update_geos(fitbounds= "locations", visible =False)
                        transaction2.update_layout(width =600, height= 700)
                        transaction2.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction2)   
                
                elif years=="Q3(July-September)":
                        year_q3=year_2021.loc[year_2021['Quarter'] == 3]
                        transaction3= px.choropleth(year_q3, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction3.update_geos(fitbounds= "locations", visible =False)
                        transaction3.update_layout(width =600, height= 700)
                        transaction3.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction3)  
                
                elif years=="Q4(October-December)":
                        year_q4=year_2021.loc[year_2021['Quarter'] == 4]
                        transaction4= px.choropleth(year_q4, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction4.update_geos(fitbounds= "locations", visible =False)
                        transaction4.update_layout(width =600, height= 700)
                        transaction4.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction4)
        elif Transaction_year=="2022":
                year_2022=merged_df.loc[merged_df['Years'] == 2022]
                if years=="All":
                        transaction_2022= px.choropleth(year_2022, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                        color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                        animation_frame="Years", animation_group="Quarter")
                        transaction_2022.update_geos(fitbounds= "locations", visible =False)
                        transaction_2022.update_layout(width =600, height= 700)
                        transaction_2022.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction_2021)  
        
                elif years=="Q1(January-March)":
                        year_q1=year_2022.loc[year_2022['Quarter'] == 1]
                        transaction1= px.choropleth(year_q1, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction1.update_geos(fitbounds= "locations", visible =False)
                        transaction1.update_layout(width =600, height= 700)
                        transaction1.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction1)   
                
                elif years=="Q2(April-June)":
                        year_q2=year_2022.loc[year_2022['Quarter'] == 2]
                        transaction2= px.choropleth(year_q2, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction2.update_geos(fitbounds= "locations", visible =False)
                        transaction2.update_layout(width =600, height= 700)
                        transaction2.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction2)
        
                elif years=="Q3(July-September)":
                        year_q3=year_2022.loc[year_2022['Quarter'] == 3]
                        transaction3= px.choropleth(year_q3, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction3.update_geos(fitbounds= "locations", visible =False)
                        transaction3.update_layout(width =600, height= 700)
                        transaction3.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction3)  
        
                elif years=="Q4(October-December)":
                        year_q4=year_2022.loc[year_2022['Quarter'] == 4]
                        transaction4= px.choropleth(year_q4, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                                color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                                animation_frame="Years", animation_group="Quarter")
                        transaction4.update_geos(fitbounds= "locations", visible =False)
                        transaction4.update_layout(width =600, height= 700)
                        transaction4.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction4)

def payment_transc_amount():
        Transactions= Aggregated_transaction[["Transaction_Name", "Transaction_Amount"]]
        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum()
        Transc_df=pd.DataFrame(Transc).reset_index()
        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                        color_discrete_sequence=px.colors.sequential.Blues_r)
        Transc_fig.update_layout(width=600, height= 500)
        return(Transc_fig)

def payment_transc_amount_all(transc_count_year,type_years):
        if transc_count_year=="2018":
                Transactions=Aggregated_transaction.loc[Aggregated_transaction["Years"]==2018]
                if type_years=="All":
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif type_years=="Q1(January-March)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==1]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif type_years=="Q2(April-June)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==2]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif type_years=="Q3(July-September)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==3]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif type_years=="Q4(October-December)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==4]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)

        elif transc_count_year=="2019":
                Transactions=Aggregated_transaction.loc[Aggregated_transaction["Years"]==2019]
                if type_years=="All":
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif type_years=="Q1(January-March)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==1]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif type_years=="Q2(April-June)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==2]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif type_years=="Q3(July-September)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==3]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif type_years=="Q4(October-December)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==4]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)

        elif transc_count_year=="2020":
                Transactions=Aggregated_transaction.loc[Aggregated_transaction["Years"]==2020]
                if type_years=="All":
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif type_years=="Q1(January-March)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==1]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif type_years=="Q2(April-June)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==2]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif type_years=="Q3(July-September)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==3]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif type_years=="Q4(October-December)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==4]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
        
        elif transc_count_year=="2021":
                Transactions=Aggregated_transaction.loc[Aggregated_transaction["Years"]==2021]
                if type_years=="All":
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif type_years=="Q1(January-March)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==1]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif type_years=="Q2(April-June)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==2]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif type_years=="Q3(July-September)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==3]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif type_years=="Q4(October-December)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==4]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
        
        elif transc_count_year=="2022":
                Transactions=Aggregated_transaction.loc[Aggregated_transaction["Years"]==2022]
                if type_years=="All":
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif type_years=="Q1(January-March)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==1]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif type_years=="Q2(April-June)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==2]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif type_years=="Q3(July-September)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==3]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif type_years=="Q4(October-December)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==4]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Amount"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                                                color_discrete_sequence=px.colors.sequential.Blues_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)

def transaction_count_animation():        
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response =requests.get(url)
        data1 = json.loads(response.content)
        frames = []
        for year in Aggregated_transaction["Years"].unique():
                        for quarter in Aggregated_transaction["Quarter"].unique():
                                transc = Aggregated_transaction[(Aggregated_transaction["Years"]==year)&(Aggregated_transaction["Quarter"]==quarter)]
                                transc1 = transc[["States","Transaction_Count"]]
                                transc1 = transc1.sort_values(by="States")
                                transc1["Years"]=year
                                transc1["Quarter"]=quarter
                                frames.append(transc1)
        merged_df = pd.concat(frames)
        transaction= px.choropleth(merged_df, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                        animation_frame="Years", animation_group="Quarter")
        transaction.update_geos(fitbounds= "locations", visible =False)
        transaction.update_layout(width =600, height= 700)
        transaction.update_layout(title_font= {"size":25})
        return st.plotly_chart(transaction) 

def transaction_count_animation_year(map_count_years,map_count_year):        
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response =requests.get(url)
        data1 = json.loads(response.content)
        frames = []
        for year in Aggregated_transaction["Years"].unique():
                        for quarter in Aggregated_transaction["Quarter"].unique():
                                transc = Aggregated_transaction[(Aggregated_transaction["Years"]==year)&(Aggregated_transaction["Quarter"]==quarter)]
                                transc1 = transc[["States","Transaction_Count"]]
                                transc1 = transc1.sort_values(by="States")
                                transc1["Years"]=year
                                transc1["Quarter"]=quarter
                                frames.append(transc1)
        merged_df = pd.concat(frames)
        if map_count_years=="2018":
                year_2018=merged_df.loc[merged_df['Years'] == 2018]
                if map_count_year=="All":
                        transaction= px.choropleth(year_2018, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)
                elif map_count_year=="Q1(January-March)":
                        year_q1=year_2018.loc[year_2018["Quarter"]==1]
                        transaction= px.choropleth(year_q1, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)
                elif map_count_year=="Q2(April-June)":
                        year_q2=year_2018.loc[year_2018["Quarter"]==2]
                        transaction= px.choropleth(year_q2, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)
                elif map_count_year=="Q3(July-September)":
                        year_q3=year_2018.loc[year_2018["Quarter"]==3]
                        transaction= px.choropleth(year_q3, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)
                elif map_count_year=="Q4(October-December)":
                        year_q4=year_2018.loc[year_2018["Quarter"]==4]
                        transaction= px.choropleth(year_q4, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)
        elif map_count_years=="2019":
                year_2019=merged_df.loc[merged_df['Years'] == 2019]
                if map_count_year=="All":
                        transaction= px.choropleth(year_2019, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)
                elif map_count_year=="Q1(January-March)":
                        year_q1=year_2019.loc[year_2019["Quarter"]==1]
                        transaction= px.choropleth(year_q1, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)
                elif map_count_year=="Q2(April-June)":
                        year_q2=year_2019.loc[year_2019["Quarter"]==2]
                        transaction= px.choropleth(year_q2, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)  
                elif map_count_year=="Q3(July-September)":
                        year_q3=year_2019.loc[year_2019["Quarter"]==3]
                        transaction= px.choropleth(year_q3, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)
                elif map_count_year=="Q4(October-December)":
                        year_q4=year_2019.loc[year_2019["Quarter"]==4]
                        transaction= px.choropleth(year_q4, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)
        elif map_count_years=="2020":
                year_2020=merged_df.loc[merged_df['Years'] == 2020]
                if map_count_year=="All":
                        transaction= px.choropleth(year_2020, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)
                elif map_count_year=="Q1(January-March)":
                        year_q1=year_2020.loc[year_2020["Quarter"]==1]
                        transaction= px.choropleth(year_q1, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)
                elif map_count_year=="Q2(April-June)":
                        year_q2=year_2020.loc[year_2020["Quarter"]==2]
                        transaction= px.choropleth(year_q2, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)
                elif map_count_year=="Q3(July-September)":
                        year_q3=year_2020.loc[year_2020["Quarter"]==3]
                        transaction= px.choropleth(year_q3, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction) 
                elif map_count_year=="Q4(October-December)":
                        year_q4=year_2020.loc[year_2020["Quarter"]==4]
                        transaction= px.choropleth(year_q4, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)
        elif map_count_years=="2021":
                year_2021=merged_df.loc[merged_df['Years'] == 2021]
                if map_count_year=="All":
                        transaction= px.choropleth(year_2021, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)
                elif map_count_year=="Q1(January-March)":
                        year_q1=year_2021.loc[year_2021["Quarter"]==1]
                        transaction= px.choropleth(year_q1, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)
                elif map_count_year=="Q2(April-June)":
                        year_q2=year_2021.loc[year_2021["Quarter"]==2]
                        transaction= px.choropleth(year_q2, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)  
                elif map_count_year=="Q3(July-September)":
                        year_q3=year_2021.loc[year_2021["Quarter"]==3]
                        transaction= px.choropleth(year_q3, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)  
                elif map_count_year=="Q4(October-December)":
                        year_q4=year_2021.loc[year_2021["Quarter"]==4]
                        transaction= px.choropleth(year_q4, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction) 
        elif map_count_years=="2022":
                year_2022=merged_df.loc[merged_df['Years'] == 2022]
                if map_count_year=="All":
                        transaction= px.choropleth(year_2022, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)
                elif map_count_year=="Q1(January-March)":
                        year_q1=year_2022.loc[year_2022["Quarter"]==1]
                        transaction= px.choropleth(year_q1, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction) 
                elif map_count_year=="Q2(April-June)":
                        year_q2=year_2022.loc[year_2022["Quarter"]==2]
                        transaction= px.choropleth(year_q2, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)   
                elif map_count_year=="Q3(July-September)":
                        year_q3=year_2022.loc[year_2022["Quarter"]==3]
                        transaction= px.choropleth(year_q3, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)  
                elif map_count_year=="Q4(October-December)":
                        year_q4=year_2022.loc[year_2022["Quarter"]==4]
                        transaction= px.choropleth(year_q4, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",
                                                        animation_frame="Years", animation_group="Quarter")
                        transaction.update_geos(fitbounds= "locations", visible =False)
                        transaction.update_layout(width =600, height= 700)
                        transaction.update_layout(title_font= {"size":25})
                        return st.plotly_chart(transaction)

def transaction_Counts():
        Transactions= Aggregated_transaction[["Transaction_Name", "Transaction_Count"]]
        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
        Transc_df=pd.DataFrame(Transc).reset_index()
        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                        color_discrete_sequence=px.colors.sequential.Redor_r)
        Transc_fig.update_layout(width=600, height= 500)
        return st.plotly_chart(Transc_fig)

def transaction_Counts_years(count_year,count_years):
        if count_year=="2018":
                Transactions=Aggregated_transaction.loc[Aggregated_transaction["Years"]==2018]
                if count_years=="All":
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif count_years=="Q1(January-March)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==1]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif count_years=="Q2(April-June)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==2]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif count_years=="Q3(July-September)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==3]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif count_years=="Q4(October-December)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==4]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
        elif count_year=="2019":
                Transactions=Aggregated_transaction.loc[Aggregated_transaction["Years"]==2019]
                if count_years=="All":
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif count_years=="Q1(January-March)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==1]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif count_years=="Q2(April-June)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==2]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif count_years=="Q3(July-September)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==3]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif count_years=="Q4(October-December)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==4]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)


        elif count_year=="2020":
                Transactions=Aggregated_transaction.loc[Aggregated_transaction["Years"]==2020]
                if count_years=="All":
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif count_years=="Q1(January-March)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==1]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif count_years=="Q2(April-June)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==2]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif count_years=="Q3(July-September)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==3]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif count_years=="Q4(October-December)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==4]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
        
        elif count_year=="2021":
                Transactions=Aggregated_transaction.loc[Aggregated_transaction["Years"]==2021]
                if count_years=="All":
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif count_years=="Q1(January-March)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==1]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif count_years=="Q2(April-June)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==2]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif count_years=="Q3(July-September)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==3]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif count_years=="Q4(October-December)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==4]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)

        elif count_year=="2022":
                Transactions=Aggregated_transaction.loc[Aggregated_transaction["Years"]==2022]
                if count_years=="All":
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif count_years=="Q1(January-March)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==1]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif count_years=="Q2(April-June)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==2]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif count_years=="Q3(July-September)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==3]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)
                elif count_years=="Q4(October-December)":
                        Transactions=Aggregated_transaction.loc[Aggregated_transaction["Quarter"]==4]
                        Transc= Transactions.groupby("Transaction_Name")["Transaction_Count"].sum().sort_values(ascending=False)
                        Transc_df=pd.DataFrame(Transc).reset_index()
                        Transc_fig= px.bar(Transc_df,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                                        color_discrete_sequence=px.colors.sequential.Redor_r)
                        Transc_fig.update_layout(width=600, height= 500)
                        return st.plotly_chart(Transc_fig)

def all_states(year_transaction,state):
        map=Map_user[["States","District_Name","Years","Registered_Users"]]
        map1=map.loc[(map["States"]==state)&(map["Years"]==year_transaction)]
        map2= map1[["District_Name", "Registered_Users"]]
        map3=map2.groupby("District_Name")["Registered_Users"].sum()
        map_df=pd.DataFrame(map3).reset_index()
        map_fig=px.bar(map_df, x= "District_Name", y= "Registered_Users", title= "DISTRICTS and REGISTERED USER",
                        color_discrete_sequence=px.colors.sequential.Bluered_r)
        map_fig.update_layout(width= 1000, height= 500)
        return st.plotly_chart(map_fig)

def reg_state_all_users(year_transaction,state):
        map= Map_user[["States", "Years", "District_Name", "Registered_Users"]]
        map1= map.loc[(Map_user["States"]==state)&(Map_user["Years"]==year_transaction)]
        map2= map1.groupby("District_Name")["Registered_Users"].sum().sort_values(ascending=False)
        map3= pd.DataFrame(map2).reset_index()

        user_fig= px.bar(map3, x= "District_Name", y="Registered_Users", title="DISTRICTS and REGISTERED USER",
                        color_discrete_sequence=px.colors.sequential.Cividis_r)
        user_fig.update_layout(width= 600, height= 500)
        return st.plotly_chart(user_fig)

def reg_state_all_transaction(year_transaction,state):
        map= Map_transaction[["States", "Years","District_Name","District_Amount"]]
        map1= map.loc[(map["States"]==state)&(map["Years"]==year_transaction)]
        map2= map1.groupby("District_Name")["District_Amount"].sum().sort_values(ascending=False)
        map3= pd.DataFrame(map2).reset_index()

        map_fig= px.bar(map3, x= "District_Name", y= "District_Amount", title= "DISTRICT and TRANSACTION AMOUNT",
                        color_discrete_sequence= px.colors.sequential.Darkmint_r)
        map_fig.update_layout(width= 600, height= 500)
        return st.plotly_chart(map_fig)

def question1():#"PhonePe Users throughout the years"
        map=Map_user[["Years","Registered_Users"]]
        map1=map.groupby("Years")["Registered_Users"].sum().sort_values(ascending=True)
        map2=pd.DataFrame(map1).reset_index().head(50)
        fig = px.bar(map2, x='Years', y='Registered_Users')
        return fig

def question2():#Top 10 States With Lowest Transaction Amount
        map=Map_transaction[["States","District_Amount"]]
        map1=map.groupby("States")["District_Amount"].sum().sort_values(ascending=True)
        map2=map1.head(10)
        map2=pd.DataFrame(map2).reset_index()
        fig = go.Figure(data=[go.Table(
                header=dict(values=list(map2.columns),fill_color='paleturquoise',align='left'),
                cells=dict(values=[map2.States, map2.District_Amount], fill_color='lavender',
                align='left'))
                                ])
        return st.plotly_chart(fig)

def question3():#Top 10 States With Highest Transaction Amount
        map=Map_transaction[["States","District_Amount"]]
        map1=map.groupby("States")["District_Amount"].sum().sort_values(ascending=False)
        map2=map1.head(10)
        map2=pd.DataFrame(map2).reset_index()
        fig = go.Figure(data=[go.Table(
                header=dict(values=list(map2.columns),fill_color='paleturquoise',align='left'),
                cells=dict(values=[map2.States, map2.District_Amount], fill_color='lavender',
                align='left'))
                                ])
        return st.plotly_chart(fig)

def question4():#PhonePe users from 2018 to 2022
        map=Map_user[["Years","Registered_Users"]]
        map1=map.groupby("Years")["Registered_Users"].sum()
        map2=pd.DataFrame(map1).reset_index()
        fig = px.line(map2, x="Years", y="Registered_Users", title='Registered Users throughout the year')
        return st.plotly_chart(fig)

def question5():#"Top brands of mobile used"
        map=Aggregated_user[["Brand_Name","Brand_Count"]]
        map1=map.groupby("Brand_Name")["Brand_Count"].sum().sort_values(ascending=False)
        map2=pd.DataFrame(map1).reset_index().head(10)
        fig = px.bar(map2, x='Brand_Name', y='Brand_Count')
        return st.plotly_chart(fig)

def question6():#"States With Highest Transaction Count"
        map=Aggregated_transaction[["States","Transaction_Count"]]
        map1=map.groupby("States")["Transaction_Count"].sum().sort_values(ascending=False)
        map2=pd.DataFrame(map1).reset_index().head(10)
        fig = px.bar(map2, x='States', y='Transaction_Count')
        return st.plotly_chart(fig)

def question7():#"States With Lowest Transaction Count"
        map=Aggregated_transaction[["States","Transaction_Count"]]
        map1=map.groupby("States")["Transaction_Count"].sum().sort_values(ascending=True)
        map2=pd.DataFrame(map1).reset_index().head(10)
        fig = px.bar(map2, x='States', y='Transaction_Count')
        return st.plotly_chart(fig)

def question8():#"Top 50 Districts With Highest Transaction Amount"
        map=Map_transaction[["District_Name","District_Amount"]]
        map1=map.groupby("District_Name")["District_Amount"].sum().sort_values(ascending=False)
        map2=pd.DataFrame(map1).reset_index().head(10)
        fig = px.bar(map2, x='District_Name', y='District_Amount')
        return st.plotly_chart(fig)

def question9():#Top 10 States with Highest PhonePe User
        map=Map_user[["States","Registered_Users"]]
        map1=map.groupby("States")["Registered_Users"].sum().sort_values(ascending=False)
        map2=map1.head(10)
        map2=pd.DataFrame(map2).reset_index()
        fig = go.Figure(data=[go.Table(
                header=dict(values=list(map2.columns),fill_color='paleturquoise',align='left'),
                cells=dict(values=[map2.States, map2.Registered_Users], fill_color='lavender',
                align='left'))
                                ])
        return st.plotly_chart(fig)

def question10():#"Top 10 Districts With Lowest Transaction Amount"
        map=Map_transaction[["District_Name","District_Amount"]]
        map1=map.groupby("District_Name")["District_Amount"].sum().sort_values(ascending=True)
        map2=pd.DataFrame(map1).reset_index().head(10)
        fig = px.bar(map2, x='District_Name', y='District_Amount')
        return st.plotly_chart(fig)

st.set_page_config(page_title="PhonePe Visualization",layout="wide")
page_bg_img='''
<style>
[data-testid="stAppViewContainer"]{
        background-color:#FAE745;   
}
</style>'''
st.markdown(page_bg_img,unsafe_allow_html=True)
st.title(":violet[PhonePe Pulse Data Insights]")
st.divider()

SELECT = option_menu(
        menu_title = None,
        options = ["About","Payments","Facts"],
        icons =["house","cash","bar-chart"],
        default_index=2,
        orientation="horizontal",
        styles={"container": {"padding": "0!important", "background-color": "white","size":"cover", "width": "100%"},
                "icon": {"color": "black", "font-size": "20px"},
                "nav-link": {"font-size": "20px", "text-align": "center", "margin": "-2px", "--hover-color": "#9334CD"},
                "nav-link-selected": {"background-color": "#9334CD"}})

if SELECT=="About":
        col1, col2 = st.columns(2)
        with col1:
                st.header("**PhonePe**")
                st.subheader("_India's Best Transaction App_")
                st.write("""_PhonePe is a digital wallet and mobile payment platform in India.It uses the Unified Payment Interface (UPI) system to allow users to send and receive money recharge mobile, DTH, data cards,make utility payments,pay at shops,invest in tax saving funds,buy insurance, mutual funds and digital gold._""")
                st.write("****FEATURES****")
                st.write("   **- Fund Transfer**")
                st.write("   **- Payment to Merchant**")
                st.write("   **- Recharge and Bill payments**")
                st.write("   **- Autopay of Bills**")
                st.write("   **- Cashback and Rewards and much more**")
                st.link_button(":violet[**DOWNLOAD THE APP NOW**]", "https://www.phonepe.com/app-download/")
        
        with col2:
                st.subheader("Video about PhonePe")
                st.video("https://youtu.be/aXnNA4mv1dU?si=HnSu_ETm4X29Lrvf")
                st.write("***To know more about PhonePe click below***")
                st.link_button(":violet[**PhonePe**]", "https://www.phonepe.com/")

if SELECT=="Payments":
        listTabs=["***Transaction***","***Transaction Count***","***Registered User Details***"]
        whitespace = 40
        tabs = st.tabs([s.center(whitespace,"\u2001") for s in listTabs])
        with tabs[0]:
                col1,col2=st.columns(2)
                with col1:
                        st.subheader("A Geo Visualization of Transferred Amounts (2018-2022)")
                        Transaction_year=st.selectbox(":violet[_Transactions Year_]",("Select Year","All","2018",
                                                                "2019",
                                                                "2020",
                                                                "2021",
                                                                "2022",
                                                                ))
                        if Transaction_year=="All":
                                transaction_locations_animation_all()
                        elif Transaction_year=="2018":
                                years=st.selectbox(":violet[Quarter]",("Quarter","All","Q1(January-March)",
                                                "Q2(April-June)",
                                                "Q3(July-September)",
                                                "Q4(October-December)"))
                                transaction_locations_animation_year(Transaction_year,years)
                        elif Transaction_year=="2019":
                                years=st.selectbox(":violet[Quarter]",("Quarter","All","Q1(January-March)",
                                                "Q2(April-June)",
                                                "Q3(July-September)",
                                                "Q4(October-December)"))
                                transaction_locations_animation_year(Transaction_year,years)
                        elif Transaction_year=="2020":
                                years=st.selectbox(":violet[Quarter]",("Quarter","All","Q1(January-March)",
                                                "Q2(April-June)",
                                                "Q3(July-September)",
                                                "Q4(October-December)"))
                                transaction_locations_animation_year(Transaction_year,years)
                        elif Transaction_year=="2021":
                                years=st.selectbox(":violet[Quarter]",("Quarter","All","Q1(January-March)",
                                                "Q2(April-June)",
                                                "Q3(July-September)",
                                                "Q4(October-December)"))
                                transaction_locations_animation_year(Transaction_year,years)
                        elif Transaction_year=="2022":
                                years=st.selectbox(":violet[Quarter]",("Quarter","All","Q1(January-March)",
                                                "Q2(April-June)",
                                                "Q3(July-September)",
                                                "Q4(October-December)"))
                                transaction_locations_animation_year(Transaction_year,years)
                with col2:
                        st.subheader("Bar Chart Analysis: Reasons for Financial Transfers (2018-2022)")
                        transc_count_year=st.selectbox(":violet[_Select Year to transaction type_]",("Select Year","All","2018",
                                                                "2019",
                                                                "2020",
                                                                "2021",
                                                                "2022",
                                                                ))
                        if transc_count_year=="All":
                                        payment_transc_amount()
                        elif transc_count_year=="2018":
                                type_years=st.selectbox(":violet[Select Quarter]",("Quarter","All","Q1(January-March)",
                                                "Q2(April-June)",
                                                "Q3(July-September)",
                                                "Q4(October-December)"))
                                payment_transc_amount_all(transc_count_year,type_years)
                        elif transc_count_year=="2019":
                                type_years=st.selectbox(":violet[Select Quarter]",("Quarter","All","Q1(January-March)",
                                                "Q2(April-June)",
                                                "Q3(July-September)",
                                                "Q4(October-December)"))
                                payment_transc_amount_all(transc_count_year,type_years)
                        elif transc_count_year=="2020":
                                type_years=st.selectbox(":violet[Select Quarter]",("Quarter","All","Q1(January-March)",
                                                "Q2(April-June)",
                                                "Q3(July-September)",
                                                "Q4(October-December)"))
                                payment_transc_amount_all(transc_count_year,type_years)
                        elif transc_count_year=="2021":
                                type_years=st.selectbox(":violet[Select Quarter]",("Quarter","All","Q1(January-March)",
                                                "Q2(April-June)",
                                                "Q3(July-September)",
                                                "Q4(October-December)"))
                                payment_transc_amount_all(transc_count_year,type_years)
                        elif transc_count_year=="2022":
                                type_years=st.selectbox(":violet[Select Quarter]",("Quarter","All","Q1(January-March)",
                                                "Q2(April-June)",
                                                "Q3(July-September)",
                                                "Q4(October-December)"))
                                payment_transc_amount_all(transc_count_year,type_years)
        with tabs[1]:
                col1,col2=st.columns(2)
                with col1:
                        st.subheader("Transaction in Indian States")
                        map_count_years=st.selectbox(":violet[_Select Year to know Transaction count_]",("Select Year","All","2018",
                                                                "2019",
                                                                "2020",
                                                                "2021",
                                                                "2022",
                                                                ))
                        if map_count_years=="All":
                                transaction_count_animation()
                        elif map_count_years=="2018":
                                map_count_year=st.selectbox(":violet[Select Quarter]",("Quarter","All","Q1(January-March)",
                                                "Q2(April-June)",
                                                "Q3(July-September)",
                                                "Q4(October-December)"))
                                transaction_count_animation_year(map_count_years,map_count_year)
                        elif map_count_years=="2019":
                                map_count_year=st.selectbox(":violet[Select Quarter]",("Quarter","All","Q1(January-March)",
                                                "Q2(April-June)",
                                                "Q3(July-September)",
                                                "Q4(October-December)"))
                                transaction_count_animation_year(map_count_years,map_count_year)
                        elif map_count_years=="2020":
                                map_count_year=st.selectbox(":violet[Select Quarter]",("Quarter","All","Q1(January-March)",
                                                "Q2(April-June)",
                                                "Q3(July-September)",
                                                "Q4(October-December)"))
                                transaction_count_animation_year(map_count_years,map_count_year)
                        elif map_count_years=="2021":
                                map_count_year=st.selectbox(":violet[Select Quarter]",("Quarter","All","Q1(January-March)",
                                                "Q2(April-June)",
                                                "Q3(July-September)",
                                                "Q4(October-December)"))
                                transaction_count_animation_year(map_count_years,map_count_year)
                        elif map_count_years=="2022":
                                map_count_year=st.selectbox(":violet[Select Quarter]",("Quarter","All","Q1(January-March)",
                                                "Q2(April-June)",
                                                "Q3(July-September)",
                                                "Q4(October-December)"))
                                transaction_count_animation_year(map_count_years,map_count_year)
                with col2:
                        st.subheader("Count of Transaction in Indian States")
                        count_year=st.selectbox(":violet[_Select Year_]",("Select Year","All","2018",
                                                                "2019",
                                                                "2020",
                                                                "2021",
                                                                "2022",
                                                                ))
                        if count_year=="All":
                                transaction_Counts()
                        elif count_year=="2018":
                                count_years=st.selectbox(":violet[Select Quarter]",("Quarter","All","Q1(January-March)",
                                                "Q2(April-June)",
                                                "Q3(July-September)",
                                                "Q4(October-December)"))
                                transaction_Counts_years(count_year,count_years)
                        elif count_year=="2019":
                                count_years=st.selectbox(":violet[Select Quarter]",("Quarter","All","Q1(January-March)",
                                                "Q2(April-June)",
                                                "Q3(July-September)",
                                                "Q4(October-December)"))
                                transaction_Counts_years(count_year,count_years)
                        elif count_year=="2020":
                                count_years=st.selectbox(":violet[Select Quarter]",("Quarter","All","Q1(January-March)",
                                                "Q2(April-June)",
                                                "Q3(July-September)",
                                                "Q4(October-December)"))
                                transaction_Counts_years(count_year,count_years)
                        elif count_year=="2021":
                                count_years=st.selectbox(":violet[Select Quarter]",("Quarter","All","Q1(January-March)",
                                                "Q2(April-June)",
                                                "Q3(July-September)",
                                                "Q4(October-December)"))
                                transaction_Counts_years(count_year,count_years)
                        elif count_year=="2022":
                                count_years=st.selectbox(":violet[Select Quarter]",("Quarter","All","Q1(January-March)",
                                                "Q2(April-June)",
                                                "Q3(July-September)",
                                                "Q4(October-December)"))
                                transaction_Counts_years(count_year,count_years)
        with tabs[2]:
                col1,col2=st.columns(2)
                with col1:
                        st.subheader("Registered Users")
                        state=st.selectbox("_Select State to know about Registered Users_",('select state','Andaman & Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh',
                                        'Assam', 'Bihar', 'Chandigarh', 'Chhattisgarh',
                                        'Dadra and Nagar Haveli and Daman and Diu', 'Delhi', 'Goa',
                                        'Gujarat', 'Haryana', 'Himachal Pradesh', 'Jammu & Kashmir',
                                        'Jharkhand', 'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep',
                                        'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram',
                                        'Nagaland', 'Odisha', 'Puducherry', 'Punjab', 'Rajasthan',
                                        'Sikkim', 'Tamil Nadu', 'Telangana', 'Tripura', 'Uttar Pradesh',
                                        'Uttarakhand', 'West Bengal'))
                        year_transaction=st.selectbox("select the year",(2018, 2019,2020,2021,2022,2023))
                        reg_state_all_users(year_transaction,state)
                with col2:
                        st.subheader("Districts and their Transaction Amount")
                        state=st.selectbox("_Select State to know phone model_",('select state','Andaman & Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh',
                                        'Assam', 'Bihar', 'Chandigarh', 'Chhattisgarh',
                                        'Dadra and Nagar Haveli and Daman and Diu', 'Delhi', 'Goa',
                                        'Gujarat', 'Haryana', 'Himachal Pradesh', 'Jammu & Kashmir',
                                        'Jharkhand', 'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep',
                                        'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram',
                                        'Nagaland', 'Odisha', 'Puducherry', 'Punjab', 'Rajasthan',
                                        'Sikkim', 'Tamil Nadu', 'Telangana', 'Tripura', 'Uttar Pradesh',
                                        'Uttarakhand', 'West Bengal'))
                        year_transaction=st.selectbox("select the Year",(2018, 2019, 2020, 2021, 2022, 2023))
                        reg_state_all_transaction(year_transaction,state)

if SELECT=="Facts":
        st.subheader("_Below are the basic insights of the data_")
        options = st.selectbox(":violet[_Insights_]",("---------------------------------------------------------------------------------------------Select The Facts You Want To Know-------------------------------------------------------------------------------------------",
                        "PhonePe Users throughout the years",
                        "Top 10 States With Lowest Transaction Amount",
                        "Top 10 States With Highest Transaction Amount",
                        "PhonePe users from 2018 to 2023",
                        "Top brands of mobile used",
                        "States With Highest Transaction Count",
                        "States With Lowest Transaction Count",
                        "Top 50 Districts With Highest Transaction Amount",
                        "Top 10 States with Highest PhonePe User",
                        ""
                        ))
        
        if options=="PhonePe Users throughout the years":
                question1()
        elif options=="Top 10 States With Lowest Transaction Amount":
                question2()
        elif options=="Top 10 States With Highest Transaction Amount":
                question3()
        elif options=="PhonePe users from 2018 to 2023":
                question4()
        elif options=="Top brands of mobile used":
                question5()
        elif options=="States With Highest Transaction Count":
                question6()
        elif options=="States With Lowest Transaction Count":
                question7()
        elif options=="Top 10 Districts With Highest Transaction Amount":
                question8()
        elif options=="Top 10 States with Highest PhonePe User":
                question9()
        elif options=="Top 10 Districts With Lowest Transaction Amount":
                question10()
