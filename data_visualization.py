import requests
import pandas as pd
import json
import psycopg2
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu

mydb=psycopg2.connect(
        host="localhost",
        user="postgres",
        password="risehigh07",
        database="phonepe_data"
        )
mycursor=mydb.cursor()

def transaction_locations__amount_all(Year):
        mydb=psycopg2.connect(
        host="localhost",
        user="postgres",
        password="risehigh07",
        database="phonepe_data")
        mycursor=mydb.cursor()
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response =requests.get(url)
        data1 = json.loads(response.content)
        query1=(f"select states,sum(transaction_amount) as Transaction, sum(Transaction_count) as count from aggregated_transaction where years={Year} group by states order by Transaction desc")
        mycursor.execute(query1)
        table1=mycursor.fetchall()
        table=pd.DataFrame(table1,columns=["States","Transaction Amount","Transaction Count"])
        transaction=px.choropleth(table, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction Amount",
                                color_continuous_scale= "Sunsetdark",range_color= (0,400000000000), hover_name= "States", title = "TRANSACTION AMOUNT")
        transaction.update_geos(fitbounds= "locations", visible =False)
        return st.plotly_chart(transaction)

def transaction_locations_animation_year(Year,Quarter):
        mydb=psycopg2.connect(
        host="localhost",
        user="postgres",
        password="risehigh07",
        database="phonepe_data"
        )
        mycursor=mydb.cursor()
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response =requests.get(url)
        data1 = json.loads(response.content)
        query1=(f"select states,sum(transaction_amount) as Transaction, sum(Transaction_count) as count from aggregated_transaction where years={Year} and quarter={Quarter} group by states order by Transaction desc")
        mycursor.execute(query1)
        table1=mycursor.fetchall()
        table=pd.DataFrame(table1,columns=["States","Transaction Amount","Transaction Count"])
        transaction=px.choropleth(table, geojson= data1, locations= "States", 
                                featureidkey= "properties.ST_NM", 
                                color= "Transaction Amount",
                                color_continuous_scale= "Sunsetdark",
                                range_color= (0,400000000000), 
                                hover_name= "States", title = "TRANSACTION AMOUNT")
        transaction.update_geos(fitbounds= "locations", visible =False)
        return st.plotly_chart(transaction)

def transaction_locations_count_all(Year):
        mydb=psycopg2.connect(
        host="localhost",
        user="postgres",
        password="risehigh07",
        database="phonepe_data"
        )
        mycursor=mydb.cursor()    
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response =requests.get(url)
        data1 = json.loads(response.content)
        query1=(f"select states,sum(transaction_amount) as Transaction, sum(Transaction_count) as count from aggregated_transaction where years={Year} group by states order by count desc")
        mycursor.execute(query1)
        table1=mycursor.fetchall()
        table=pd.DataFrame(table1,columns=["States","Transaction Amount","Transaction Count"])
        transaction= px.choropleth(table, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                                        color_continuous_scale= "Sunsetdark", range_color= (0,3000000), hover_name= "States", title = "TRANSACTION COUNT",)
        transaction.update_geos(fitbounds= "locations", visible =False)
        transaction.update_layout(title_font= {"size":20})
        return st.plotly_chart(transaction)

def transaction_count_animation_year(Year,Quarter):
        mydb=psycopg2.connect(
        host="localhost",
        user="postgres",
        password="risehigh07",
        database="phonepe_data"
        )
        mycursor=mydb.cursor()       
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response =requests.get(url)
        data1 = json.loads(response.content)
        query1=(f"select states,sum(transaction_amount) as Transaction, sum(Transaction_count) as count from aggregated_transaction where years={Year} and quarter={Quarter} group by states order by count desc")
        mycursor.execute(query1)
        table1=mycursor.fetchall()
        table=pd.DataFrame(table1,columns=["States","Transaction Amount","Transaction Count"])
        transaction=px.choropleth(table, geojson= data1, locations= "States", 
                                featureidkey= "properties.ST_NM", 
                                color= "Transaction Count",
                                color_continuous_scale= "Sunsetdark",
                                range_color= (0,400000000000), 
                                hover_name= "States", title = "TRANSACTION COUNT")
        transaction.update_geos(fitbounds= "locations", visible =False)
        return st.plotly_chart(transaction)

def payment_transc_type_amount(Year):
        mydb=psycopg2.connect(
        host="localhost",
        user="postgres",
        password="risehigh07",
        database="phonepe_data"
        )
        mycursor=mydb.cursor()
        query1=(f"select transaction_name,sum(transaction_amount) as Amount from aggregated_transaction where years={Year} group by transaction_name order by Amount desc")
        mycursor.execute(query1)
        table1=mycursor.fetchall()
        table=pd.DataFrame(table1,columns=["Transaction Name","Transaction Amount"])
        Transc_fig= px.bar(table,x= "Transaction Name",y= "Transaction Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                color_discrete_sequence=px.colors.sequential.Blues_r)
        Transc_fig.update_layout(width=600, height= 500)
        return st.plotly_chart(Transc_fig)

def payment_transc_type_all_amount(Year,Quarter):
        mydb=psycopg2.connect(
        host="localhost",
        user="postgres",
        password="risehigh07",
        database="phonepe_data"
        )
        mycursor=mydb.cursor()
        query1=(f"select transaction_name,sum(transaction_amount) as Amount from aggregated_transaction where years={Year} and quarter={Quarter} group by transaction_name order by Amount desc")
        mycursor.execute(query1)
        table1=mycursor.fetchall()
        table=pd.DataFrame(table1,columns=["Transaction Name","Transaction Amount"])
        Transc_fig= px.bar(table,x= "Transaction Name",y= "Transaction Amount",title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                color_discrete_sequence=px.colors.sequential.Blues_r)
        Transc_fig.update_layout(width=600, height= 500)
        return st.plotly_chart(Transc_fig)

def registered_user(Year):
        mydb=psycopg2.connect(
        host="localhost",
        user="postgres",
        password="risehigh07",
        database="phonepe_data"
        )
        mycursor=mydb.cursor()
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response =requests.get(url)
        data1 = json.loads(response.content)
        query1=(f"select states,sum(registerd_users) as Users from map_user where years={Year} group by states order by Users desc")
        mycursor.execute(query1)
        table1=mycursor.fetchall()
        table=pd.DataFrame(table1,columns=["States","Registered Users"])
        transaction=px.choropleth(table, geojson= data1, locations= "States", 
                                featureidkey= "properties.ST_NM", 
                                color= "Registered Users",
                                color_continuous_scale= "Sunsetdark",
                                range_color= (0,400000000000), 
                                hover_name= "States", title = "REGISTERED USERS")
        transaction.update_geos(fitbounds= "locations", visible =False)
        return st.plotly_chart(transaction)

def registered_user_all(Year,Quarter):
        mydb=psycopg2.connect(
        host="localhost",
        user="postgres",
        password="risehigh07",
        database="phonepe_data"
        )
        mycursor=mydb.cursor()
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response =requests.get(url)
        data1 = json.loads(response.content)
        query1=(f"select states,sum(registerd_users) as Users from map_user where years={Year} and quarter={Quarter} group by states order by Users desc")
        mycursor.execute(query1)
        table1=mycursor.fetchall()
        table=pd.DataFrame(table1,columns=["States","Registered Users"])
        transaction=px.choropleth(table, geojson= data1, locations= "States", 
                                featureidkey= "properties.ST_NM", 
                                color= "Registered Users",
                                color_continuous_scale= "Sunsetdark",
                                range_color= (0,400000000000), 
                                hover_name= "States", title = "REGISTERED USERS")
        transaction.update_geos(fitbounds= "locations", visible =False)
        return st.plotly_chart(transaction)

def payment_transc_type_count(Year):
        mydb=psycopg2.connect(
        host="localhost",
        user="postgres",
        password="risehigh07",
        database="phonepe_data"
        )
        mycursor=mydb.cursor()
        query1=(f"select transaction_name,sum(transaction_count) as Count from aggregated_transaction where years={Year} group by transaction_name order by Count desc")
        mycursor.execute(query1)
        table1=mycursor.fetchall()
        table=pd.DataFrame(table1,columns=["Transaction Name","Transaction Count"])
        Transc_fig= px.bar(table,x= "Transaction Name",y= "Transaction Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                color_discrete_sequence=px.colors.sequential.Blues_r)
        Transc_fig.update_layout(width=600, height= 500)
        return st.plotly_chart(Transc_fig)

def payment_transc_type_count_all(Year,Quarter):
        mydb=psycopg2.connect(
        host="localhost",
        user="postgres",
        password="risehigh07",
        database="phonepe_data"
        )
        mycursor=mydb.cursor()
        query1=(f"select transaction_name,sum(transaction_count) as Amount from aggregated_transaction where years={Year} and quarter={Quarter} group by transaction_name order by Amount desc")
        mycursor.execute(query1)
        table1=mycursor.fetchall()
        table=pd.DataFrame(table1,columns=["Transaction Name","Transaction Count"])
        Transc_fig= px.bar(table,x= "Transaction Name",y= "Transaction Count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                color_discrete_sequence=px.colors.sequential.Blues_r)
        Transc_fig.update_layout(width=600, height= 500)
        return st.plotly_chart(Transc_fig)

#setting the streamlit application
st.set_page_config(page_title="PhonePe Visualization",layout="wide")
page_bg_img='''
<style>
[data-testid="stAppViewContainer"]{
        background-color:#FAE745;   
}
</style>'''
st.markdown(page_bg_img,unsafe_allow_html=True)
st.title(":violet[PhonePe Pulse Data Insights]")
st.subheader(":black[This is a User-Friendly Tool to know the insights about PhonePe]")

SELECT = option_menu(
        menu_title = None,
        options = ["About","Top Insights","Facts"],
        icons =["house","map","bar-chart"],
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

if SELECT=="Top Insights":
        listTabs=["***Transaction Amount***","***Transaction Count***","***PhonePe Users***","***Transaction Type***"]
        whitespace = 33
        tabs = st.tabs([s.center(whitespace,"\u2001") for s in listTabs])
        with tabs[0]:
                Year=st.selectbox("Year",("Select Year to know Transaction Amount",2018,2019,2020,2021,2022))
                Quarter=st.selectbox("Quarter",("Select Quarter to know Transaction Amount","All",1,2,3,4))
                if Year!="Select Year" and (Quarter=="All"):
                        transaction_locations__amount_all(Year)
                elif (Quarter==1) or (Quarter==2) or (Quarter==3) or (Quarter==4):
                        transaction_locations_animation_year(Year,Quarter)
        with tabs[1]:
                Year=st.selectbox("Year",("Select to know Transaction Count",2018,2019,2020,2021,2022))
                Quarter=st.selectbox("Quarter",("Select Quarter to know Transaction Count","All",1,2,3,4))
                if Year!="Select" and (Quarter=="All"):
                        transaction_locations_count_all(Year)
                elif Quarter==1 or Quarter==2 or Quarter==3 or Quarter==4 and Year!="Select":
                        transaction_count_animation_year(Year,Quarter)
        with tabs[2]:
                Year=st.selectbox("Year",("Select Year to to know about Users",2018,2019,2020,2021,2022))
                Quarter=st.selectbox("Quarter",("Select Quarter to know about Users","All",1,2,3,4))
                if Year!="Select" and (Quarter=="All"):
                        registered_user(Year)
                elif Quarter==1 or Quarter==2 or Quarter==3 or Quarter==4 and Year!="Select":
                        registered_user_all(Year,Quarter)
        with tabs[3]:
                col1,col2=st.columns(2)
                with col1:
                        Year=st.selectbox("Year",("Select Year to know transaction Type Amount",2018,2019,2020,2021,2022))
                        Quarter=st.selectbox("Quarter",("Select Quarter to know Transaction Type Amount","All",1,2,3,4))
                        if Year!="Select" and (Quarter=="All"):
                                payment_transc_type_amount(Year)
                        elif Quarter==1 or Quarter==2 or Quarter==3 or Quarter==4 and Year!="Select":
                                payment_transc_type_all_amount(Year,Quarter)
                with col2:
                        Year=st.selectbox("Year",("Select Year to know transaction Type Count",2018,2019,2020,2021,2022))
                        Quarter=st.selectbox("Quarter",("Select Quarter to know Transaction Type Count","All",1,2,3,4))
                        if Year!="Select" and (Quarter=="All"):
                                payment_transc_type_count(Year)
                        elif Quarter==1 or Quarter==2 or Quarter==3 or Quarter==4 and Year!="Select":
                                payment_transc_type_count_all(Year,Quarter)
if SELECT=="Facts":
        st.subheader("Choose the options below to know about facts of the data_")
        options = st.selectbox(":violet[_Insights_]",("---------------------------------------------------------------------------------------------Select The Facts You Want To Know-------------------------------------------------------------------------------------------",
                        "Top brands of mobile used",
                        "Top 10 District With Lowest Transaction Amount",
                        "Top 10 District With Highest Transaction Amount",
                        "PhonePe users from 2018 to 2023",
                        "Top 10 States with Highest PhonePe User",
                        "Top 10 States with Lowest PhonePe User",
                        "Top 10 Districts with Highest PhonePe User",
                        "Top 10 Districts with Lowest PhonePe User",
                        "Top 10 District with Highest Transaction Count",
                        "Top 10 District With Lowest Transaction Count",
                        ))
        if options=="Top brands of mobile used":
                Year=st.selectbox("Year",("Select Year to Know about Brands",2018,2019,2020,2021,2022))
                if Year!="Select Year to Know about Brands":
                        query1=(f"select brand_name,sum(brand_count) as Count from aggregated_user where years={Year} group by brand_name order by Count desc")
                        mycursor.execute(query1)
                        table=mycursor.fetchall()
                        table1=pd.DataFrame(table,columns=["Brand","Count"])
                        fig = px.bar(table1, x='Brand', y='Count')
                        st.plotly_chart(fig)
        elif options=="Top 10 District With Lowest Transaction Amount":
                Year=st.selectbox("Year",("Select Year for lowest transaction",2018,2019,2020,2021,2022))
                if Year!="Select Year for lowest transaction":
                        query2=(f"select district_name,sum(district_amount) as Amount from map_transaction where years={Year} group by district_name order by Amount asc")
                        mycursor.execute(query2)
                        table=mycursor.fetchall()
                        table2=pd.DataFrame(table,columns=["District","Transaction Amount"])
                        tab=table2.head(10)
                        fig = px.pie(tab,values="Transaction Amount",names="District",title="Top 10 Lowest Transactions" )        
                        st.plotly_chart(fig)
        elif options=="Top 10 District With Highest Transaction Amount":
                Year=st.selectbox("Year",("Select Year for highest transaction",2018,2019,2020,2021,2022))
                if Year!="Select Year for highest transaction":
                        query3=(f"select district_name,sum(district_amount) as Amount from map_transaction where years={Year} group by district_name order by Amount desc")
                        mycursor.execute(query3)
                        table=mycursor.fetchall()
                        table2=pd.DataFrame(table,columns=["District","Transaction Amount"])
                        tab=table2.head(10)
                        fig = px.pie(tab,values="Transaction Amount",names="District",title="Top 10 Lowest Transactions" )        
                        st.plotly_chart(fig)
        elif options=="PhonePe users from 2018 to 2023":
                query4=("select years,sum(registerd_users) as users from map_user group by years order by years")
                mycursor.execute(query4)
                table=mycursor.fetchall()
                table2=pd.DataFrame(table,columns=["Years","PhonePe Users"])
                fig = px.line(table2,x="Years",y="PhonePe Users",title="No. of Users" )        
                st.plotly_chart(fig)                
        elif options=="Top 10 States with Highest PhonePe User":
                Year=st.selectbox("Year",("Select Year for highest PhonePe User",2018,2019,2020,2021,2022))
                if Year!="Select Year for highest PhonePe User":
                        query5=(f"select states,sum(registerd_users) as users from map_user where years={Year} group by states order by users desc")
                        mycursor.execute(query5)
                        table=mycursor.fetchall()
                        table2=pd.DataFrame(table,columns=["State","Registered Users"])
                        tab=table2.head(10)
                        fig = px.pie(tab,values="Registered Users",names="State",title="Top 10 Highest PhonePe Users" )        
                        st.plotly_chart(fig)
        elif options=="Top 10 States with Lowest PhonePe User":
                Year=st.selectbox("Year",("Select Year for highest PhonePe User",2018,2019,2020,2021,2022))
                if Year!="Select Year for highest PhonePe User":
                        query5=(f"select states,sum(registerd_users) as users from map_user where years={Year} group by states order by users asc")
                        mycursor.execute(query5)
                        table=mycursor.fetchall()
                        table2=pd.DataFrame(table,columns=["State","Registered Users"])
                        tab=table2.head(10)
                        fig = px.pie(tab,values="Registered Users",names="State",title="Top 10 Lowest PhonePe Users" )        
                        st.plotly_chart(fig)
        elif options=="Top 10 Districts with Highest PhonePe User":
                Year=st.selectbox("Year",("Select Year for highest transaction",2018,2019,2020,2021,2022))
                if Year!="Select Year for highest transaction":
                        query3=(f"select district_name,sum(registerd_users) as users from map_user where years={Year} group by district_name order by users desc")
                        mycursor.execute(query3)
                        table=mycursor.fetchall()
                        table2=pd.DataFrame(table,columns=["District","Registered Users"])
                        tab=table2.head(10)
                        fig = px.pie(tab,values="Registered Users",names="District",title="Top 10 Highest PhonePe Users" )        
                        st.plotly_chart(fig)
        elif options=="Top 10 Districts with Lowest PhonePe User":
                Year=st.selectbox("Year",("Select Year for highest transaction",2018,2019,2020,2021,2022))
                if Year!="Select Year for highest transaction":
                        query3=(f"select district_name,sum(registerd_users) as users from map_user where years={Year} group by district_name order by users asc")
                        mycursor.execute(query3)
                        table=mycursor.fetchall()
                        table2=pd.DataFrame(table,columns=["District","Registered Users"])
                        tab=table2.head(10)
                        fig = px.pie(tab,values="Registered Users",names="District",title="Top 10 Lowest PhonePe Users" )        
                        st.plotly_chart(fig)
        elif options=="Top 10 District with Highest Transaction Count":
                Year=st.selectbox("Year",("Select Year for highest Count",2018,2019,2020,2021,2022))
                if Year!="Select Year for highest Count":
                        mycursor.execute("select * from map_transaction")
                        table=mycursor.fetchall()
                        table2=pd.DataFrame(table,columns=["State","Year","Quarter","District Name","Transaction Amount","Transaction Count"])
                        tab=table2 [["State","Year","District Name","Transaction Count"]]   
                        t1=tab.loc[tab["Year"]==Year]
                        a=t1.groupby(["State","District Name"])["Transaction Count"].sum().sort_values(ascending=False)
                        map2=pd.DataFrame(a).reset_index()
                        m=map2.head(10)
                        fig = px.sunburst(m, path=['State', 'District Name'], values='Transaction Count')
                        st.plotly_chart(fig)
        elif options=="Top 10 District With Lowest Transaction Count":
                Year=st.selectbox("Year",("Select Year for lowest Count",2018,2019,2020,2021,2022))
                if Year!="Select Year for lowest Count":
                        mycursor.execute("select * from map_transaction")
                        table=mycursor.fetchall()
                        table2=pd.DataFrame(table,columns=["State","Year","Quarter","District Name","Transaction Amount","Transaction Count"])
                        tab=table2 [["State","Year","District Name","Transaction Count"]]   
                        t1=tab.loc[tab["Year"]==Year]
                        a=t1.groupby(["State","District Name"])["Transaction Count"].sum().sort_values(ascending=True)
                        map2=pd.DataFrame(a).reset_index()
                        m=map2.head(10)
                        fig = px.sunburst(m, path=['State', 'District Name'], values='Transaction Count',color_continuous_scale='RdBu')
                        st.plotly_chart(fig)
