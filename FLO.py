#   İŞ PROBLEMİ
#   Online ayakkabı mağazası olan FLO müşterilerini segmentlere ayırıp bu segmentlere göre
#   pazarlama stratejileri belirlemek istiyor.
#   Buna yönelik olarak müşterilerin davranışları tanımlanacak
#   ve bu davranışlardaki öbeklenmelere göre gruplar oluşturulacak.

#VERİ SETİ HİKAYESİ
#Veri seti Flo’dan son alışverişlerini 2020 - 2021 yıllarında OmniChannel
# (hem online hem offline alışveriş yapan)
# olarak yapan müşterilerin geçmiş alışveriş davranışlarından elde edilen
# bilgilerden oluşmaktadır.

#master_id:Eşsiz müşteri numarası
#order_channel:Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile)
#last_order_channel:En son alışverişin yapıldığı kanal
#first_order_date:Müşterinin yaptığı ilk alışveriş tarihi
#last_order_date:Müşterinin yaptığı son alışveriş tarihi
#last_order_date_online:Müşterinin online platformda yaptığı son alışveriş tarihi
#last_order_date_offline:Müşterinin offline platformda yaptığı son alışveriş tarihi
#order_num_total_ever_online:Müşterinin online platformda yaptığı toplam alışveriş sayısı
#order_num_total_ever_offline:Müşterinin offline'da yaptığı toplam alışveriş sayısı
#customer_value_total_ever_offline:Müşterinin offline alışverişlerinde ödediği toplam ücret
#customer_value_total_ever_online:Müşterinin online alışverişlerinde ödediği toplam ücret
#interested_in_categories_12:Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi

import datetime as dt
import pandas as pd
pd.set_option('display.max_columns',None)
pd.set_option('display.width',500)
pd.set_option('display.float_format',lambda x:'%.3f' % x)

#GÖREV1
#ADIM1
df_= pd.read_csv("/Users/irem/Desktop/FLO_RFM_Analizi/flo_data_20k.csv")
df_FLO=df_.copy()

#ADIM2
df_FLO.head(10)
df_FLO.columns
df_FLO.describe().T
df_FLO.isnull().sum()
df_FLO.dtypes

#ADIM3
df_FLO["order_num_total"]=df_FLO["order_num_total_ever_online"] + df_FLO["order_num_total_ever_offline"] #toplam alışveriş sayısı
df_FLO["customer_value_total"]=df_FLO["customer_value_total_ever_online"] + df_FLO["customer_value_total_ever_offline"] #toplam harcama


#ADIM4
df_FLO["first_order_date"]=pd.to_datetime(df_FLO["first_order_date"])
df_FLO["last_order_date"]=pd.to_datetime(df_FLO["last_order_date"])
df_FLO["last_order_date_online"]=pd.to_datetime(df_FLO["last_order_date_online"])
df_FLO["last_order_date_offline"]=pd.to_datetime(df_FLO["last_order_date_offline"])
df_FLO.info()

df_FLO.shape[0] #toplam müşteri sayısı
df_FLO["master_id"].nunique()

#ADIM5
df_FLO.groupby("order_channel").agg({"master_id":"count",
                                 "order_num_total":"sum",
                                 "customer_value_total":"sum"})

#ADIM6
df_FLO.groupby("master_id").agg({"customer_value_total":"sum"}).sort_values("customer_value_total",ascending=False).head(10)

#ADIM7
df_FLO.groupby("master_id").agg({"order_num_total":"sum"}).sort_values("order_num_total",ascending=False).head(10)

#ADIM8
def data_prep(dataframe):
    dataframe["order_num_total"] = dataframe["order_num_total_ever_online"] + dataframe["order_num_total_ever_offline"]
    dataframe["customer_value_total"] = dataframe["customer_value_total_ever_offline"] + dataframe["customer_value_total_ever_online"]
    date_columns = dataframe.columns[dataframe.columns.str.contains("date")]
    dataframe[date_columns] = dataframe[date_columns].apply(pd.to_datetime)
    return df

#GÖREV2
#recency
df_FLO["last_order_date"].max()
analysis_date=dt.datetime(2021,6,1)
type(analysis_date)

rfm = pd.DataFrame()
rfm["customer_id"] = df_FLO["master_id"]
rfm["recency"] = (analysis_date - df_FLO["last_order_date"]).astype('timedelta64[D]')
rfm["frequency"] = df_FLO["order_num_total"]
rfm["monetary"] = df_FLO["customer_value_total"]



#GOREV3
rfm["recency_score"]=pd.qcut(rfm["recency"],5,labels=[5,4,3,2,1])
rfm["monetary_score"]=pd.qcut(rfm["monetary"],5,labels=[1,2,3,4,5])
rfm["frequency_score"]=pd.qcut(rfm["frequency"].rank(method="first"),5,labels=[1,2,3,4,5])
rfm.head()

rfm["RF_SCORE"]=(rfm["recency_score"].astype(str)+rfm["frequency_score"].astype(str))

#GOREV4

seg_map={
    r'[1-2][1-2]':'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5':'cant_loose',
    r'3[1-2]':'about_to_sleep',
    r'33':'need_attention',
    r'[3-4][4-5]':'loyal_customers',
    r'41':'promising',
    r'51':'new_customers',
    r'[4-5][2-3]':'potential_loyalist',
    r'5[4-5]':'champions'
}

rfm["segment"]=rfm["RF_SCORE"].replace(seg_map,regex=True)

#GOREV5
#ADIM1
rfm[["segment","recency","frequency","monetary"]].groupby("segment").agg("mean")

#ADIM2
#a)
new_brand_=rfm.loc[(rfm['segment']=='champions') | (rfm['segment']=='loyal_customers'),'customer_id']
df_FLO.loc[df_FLO['interested_in_categories_12'].str.contains('KADIN'),'master_id']

df_FLO.loc[df_FLO['master_id'].isin(new_brand_.values)]

#b)
target_segments_customer_ids = rfm[rfm["segment"].isin(["cant_loose","hibernating","new_customers"])]["customer_id"]
cust_ids = df_FLO[(df_FLO["master_id"].isin(target_segments_customer_ids)) & ((df_FLO["interested_in_categories_12"].str.contains("ERKEK"))|(df_FLO["interested_in_categories_12"].str.contains("COCUK")))]["master_id"]


cust_ids.to_csv("indirim_hedef_müşteri_ids.csv", index=False)








