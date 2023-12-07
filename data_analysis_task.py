# Импортируем pandas для анализа данных
import pandas as pd

# Импортируем три датасета
ads = pd.read_csv("data/ads.csv")
leads = pd.read_csv("data/leads.csv")
purchases = pd.read_csv("data/purchases.csv")

# В датафреймах присутствуют переменные с датами, которые желательно переконвертировать в тип данных datetime
ads["created_at"] = pd.to_datetime(ads["created_at"])
leads["lead_created_at"] = pd.to_datetime(leads["lead_created_at"])
purchases["purchase_created_at"] = pd.to_datetime(purchases["purchase_created_at"])

# В ads столбцы d_utm_campaign и d_utm_content в формате int. Переконвертируем их в str
ads["d_utm_campaign"] = ads["d_utm_campaign"].astype("str")
ads["d_utm_content"] = ads["d_utm_content"].astype("str")

# В датафрейме ads столбец d_utm_term состоит исключительно из NaN. Его можно удалить
ads.drop("d_utm_term", axis=1, inplace=True)

# Создам дополнительные столбцы с датами в формате год-месяц. Они понадобятся для группировки
ads["ads_year_month"] = ads["created_at"].dt.strftime("%Y-%m")
leads["leads_year_month"] = leads["lead_created_at"].dt.strftime("%Y-%m")
purchases["purchases_year_month"] = purchases["purchase_created_at"].dt.strftime(
    "%Y-%m"
)

# Для финальной таблички за основу возьмем датафрейм ads. На основе его меток и их содержимого будем джойнить с датафреймом leads.
ads_clicks_cost = (
    ads.groupby(["ads_year_month", "d_utm_source", "d_utm_medium", "d_utm_campaign"])
    .agg({"m_clicks": "sum", "m_cost": "sum"})
    .reset_index()
    .rename(columns={"m_clicks": "clicks", "m_cost": "campaign_cost"})
)

# Перед тем соединить ads и leads, проведу чистку leads на предмет значений, отсутствующих в метках ads, а также NaN
leads_cl = leads[
    (leads["d_lead_utm_source"] == "yandex") & (leads["d_lead_utm_medium"] == "cpc")
]
leads_cl = leads_cl[
    leads_cl["d_lead_utm_campaign"].isin(ads["d_utm_campaign"].unique())
]
leads_cl = leads_cl[
    leads_cl["d_lead_utm_campaign"].isin(ads["d_utm_campaign"].unique())
]
leads_cl = leads_cl[leads_cl["d_lead_utm_content"].isin(ads["d_utm_content"].unique())]

# Удаляем столбец d_lead_utm_term из leads (в ads уже удалили - он был пустой). Также дропаем строки со значением NaN из client_id т.к. с отсутствующими данными не удастся сджойнить датафрейм с purchases по client_id
leads_cl.drop("d_lead_utm_term", axis=1, inplace=True)
leads_cl.dropna(subset=["client_id"], axis="rows", inplace=True)

# Джойним датафремы ads и leads
ads_leads = pd.merge(
    ads,
    leads_cl,
    how="inner",
    left_on=[
        "created_at",
        "d_utm_source",
        "d_utm_medium",
        "d_utm_campaign",
        "d_utm_content",
    ],
    right_on=[
        "lead_created_at",
        "d_lead_utm_source",
        "d_lead_utm_medium",
        "d_lead_utm_campaign",
        "d_lead_utm_content",
    ],
)

# Джойним ads_leads и purchases
df = pd.merge(ads_leads, purchases, how="inner", on="client_id")

# Создаем столбец с кол-вом дней от создания лида до покупки
df["lead_purchase_prd"] = (df["purchase_created_at"] - df["lead_created_at"]).dt.days


# В переменной min_period держим purchase_id и минимальный период лида до покупки, чтобы соблюсти правило из задания
min_period = (
    df.groupby("purchase_id")["lead_purchase_prd"]
    .min()
    .reset_index()
    .rename(columns={"lead_purchase_prd": "min_lead_purchase_prd"})
)

# Джойним эту переменную с датафреймом
df_2 = pd.merge(df, min_period, how="inner", on="purchase_id")

# Атрибуция лид - покупка:
# **-Не должно быть отрицательных периодов**
# **-Минимальный период от лида до покупки**
# **-Период не больше 15 дней**
df_final = df_2[
    (df_2["lead_purchase_prd"] >= 0)
    & (df_2["lead_purchase_prd"] == df_2["min_lead_purchase_prd"])
    & (df_2["lead_purchase_prd"] <= 15)
]


# Избавляемся от дубликатов
df_final.drop_duplicates(["client_id", "lead_id"], inplace=True)

# В столбце m_purchase_amount присутствуют покупки на нулевую сумму. От строк с такими данными стоит избавиться
df_final = df_final[df_final["m_purchase_amount"] != 0]

# Снова проверяем на дубликаты
df_final.drop_duplicates(["client_id", "purchase_id"], inplace=True)

df_final.sort_values(by="purchase_created_at", inplace=True)

# Для финальной таблички нам важно знать кол-во лидов на рекламную кампанию
num_leads = (
    ads_leads.groupby(
        [
            "ads_year_month",
            "d_lead_utm_source",
            "d_lead_utm_medium",
            "d_lead_utm_campaign",
        ]
    )
    .agg({"lead_id": "nunique"})
    .reset_index()
    .rename(columns={"lead_id": "leads_count"})
)

# Также нам важно знать кол-во уникальных purchase_id и сумму покупок
sales = (
    df_final.groupby(
        ["ads_year_month", "d_utm_source", "d_utm_medium", "d_utm_campaign"]
    )
    .agg({"purchase_id": "nunique", "m_purchase_amount": "sum"})
    .reset_index()
    .rename(columns={"purchase_id": "sales_num", "m_purchase_amount": "revenue"})
)

# Джойним ранее созданные группированные датафреймы
df_grouped = pd.merge(
    ads_clicks_cost,
    sales,
    how="left",
    on=["ads_year_month", "d_utm_source", "d_utm_medium", "d_utm_campaign"],
)

df_grouped = pd.merge(
    df_grouped,
    num_leads,
    how="left",
    left_on=["ads_year_month", "d_utm_source", "d_utm_medium", "d_utm_campaign"],
    right_on=[
        "ads_year_month",
        "d_lead_utm_source",
        "d_lead_utm_medium",
        "d_lead_utm_campaign",
    ],
)

# Считаем CPL и ROAS
df_grouped["CPL"] = (df_grouped["campaign_cost"] / df_grouped["leads_count"]).round(2)
df_grouped["ROAS"] = (df_grouped["revenue"] / df_grouped["campaign_cost"]).round(2)

df_grouped = df_grouped[
    [
        "ads_year_month",
        "d_utm_source",
        "d_utm_medium",
        "d_utm_campaign",
        "clicks",
        "campaign_cost",
        "leads_count",
        "revenue",
        "CPL",
        "ROAS",
    ]
]

# Убираем строки где стоимость компании равна нулю
df_grouped = df_grouped[df_grouped["campaign_cost"] != 0]

# Сохраняем итоговую табличку в excel формате
df_grouped.to_excel("analytics_result.xlsx", index=False)
