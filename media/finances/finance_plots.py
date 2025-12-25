import datetime
import polars as pl
from plotly.colors import qualitative
import plotly.express as px


cost_column = "Cost (€)"

scale = 3
size = 150
width = 3 * size
height = 2 * size
margin = dict(l=20, r=20, t=40, b=20)
mapping_columns = {
    "microsoft.compute/disks": "Databricks",
    "microsoft.web/sites": "Others",
    "microsoft.network/publicipaddresses": "Databricks",
    "microsoft.compute/virtualmachines": "Databricks",
    "microsoft.operationalinsights/workspaces": "Others",
    "microsoft.network/natgateways": "Databricks",
    "microsoft.datafactory/factories": "ADF",
    "microsoft.sql/servers": "Others",
    "microsoft.storage/storageaccounts": "ADLS",
    "microsoft.databricks/workspaces": "Databricks",
}
PALETTE = qualitative.Safe
column_to_color_map = {
    "Databricks": PALETTE[0],
    "ADLS": PALETTE[1],
    "ADF": PALETTE[2],
    "Others": PALETTE[3],
}
category_order = {"Resource": ["ADLS", "ADF", "Databricks", "Others"]}

df = pl.read_csv("cost-analysis.csv")

df = df.select(
    pl.col("UsageDate").cast(pl.Date).alias("Date"),
    pl.col("Cost"),
    pl.col("Cost").alias(cost_column),
    pl.col("ResourceType").replace_strict(mapping_columns).alias("Resource"),
)


# Total cost
total_cost = df["Cost"].sum()

fig = px.bar(
    df,
    x="Date",
    y=cost_column,
    color="Resource",
    color_discrete_map=column_to_color_map,
    category_orders=category_order,
    title=f"<em>Total cost ({total_cost:.2f} €)",
    template="seaborn",
    width=width,
    height=height,
)
fig.update_layout(margin=margin)

fig.write_image("total_cost.jpeg", scale=scale)

# Development time cost
dev_df = df.filter(
    pl.col("Date").is_between(
        pl.lit("2025-10-01", pl.Date), pl.lit("2025-12-14", pl.Date)
    )
)
dev_cost = dev_df["Cost"].sum()

fig = px.bar(
    dev_df,
    x="Date",
    y=cost_column,
    color="Resource",
    color_discrete_map=column_to_color_map,
    category_orders=category_order,
    title=f"<em>Development Cost ({dev_cost:.2f} €)",
    template="seaborn",
    width=width,
    height=height,
)
fig.update_layout(margin=margin)

fig.write_image("development_cost.jpeg", scale=scale)

# Development time cost no Databricks
df_no_db = df.filter(
    pl.col("Date").is_between(
        pl.lit("2025-10-01", pl.Date), pl.lit("2025-12-14", pl.Date)
    )
    & (pl.col("Resource") != pl.lit("Databricks"))
)
cost_no_db = df_no_db["Cost"].sum()

fig = px.bar(
    df_no_db,
    x="Date",
    y=cost_column,
    color="Resource",
    color_discrete_map=column_to_color_map,
    category_orders=category_order,
    title=f"<em>Development cost without Databricks ({cost_no_db:.2f} €)",
    template="seaborn",
    width=width,
    height=height,
)
fig.update_layout(margin=margin)

fig.write_image("development_cost_no_databricks.jpeg", scale=scale)


# Running cost
run_df = df.filter(
    pl.col("Date").is_between(
        pl.lit("2025-12-15", pl.Date),
        pl.lit(datetime.date.today() - datetime.timedelta(days=1), pl.Date),
    )
)
run_cost = run_df["Cost"].sum()

fig = px.bar(
    run_df,
    x="Date",
    y=cost_column,
    color="Resource",
    color_discrete_map=column_to_color_map,
    category_orders=category_order,
    title=f"<em>Running cost ({run_cost:.2f} €)",
    template="seaborn",
    width=width,
    height=height,
)
fig.update_layout(margin=margin)

fig.write_image("running_cost.jpeg", scale=scale)
