# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "langchain==0.3.27",
#   "langchain-core==0.3.72",
#   "langchain-community==0.3.27",
#   "langgraph==0.6.2",
#   "langchain-openai==0.3.27",
# ]
# ///
# MAGIC %pip install langchain==0.3.27 langchain-core==0.3.72 langchain-community==0.3.27 langgraph==0.6.2 langchain-openai==0.3.27

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import langchain
import langgraph
import langchain_openai

print("LangChain:", langchain.__version__)

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/UBER/uber-taxi-agentic-etl/src")
from etl_pipeline import run_etl

# COMMAND ----------

import importlib, sys

src_path = "/Workspace/UBER/uber-taxi-agentic-etl/src"
if src_path in sys.path:
    sys.path.remove(src_path)
sys.path.insert(0, src_path)

for mod_name in ['config', 'etl_pipeline']:
    sys.modules.pop(mod_name, None)
importlib.invalidate_caches()

from etl_pipeline import run_etl

result = run_etl(spark)

print("ETL execution result:")
print(result)
 

# COMMAND ----------

from config import (
    bronze_path,
    silver_path,
    gold_daily_path,
    gold_location_path,
    gold_payment_path
)

bronze_df = spark.read.format("delta").load(bronze_path)
silver_df = spark.read.format("delta").load(silver_path)

bronze_df.show(5, truncate=False)
silver_df.show(5, truncate=False)

spark.read.format("delta").load(gold_daily_path).show(10, truncate=False)
spark.read.format("delta").load(gold_location_path).show(10, truncate=False)
spark.read.format("delta").load(gold_payment_path).show(10, truncate=False)

# COMMAND ----------

import sys

sys.path.insert(0, "/Workspace/green-taxi-agentic-etl/src")

from agents import build_agent_graph
etl_agent_graph = build_agent_graph(spark)
print("LangGraph ETL agents initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC Execute the full ETL pipeline through the supervisor

# COMMAND ----------

result = etl_agent_graph.invoke({
    "query": "Run ETL on the Green Taxi CSV, "
             "validate Silver data, and analyze Gold metrics."
})

print("Selected route:", result.get("route"))
print("ETL result:", result.get("etl_result"))
print("Quality result:", result.get("quality_result"))
print("Analytics:", result.get("analytics_result"))
print("Answer:", result.get("answer"))

# COMMAND ----------

queries = [
    "Check null values and invalid records in Silver",
    "Analyze the top pickup locations by number of trips",
    "Explain the Bronze, Silver, and Gold layers"
]

for query in queries:
    result = etl_agent_graph.invoke({"query": query})

    print("Question:", query)
    print("Route:", result.get("route"))
    print("Answer:", result.get("answer"))
    print("Quality report:", result.get("quality_result"))

# COMMAND ----------

# Plot the Langgraph
from IPython.display import Image, display
display(Image(etl_agent_graph.get_graph().draw_mermaid_png()))

# COMMAND ----------

