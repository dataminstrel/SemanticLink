# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "25325de8-036a-405e-a549-3f17d73df964",
# META       "default_lakehouse_name": "LH_SemanticLink_Data",
# META       "default_lakehouse_workspace_id": "3c6b12fe-61b2-41f0-80d2-064626805af8",
# META       "known_lakehouses": [
# META         {
# META           "id": "25325de8-036a-405e-a549-3f17d73df964"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# CELL ********************

import sempy.fabric as fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fnc_PrepareColumns(_Columns):
    _Columns.columns = _Columns.columns.str.replace('[^a-zA-Z0-9]', '', regex=True)
    _Columns.columns = _Columns.columns.str.replace('[ ]', '', regex=True)
    return _Columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Table_Name = 'Landing_Fabric_Capacities'
# Define Lakehouse name and description.
LH_Name = "LH_SemanticLink_Data"
LH_desc = "Lakehouse for Power BI usage monitoring"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mount the Lakehouse for direct file system access
lakehouse = mssparkutils.lakehouse.get(LH_Name)
mssparkutils.fs.mount(lakehouse.get("properties").get("abfsPath"), f"/{LH_Name}")

# Retrieve and store local and ABFS paths of the mounted Lakehouse
local_path = mssparkutils.fs.getMountPath(f"/{LH_Name}")
lh_abfs_path = lakehouse.get("properties").get("abfsPath")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Alle benodigde lijsten om de code vlot te laten lopen

# CELL ********************

#fill list of workspaces
workspaces = fabric.list_capacities()
workspaces = fnc_PrepareColumns(workspaces)
sparkdf = spark.createDataFrame(workspaces)
sparkdf.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(f"{lh_abfs_path}/Tables/{Table_Name}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(workspaces)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
