# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy tables
# MAGIC
# MAGIC **Purpose**: This notebook runs all the DDL files defined with \[TBL\]
# MAGIC
# MAGIC ## Dependencies
# MAGIC 1. A table file will only be deployed if it follows the standard for the prefix of \[TBL\]
# MAGIC
# MAGIC ## Process
# MAGIC 1. `os.walk` -> this will go over all the files in the repo to find DDL files named like \[TBL\]
# MAGIC 2. `os.walk` (mount) -> this will gather a list of all the mount points
# MAGIC 3. `run_notebooks` -> this will run the notebooks in the parallel, making sure the mounts first, then databases and then tables
# MAGIC     - mounts are ran first because we define JDBC connections in our mount files which can relate to databases

# COMMAND ----------

dbutils.widgets.dropdown("pillar", "NDA", ["NDA", "PDA", "CEDAR", "SODA", "TEST"])

# COMMAND ----------

dbutils.widgets.dropdown("mode", "info", ["info", "execute"])

# COMMAND ----------

dbutils.widgets.text("diff-file","")

# COMMAND ----------

dbutils.widgets.dropdown("repo_name", "ESG_CDL", ["ESG_CDL","ESG_Launchpad"])

# COMMAND ----------

import os

from concurrent.futures import ThreadPoolExecutor

from py4j.protocol import Py4JJavaError

# COMMAND ----------

pillar = dbutils.widgets.get("pillar").lower()
pillar_u = dbutils.widgets.get("pillar").upper()
repo_name = dbutils.widgets.get("repo_name")
mode = dbutils.widgets.get("mode")
diff_file_name = dbutils.widgets.get("diff-file")
workspace_prefix = '/Workspace'
repo_path = f'/Repos/CodeRepo-{pillar_u}/{repo_name}/'
notebook_jobs_folder = 'notebook_jobs'
path = f'{workspace_prefix}{repo_path}{notebook_jobs_folder}{os.sep}{pillar}'

table_ddls = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import env parameters

# COMMAND ----------

import json
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
params = json.load(open("/Workspace/Repos/CodeRepo-CORE/ESG_Core/config/params.json"))

if workspace_url in params:
    notebook_params = {key: value for key, value in params[workspace_url].items() if isinstance(value, str)}
    try:
        diff_file = open(f"/Volumes/{notebook_params['catalog']}/tech/ado/{diff_file_name}").read()
    except FileNotFoundError:
        diff_file = open(f'/dbfs/ADO_deployment/{diff_file_name}').read()
else:
    notebook_params = {}
    diff_file = ''

# COMMAND ----------

def submit_notebook(notebook, params=None):
    """Function used to handle the dbutils.notebook.run"""
    try:
        print(f'- Running notebook file `{notebook}`')
        notebook_path = remove_notebook_extensions(notebook)
        dbutils.notebook.run(notebook_path, 1200, params)
    except Py4JJavaError as e:
        print(f"Failed to run the notebook `{notebook}`")
        raise ValueError(f"Failed to run the notebook `{notebook}`", e)
                     
    
def parallel_notebooks(notebooks, num_parallel):
    """Function used to run multiple ddl notebooks in parallel"""
    
    with ThreadPoolExecutor(max_workers=num_parallel) as ec:
        return [ec.submit(submit_notebook, notebook) for notebook in notebooks]
          
        
def run_notebooks(notebooks):
    """Run all ddl notebooks in parallel"""
    ddl_result = parallel_notebooks(notebooks, 1)

    result = [res.result(timeout=7200) for res in ddl_result]

    print(result) 

# COMMAND ----------

def sort_function(e):
  var = e.rfind("/")
  return e[var+1:len(e)]

# COMMAND ----------

def remove_notebook_extensions(path):
    """Remove notebook extensions (.sql, .py, .scala, .r, .ipynb)"""
    notebook_extensions = ('.sql', '.py', '.scala', '.r', '.ipynb')
    while path.lower().endswith(notebook_extensions):
        path = os.path.splitext(path)[0]
    return path

# COMMAND ----------

# Walk the directory for table files
for root, dirs, files in os.walk(path):
    for file in files:
        if ('[TBL]' in file or '[VW]' in file or '[DB]' in file or '[VOL]' in file or '[FN]' in file) and 'DDL' in root:
            if f'{root.replace(workspace_prefix,"").replace(repo_path,"")}{os.sep}{file}' in diff_file:
                table_ddls.append(f'{root.replace(workspace_prefix,"")}{os.sep}{file}')

table_ddls.sort(key=sort_function)

# COMMAND ----------

print(f'***TableDDLs***')
print("\n".join(table_ddls))

if mode == 'execute':
    for ddl in table_ddls:
        submit_notebook(ddl, notebook_params)

# COMMAND ----------

# MAGIC %run ../../notebook_jobs/core/grants/grant_access_to_catalog

# COMMAND ----------

grant_errors = []

# COMMAND ----------

try: 
    grant_errors = grant_all_privs()
except Exception as e:
    print(str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manage Schema Ownership
# MAGIC
# MAGIC This section transfers ownership of schemas owned by the current user to the admin group

# COMMAND ----------

# MAGIC %run ../../notebook_jobs/core/grants/schema_ownership_manager

# COMMAND ----------

# Manage schema ownership with the imported module
ownership_results = manage_schema_ownership(spark, catalog, env_group_prefix)

# COMMAND ----------

exit_info = "\n******GRANTS*******\n\n" + "\n".join(grant_errors) + "\n\n"
exit_info = exit_info + "******SCHEMA OWNERSHIP*******\n\n" + "\n".join(ownership_results) + "\n\n"
exit_info = exit_info + "******DDLS*******\n\n" + "\n".join(table_ddls)
dbutils.notebook.exit(exit_info)
