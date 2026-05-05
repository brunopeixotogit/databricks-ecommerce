# Databricks notebook source
# MAGIC %md
# MAGIC # BricksShop — FAISS index rebuild
# MAGIC
# MAGIC Refreshes the embeddings Delta table and writes a new versioned
# MAGIC FAISS index file to the artifacts Volume. Pointer is updated last,
# MAGIC so a partial run never poisons the latest pointer.
# MAGIC
# MAGIC Wired into the bundle as the `bricksshop-faiss-rebuild` job.

# COMMAND ----------

# MAGIC %pip install --quiet sentence-transformers faiss-cpu numpy
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Make the project root importable. The notebook lives in /notebooks/
# and the build helpers live in /pipelines/faiss/.
import os, sys
HERE = os.path.dirname(os.path.abspath("__file__")) if "__file__" in dir() else os.getcwd()
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# COMMAND ----------

from pipelines.faiss.build import build

summary = build(show_progress=True)
print(summary)
