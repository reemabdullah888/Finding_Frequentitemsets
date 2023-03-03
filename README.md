# Finding_Frequentitemsets
Implementing the SON Algorithm using the Spark Framework to find frequent itemsets in two large datasets more efficiently in a distributed environment

* Dataset:  Ta Feng dataset (https://bit.ly/2miWqFS) 

To run the code online using Vocareum:
On Vocareum, you can call `spark-submit` located at
`/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit`. (Do not use the one at
/usr/local/bin/spark-submit (2.3.0)). We use `--executor-memory 4G --driver-memory 4G` on Vocareum
for grading.

Execution example:
Python: spark-submit task1.py <case number> <support> <input_file_path> <output_file_path>

Execution example:
Python: spark-submit task2.py <filter threshold> <support> <input_file_path> <output_file_path>
Scala: spark-submit --class task2 hw2.jar <filter threshold> <support> <input_file_path>
<output_file_path>

