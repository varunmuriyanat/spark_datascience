
import databricks.koalas as ks
from pyspark.sql.functions import desc
from pyspark.sql import functions as F
import gc

def do_pandas(df):
  grouped = df.groupby(['pod_id','trip_id'])
  gdf = df.groupby(['pod_id','trip_id']).agg({'timestamp': ['min','max']})
  gdf.columns = ['timestamp_first','timestamp_last']
  gdf['trip_time_sec'] = gdf['timestamp_last'] - gdf['timestamp_first']
  gdf['trip_time_hours'] = gdf['trip_time_sec'] / 3600.0 
  x = gdf.describe()
  return gdf, x
  
  
def do_koalas(df):
  gdf, x = do_pandas(df)
  return gdf.to_pandas(), x.to_pandas()

def do_spark(sdf):
  sdf2 = sdf.groupBy("pod_id", "trip_id").agg(F.max('timestamp').alias('timestamp_last'), F.min('timestamp').alias('timestamp_first'))
  sdf3 = sdf2.withColumn('trip_time_sec', sdf2['timestamp_last'] - sdf2['timestamp_first'])
  sdf4 = sdf3.withColumn('trip_time_hours', sdf3['trip_time_sec']/3600.0)
  return sdf4.toPandas(), sdf4.select(F.col('timestamp_last'),F.col('timestamp_first'),F.col('trip_time_sec'),F.col('trip_time_hours')).summary().toPandas()

nr_retries = 10

timings = pd.DataFrame(columns = ['nr_data','pandas','pyspark','koalas'])

for size in [1e8, 3.2e7,  1e7, 3.2e6]:
  df = create_df(int(size))
  
  time1 = []
  for i in range(nr_retries):
    print(size, 'pandas',i)
    t = time.time()
    do_pandas(df)
    time1.append( (time.time() - t))
    gc.collect()

  sdf = spark.createDataFrame(df)
  
  time2 = []
  for i in range(nr_retries):
    print(size, 'pyspark',i)
    t = time.time()
    do_spark(sdf)
    time2.append((time.time() - t))
    gc.collect()
  
  kdf = ks.from_pandas(df)
  
  time3 = []
  for i in range(nr_retries):
    print(size, 'koalas',i)
    t = time.time()
    do_koalas(kdf)
    time3.append( (time.time() - t))
    gc.collect()

  print('\nsize :', size, 'time_pandas', time1, 'time_pyspark', time2, 'time_koalas', time3)
  timings = timings.append(pd.Series({'nr_data':size,'pandas':time1,'pyspark':time2,'koalas':time3}), ignore_index = True)
