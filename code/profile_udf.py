from pyspark.sql.functions import pandas_udf, PandasUDFType, desc
import pyspark.sql.functions as F
from pyspark.sql.types import *
import gc

def do_pandas(df):
  def calc_distance_from_speed( gdf ):
    gdf = gdf.sort_values('timestamp')
    gdf['time_diff'] = gdf['timestamp'].diff().fillna(0.0)
    return pd.DataFrame({
      'distance_miles':[ (gdf['time_diff']*gdf['speed_mph']).sum()], 
      'travel_time_sec': [ gdf['timestamp'].iloc[-1] - gdf['timestamp'].iloc[0] ]
    })

  results = df.groupby(['pod_id','trip_id']).apply( calc_distance_from_speed)
  results['distance_km'] = results['distance_miles'] * 1.609
  results['avg_speed_mph'] = results['distance_miles'] / results['travel_time_sec'] / 60.0
  results['avg_speed_kph'] = results['avg_speed_mph'] * 1.609
  return results.describe()
  
  
def do_koalas(df):

  def calc_distance_from_speed_ks( gdf ) -> ks.DataFrame[ str, str, float , float]:
    gdf = gdf.sort_values('timestamp')
    gdf['meanspeed'] = (gdf['timestamp'].diff().fillna(0.0)*gdf['speed_mph']).sum()
    gdf['triptime'] = (gdf['timestamp'].iloc[-1] - gdf['timestamp'].iloc[0])
    return gdf[['pod_id','trip_id','meanspeed','triptime']].iloc[0:1]

  results = kdf.groupby(['pod_id','trip_id']).apply( calc_distance_from_speed_ks)
  # due to current limitations of the package, groupby.apply() returns c0 .. c3 column names
  results.columns = ['pod_id', 'trip_id', 'distance_miles', 'travel_time_sec']
  # spark groupby does not set the groupby cols as index and does not sort them
  results = results.set_index(['pod_id','trip_id']).sort_index()
  results['distance_km'] = results['distance_miles'] * 1.609
  results['avg_speed_mph'] = results['distance_miles'] / results['travel_time_sec'] / 60.0
  results['avg_speed_kph'] = results['avg_speed_mph'] * 1.609
  results.describe().to_pandas()

def do_spark(sdf):

  schema = StructType([
      StructField("pod_id", StringType()),
      StructField("trip_id", StringType()),
      StructField("distance_miles", DoubleType()),
      StructField("travel_time_sec", DoubleType())
  ])

  @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
  def calc_distance_from_speed( gdf ):
    gdf = gdf.sort_values('timestamp')
    print(gdf)
    gdf['time_diff'] = gdf['timestamp'].diff().fillna(0.0)
    return pd.DataFrame({
      'pod_id':[gdf['pod_id'].iloc[0]],
      'trip_id':[gdf['trip_id'].iloc[0]],
      'distance_miles':[ (gdf['time_diff']*gdf['speed_mph']).sum()], 
      'travel_time_sec': [ gdf['timestamp'].iloc[-1]-gdf['timestamp'].iloc[0] ]
    })

  sdf = sdf.groupby("pod_id","trip_id").apply(calc_distance_from_speed)
  sdf = sdf.withColumn('distance_km',F.col('distance_miles') * 1.609)
  sdf = sdf.withColumn('avg_speed_mph',F.col('distance_miles')/ F.col('travel_time_sec') / 60.0)
  sdf = sdf.withColumn('avg_speed_kph',F.col('avg_speed_mph') * 1.609)
  return sdf.summary().toPandas()

nr_retries = 10

timings_udf = pd.DataFrame(columns = ['nr_data','pandas','pyspark','koalas'])

for size in [ 1e6, 3.2e5, 1e5, 3.2e4]:
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

  print('size :', size, 'time_pandas', time1, 'time_pyspark', time2, 'time_koalas', time3)
  timings_udf = timings_udf.append(pd.Series({'nr_data':size,'pandas':time1,'pyspark':time2,'koalas':time3}), ignore_index = True)