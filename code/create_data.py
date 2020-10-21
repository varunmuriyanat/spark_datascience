import pandas as pd
import numpy as np
import random

### create_df wil create a sample pandas df with nr_rows rows
def create_df( nr_rows ):
  nr_pods = int(nr_rows/4000)
  nr_trips = int(nr_rows/4000)
  pods = ["pod_" + str(i) for i in range(nr_pods)]
  trips = ["trip_" + str(i) for i in range(nr_pods)]

  df = pd.DataFrame({
                "pod_id": [random.choice(pods) for _ in range(nr_rows)], 
                "trip_id": [random.choice(trips) for _ in range(nr_rows)], 
                "timestamp":np.random.rand(nr_rows)*35*60,
                "speed_mph": np.random.rand(nr_rows)*670.0
                })
  return df