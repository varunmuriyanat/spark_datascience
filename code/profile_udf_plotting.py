import plotly.graph_objects as go
import plotly.offline as py
import numpy as np

fig = go.Figure(data=[
    go.Bar(name='pandas', x=timings_udf['nr_data'].apply(lambda x: " 1e" + str(np.log10(x))[:3]), y=timings_udf['pandas_mean'], 
      error_y=dict(
            type='data',
            symmetric=False,
            array=timings_udf['pandas_max'],
            arrayminus=timings_udf['pandas_min'],
        )),
  go.Bar(name='koalas', x=timings_udf['nr_data'].apply(lambda x: " 1e" + str(np.log10(x))[:3]), y=timings_udf['koalas_mean'], 
      error_y=dict(
            type='data',
            symmetric=False,
            array=timings_udf['koalas_max'],
            arrayminus=timings_udf['koalas_min'],
        )),
  go.Bar(name='pyspark', x=timings_udf['nr_data'].apply(lambda x: " 1e" + str(np.log10(x))[:3]), y=timings_udf['pyspark_mean'], 
      error_y=dict(
            type='data',
            symmetric=False,
            array=timings_udf['pyspark_max'],
            arrayminus=timings_udf['pyspark_min'],
        ))
])
# Change the bar mode
fig.update_layout(barmode='group')
fig.update_layout(
    title=go.layout.Title(
        text="pandas / pyspark / koalas profiling - UDF & others <br>(the lower the better)",
        xref="container",
        x=0.5
    ),
    xaxis=go.layout.XAxis(
        title=go.layout.xaxis.Title(
            text="rows [#]"
        )
    ),
    yaxis=go.layout.YAxis(
        title=go.layout.yaxis.Title(
            text="time [s]"
        )
    )
)
displayHTML(py.plot( fig, output_type='div') )