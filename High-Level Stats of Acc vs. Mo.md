```python
from pyhive import presto
import time
import pandas as pd
import numpy as np
import datetime
from scipy import stats
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
%matplotlib inline
sns.set_style('whitegrid')

import warnings
warnings.simplefilter('ignore')
```


```python
def presto_query(query, name='running'):
    """
    param query (str): A SQL query to be run on the database
    param name (str): What to refer to the query as in console updates (optional)
    
    Takes a SQL query and returns the result from the database, using the pyHive library.
    """
    # Set up connection to database
    connection = presto.connect(username='esun', host='trino.liftoff.io', port=8889, catalog = 'hive')
    cur = connection.cursor()
    print('Query ' + str(name))
    
    # Execute the query
    cur.execute(query)
    
    # Poll initial status, return link to info page
    status = cur.poll()
    print(status.get('infoUri'))
    state = 'PLANNING'
    
    # Poll state regularly, update console
    timeElapsed = 0
    while state != 'FINISHED':
        time.sleep(10)
        timeElapsed += 10
        status = cur.poll()
        stats = status.get('stats')
        state = stats.get('state')
        cpuTime = round(stats.get('cpuTimeMillis') / 1000 / 3600, 2)
        print('Time elapsed: ' + str(timeElapsed) + 's | CPU time: ' + str(cpuTime) + 'h | state: ' + state)

    
    # Collect data into output dataframe
    output = pd.DataFrame(cur.fetchall())
    
    # Update column names in output
    col_names = [i[0] for i in cur.description]
    output.columns = col_names
    
    # Close connection and return data
    cur.close()
    return output
```


```python
import pandas as pd
playrix = pd.read_csv('playrix.csv')
king = pd.read_csv('king.csv')
Acc_device_level_installs = pd.read_csv('Acc_device_level_installs.csv')
Acc_device_level_post_installs = pd.read_csv('Acc_device_level_post_installs.csv')
MO_device_level_installs = pd.read_csv('MO_device_level_installs.csv')
MO_device_level_post_installs = pd.read_csv('MO_device_level_post_installs.csv')
```


```python
Acc_device_level_installs = Acc_device_level_installs.rename(columns = {'device_id': 'req_device_id',
                                           'auction_id': 'event_id'})

Acc_device_level_post_installs = Acc_device_level_post_installs.rename(columns = {'device_id': 'req_device_id',
                                           'auction_id': 'event_id'})
MO_device_level_installs = MO_device_level_installs.rename(columns = {'device_id': 'req_device_id',
                                           'auction_id': 'event_id'})

MO_device_level_post_installs = MO_device_level_post_installs.rename(columns = {'device_id': 'req_device_id',
                                           'auction_id': 'event_id'})
```


```python
acc_install_merge_all_p = pd.merge(
    Acc_device_level_installs,
    playrix,
    how = 'inner',
    on = ['event_id','req_device_id']
)

acc_install_merge_event_id_p = pd.merge(
    Acc_device_level_installs,
    playrix,
    how = 'inner',
    on = ['event_id']   
)

acc_install_merge_device_id_p = pd.merge(
    Acc_device_level_installs,
    playrix,
    how = 'inner',
    on = ['req_device_id']
)



acc_post_install_merge_all_p = pd.merge(
    Acc_device_level_post_installs,
    playrix,
    how = 'inner',
    on = ['event_id','req_device_id']
)

acc_post_install_merge_event_id_p = pd.merge(
    Acc_device_level_post_installs,
    playrix,
    how = 'inner',
    on = ['event_id']   
)

acc_post_install_merge_device_id_p = pd.merge(
    Acc_device_level_post_installs,
    playrix,
    how = 'inner',
    on = ['req_device_id']
)
```


```python
# sanity check

print(f'playrix all logs: {len(playrix)}')
print(f'acc install all logs: {len(Acc_device_level_installs)}')
print(f'acc post install all logs: {len(Acc_device_level_post_installs)}')
print('-----')

print(f'playrix all auctions: {playrix.event_id.nunique()}')
print(f'acc all auctions-ins: {Acc_device_level_installs.event_id.nunique()}')
print(f'acc all auctions-post ins: {Acc_device_level_post_installs.event_id.nunique()}')
print(f'playrix all users: {playrix.req_device_id.nunique()}')
print(f'acc all users-ins: {Acc_device_level_installs.req_device_id.nunique()}')
print(f'acc all users-post ins: {Acc_device_level_post_installs.req_device_id.nunique()}')

print('-----')
print(f'merge by auction X device - ins - logs: {len(acc_install_merge_all_p)}')
print(f'merge by auction X device - ins - auctions: {acc_install_merge_all_p.event_id.nunique()}')
print(f'merge by auction X device - ins - users: {acc_install_merge_all_p.req_device_id.nunique()}')
print(f'merge by auction - ins - logs: {len(acc_install_merge_event_id_p)}')
print(f'merge by auction - ins - auctions: {acc_install_merge_event_id_p.event_id.nunique()}')
print(f'merge by auction - ins - users: {acc_install_merge_event_id_p.req_device_id_y.nunique()}')
print(f'merge by device - ins - logs: {len(acc_install_merge_device_id_p)}')
print(f'merge by device - ins - auctions: {acc_install_merge_device_id_p.event_id_y.nunique()}')
print(f'merge by device - ins - users: {acc_install_merge_device_id_p.req_device_id.nunique()}')

print('-----')
print(f'merge by auction X device - post ins - logs: {len(acc_post_install_merge_all_p)}')
print(f'merge by auction X device - post ins - auctions: {acc_post_install_merge_all_p.event_id.nunique()}')
print(f'merge by auction X device - post ins - users: {acc_post_install_merge_all_p.req_device_id.nunique()}')
print(f'merge by auction - post ins - logs: {len(acc_post_install_merge_event_id_p)}')
print(f'merge by auction - post ins - auctions: {acc_post_install_merge_event_id_p.event_id.nunique()}')
print(f'merge by auction - post ins - users: {acc_post_install_merge_event_id_p.req_device_id_y.nunique()}')
print(f'merge by device - post ins - logs: {len(acc_post_install_merge_device_id_p)}')
print(f'merge by device - post ins - auctions: {acc_post_install_merge_device_id_p.event_id_y.nunique()}')
print(f'merge by device - post ins - users: {acc_post_install_merge_device_id_p.req_device_id.nunique()}')
```

    playrix all logs: 1519670
    acc install all logs: 876159
    acc post install all logs: 779052
    -----
    playrix all auctions: 1519670
    acc all auctions-ins: 876159
    acc all auctions-post ins: 778574
    playrix all users: 514186
    acc all users-ins: 705863
    acc all users-post ins: 605892
    -----
    merge by auction X device - ins - logs: 39
    merge by auction X device - ins - auctions: 39
    merge by auction X device - ins - users: 39
    merge by auction - ins - logs: 44
    merge by auction - ins - auctions: 44
    merge by auction - ins - users: 44
    merge by device - ins - logs: 8639
    merge by device - ins - auctions: 8410
    merge by device - ins - users: 2681
    -----
    merge by auction X device - post ins - logs: 26
    merge by auction X device - post ins - auctions: 26
    merge by auction X device - post ins - users: 26
    merge by auction - post ins - logs: 31
    merge by auction - post ins - auctions: 31
    merge by auction - post ins - users: 31
    merge by device - post ins - logs: 4287
    merge by device - post ins - auctions: 4238
    merge by device - post ins - users: 1340



```python
mo_install_merge_device_id_p = pd.merge(
    MO_device_level_installs,
    playrix,
    how = 'inner',
    on = ['req_device_id']
)


mo_post_install_merge_device_id_p = pd.merge(
    MO_device_level_post_installs,
    playrix,
    how = 'inner',
    on = ['req_device_id']
)
```


```python
# sanity check
print(f'playrix all logs: {len(playrix)}')
print(f'mo install all logs: {len(MO_device_level_installs)}')
print(f'mo post install all logs: {len(MO_device_level_post_installs)}')
print('-----')

print(f'playrix all auctions: {playrix.event_id.nunique()}')
print(f'playrix all users: {playrix.req_device_id.nunique()}')
print(f'mo all auctions-ins: {MO_device_level_installs.req_device_id.nunique()}')
print(f'mo all auctions-post ins: {MO_device_level_post_installs.req_device_id.nunique()}')

print('-----')
print(f'merge by device - ins - logs: {len(mo_install_merge_device_id_p)}')
print(f'merge by device - ins - auctions: {mo_install_merge_device_id_p.event_id.nunique()}')
print(f'merge by device - ins - users: {mo_install_merge_device_id_p.req_device_id.nunique()}')
print(f'merge by device - post ins - logs: {len(mo_post_install_merge_device_id_p)}')
print(f'merge by device - post ins - auctions: {mo_post_install_merge_device_id_p.event_id.nunique()}')
print(f'merge by device - post ins - users: {mo_post_install_merge_device_id_p.req_device_id.nunique()}')
```

    playrix all logs: 1519670
    mo install all logs: 317468
    mo post install all logs: 15941
    -----
    playrix all auctions: 1519670
    playrix all users: 514186
    mo all auctions-ins: 317467
    mo all auctions-post ins: 15940
    -----
    merge by device - ins - logs: 9593
    merge by device - ins - auctions: 9593
    merge by device - ins - users: 2720
    merge by device - post ins - logs: 1188
    merge by device - post ins - auctions: 1188
    merge by device - post ins - users: 322



```python
mo_cpi = mo_install_merge_device_id_p[['req_os','req_payout_type','MO_attr_installs','Mo_win_bid_price']].groupby(['req_os','req_payout_type'], as_index = False).agg('sum')
mo_cpi['mo_cpi'] = mo_cpi['Mo_win_bid_price']/mo_cpi['MO_attr_installs'] 

mo_cpi_agg = mo_install_merge_device_id_p[['req_os','MO_attr_installs','Mo_win_bid_price']].groupby(['req_os'], as_index = False).agg('sum')
mo_cpi_agg['mo_cpi'] = mo_cpi_agg['Mo_win_bid_price']/mo_cpi_agg['MO_attr_installs'] 

mo_roas = mo_post_install_merge_device_id_p[['req_os','req_payout_type','MO_attr_customer_revenue_micros','Mo_win_bid_price']].groupby(['req_os','req_payout_type'], as_index = False).agg('sum')
mo_roas['mo_roas'] = mo_roas['MO_attr_customer_revenue_micros']/mo_roas['Mo_win_bid_price']/1000000 *100

mo_roas_agg = mo_post_install_merge_device_id_p[['req_os','MO_attr_customer_revenue_micros','Mo_win_bid_price']].groupby(['req_os'], as_index = False).agg('sum')
mo_roas_agg['mo_roas'] = mo_roas_agg['MO_attr_customer_revenue_micros']/mo_roas_agg['Mo_win_bid_price']/1000000 *100


# merge by device
acc_cpi_device = acc_install_merge_device_id_p[['req_os','req_payout_type','Acc_attr_installs','Lo_win_bid_price']].groupby(['req_os','req_payout_type'], as_index = False).agg('sum')
acc_cpi_device['acc_cpi'] = acc_cpi_device['Lo_win_bid_price']/acc_cpi_device['Acc_attr_installs'] 

acc_cpi_device_agg = acc_install_merge_device_id_p[['req_os','Acc_attr_installs','Lo_win_bid_price']].groupby(['req_os'], as_index = False).agg('sum')
acc_cpi_device_agg['acc_cpi'] = acc_cpi_device_agg['Lo_win_bid_price']/acc_cpi_device_agg['Acc_attr_installs'] 

acc_roas_device = acc_post_install_merge_device_id_p[['req_os','req_payout_type','attributed_customer_revenue','Lo_win_bid_price']].groupby(['req_os','req_payout_type'], as_index = False).agg('sum')
acc_roas_device['acc_roas'] = acc_roas_device['attributed_customer_revenue']/acc_roas_device['Lo_win_bid_price'] *100

acc_roas_device_agg = acc_post_install_merge_device_id_p[['req_os','attributed_customer_revenue','Lo_win_bid_price']].groupby(['req_os'], as_index = False).agg('sum')
acc_roas_device_agg['acc_roas'] = acc_roas_device_agg['attributed_customer_revenue']/acc_roas_device_agg['Lo_win_bid_price'] *100

# merged by auctions
acc_cpi_auction = acc_install_merge_event_id_p[['req_os','req_payout_type','Acc_attr_installs','Lo_win_bid_price']].groupby(['req_os','req_payout_type'], as_index = False).agg('sum')
acc_cpi_auction['acc_cpi'] = acc_cpi_auction['Lo_win_bid_price']/acc_cpi_auction['Acc_attr_installs'] 

acc_cpi_auction_agg = acc_install_merge_event_id_p[['req_os','Acc_attr_installs','Lo_win_bid_price']].groupby(['req_os'], as_index = False).agg('sum')
acc_cpi_auction_agg['acc_cpi'] = acc_cpi_auction_agg['Lo_win_bid_price']/acc_cpi_auction_agg['Acc_attr_installs'] 

acc_roas_auction = acc_post_install_merge_event_id_p[['req_os','req_payout_type','attributed_customer_revenue','Lo_win_bid_price']].groupby(['req_os','req_payout_type'], as_index = False).agg('sum')
acc_roas_auction['acc_roas'] = acc_roas_auction['attributed_customer_revenue']/acc_roas_auction['Lo_win_bid_price'] *100

acc_roas_auction_agg = acc_post_install_merge_event_id_p[['req_os','attributed_customer_revenue','Lo_win_bid_price']].groupby(['req_os'], as_index = False).agg('sum')
acc_roas_auction_agg['acc_roas'] = acc_roas_auction_agg['attributed_customer_revenue']/acc_roas_auction_agg['Lo_win_bid_price'] *100

# merged by device X auctions
acc_cpi_all = acc_install_merge_all_p[['req_os','req_payout_type','Acc_attr_installs','Lo_win_bid_price']].groupby(['req_os','req_payout_type'], as_index = False).agg('sum')
acc_cpi_all['acc_cpi'] = acc_cpi_all['Lo_win_bid_price']/acc_cpi_all['Acc_attr_installs'] 

acc_cpi_all_agg = acc_install_merge_all_p[['req_os','Acc_attr_installs','Lo_win_bid_price']].groupby(['req_os'], as_index = False).agg('sum')
acc_cpi_all_agg['acc_cpi'] = acc_cpi_all_agg['Lo_win_bid_price']/acc_cpi_all_agg['Acc_attr_installs'] 

acc_roas_all = acc_post_install_merge_all_p[['req_os','req_payout_type','attributed_customer_revenue','Lo_win_bid_price']].groupby(['req_os','req_payout_type'], as_index = False).agg('sum')
acc_roas_all['acc_roas'] = acc_roas_all['attributed_customer_revenue']/acc_roas_all['Lo_win_bid_price'] *100

acc_roas_all_agg = acc_post_install_merge_all_p[['req_os','attributed_customer_revenue','Lo_win_bid_price']].groupby(['req_os'], as_index = False).agg('sum')
acc_roas_all_agg['acc_roas'] = acc_roas_all_agg['attributed_customer_revenue']/acc_roas_all_agg['Lo_win_bid_price'] *100

display(mo_cpi)
display(mo_cpi_agg)
display(mo_roas)
display(mo_roas_agg)
display(acc_cpi_all)
display(acc_cpi_all_agg)
display(acc_roas_all)
display(acc_roas_all_agg)
display(acc_cpi_auction)
display(acc_cpi_auction_agg)
display(acc_roas_auction)
display(acc_roas_auction_agg)
display(acc_cpi_device)
display(acc_cpi_device_agg)
display(acc_roas_device)
display(acc_roas_device_agg)


```


<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>req_os</th>
      <th>req_payout_type</th>
      <th>MO_attr_installs</th>
      <th>Mo_win_bid_price</th>
      <th>mo_cpi</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>android</td>
      <td>CPM</td>
      <td>8037</td>
      <td>527530.483</td>
      <td>65.637736</td>
    </tr>
    <tr>
      <th>1</th>
      <td>android</td>
      <td>FLAT_CPM</td>
      <td>1651</td>
      <td>98182.884</td>
      <td>59.468737</td>
    </tr>
    <tr>
      <th>2</th>
      <td>android</td>
      <td>REVENUE_SHARE</td>
      <td>334</td>
      <td>16853.912</td>
      <td>50.460814</td>
    </tr>
  </tbody>
</table>
</div>



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>req_os</th>
      <th>MO_attr_installs</th>
      <th>Mo_win_bid_price</th>
      <th>mo_cpi</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>android</td>
      <td>10022</td>
      <td>642567.279</td>
      <td>64.115673</td>
    </tr>
  </tbody>
</table>
</div>



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>req_os</th>
      <th>req_payout_type</th>
      <th>MO_attr_customer_revenue_micros</th>
      <th>Mo_win_bid_price</th>
      <th>mo_roas</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>android</td>
      <td>CPM</td>
      <td>22830000</td>
      <td>46901.565</td>
      <td>0.048676</td>
    </tr>
    <tr>
      <th>1</th>
      <td>android</td>
      <td>FLAT_CPM</td>
      <td>0</td>
      <td>13649.235</td>
      <td>0.000000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>android</td>
      <td>REVENUE_SHARE</td>
      <td>0</td>
      <td>751.108</td>
      <td>0.000000</td>
    </tr>
  </tbody>
</table>
</div>



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>req_os</th>
      <th>MO_attr_customer_revenue_micros</th>
      <th>Mo_win_bid_price</th>
      <th>mo_roas</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>android</td>
      <td>22830000</td>
      <td>61301.908</td>
      <td>0.037242</td>
    </tr>
  </tbody>
</table>
</div>



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>req_os</th>
      <th>req_payout_type</th>
      <th>Acc_attr_installs</th>
      <th>Lo_win_bid_price</th>
      <th>acc_cpi</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>android</td>
      <td>CPM</td>
      <td>18.0</td>
      <td>2047.242268</td>
      <td>113.735682</td>
    </tr>
    <tr>
      <th>1</th>
      <td>android</td>
      <td>FLAT_CPM</td>
      <td>1.0</td>
      <td>100.861594</td>
      <td>100.861594</td>
    </tr>
    <tr>
      <th>2</th>
      <td>android</td>
      <td>REVENUE_SHARE</td>
      <td>3.0</td>
      <td>305.630854</td>
      <td>101.876951</td>
    </tr>
    <tr>
      <th>3</th>
      <td>iOS</td>
      <td>CPM</td>
      <td>4.0</td>
      <td>493.665188</td>
      <td>123.416297</td>
    </tr>
  </tbody>
</table>
</div>



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>req_os</th>
      <th>Acc_attr_installs</th>
      <th>Lo_win_bid_price</th>
      <th>acc_cpi</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>android</td>
      <td>22.0</td>
      <td>2453.734716</td>
      <td>111.533396</td>
    </tr>
    <tr>
      <th>1</th>
      <td>iOS</td>
      <td>4.0</td>
      <td>493.665188</td>
      <td>123.416297</td>
    </tr>
  </tbody>
</table>
</div>



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>req_os</th>
      <th>req_payout_type</th>
      <th>attributed_customer_revenue</th>
      <th>Lo_win_bid_price</th>
      <th>acc_roas</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>android</td>
      <td>CPM</td>
      <td>43.87</td>
      <td>1593.501126</td>
      <td>2.753057</td>
    </tr>
    <tr>
      <th>1</th>
      <td>android</td>
      <td>FLAT_CPM</td>
      <td>0.00</td>
      <td>41.238420</td>
      <td>0.000000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>android</td>
      <td>REVENUE_SHARE</td>
      <td>0.00</td>
      <td>244.850893</td>
      <td>0.000000</td>
    </tr>
    <tr>
      <th>3</th>
      <td>iOS</td>
      <td>CPM</td>
      <td>0.00</td>
      <td>398.490411</td>
      <td>0.000000</td>
    </tr>
  </tbody>
</table>
</div>



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>req_os</th>
      <th>attributed_customer_revenue</th>
      <th>Lo_win_bid_price</th>
      <th>acc_roas</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>android</td>
      <td>43.87</td>
      <td>1879.590439</td>
      <td>2.334019</td>
    </tr>
    <tr>
      <th>1</th>
      <td>iOS</td>
      <td>0.00</td>
      <td>398.490411</td>
      <td>0.000000</td>
    </tr>
  </tbody>
</table>
</div>



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>req_os</th>
      <th>req_payout_type</th>
      <th>Acc_attr_installs</th>
      <th>Lo_win_bid_price</th>
      <th>acc_cpi</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>android</td>
      <td>CPM</td>
      <td>18.0</td>
      <td>2047.242268</td>
      <td>113.735682</td>
    </tr>
    <tr>
      <th>1</th>
      <td>android</td>
      <td>FLAT_CPM</td>
      <td>1.0</td>
      <td>100.861594</td>
      <td>100.861594</td>
    </tr>
    <tr>
      <th>2</th>
      <td>android</td>
      <td>REVENUE_SHARE</td>
      <td>3.0</td>
      <td>305.630854</td>
      <td>101.876951</td>
    </tr>
    <tr>
      <th>3</th>
      <td>iOS</td>
      <td>CPM</td>
      <td>9.0</td>
      <td>788.028598</td>
      <td>87.558733</td>
    </tr>
  </tbody>
</table>
</div>



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>req_os</th>
      <th>Acc_attr_installs</th>
      <th>Lo_win_bid_price</th>
      <th>acc_cpi</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>android</td>
      <td>22.0</td>
      <td>2453.734716</td>
      <td>111.533396</td>
    </tr>
    <tr>
      <th>1</th>
      <td>iOS</td>
      <td>9.0</td>
      <td>788.028598</td>
      <td>87.558733</td>
    </tr>
  </tbody>
</table>
</div>



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>req_os</th>
      <th>req_payout_type</th>
      <th>attributed_customer_revenue</th>
      <th>Lo_win_bid_price</th>
      <th>acc_roas</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>android</td>
      <td>CPM</td>
      <td>43.87</td>
      <td>1593.501126</td>
      <td>2.753057</td>
    </tr>
    <tr>
      <th>1</th>
      <td>android</td>
      <td>FLAT_CPM</td>
      <td>0.00</td>
      <td>41.238420</td>
      <td>0.000000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>android</td>
      <td>REVENUE_SHARE</td>
      <td>0.00</td>
      <td>244.850893</td>
      <td>0.000000</td>
    </tr>
    <tr>
      <th>3</th>
      <td>iOS</td>
      <td>CPM</td>
      <td>4.99</td>
      <td>692.853821</td>
      <td>0.720210</td>
    </tr>
  </tbody>
</table>
</div>



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>req_os</th>
      <th>attributed_customer_revenue</th>
      <th>Lo_win_bid_price</th>
      <th>acc_roas</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>android</td>
      <td>43.87</td>
      <td>1879.590439</td>
      <td>2.334019</td>
    </tr>
    <tr>
      <th>1</th>
      <td>iOS</td>
      <td>4.99</td>
      <td>692.853821</td>
      <td>0.720210</td>
    </tr>
  </tbody>
</table>
</div>



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>req_os</th>
      <th>req_payout_type</th>
      <th>Acc_attr_installs</th>
      <th>Lo_win_bid_price</th>
      <th>acc_cpi</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>android</td>
      <td>CPM</td>
      <td>3232.0</td>
      <td>186663.973388</td>
      <td>57.754942</td>
    </tr>
    <tr>
      <th>1</th>
      <td>android</td>
      <td>FLAT_CPM</td>
      <td>774.0</td>
      <td>79444.538223</td>
      <td>102.641522</td>
    </tr>
    <tr>
      <th>2</th>
      <td>android</td>
      <td>REVENUE_SHARE</td>
      <td>136.0</td>
      <td>15094.144730</td>
      <td>110.986358</td>
    </tr>
    <tr>
      <th>3</th>
      <td>iOS</td>
      <td>CPM</td>
      <td>150.0</td>
      <td>19060.748056</td>
      <td>127.071654</td>
    </tr>
    <tr>
      <th>4</th>
      <td>iOS</td>
      <td>FLAT_CPM</td>
      <td>23.0</td>
      <td>4969.777258</td>
      <td>216.077272</td>
    </tr>
    <tr>
      <th>5</th>
      <td>iOS</td>
      <td>REVENUE_SHARE</td>
      <td>2.0</td>
      <td>41.266722</td>
      <td>20.633361</td>
    </tr>
  </tbody>
</table>
</div>



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>req_os</th>
      <th>Acc_attr_installs</th>
      <th>Lo_win_bid_price</th>
      <th>acc_cpi</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>android</td>
      <td>4142.0</td>
      <td>281202.656341</td>
      <td>67.890550</td>
    </tr>
    <tr>
      <th>1</th>
      <td>iOS</td>
      <td>175.0</td>
      <td>24071.792036</td>
      <td>137.553097</td>
    </tr>
  </tbody>
</table>
</div>



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>req_os</th>
      <th>req_payout_type</th>
      <th>attributed_customer_revenue</th>
      <th>Lo_win_bid_price</th>
      <th>acc_roas</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>android</td>
      <td>CPM</td>
      <td>2403.579859</td>
      <td>89990.735608</td>
      <td>2.670919</td>
    </tr>
    <tr>
      <th>1</th>
      <td>android</td>
      <td>FLAT_CPM</td>
      <td>1338.509926</td>
      <td>49217.332346</td>
      <td>2.719591</td>
    </tr>
    <tr>
      <th>2</th>
      <td>android</td>
      <td>REVENUE_SHARE</td>
      <td>79.859996</td>
      <td>9632.816588</td>
      <td>0.829041</td>
    </tr>
    <tr>
      <th>3</th>
      <td>iOS</td>
      <td>CPM</td>
      <td>288.369965</td>
      <td>8385.839722</td>
      <td>3.438773</td>
    </tr>
    <tr>
      <th>4</th>
      <td>iOS</td>
      <td>FLAT_CPM</td>
      <td>0.000000</td>
      <td>2719.489737</td>
      <td>0.000000</td>
    </tr>
    <tr>
      <th>5</th>
      <td>iOS</td>
      <td>REVENUE_SHARE</td>
      <td>0.000000</td>
      <td>41.266722</td>
      <td>0.000000</td>
    </tr>
  </tbody>
</table>
</div>



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>req_os</th>
      <th>attributed_customer_revenue</th>
      <th>Lo_win_bid_price</th>
      <th>acc_roas</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>android</td>
      <td>3821.949781</td>
      <td>148840.884542</td>
      <td>2.567809</td>
    </tr>
    <tr>
      <th>1</th>
      <td>iOS</td>
      <td>288.369965</td>
      <td>11146.596181</td>
      <td>2.587067</td>
    </tr>
  </tbody>
</table>
</div>

