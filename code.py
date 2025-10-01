import requests
import json
from os.path import expanduser
from requests.auth import HTTPBasicAuth

# with open(expanduser('brain_credentials_copy.txt')) as f:
#     credentials = json.load(f)
#username,password = credentials
username = "2571517846@qq.com"
password = "a928776576"

sess =requests.Session()

sess.auth = HTTPBasicAuth(username,password)

response = sess.post('https://api.worldquantbrain.com/authentication')

print(response.status_code)
print(response.json()) 

import pandas as pd
import requests
# 2.去 数据字段 API 把某个数据集里的所有可用字段拉下来
    #searchScope = 市场范围（region, universe, delay, instrumentType）
        #dataset_id = 数据集 ID（比如 fundamental6）

def get_datafields(s, searchScope, dataset_id: str = '', search: str = ''):
    instrument_type = searchScope['instrumentType']
    region = searchScope['region']
    delay = searchScope['delay']
    universe = searchScope['universe']
    
    # 拼接 API 请求 URL
    # 如果 search 没给 → 获取整个 dataset。
    if len(search) == 0:
        url_template = "https://api.worldquantbrain.com/data-fields?" \
        f"&instrumentType={instrument_type}" \
        f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50" \
        "&offset={x}"
        count = s.get(url_template.format(x=0)).json()['count']
    
    # 如果 search 给了 → 获取符合 search 条件的前 100 个字段。
    else:
        url_template = f"https://api.worldquantbrain.com/data-fields?" \
        f"instrumentType={instrument_type}" \
        f"&region={region}&delay={str(delay)}&universe={universe}&limit=50&search={search}&offset={{x}}"
        count = 100
    # 4. 拉取数据字段并拼成 DataFrame
    datafields_list = []
    for x in range(0, count, 50):
        datafields = s.get(url_template.format(x=x))
        if datafields.status_code == 200:
            datafields_list.append(datafields.json()['results'])
        else:
            print(f"Error fetching data at offset {x}: {datafields.status_code}")
    
    datafields_list_flat = [item for sublist in datafields_list for item in sublist]
    
    datafields_df = pd.DataFrame(datafields_list_flat)
    
    return datafields_df

searchScope = {'region': 'USA', 'delay': 1, 'universe': 'TOP3000','instrumentType': 'EQUITY'}
fundamental6 = get_datafields(s = sess, searchScope = searchScope, dataset_id = 'fundamental6')

fundamental6 = fundamental6[fundamental6['type'] == 'MATRIX']
fundamental6.head()

datafields_list_fundamental6 = fundamental6['id'].values

datafields_list_fundamental6

start_index = 0 # 起点
alpha_list = []
# 遍历每一个 fundamental6 字段
# 自动拼接成 Alpha 表达式
for idx, datafield in enumerate(datafields_list_fundamental6[start_index:],start=start_index):
    print(f"#{idx}: 正在封装 Alpha 表达式")
    alpha_expression = f"-ts_delta({datafield}, 5)"
    print(alpha_expression)
    simulation_data = {
        'type': 'REGULAR',
        'settings': {
            'instrumentType': 'EQUITY',
            'region': 'USA',
            'universe': 'TOP3000',
            'delay': 1,
            'decay': 6,
            'neutralization': 'SUBINDUSTRY',
            'truncation': 0.08,
            'pasteurization': 'ON',
            'unitHandling': 'VERIFY',
            'nanHandling': 'ON',
            'language': 'FASTEXPR',
            'visualization': False,
        },
        'regular': alpha_expression
    }
    
    alpha_list.append(simulation_data)

print(f"there are {len(alpha_list)} Alphas to simulate")

from time import sleep

start_idx = 1  # 修改为中断时的编号，比如上次跑到 119，就从120继续

for idx, alpha in enumerate(alpha_list):
    if idx < start_idx:
        continue  # 跳过已经完成的
    
    sim_resp = sess.post(
        'https://api.worldquantbrain.com/simulations',
        json=alpha,
    )

    try:
        sim_progress_url = sim_resp.headers['location' ]
        while True:
            sim_progress_resp = sess.get(sim_progress_url)
            retry_after_sec = float(sim_progress_resp.headers.get("Retry-After", 0))
            if retry_after_sec == 0:  # simulation done!
                break
            sleep(retry_after_sec)

        result_json = sim_progress_resp.json()
        alpha_id = result_json.get("alpha", "N/A")
        status = result_json.get("status", "UNKNOWN")
        print(f"✅ #{idx} Done: alpha_id={alpha_id}, status={status}")

    except Exception as e:
        print(f"⚠️ #{idx} Failed: {e}")
        sleep(10)