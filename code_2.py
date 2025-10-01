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

def get_fundamental6s(s, searchScope, dataset_id: str = '', search: str = ''):
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
    fundamental6s_list = []
    for x in range(0, count, 50):
        fundamental6s = s.get(url_template.format(x=x))
        if fundamental6s.status_code == 200:
            fundamental6s_list.append(fundamental6s.json()['results'])
        else:
            print(f"Error fetching data at offset {x}: {fundamental6s.status_code}")
    
    fundamental6s_list_flat = [item for sublist in fundamental6s_list for item in sublist]
    
    fundamental6s_df = pd.DataFrame(fundamental6s_list_flat)
    
    return fundamental6s_df

searchScope = {'region': 'USA', 'delay': 1, 'universe': 'TOP3000','instrumentType': 'EQUITY'}
fundamental6 = get_fundamental6s(s = sess, searchScope = searchScope, dataset_id = 'fundamental6')

fundamental6 = fundamental6[fundamental6['type'] == 'MATRIX']
fundamental6.head()

fundamental6s_list_fundamental6 = fundamental6['id'].values

fundamental6s_list_fundamental6

start_index = 0 # 起点
alpha_list = []
# 遍历每一个 fundamental6 字段
# 自动拼接成 Alpha 表达式
for idx, fundamental6 in enumerate(fundamental6s_list_fundamental6[start_index:],start=start_index):
    print(f"#{idx}: 正在封装 Alpha 表达式")
    alpha_expression = f" (-1 * Ts_Rank(rank({fundamental6}), 5))"
    print(alpha_expression)
    simulation_data = {
        'type': 'REGULAR',
        'settings': {
            'instrumentType': 'EQUITY',
            'region': 'USA',
            'universe': 'TOP3000',
            'delay': 1,
            'decay': 6,
            'neutralization': 'MARKET',
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
import logging

alpha_fail_attempt_tolerance = 15 # 每个alpha允许的最大失败尝试次数
submit_num = 0

# 从第6个元素开始迭代alpha_list
for alpha in alpha_list:
    keep_trying = True  # 控制while循环继续的标志
    failure_count = 0  # 记录失败尝试次数的计数器


    while keep_trying:
        try:
            # 尝试发送POST请求
            sim_resp = sess.post(
                'https://api.worldquantbrain.com/simulations',
                json=alpha  # 将当前alpha（一个JSON）发送到服务器
            )

            submit_num = 1+submit_num

            # 从响应头中获取位置
            sim_progress_url = sim_resp.headers['Location']
            logging.info(f'Alpha location is: {sim_progress_url}')  # 记录位置
            print(f'No.{submit_num}:Alpha location is: {sim_progress_url}')  # 打印位置
            keep_trying = False  # 成功获取位置，退出while循环

        except Exception as e:
            # 处理异常：记录错误，让程序休眠15秒后重试
            logging.error(f"No Location, sleep 15 and retry, error message: {str(e)}")
            print("No Location, sleep 15 and retry")
            sleep(15)  # 休眠15秒后重试
            failure_count += 1  # 增加失败尝试次数

            # 检查失败尝试次数是否达到容忍上限
            if failure_count >= alpha_fail_attempt_tolerance:
                sess = sign_in()  # 重新登录会话
                failure_count = 0  # 重置失败尝试次数
                logging.error(f"No location for too many times, move to next alpha {alpha['regular']}")  # 记录错误
                print(f"No location for too many times, move to next alpha {alpha['regular']}")  # 打印信息
                break  # 退出while循环，移动到for循环中的下一个alpha