import pandas as pd
import re
import requests
import json
import time
import datetime
import os

# ====== 飞书多维表格API相关配置 ======
TOKEN_FILE = "feishu_token.json"
app_token = "MEwYb1UL4aSSpIsn6V5c9gzXnKe"  # 多维表格的app_token
app_id = "cli_a8fdaf1afff39013"      # 填入你的应用 app_id（在开发者后台查看）
app_secret = "o6qUUHKhMhymwaVM4u40H2zQAFrhdHm7"  # 你的应用 app_secret

def extract_total_views(views_str):
    """提取播放量，处理两种不同格式"""
    if not views_str or pd.isna(views_str):
        return 0
        
    views_str = str(views_str)
    
    # 处理格式2: 总播放：1715(+9) 这种格式
    total_match = re.search(r'总播放[：:]\s*(\d+)(?:\(\+\d+\))?', views_str)
    if total_match:
        return int(total_match.group(1))
    
    # 如果没有总播放数，尝试加总所有数字
    numbers = re.findall(r'\d+', views_str)
    if numbers:
        return sum(int(num) for num in numbers)
    
    return 0

class TokenManager:
    def __init__(self):
        self.token_file = TOKEN_FILE
        self.token_data = self._load_token_data()

    def _load_token_data(self):
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def _save_token_data(self):
        with open(self.token_file, 'w') as f:
            json.dump(self.token_data, f)

    def get_tenant_access_token(self):
        """获取或刷新tenant_access_token"""
        current_time = int(time.time())
        
        # 检查现有token是否还有效
        if (self.token_data.get('tenant_access_token') and 
            self.token_data.get('expire_time', 0) > current_time + 60):  # 留60秒余量
            return self.token_data['tenant_access_token']

        # 获取新token
        url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal"
        data = {
            "app_id": app_id,
            "app_secret": app_secret
        }
        try:
            response = requests.post(url, json=data)
            resp_data = response.json()
            print(f"获取新token返回: {resp_data}")
            
            if resp_data.get("code") == 0:
                token = resp_data.get("tenant_access_token")
                expire = resp_data.get("expire")  # 通常是7200秒
                
                # 保存token和过期时间
                self.token_data = {
                    'tenant_access_token': token,
                    'expire_time': current_time + expire
                }
                self._save_token_data()
                return token
                
            raise Exception(f"获取tenant_access_token失败: {resp_data}")
        except Exception as e:
            print(f"获取token异常: {e}")
            raise

def get_tables(token_manager, app_token):
    """获取表格ID，带重试机制"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"
            token = token_manager.get_tenant_access_token()
            headers = {'Authorization': f'Bearer {token}'}
            resp = requests.get(url, headers=headers)
            data = resp.json()
            print(f"获取表格返回: {data}")
            
            if data.get("code") == 0:
                return data["data"]["items"][0]["table_id"]
            elif data.get("code") in [99991677, 99991661]:  # token失效
                print("Token失效，尝试刷新...")
                token_manager.token_data = {}  # 清除缓存的token
                continue
            else:
                raise Exception(f"获取table_id失败: {data}")
        except Exception as e:
            if attempt == max_retries - 1:  # 最后一次尝试
                raise
            print(f"获取表格失败，重试中: {e}")
            time.sleep(1)  # 等待1秒后重试

def add_record(token_manager, app_token, table_id, fields):
    """添加记录，带重试机制"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
            token = token_manager.get_tenant_access_token()
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            data = {"fields": fields}
            print(f"请求URL: {url}")
            print(f"请求Headers: {headers}")
            print(f"请求Data: {data}")
            
            resp = requests.post(url, headers=headers, json=data)
            result = resp.json()
            print(f"写入表格返回: {result}")
            
            if result.get("code") == 0:
                return result
            elif result.get("code") in [99991677, 99991661]:  # token失效
                print("Token失效，尝试刷新...")
                token_manager.token_data = {}  # 清除缓存的token
                continue
            else:
                raise Exception(f"写入记录失败: {result}")
        except Exception as e:
            if attempt == max_retries - 1:  # 最后一次尝试
                raise
            print(f"写入记录失败，重试中: {e}")
            time.sleep(1)  # 等待1秒后重试

def date_to_timestamp(date_str):
    """将日期字符串转换为时间戳
    date_str 格式为 '2025-07-16' 或 '07.16'
    返回毫秒级时间戳
    """
    try:
        if '-' in date_str:
            dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        elif '.' in date_str:
            month_day = date_str.split('.')
            if len(month_day) >= 2:  # 至少有月和日
                month = month_day[0].zfill(2)
                day = month_day[1].zfill(2)
                dt = datetime.datetime.strptime(f"2025-{month}-{day}", "%Y-%m-%d")
        # 设置时间为当天的00:00:00
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        # 转换为毫秒级时间戳
        return int(dt.timestamp() * 1000)
    except Exception as e:
        print(f"日期转换失败: {e}")
        return None

def parse_file_info(file_name):
    """从文件名提取归属和日期
    支持以下格式：
    - 北斗_07.25.10.2.csv/.xlsx  (归属_月.日.其他信息.csv/.xlsx)
    - 中科_07.26.csv/.xlsx       (归属_月.日.csv/.xlsx)
    """
    try:
        # 移除文件扩展名
        if file_name.endswith('.csv'):
            base = file_name[:-4]
        elif file_name.endswith('.xlsx'):
            base = file_name[:-5]
        else:
            base = file_name
            
        parts = base.split('_')
        if len(parts) < 2:  # 至少需要归属和日期两部分
            return "", None
            
        owner = parts[0]  # 第一部分是归属
        date_part = parts[1]  # 第二部分包含日期
        
        # 提取月和日
        date_parts = date_part.split('.')
        if len(date_parts) >= 2:  # 至少包含月和日
            month = date_parts[0].zfill(2)  # 补齐前导零
            day = date_parts[1].zfill(2)    # 补齐前导零
            date_str = f"2025-{month}-{day}"
            try:
                timestamp = date_to_timestamp(date_str)
                if timestamp:
                    print(f"成功解析文件名 {file_name}: 归属={owner}, 日期={date_str}")
                    return owner, timestamp
            except Exception as e:
                print(f"解析日期失败: {e}")
                
    except Exception as e:
        print(f"解析文件名失败 {file_name}: {e}")
    
    return "", None

def read_data_file(file_path):
    """读取数据文件，支持CSV和Excel格式"""
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith('.xlsx'):
        return pd.read_excel(file_path)
    else:
        raise ValueError(f"不支持的文件格式: {file_path}")

def process_data_file(file_path):
    """处理单个数据文件（CSV或Excel），返回播放量和上传数"""
    df = read_data_file(file_path)
    
    # 1. 处理上传成功数量
    upload_success_rows = df[df['状态'].astype(str).str.contains("上传成功", na=False)]
    total_uploads = 0
    for idx, row in upload_success_rows.iterrows():
        matches = re.findall(r'上传成功#(\d+)个', str(row['状态']))
        if matches:
            n = sum(int(match) for match in matches)
            total_uploads += n
    
    # 2. 处理播放量
    file_views_all = 0
    
    # 检查是否有播放量列
    play_column = None
    for col in df.columns:
        if '播放' in str(col):
            play_column = col
            # 如果列名中包含数字（如 播放(653996)），直接提取
            header_match = re.search(r'播放\((\d+)\)', str(col))
            if header_match:
                file_views_all = int(header_match.group(1))
                break
    
    # 如果没有从表头获取到播放量，则处理内容
    if file_views_all == 0 and play_column is not None:
        for views_str in df[play_column]:
            views = extract_total_views(views_str)
            file_views_all += views
    
    return file_views_all, total_uploads

def get_data_files():
    """自动获取当前目录下的所有CSV和Excel文件"""
    data_files = []
    for file in os.listdir('.'):
        if file.endswith('.csv') or file.endswith('.xlsx'):
            # 排除系统文件和临时文件
            if not file.startswith('.') and not file.startswith('~'):
                data_files.append(file)
    return sorted(data_files)

# ====== 统计数据部分 ======
# 自动检测数据文件，也可以手动指定
auto_files = get_data_files()
print(f"检测到的数据文件: {auto_files}")

# 可以选择使用自动检测的文件或手动指定
use_auto = input("是否使用自动检测的文件？(y/n，默认y): ").strip().lower()
if use_auto in ['', 'y', 'yes']:
    file_list = auto_files
else:
    # 手动指定文件列表
    file_list = ["中科_07.23.06.csv","中科_07.23.00.csv"]

all_total_uploads = 0
all_total_views_all = 0
total_xianliu = 0
total_weixianliu = 0
total_failed_judge = 0

# 只询问一次默认上传视频数量
default_upload_count = int(input("请输入每条记录的默认上传视频数量（例如 3 或 4）: "))

for file_path in file_list:
    try:
        df = read_data_file(file_path)

        # 计算上传成功总数
        if "上传数量" in df.columns:
            upload_column = df["上传数量"].fillna(0).astype(int)
            success_counts = default_upload_count - upload_column
            success_counts = success_counts.apply(lambda x: max(x, 0))  # 防止负数
            all_total_uploads += success_counts.sum()
        else:
            print("未找到“上传数量”列，无法计算上传成功数量。")


        # 提取限流统计
        for status in df["状态"].dropna().astype(str).tolist():
            if "最近一小时发布视频" in status:
                match_detail = re.search(r"(\d+)个未限流，(\d+)个限流，(\d+)个判断失败", status)
                if match_detail:
                    total_weixianliu += int(match_detail.group(1))
                    total_xianliu += int(match_detail.group(2))
                    total_failed_judge += int(match_detail.group(3))

        # 提取播放量
        file_views_all = 0
        play_column = None
        for col in df.columns:
            if '播放' in str(col):
                play_column = col
                header_match = re.search(r'播放\((\d+)\)', str(col))
                if header_match:
                    file_views_all = int(header_match.group(1))
                    break
        if file_views_all == 0 and play_column:
            for views_str in df[play_column]:
                file_views_all += extract_total_views(views_str)

        all_total_views_all += file_views_all

        print(f"{file_path} - 播放量: {file_views_all}, 上传成功: {all_total_uploads}")

    except Exception as e:
        print(f"{file_path}: 读取失败 ({e})")

# ====== 计算存活数据 ======
survive_count = all_total_uploads - total_xianliu - total_failed_judge
survive_rate = survive_count / all_total_uploads if all_total_uploads > 0 else 0

print(f"总上传成功：{all_total_uploads}")
print(f"总限流：{total_xianliu}")
print(f"总未限流：{total_weixianliu}")
print(f"总判断失败：{total_failed_judge}")
print(f"成功存活量：{survive_count}")
print(f"存活率：{survive_rate:.2%}")

# ====== 写入飞书多维表格 ======
try:
    token_manager = TokenManager()
    table_id = get_tables(token_manager, app_token)
    print(f"获取到的table_id: {table_id}")

    # 提取归属和日期（取第一个文件作为主参考）
    owners = set()
    timestamp = None
    for file_path in file_list:
        owner, file_timestamp = parse_file_info(file_path)
        if owner:
            owners.add(owner)
        if file_timestamp and timestamp is None:  # 使用第一个有效的时间戳
            timestamp = file_timestamp
    
    owner_str = ','.join(owners)

    fields = {
        "归属": owner_str,
        "日期": timestamp if timestamp else None,  # 使用时间戳
        "发布视频数量": int(all_total_uploads),  # 转换为Python原生int
        "总播放量": int(all_total_views_all),    # 转换为Python原生int
        "成功存活量": int(survive_count),        # 转换为Python原生int
        "存活率": f"{survive_rate:.2%}",
    }

    print(f"即将写入飞书多维表格的数据: {fields}")
    result = add_record(token_manager, app_token, table_id, fields)
    print("数据写入成功！")
except Exception as e:
    print(f"写入飞书多维表格失败: {e}")
