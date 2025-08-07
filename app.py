import streamlit as st
import pandas as pd
import re
import requests
import json
import time
import datetime
import os
from io import StringIO

# 页面配置
st.set_page_config(
    page_title="数据统计分析工具",
    page_icon="📊",
    layout="wide"
)

# ====== 飞书多维表格API相关配置 ======
TOKEN_FILE = "feishu_token.json"
app_token = "MEwYb1UL4aSSpIsn6V5c9gzXnKe"
app_id = "cli_a8fdaf1afff39013"
app_secret = "o6qUUHKhMhymwaVM4u40H2zQAFrhdHm7"

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

class FeishuLabTableReader:
    """飞书Lab表格读取器"""
    
    def __init__(self, app_id: str, app_secret: str):
        """
        初始化Lab表格读取器
        
        Args:
            app_id: 飞书应用的App ID
            app_secret: 飞书应用的App Secret
        """
        self.app_id = app_id
        self.app_secret = app_secret
        self.tenant_access_token = None
        self.app_token = "CSs1bClvYaIGl5snunycUcFpngf"  # lab表格的app_token
        self.table_id = "tblJc7IxgQhQMkKN"  # lab盒子每日收入表格ID
        self.base_url = "https://open.feishu.cn/open-apis/bitable/v1"
        
    def get_tenant_access_token(self) -> str:
        """获取tenant_access_token"""
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        
        payload = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            
            result = response.json()
            
            if result.get("code") == 0:
                self.tenant_access_token = result.get("tenant_access_token")
                return self.tenant_access_token
            else:
                raise Exception(f"获取访问凭证失败: {result.get('msg', '未知错误')}")
                
        except requests.RequestException as e:
            raise Exception(f"网络请求失败: {str(e)}")
    
    def get_headers(self) -> dict:
        """获取请求头"""
        if not self.tenant_access_token:
            self.get_tenant_access_token()
            
        return {
            "Authorization": f"Bearer {self.tenant_access_token}",
            "Content-Type": "application/json"
        }
    
    def get_table_fields(self) -> list:
        """获取lab表格的字段信息"""
        url = f"{self.base_url}/apps/{self.app_token}/tables/{self.table_id}/fields"
        
        try:
            response = requests.get(url, headers=self.get_headers())
            response.raise_for_status()
            
            result = response.json()
            if result.get("code") == 0:
                fields = result.get("data", {}).get("items", [])
                return fields
            else:
                raise Exception(f"获取字段信息失败: {result.get('msg', '未知错误')}")
                
        except requests.RequestException as e:
            raise Exception(f"网络请求失败: {str(e)}")
    
    def get_table_records(self, page_size: int = 20) -> list:
        """获取lab表格的记录"""
        url = f"{self.base_url}/apps/{self.app_token}/tables/{self.table_id}/records"
        
        params = {
            "page_size": page_size
        }
        
        try:
            response = requests.get(url, headers=self.get_headers(), params=params)
            response.raise_for_status()
            
            result = response.json()
            if result.get("code") == 0:
                records = result.get("data", {}).get("items", [])
                return records
            else:
                raise Exception(f"获取记录失败: {result.get('msg', '未知错误')}")
                
        except requests.RequestException as e:
            raise Exception(f"网络请求失败: {str(e)}")
    

    def get_all_records(self) -> list:
        """获取所有记录（分页获取）"""
        all_records = []
        page_token = None
        
        while True:
            url = f"{self.base_url}/apps/{self.app_token}/tables/{self.table_id}/records"
            params = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token
            
            try:
                response = requests.get(url, headers=self.get_headers(), params=params)
                response.raise_for_status()
                
                result = response.json()
                if result.get("code") == 0:
                    data = result.get("data", {})
                    records = data.get("items", [])
                    all_records.extend(records)
                    
                    # 检查是否还有更多数据
                    page_token = data.get("page_token")
                    if not page_token:
                        break
                else:
                    raise Exception(f"获取记录失败: {result.get('msg', '未知错误')}")
                    
            except requests.RequestException as e:
                raise Exception(f"网络请求失败: {str(e)}")
        
        st.write(f"📄 总共获取到 {len(all_records)} 条记录")
        return all_records

    def get_order_amount_by_owner_and_date(self, owner: str, target_date: datetime.date) -> float:
        """根据归属和日期筛选数据，计算订单金额总和"""
        try:
            st.write(f"🔍 搜索条件: 分组='{owner}', 日期='{target_date}'")
            
            # 获取字段信息
            fields = self.get_table_fields()
            field_map = {field['field_id']: field['field_name'] for field in fields}
            
            # 找到关键字段的ID
            date_field_id = "订单日期"
            group_field_id = "分组" 
            amount_field_id = "订单金额(元)"
            
            st.write(f"📋 使用关键字段: 分组={group_field_id}, 日期={date_field_id}, 金额={amount_field_id}")
            
            # 转换目标日期为时间戳
            target_timestamp = None
            try:
                target_dt = datetime.datetime.combine(target_date, datetime.time.min)
                target_timestamp = int(target_dt.timestamp() * 1000)  # 转为毫秒时间戳
                st.write(f"🕐 目标日期时间戳: {target_timestamp}")
            except Exception as e:
                st.warning(f"⚠️ 日期解析失败: {e}")
                target_timestamp = None
            
            # 获取所有记录
            all_records = self.get_all_records()
            
            # 筛选数据
            matched_records = []
            total_amount = 0
            
            for record in all_records:
                record_fields = record.get('fields', {})
                
                # 检查分组
                group_value = record_fields.get(group_field_id)
                if not group_value or owner not in str(group_value):
                    continue
                
                # 检查日期（如果指定了）
                if target_timestamp and date_field_id:
                    date_value = record_fields.get(date_field_id)
                    if date_value:
                        try:
                            # 将数据库中的时间戳转换为日期进行比较
                            record_timestamp = int(date_value)
                            # 检查是否是同一天（允许一天的误差范围）
                            day_in_ms = 24 * 60 * 60 * 1000
                            if abs(record_timestamp - target_timestamp) >= day_in_ms:
                                continue
                        except (ValueError, TypeError):
                            # 如果不是时间戳格式，尝试字符串匹配
                            date_str = str(date_value)
                            target_date_str = target_date.strftime("%Y-%m-%d")
                            if target_date_str not in date_str and target_date_str.replace('-', '/') not in date_str:
                                continue
                
                # 获取金额
                amount_value = record_fields.get(amount_field_id)
                if amount_value:
                    try:
                        # 处理不同的数字格式
                        if isinstance(amount_value, (int, float)):
                            amount = float(amount_value)
                        else:
                            # 移除可能的货币符号和逗号
                            amount_str = str(amount_value).replace('￥', '').replace(',', '').strip()
                            amount = float(amount_str)
                        
                        total_amount += amount
                        
                        # 格式化日期显示
                        date_display = record_fields.get(date_field_id, '未知')
                        if isinstance(date_display, int):
                            try:
                                # 将时间戳转换为可读日期
                                date_obj = datetime.datetime.fromtimestamp(date_display / 1000)
                                date_display = date_obj.strftime('%Y-%m-%d')
                            except:
                                date_display = str(date_display)
                        
                        matched_records.append({
                            'group': group_value,
                            'date': date_display,
                            'amount': amount,
                            'record': record_fields
                        })
                    except (ValueError, TypeError):
                        st.warning(f"⚠️ 无法解析金额: {amount_value}")
                        continue
            
            # 显示结果
            st.write(f"🎯 搜索结果: 找到匹配记录 {len(matched_records)} 条, 总金额: ¥{total_amount:,.2f}")
            
            if matched_records:
                st.write("📋 匹配的记录:")
                # 创建DataFrame显示结果
                display_data = []
                for item in matched_records:
                    display_data.append({
                        '分组': item['group'],
                        '日期': item['date'],
                        '金额': f"¥{item['amount']:.2f}"
                    })
                
                df_result = pd.DataFrame(display_data)
                st.dataframe(df_result, height=200)
            
            return total_amount
            
        except Exception as e:
            st.error(f"计算订单金额失败: {str(e)}")
            return 0.0

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
            self.token_data.get('expire_time', 0) > current_time + 60):
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
            
            if resp_data.get("code") == 0:
                token = resp_data.get("tenant_access_token")
                expire = resp_data.get("expire")
                
                self.token_data = {
                    'tenant_access_token': token,
                    'expire_time': current_time + expire
                }
                self._save_token_data()
                return token
                
            raise Exception(f"获取tenant_access_token失败: {resp_data}")
        except Exception as e:
            st.error(f"获取token异常: {e}")
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
            
            if data.get("code") == 0:
                return data["data"]["items"][0]["table_id"]
            elif data.get("code") in [99991677, 99991661]:
                token_manager.token_data = {}
                continue
            else:
                raise Exception(f"获取table_id失败: {data}")
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(1)

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
            
            resp = requests.post(url, headers=headers, json=data)
            result = resp.json()
            
            if result.get("code") == 0:
                return result
            elif result.get("code") in [99991677, 99991661]:
                token_manager.token_data = {}
                continue
            else:
                raise Exception(f"写入记录失败: {result}")
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(1)

def date_to_timestamp(date_str):
    """将日期字符串转换为时间戳"""
    try:
        if '-' in date_str:
            dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        elif '.' in date_str:
            month_day = date_str.split('.')
            if len(month_day) >= 2:
                month = month_day[0].zfill(2)
                day = month_day[1].zfill(2)
                dt = datetime.datetime.strptime(f"2025-{month}-{day}", "%Y-%m-%d")
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return int(dt.timestamp() * 1000)
    except Exception as e:
        st.error(f"日期转换失败: {e}")
        return None

def read_data_file(file_content, file_name):
    """读取上传的文件内容"""
    try:
        if file_name.endswith('.csv'):
            return pd.read_csv(StringIO(file_content.decode('utf-8')))
        elif file_name.endswith('.xlsx'):
            from io import BytesIO
            return pd.read_excel(BytesIO(file_content))
        else:
            raise ValueError(f"不支持的文件格式: {file_name}")
    except Exception as e:
        st.error(f"读取文件失败: {e}")
        return None

def process_data_file(df, default_upload_count):
    """处理数据文件，返回统计结果"""
    results = {
        'total_uploads': 0,
        'total_views': 0
    }
    
    # 计算上传成功总数
    if "上传数量" in df.columns:
        upload_column = df["上传数量"].fillna(0).astype(int)
        success_counts = default_upload_count - upload_column
        success_counts = success_counts.apply(lambda x: max(x, 0))
        results['total_uploads'] = success_counts.sum()
    
    # 提取播放量
    play_column = None
    for col in df.columns:
        if '播放' in str(col):
            play_column = col
            header_match = re.search(r'播放\((\d+)\)', str(col))
            if header_match:
                results['total_views'] = int(header_match.group(1))
                break
    
    if results['total_views'] == 0 and play_column:
        for views_str in df[play_column]:
            results['total_views'] += extract_total_views(views_str)
    
    return results

# Streamlit 界面
def main():
    st.title("📊 数据统计分析工具")
    st.markdown("---")
    
    # 侧边栏配置
    with st.sidebar:
        st.header("⚙️ 配置参数")
        
        # 归属输入
        owner = st.text_input("归属", placeholder="例如：中科、北斗")
        
        # 日期输入
        date_input = st.date_input("日期", datetime.date.today())
        
        # 默认上传视频数量
        default_upload_count = st.number_input("默认上传视频数量", min_value=1, value=3)
        
        st.markdown("---")
        st.markdown("### 📋 使用说明")
        st.markdown("""
        1. 填写归属信息
        2. 选择日期
        3. 设置默认上传视频数量
        4. 上传CSV或Excel文件
        5. 点击分析按钮
        
        💡 **注意**: Lab订单金额会自动查询前一天的数据
        """)
    
    # 主界面
    st.header("📁 文件上传")
    uploaded_files = st.file_uploader(
        "选择数据文件",
        type=['csv', 'xlsx'],
        accept_multiple_files=True,
        help="支持CSV和Excel格式，可以同时上传多个文件"
    )
    
    if uploaded_files:
        st.success(f"✅ 已上传 {len(uploaded_files)} 个文件")
        
        # 显示文件列表
        with st.expander("📋 查看上传的文件", expanded=True):
            for i, file in enumerate(uploaded_files, 1):
                file_size = len(file.getvalue()) / 1024  # KB
                st.write(f"{i}. 📄 **{file.name}** ({file_size:.1f} KB)")
    
    # 操作按钮
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        analyze_button = st.button("🚀 开始分析", type="primary", use_container_width=True, disabled=not (uploaded_files and owner))
    
    with col2:
        if st.button("🔄 清除结果", use_container_width=True):
            st.rerun()
    
    with col3:
        # 添加示例数据下载
        if st.button("📥 下载示例", use_container_width=True):
            st.info("💡 示例数据格式说明已显示在下方")
    
    # 初始化 session state
    if 'analysis_results' not in st.session_state:
        st.session_state.analysis_results = None
    if 'analysis_params' not in st.session_state:
        st.session_state.analysis_params = None
    
    # 分析处理
    if analyze_button and uploaded_files and owner:
        with st.spinner("正在分析数据..."):
            try:
                # 初始化统计结果
                all_results = {
                    'total_uploads': 0,
                    'total_views': 0
                }
                
                # 处理每个文件
                progress_bar = st.progress(0)
                for i, uploaded_file in enumerate(uploaded_files):
                    file_content = uploaded_file.read()
                    df = read_data_file(file_content, uploaded_file.name)
                    
                    if df is not None:
                        file_results = process_data_file(df, default_upload_count)
                        
                        # 累加结果
                        for key in all_results:
                            all_results[key] += file_results[key]
                        
                        st.write(f"✅ {uploaded_file.name}: 播放量 {file_results['total_views']}, 上传成功 {file_results['total_uploads']}")
                    
                    progress_bar.progress((i + 1) / len(uploaded_files))
                
                # 自动获取Lab订单金额（查询前一天的数据）
                st.write("🔍 正在获取Lab订单金额...")
                try:
                    # 计算前一天的日期
                    previous_date = date_input - datetime.timedelta(days=1)
                    st.write(f"💡 查询前一天的订单金额: {previous_date}")
                    
                    lab_reader = FeishuLabTableReader(app_id, app_secret)
                    order_amount = lab_reader.get_order_amount_by_owner_and_date(owner, previous_date)
                    
                    if order_amount > 0:
                        st.write(f"💰 获取到Lab订单金额: ¥{order_amount:.2f}")
                    else:
                        st.write("⚠️ 未找到匹配的Lab订单数据，订单金额为 ¥0.00")
                        order_amount = 0.0
                        
                except Exception as e:
                    st.warning(f"⚠️ 获取Lab订单金额失败: {str(e)}，将使用默认值 ¥0.00")
                    order_amount = 0.0
                
                # 保存结果到 session state
                st.session_state.analysis_results = {
                    'all_results': all_results,
                    'order_amount': order_amount
                }
                st.session_state.analysis_params = {
                    'owner': owner,
                    'date_input': date_input,
                    'file_count': len(uploaded_files)
                }
                
            except Exception as e:
                st.error(f"❌ 分析失败: {e}")
    
    # 显示分析结果（如果存在）
    if st.session_state.analysis_results is not None:
        results = st.session_state.analysis_results
        params = st.session_state.analysis_params
        
        all_results = results['all_results']
        
        # 显示结果
        st.markdown("---")
        st.header("📈 分析结果")
        
        # 创建指标卡片
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("总上传成功", all_results['total_uploads'])
        with col2:
            st.metric("总播放量", f"{all_results['total_views']:,}")
        with col3:
            order_amount = st.session_state.analysis_results.get('order_amount', 0)
            st.metric("Lab订单金额", f"¥{order_amount:.2f}")
        with col4:
            st.metric("文件数量", params['file_count'])
        
        # 详细统计
        with st.expander("📊 详细统计"):
            order_amount = st.session_state.analysis_results.get('order_amount', 0)
            previous_date = params['date_input'] - datetime.timedelta(days=1)
            st.write("**汇总数据:**")
            st.write(f"- 归属: {params['owner']}")
            st.write(f"- 日期: {params['date_input']}")
            st.write(f"- 文件数量: {params['file_count']}")
            st.write(f"- 总上传成功: {all_results['total_uploads']}")
            st.write(f"- 总播放量: {all_results['total_views']:,}")
            st.write(f"- Lab订单金额: ¥{order_amount:.2f} (查询日期: {previous_date})")
        

        
        # 写入飞书表格
        if st.button("💾 保存到飞书表格", type="primary"):
            with st.spinner("正在保存到飞书表格..."):
                try:
                    token_manager = TokenManager()
                    table_id = get_tables(token_manager, app_token)
                    
                    # 转换日期为时间戳
                    timestamp = date_to_timestamp(params['date_input'].strftime("%Y-%m-%d"))
                    
                    # 获取订单金额
                    order_amount = st.session_state.analysis_results.get('order_amount', 0)
                    
                    fields = {
                        "归属": params['owner'],
                        "日期": timestamp,
                        "发布视频数量": int(all_results['total_uploads']),
                        "总播放量": int(all_results['total_views']),
                        "出单金额": float(order_amount)  # 添加出单金额字段
                    }
                    
                    result = add_record(token_manager, app_token, table_id, fields)
                    st.success("✅ 数据已成功保存到飞书表格！")
                    
                    # 清除结果，避免重复保存
                    if st.button("🔄 清除结果"):
                        st.session_state.analysis_results = None
                        st.session_state.analysis_params = None
                        st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ 保存失败: {e}")
                    st.write("错误详情:", str(e))
                
    elif analyze_button:
        if not uploaded_files:
            st.warning("⚠️ 请先上传文件")
        if not owner:
            st.warning("⚠️ 请填写归属信息")
    
    # 底部说明
    st.markdown("---")
    with st.expander("📖 数据格式说明"):
        st.markdown("""
        ### 支持的文件格式
        - **CSV文件**: `.csv` 格式
        - **Excel文件**: `.xlsx` 格式
        
        ### 必需的数据列
        - **上传数量**: 上传失败的视频数量
        - **播放量相关列**: 列名包含"播放"字样的列
        
        ### 示例数据结构
        ```
        状态 | 上传数量 | 播放(1234567) | 其他列...
        上传成功#3个 | 0 | 总播放：1715(+9) | ...
        ```
        """)
    
    # 版权信息
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #666; font-size: 0.8em;'>"
        "📊 数据统计分析工具 | 基于 Streamlit 构建"
        "</div>", 
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()