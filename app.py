import streamlit as st
import pandas as pd
import re
import requests
import json
import time
import datetime
import os
import tempfile
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
        'total_views': 0,
        'total_xianliu': 0,
        'total_weixianliu': 0,
        'total_failed_judge': 0
    }
    
    # 计算上传成功总数
    if "上传数量" in df.columns:
        upload_column = df["上传数量"].fillna(0).astype(int)
        success_counts = default_upload_count - upload_column
        success_counts = success_counts.apply(lambda x: max(x, 0))
        results['total_uploads'] = success_counts.sum()
    
    # 提取限流统计
    for status in df["状态"].dropna().astype(str).tolist():
        if "最近一小时发布视频" in status:
            match_detail = re.search(r"(\d+)个未限流，(\d+)个限流，(\d+)个判断失败", status)
            if match_detail:
                results['total_weixianliu'] += int(match_detail.group(1))
                results['total_xianliu'] += int(match_detail.group(2))
                results['total_failed_judge'] += int(match_detail.group(3))
    
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
        owner = st.text_input("归属", placeholder="例如：中科、北斗、叮当")
        
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
                    'total_views': 0,
                    'total_xianliu': 0,
                    'total_weixianliu': 0,
                    'total_failed_judge': 0
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
                
                # 计算存活数据
                survive_count = all_results['total_uploads'] - all_results['total_xianliu'] - all_results['total_failed_judge']
                survive_rate = survive_count / all_results['total_uploads'] if all_results['total_uploads'] > 0 else 0
                
                # 保存结果到 session state
                st.session_state.analysis_results = {
                    'all_results': all_results,
                    'survive_count': survive_count,
                    'survive_rate': survive_rate
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
        survive_count = results['survive_count']
        survive_rate = results['survive_rate']
        
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
            st.metric("成功存活量", survive_count)
        with col4:
            st.metric("存活率", f"{survive_rate:.2%}")
        
        # 详细统计
        with st.expander("📊 详细统计"):
            col1, col2 = st.columns(2)
            with col1:
                st.write("**限流统计:**")
                st.write(f"- 总限流: {all_results['total_xianliu']}")
                st.write(f"- 总未限流: {all_results['total_weixianliu']}")
                st.write(f"- 总判断失败: {all_results['total_failed_judge']}")
            
            with col2:
                st.write("**汇总数据:**")
                st.write(f"- 归属: {params['owner']}")
                st.write(f"- 日期: {params['date_input']}")
                st.write(f"- 文件数量: {params['file_count']}")
        
        # 写入飞书表格
        if st.button("💾 保存到飞书表格", type="primary"):
            with st.spinner("正在保存到飞书表格..."):
                try:
                    token_manager = TokenManager()
                    table_id = get_tables(token_manager, app_token)
                    
                    # 转换日期为时间戳
                    timestamp = date_to_timestamp(params['date_input'].strftime("%Y-%m-%d"))
                    
                    fields = {
                        "归属": params['owner'],
                        "日期": timestamp,
                        "发布视频数量": int(all_results['total_uploads']),
                        "总播放量": int(all_results['total_views']),
                        "成功存活量": int(survive_count),
                        "存活率": f"{survive_rate:.2%}",
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
        - **状态**: 包含上传状态和限流信息
        - **上传数量**: 上传失败的视频数量
        - **播放量相关列**: 列名包含"播放"字样的列
        
        ### 示例数据结构
        ```
        状态 | 上传数量 | 播放(1234567) | 其他列...
        上传成功#3个 | 0 | 总播放：1715(+9) | ...
        最近一小时发布视频：5个未限流，2个限流，1个判断失败 | 1 | ... | ...
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