import streamlit as st
import redis
import json
import pandas as pd
from datetime import datetime, timedelta
import time
import sys

# --- BƯỚC QUAN TRỌNG: ĐIỀN THÔNG TIN CLOUD CỦA BẠN VÀO ĐÂY ---
# Phải giống hệt file producer_cloud.py
REDIS_HOST = "redis-18772.c277.us-east-1-3.ec2.cloud.redislabs.com"
REDIS_PORT = 18772
REDIS_PASSWORD = "E5mAvNKAQagrqsm5o1PcemVEoSk96rQu"
# -------------------------------------------------------------

RAW_TOPIC_NAME = "price_raw_topic" # Kênh để lắng nghe

st.set_page_config(
    page_title="Live Dashboard (Streaming Data)",
    page_icon="📈",
    layout="wide",
)

st.title("📈 Live Dashboard (Xử lý Streaming Data)")
st.info(f"Đang lắng nghe từ: `{RAW_TOPIC_NAME}`")

# --- Kết nối Redis (Hàm cache_resource để kết nối 1 lần) ---
@st.cache_resource
def get_redis_connection():
    """Tạo kết nối Redis Cloud và subscribe vào kênh."""
    try:
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            db=0
        )
        r.ping()
        p = r.pubsub(ignore_subscribe_messages=True)
        p.subscribe(RAW_TOPIC_NAME)
        #st.success(f"Đã kết nối và lắng nghe Redis Cloud tại {REDIS_HOST}")
        return p
    except redis.exceptions.ConnectionError as e:
        st.error(f"Lỗi: Không thể kết nối Redis Cloud.")
        st.error("Vui lòng kiểm tra lại 3 biến REDIS_HOST, PORT, PASSWORD.")
        return None

pubsub_connection = get_redis_connection()

if pubsub_connection is None:
    sys.exit(1)

# --- Khởi tạo State (Biến nhớ) ---
if 'data_history' not in st.session_state:
    st.session_state.data_history = []
if 'latest_data' not in st.session_state:
    st.session_state.latest_data = {}

# --- Placeholder (Vị trí giữ chỗ) ---
placeholder_metrics = st.empty()
placeholder_chart = st.empty()

# --- Vòng lặp chính của Streamlit (Consumer) ---
while True:
    # 1. LẤY DỮ LIỆU TỪ REDIS CLOUD
    message = pubsub_connection.get_message()
    
    if message:
        try:
            data = json.loads(message['data'])
            data["ts"] = datetime.fromisoformat(data["ts"])
            st.session_state.data_history.append(data)
            st.session_state.latest_data = data
        except (json.JSONDecodeError, TypeError):
            pass 

    # 2. LỌC DỮ LIỆU CŨ (Chỉ giữ 1 giờ)
    now = datetime.now()
    one_hour_ago = now - timedelta(hours=1)
    
    st.session_state.data_history = [
        d for d in st.session_state.data_history if d["ts"] > one_hour_ago
    ]

    # Nếu không có dữ liệu, hiển thị chờ
    if not st.session_state.data_history:
        with placeholder_metrics.container():
            st.info("Đang chờ dữ liệu từ producer...")
        time.sleep(1) 
        st.rerun() 

    # 3. XỬ LÝ (PANDAS)
    df = pd.DataFrame(st.session_state.data_history).set_index("ts")
    
    # 4. TÍNH TOÁN THỐNG KÊ
    one_min_ago = now - timedelta(minutes=1)
    df_1_min = df[df.index > one_min_ago]
    df_1_hour = df
    last_data = st.session_state.latest_data

    # 5. CẬP NHẬT GIAO DIỆN
    with placeholder_metrics.container():
        st.header(f"Giá hiện tại (cập nhật lúc: {last_data.get('ts', now).strftime('%H:%M:%S')})")
        
        col1, col2 = st.columns(2)
        col1.metric(
            label="Giá Vàng (triệu VND/lượng)", 
            value=f"{last_data.get('gold', 0):.2f}",
            delta=round(last_data.get('gold', 0) - df_1_min['gold'].mean(), 2) if not df_1_min.empty else 0
        )
        col2.metric(
            label="Tỷ giá USD (VND/USD)", 
            value=f"{last_data.get('usd', 0):.4f}",
            delta=round(last_data.get('usd', 0) - df_1_min['usd'].mean(), 4) if not df_1_min.empty else 0
        )
        
        st.markdown("---")
        st.header("Thống kê theo cửa sổ thời gian")
        col_1min, col_1hour_display = st.columns(2)

        with col_1min:
            st.subheader("Trong 1 Phút Gần Nhất")
            if not df_1_min.empty:
                st.dataframe(df_1_min['gold'].agg(['max', 'min', 'mean']).to_frame('Giá trị').T)
                st.dataframe(df_1_min['usd'].agg(['max', 'min', 'mean']).to_frame('Giá trị').T)
        
        with col_1hour_display:
            st.subheader("Trong 1 Giờ Gần Nhất")
            if not df_1_hour.empty:
                st.dataframe(df_1_hour['gold'].agg(['max', 'min', 'mean']).to_frame('Giá trị').T)
                st.dataframe(df_1_hour['usd'].agg(['max', 'min', 'mean']).to_frame('Giá trị').T)

    with placeholder_chart.container():
        st.markdown("---")
        st.header("Biểu đồ xu hướng (1 giờ gần nhất)")
        st.line_chart(df[['gold', 'usd']])

    # 6. NGỦ VÀ CHẠY LẠI
    time.sleep(1)
    st.rerun()