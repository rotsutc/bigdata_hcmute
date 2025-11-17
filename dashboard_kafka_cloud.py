import streamlit as st
import redis
import json
import pandas as pd
from datetime import datetime, timedelta
import time
import sys
import pytz

# --- TIMEZONE GMT+7 ---
tz = pytz.timezone("Asia/Ho_Chi_Minh")

# --- Upstash Redis URL ---
REDIS_UPSTASH_URL = "rediss://default:AZXjAAIncDI3ZjE3ZDE0YWJjZDI0Njc2OTM5N2E4ZjYzMjc0ZGM2MHAyMzgzNzE@firm-grubworm-38371.upstash.io:6379"

RAW_TOPIC_NAME = "price_raw_topic"

st.set_page_config(
    page_title="Live Dashboard (Kafka Cloud)",
    page_icon="📈",
    layout="wide",
)

st.title("📈 Live Dashboard (Đọc từ KAFKA CLOUD)")
st.info(f"Đang lắng nghe từ: `{RAW_TOPIC_NAME}`")

# --- Redis connection ---
@st.cache_resource
def get_redis_connection():
    try:
        r = redis.Redis.from_url(
            REDIS_UPSTASH_URL,
            decode_responses=True
        )
        r.ping()
        p = r.pubsub(ignore_subscribe_messages=True)
        p.subscribe(RAW_TOPIC_NAME)
        return p
    except redis.exceptions.ConnectionError as e:
        st.error("❌ KKhông thể kết nối Kafka Consumer!")
        st.error("Vui lòng kiểm tra lại URL.")
        st.error(f"Chi tiết lỗi: {e}")
        return None

pubsub_connection = get_redis_connection()
if pubsub_connection is None:
    sys.exit(1)

# --- State ---
if 'data_history' not in st.session_state:
    st.session_state.data_history = []
if 'latest_data' not in st.session_state:
    st.session_state.latest_data = {}

placeholder_metrics = st.empty()
placeholder_chart = st.empty()

# --- Main streaming loop ---
while True:

    # 1. Lấy dữ liệu từ Redis Pub/Sub
    message = pubsub_connection.get_message()

    if message:
        try:
            data = json.loads(message['data'])

            # Chuẩn hóa GMT+7
            ts = datetime.fromisoformat(data["ts"])
            data["ts"] = ts.astimezone(tz)

            st.session_state.data_history.append(data)
            st.session_state.latest_data = data

        except Exception:
            pass

    # 2. Lọc dữ liệu chỉ giữ 1 giờ
    now = datetime.now(tz)
    one_hour_ago = now - timedelta(hours=1)

    st.session_state.data_history = [
        d for d in st.session_state.data_history if d["ts"] > one_hour_ago
    ]

    if not st.session_state.data_history:
        with placeholder_metrics.container():
            st.info("Đang chờ dữ liệu từ producer...")
        time.sleep(1)
        st.rerun()

    # 3. DataFrame
    df = pd.DataFrame(st.session_state.data_history).set_index("ts")

    # 4. Tính thống kê
    one_min_ago = now - timedelta(minutes=1)
    df_1_min = df[df.index > one_min_ago]
    df_1_hour = df
    last_data = st.session_state.latest_data

    # 5. Update UI
    with placeholder_metrics.container():
        st.header(
            f"Giá hiện tại (cập nhật lúc: {last_data.get('ts', now).strftime('%H:%M:%S')})"
        )

        col1, col2 = st.columns(2)
        col1.metric(
            label="Giá vàng (triệu VND/lượng)",
            value=f"{last_data.get('gold', 0):.2f}",
            delta=round(last_data.get('gold', 0) - df_1_min['gold'].mean(), 2)
            if not df_1_min.empty else 0
        )
        col2.metric(
            label="Tỷ giá USD (VND/USD)",
            value=f"{last_data.get('usd', 0):.4f}",
            delta=round(last_data.get('usd', 0) - df_1_min['usd'].mean(), 4)
            if not df_1_min.empty else 0
        )

        st.markdown("---")
        st.header("Thống kê theo cửa sổ thời gian")
        col_1min, col_1hour_display = st.columns(2)

        with col_1min:
            st.subheader("Trong 1 phút gần nhất")
            if not df_1_min.empty:
                st.dataframe(df_1_min['gold'].agg(['max', 'min', 'mean']).to_frame('Giá trị').T)
                st.dataframe(df_1_min['usd'].agg(['max', 'min', 'mean']).to_frame('Giá trị').T)

        with col_1hour_display:
            st.subheader("Trong 1 giờ gần nhất")
            if not df_1_hour.empty:
                st.dataframe(df_1_hour['gold'].agg(['max', 'min', 'mean']).to_frame('Giá trị').T)
                st.dataframe(df_1_hour['usd'].agg(['max', 'min', 'mean']).to_frame('Giá trị').T)

    with placeholder_chart.container():
        st.markdown("---")
        st.header("Biểu đồ xu hướng (1 giờ gần nhất)")
        st.line_chart(df[['gold', 'usd']])

    # 6. Sleep & rerun
    time.sleep(1)
    st.rerun()
