import streamlit as st
import time
import random
from datetime import datetime
import json
import redis
import sys

# --- BƯỚC QUAN TRỌNG: ĐIỀN THÔNG TIN CLOUD CỦA BẠN VÀO ĐÂY ---
# Lấy từ tài khoản Redis.com miễn phí của bạn (xem Bước 1)
REDIS_HOST = "redis-18772.c277.us-east-1-3.ec2.cloud.redislabs.com"  # Ví dụ: redis-12345.c1.us-east-1-2.ec2.cloud.redislabs.com
REDIS_PORT = 18772                      # Ví dụ: 12345
REDIS_PASSWORD = "E5mAvNKAQagrqsm5o1PcemVEoSk96rQu" # Mật khẩu của bạn
# -------------------------------------------------------------

RAW_TOPIC_NAME = "price_raw_topic" # Kênh để gửi dữ liệu

st.set_page_config(
    page_title="Producer (Gửi dữ liệu giả lập)",
    page_icon="📡",
    layout="wide",
)

# --- Kết nối Redis (Giờ đây kết nối ra Cloud) ---
try:
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        db=0,
        decode_responses=True
    )
    r.ping() # Kiểm tra kết nối
    #st.sidebar.success(f"Đã kết nối Redis Cloud tại {REDIS_HOST}")
except redis.exceptions.ConnectionError as e:
    st.error(f"Lỗi: Không thể kết nối Redis Cloud.")
    st.error("Vui lòng kiểm tra lại 3 biến REDIS_HOST, PORT, PASSWORD.")
    st.error(f"Chi tiết lỗi: {e}")
    sys.exit(1)


st.title("📡 Producer (Gửi dữ liệu giả lập)")

# Khởi tạo state
if "run" not in st.session_state:
    st.session_state.run = False
if "latest_data" not in st.session_state:
    st.session_state.latest_data = ""

# Hàm sinh dữ liệu
def generate_record():
    return {
        "ts": datetime.now().isoformat(), 
        "gold": round(random.uniform(70, 80), 2), 
        "usd": round(random.uniform(25.40, 25.50), 4),
    }

# Buttons
col1, col2 = st.columns(2)
if col1.button("▶️ Start Streaming"):
    st.session_state.run = True
if col2.button("⏹ Stop Streaming"):
    st.session_state.run = False

placeholder = st.empty()

# Vòng lặp: Nếu đang chạy -> gửi dữ liệu vào Redis Cloud
if st.session_state.run:
    st.success("Trạng thái: Đang chạy... (gửi 1 sự kiện/giây)")
    
    while st.session_state.run:
        new_data = generate_record()
        json_data = json.dumps(new_data)
        
        try:
            r.publish(RAW_TOPIC_NAME, json_data)
            st.session_state.latest_data = json_data
        except Exception as e:
            st.error(f"Lỗi khi publish vào Redis Cloud: {e}")
            st.session_state.run = False 
        
        with placeholder.container():
            st.subheader(f"Đang gửi dữ liệu giả lập: `{RAW_TOPIC_NAME}`")
            st.code(st.session_state.latest_data, language="json")

        time.sleep(1)
else:
    st.warning("Trạng thái: Đã dừng.")

st.subheader("Bản tin cuối cùng đã gửi:")
if st.session_state.latest_data:
    st.code(st.session_state.latest_data, language="json")
else:
    st.info("Chưa có dữ liệu gửi đi.")