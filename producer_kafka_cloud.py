import streamlit as st
import time
import random
from datetime import datetime
import json
import redis
import sys
import pytz

# =============================
# 🔐 Upstash Redis URL
# =============================
REDIS_UPSTASH_URL = "rediss://default:AZXjAAIncDI3ZjE3ZDE0YWJjZDI0Njc2OTM5N2E4ZjYzMjc0ZGM2MHAyMzgzNzE@firm-grubworm-38371.upstash.io:6379"

RAW_TOPIC_NAME = "price_raw_topic"

st.set_page_config(
    page_title="Producer (Kafka Cloud)",
    page_icon="📡",
    layout="centered",
)

# Timezone VN
tz = pytz.timezone("Asia/Ho_Chi_Minh")

# =============================
# 🔌 CONNECT UPSTASH REDIS
# =============================
try:
    r = redis.Redis.from_url(
        REDIS_UPSTASH_URL,
        decode_responses=True
    )
    r.ping()
except redis.exceptions.ConnectionError as e:
    st.error("❌ Không thể kết nối Kafka Producer!")
    st.error("Vui lòng kiểm tra lại REDIS_UPSTASH_URL.")
    st.error(f"Chi tiết lỗi: {e}")
    sys.exit(1)

st.title("📡 Producer (Gửi dữ liệu lên KAFKA CLOUD)")

# =============================
# 🔄 STREAM STATE
# =============================
if "run" not in st.session_state:
    st.session_state.run = False
if "latest_data" not in st.session_state:
    st.session_state.latest_data = ""

# =============================
# 📌 HÀM SINH DỮ LIỆU
# =============================
def generate_record():
    return {
        "ts": datetime.now(tz).isoformat(),
        "gold": round(random.uniform(140, 150), 2),
        "usd": round(random.uniform(25.10, 26.50), 4),
    }

# =============================
# 🎛 BUTTON UI
# =============================
col1, col2 = st.columns(2)
if col1.button("▶️ Start Streaming"):
    st.session_state.run = True
if col2.button("⏹ Stop Streaming"):
    st.session_state.run = False

placeholder = st.empty()

# =============================
# 🚀 MAIN STREAMING LOOP
# =============================
if st.session_state.run:
    st.success("Đang chạy... (Gửi 1 bản tin mỗi giây)")

    while st.session_state.run:
        new_data = generate_record()
        json_data = json.dumps(new_data)

        try:
            r.publish(RAW_TOPIC_NAME, json_data)
            st.session_state.latest_data = json_data
        except Exception as e:
            st.error(f"❌ Lỗi khi gửi (publish) vào Kafka Cloud: {e}")
            st.session_state.run = False

        with placeholder.container():
            st.subheader(f"Đang gửi vào Kafka Topic: `{RAW_TOPIC_NAME}`")
            st.code(st.session_state.latest_data, language="json")

        time.sleep(1)

else:
    st.warning("⏹ Đã dừng streaming.")

# =============================
# 📦 HIỂN THỊ BẢN TIN CUỐI
# =============================
st.subheader("Bản tin cuối cùng đã gửi:")
if st.session_state.latest_data:
    st.code(st.session_state.latest_data, language="json")
else:
    st.info("Chưa có dữ liệu nào được gửi.")
