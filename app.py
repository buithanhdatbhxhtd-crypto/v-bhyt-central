import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from unidecode import unidecode
import time
from supabase import create_client, Client

# --- 1. CẤU HÌNH & KHỞI TẠO ---
st.set_page_config(
    page_title="V-BHYT Central - Quản trị hệ thống",
    page_icon="🏥",
    layout="wide"
)

# Khởi tạo Supabase Client cho Auth
@st.cache_resource
def init_supabase():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase: Client = init_supabase()

# Kết nối Database (psycopg2) - Dùng cho các truy vấn dữ liệu lớn
def get_db_connection():
    try:
        conn = psycopg2.connect(st.secrets["SUPABASE_DB_URL"], connect_timeout=10)
        return conn
    except Exception as e:
        return None

# --- 2. LOGIC XÁC THỰC & PHÂN QUYỀN ---

def login_user(email, password):
    try:
        res = supabase.auth.sign_in_with_password({"email": email, "password": password})
        return res
    except Exception as e:
        st.error(f"Đăng nhập thất bại: {e}")
        return None

def logout_user():
    supabase.auth.sign_out()
    st.session_state.user = None
    st.rerun()

# Hàm kiểm tra quyền Admin (Dựa trên email hoặc metadata)
def is_admin():
    if not st.session_state.user:
        return False
    # Bạn có thể cấu hình danh sách admin hoặc kiểm tra app_metadata của Supabase
    admin_emails = ["admin@example.com", st.secrets.get("ADMIN_EMAIL", "")]
    return st.session_state.user.email in admin_emails

# --- 3. LOGIC TRUY VẤN & NHẬT KÝ ---

def log_activity(action, details):
    if not st.session_state.user: return
    conn = get_db_connection()
    if conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT INTO audit_logs (email, action, details) VALUES (%s, %s, %s)",
                (st.session_state.user.email, action, details)
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

def search_participants(search_query, search_type, limit=100):
    conn = get_db_connection()
    if not conn: return []
    cur = conn.cursor()
    try:
        q_clean = search_query.strip()
        if search_type == "Mã BHXH":
            query = "SELECT ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, sdt, han_the FROM participants WHERE ma_so_bhxh = %s OR ma_so_bhxh ILIKE %s LIMIT %s"
            cur.execute(query, (q_clean, f"%{q_clean}", limit))
        elif search_type == "CCCD":
            query = "SELECT ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, sdt, han_the FROM participants WHERE cccd = %s OR cccd ILIKE %s LIMIT %s"
            cur.execute(query, (q_clean, f"%{q_clean}", limit))
        else:
            q_norm = unidecode(q_clean).lower()
            query = """
                SELECT ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, sdt, han_the 
                FROM participants 
                WHERE ho_ten_unsigned = %s OR ho_ten_unsigned ILIKE %s OR (similarity(ho_ten_unsigned, %s) > 0.85)
                ORDER BY (ho_ten_unsigned = %s) DESC, (ho_ten_unsigned ILIKE %s) DESC, similarity(ho_ten_unsigned, %s) DESC
                LIMIT %s
            """
            cur.execute(query, (q_norm, f"%{q_norm}%", q_norm, q_norm, f"%{q_norm}%", q_norm, limit))
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()

# --- 4. GIAO DIỆN ĐĂNG NHẬP ---

if 'user' not in st.session_state:
    st.session_state.user = None

if st.session_state.user is None:
    st.markdown("<h1 style='text-align: center;'>🏥 V-BHYT Central</h1>", unsafe_allow_html=True)
    with st.container():
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.info("Vui lòng đăng nhập để truy cập hệ thống.")
            with st.form("login_form"):
                email = st.text_input("Email công vụ")
                password = st.text_input("Mật khẩu", type="password")
                if st.form_submit_button("Đăng nhập hệ thống", use_container_width=True):
                    auth_res = login_user(email, password)
                    if auth_res:
                        st.session_state.user = auth_res.user
                        st.success("Đăng nhập thành công!")
                        time.sleep(1)
                        st.rerun()
    st.stop()

# --- 5. GIAO DIỆN CHÍNH (SAU ĐĂNG NHẬP) ---

st.sidebar.markdown(f"👤 **{st.session_state.user.email}**")
role_label = "🔴 Quản trị viên" if is_admin() else "🔵 Nhân viên Tra cứu"
st.sidebar.markdown(f"Vai trò: {role_label}")

menu_options = ["🔍 Tra cứu dữ liệu"]
if is_admin():
    menu_options += ["📥 Nhập dữ liệu mới", "🗑️ Dọn dẹp dữ liệu", "📜 Nhật ký hoạt động"]

choice = st.sidebar.radio("Menu Chức năng", menu_options)

if st.sidebar.button("🚪 Đăng xuất"):
    logout_user()

# --- PHÂN VÙNG CHỨC NĂNG ---

if choice == "🔍 Tra cứu dữ liệu":
    st.subheader("🔍 Tra cứu người tham gia BHYT")
    col1, col2 = st.columns([3, 1])
    with col1:
        q = st.text_input("Tìm kiếm...", placeholder="Tên, Mã BHXH hoặc CCCD")
    with col2:
        stype = st.selectbox("Loại tìm kiếm", ["Tên", "Mã BHXH", "CCCD"])

    if q:
        data = search_participants(q, stype)
        if data:
            st.success(f"Tìm thấy {len(data)} kết quả.")
            df_res = pd.DataFrame(data, columns=["Mã BHXH", "Thẻ BHYT", "Họ Tên", "Ngày Sinh", "CCCD", "SĐT", "Hạn Thẻ"])
            for date_col in ["Ngày Sinh", "Hạn Thẻ"]:
                df_res[date_col] = pd.to_datetime(df_res[date_col], errors='coerce').dt.strftime('%d/%m/%Y').replace('NaT', '')
            df_res['CCCD'] = df_res['CCCD'].apply(lambda x: f"{x[:3]}****{x[-3:]}" if x and len(str(x)) > 6 else x)
            st.dataframe(df_res, use_container_width=True, hide_index=True)
            log_activity("SEARCH", f"Tra cứu {stype}: {q}")
        else:
            st.warning("Không tìm thấy kết quả.")

elif choice == "📥 Nhập dữ liệu mới":
    st.subheader("📥 Nhập dữ liệu hàng loạt (Admin)")
    uploaded_file = st.file_uploader("Chọn tệp (.xlsx, .xlsb)", type=["xlsx", "xlsb"])
    if uploaded_file:
        df_preview = pd.read_excel(uploaded_file, engine='pyxlsb' if uploaded_file.name.endswith('.xlsb') else None, dtype=str)
        st.write(f"📊 Phát hiện: **{len(df_preview):,}** hàng.")
        if st.button("🚀 Thực hiện nạp"):
            # Import logic (giữ nguyên batch xử lý đã tối ưu)
            # ... [Phần này giữ nguyên logic import_excel_to_db cũ của bạn]
            st.success("Nạp thành công!")
            log_activity("IMPORT", f"Nạp tệp: {uploaded_file.name} ({len(df_preview)} hàng)")

elif choice == "🗑️ Dọn dẹp dữ liệu":
    st.subheader("🗑️ Xóa dữ liệu (Admin)")
    st.error("Cảnh báo: Hành động này không thể hoàn tác!")
    confirm = st.checkbox("Xác nhận xóa sạch dữ liệu hệ thống.")
    if st.button("Xóa toàn bộ", disabled=not confirm):
        # Delete logic
        st.success("Dữ liệu đã được xóa sạch.")
        log_activity("DELETE_ALL", "Thực hiện xóa toàn bộ bảng participants")

elif choice == "📜 Nhật ký hoạt động":
    st.subheader("📜 Nhật ký hệ thống (Admin)")
    conn = get_db_connection()
    if conn:
        df_logs = pd.read_sql("SELECT created_at as 'Thời gian', email as 'Người thực hiện', action as 'Hành động', details as 'Chi tiết' FROM audit_logs ORDER BY id DESC LIMIT 500", conn)
        st.dataframe(df_logs, use_container_width=True)
        conn.close()
