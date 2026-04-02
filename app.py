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

# Kết nối Database (psycopg2)
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

def is_admin():
    if not st.session_state.user:
        return False
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
                (st.session_state.user.email, action, str(details))
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

# --- 4. LOGIC XỬ LÝ DỮ LIỆU LỚN (ADMIN) ---

def import_excel_to_db(df):
    df.columns = [str(c).strip().lower() for c in df.columns]
    mapping = {
        'ma so bhxh': 'ma_so_bhxh', 'ma the bhyt': 'ma_the_bhyt',
        'ho ten': 'ho_ten', 'ngay sinh': 'ngay_sinh',
        'socmnd': 'cccd', 'sodient': 'sdt',
        'diachilh': 'dia_chi', 'hantheden': 'han_the'
    }
    df = df.rename(columns=mapping)
    
    with st.spinner("Đang chuẩn hóa dữ liệu..."):
        for col in ['ngay_sinh', 'han_the']:
            if col in df.columns:
                temp_dt = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                df[col] = temp_dt.apply(lambda x: x.date() if pd.notnull(x) else None)
            else:
                df[col] = None

        str_cols = ['ma_so_bhxh', 'ma_the_bhyt', 'ho_ten', 'cccd', 'sdt', 'dia_chi']
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
                null_values = ['nan', 'None', 'NAT', 'NaT', '<NA>', '', 'NaN', 'NAN', 'null', 'NULL']
                df[col] = df[col].where(~df[col].isin(null_values), None)
            else:
                df[col] = None

    data_tuples = list(df[['ma_so_bhxh', 'ma_the_bhyt', 'ho_ten', 'ngay_sinh', 'cccd', 'sdt', 'dia_chi', 'han_the']].itertuples(index=False, name=None))
    del df

    sql = """
        INSERT INTO participants (ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, sdt, dia_chi, han_the)
        VALUES %s
        ON CONFLICT (ma_so_bhxh) DO UPDATE SET
            ma_the_bhyt = EXCLUDED.ma_the_bhyt,
            ho_ten = EXCLUDED.ho_ten,
            ngay_sinh = EXCLUDED.ngay_sinh,
            cccd = EXCLUDED.cccd,
            sdt = EXCLUDED.sdt,
            dia_chi = EXCLUDED.dia_chi,
            han_the = EXCLUDED.han_the,
            updated_at = NOW();
    """
    
    batch_size = 5000 
    total_len = len(data_tuples)
    
    for i in range(0, total_len, batch_size):
        batch = data_tuples[i:i + batch_size]
        conn = get_db_connection()
        if not conn: break
        cur = conn.cursor()
        try:
            execute_values(cur, sql, batch)
            conn.commit()
            yield min(i + batch_size, total_len)
        except Exception as e:
            conn.rollback()
            break
        finally:
            cur.close()
            conn.close()
        time.sleep(0.1)

def delete_all_data():
    conn = get_db_connection()
    if not conn: return False
    cur = conn.cursor()
    try:
        cur.execute("TRUNCATE TABLE participants RESTART IDENTITY;")
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

# --- 5. GIAO DIỆN ĐĂNG NHẬP ---

if 'user' not in st.session_state:
    st.session_state.user = None

if st.session_state.user is None:
    st.markdown("<h1 style='text-align: center;'>🏥 V-BHYT Central</h1>", unsafe_allow_html=True)
    with st.container():
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            with st.form("login_form"):
                email = st.text_input("Email công vụ")
                password = st.text_input("Mật khẩu", type="password")
                if st.form_submit_button("Đăng nhập hệ thống", use_container_width=True):
                    auth_res = login_user(email, password)
                    if auth_res:
                        st.session_state.user = auth_res.user
                        st.rerun()
    st.stop()

# --- 6. GIAO DIỆN CHÍNH ---

st.sidebar.markdown(f"👤 **{st.session_state.user.email}**")
role_label = "🔴 Quản trị viên" if is_admin() else "🔵 Nhân viên Tra cứu"
st.sidebar.markdown(f"Vai trò: {role_label}")

menu_options = ["🔍 Tra cứu dữ liệu"]
if is_admin():
    menu_options += ["📥 Nhập dữ liệu mới", "🗑️ Dọn dẹp dữ liệu", "📜 Nhật ký hoạt động"]

choice = st.sidebar.radio("Menu Chức năng", menu_options)

if st.sidebar.button("🚪 Đăng xuất"):
    logout_user()

if choice == "🔍 Tra cứu dữ liệu":
    st.subheader("🔍 Tra cứu người tham gia BHYT")
    col1, col2 = st.columns([3, 1])
    with col1:
        q = st.text_input("Tìm kiếm...", placeholder="Tên, Mã BHXH hoặc số CCCD")
    with col2:
        stype = st.selectbox("Loại tìm kiếm", ["Tên", "Mã BHXH", "CCCD"])

    if q:
        with st.spinner("Đang truy xuất..."):
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
        try:
            engine = 'pyxlsb' if uploaded_file.name.endswith('.xlsb') else None
            df_preview = pd.read_excel(uploaded_file, engine=engine, dtype=str)
            st.write(f"📊 Phát hiện: **{len(df_preview):,}** hàng.")
            if st.button("🚀 Thực hiện nạp"):
                progress_bar = st.progress(0)
                status_text = st.empty()
                total = len(df_preview)
                success_count = 0
                for count in import_excel_to_db(df_preview):
                    progress_bar.progress(count / total)
                    status_text.text(f"Đang xử lý: {count:,} / {total:,} hàng...")
                    success_count = count
                if success_count > 0:
                    st.success(f"✅ Thành công! Đã nạp {success_count:,} dòng.")
                    log_activity("IMPORT", f"Nạp tệp: {uploaded_file.name} ({len(df_preview)} hàng)")
        except Exception as e:
            st.error(f"Lỗi: {e}")

elif choice == "🗑️ Dọn dẹp dữ liệu":
    st.subheader("🗑️ Xóa dữ liệu (Admin)")
    confirm = st.checkbox("Xác nhận xóa sạch dữ liệu hệ thống.")
    if st.button("Xóa toàn bộ", disabled=not confirm):
        if delete_all_data():
            st.success("Dữ liệu đã được xóa sạch.")
            log_activity("DELETE_ALL", "Thực hiện xóa toàn bộ bảng participants")

elif choice == "📜 Nhật ký hoạt động":
    st.subheader("📜 Nhật ký hệ thống (Admin)")
    conn = get_db_connection()
    if conn:
        try:
            # Sửa lỗi Alias dùng nháy kép "" thay vì nháy đơn '' cho PostgreSQL
            query = """
                SELECT 
                    created_at AS "Thời gian", 
                    email AS "Người thực hiện", 
                    action AS "Hành động", 
                    details AS "Chi tiết" 
                FROM audit_logs 
                ORDER BY id DESC 
                LIMIT 500
            """
            df_logs = pd.read_sql(query, conn)
            st.dataframe(df_logs, use_container_width=True)
        except Exception as e:
            st.error(f"Lỗi truy vấn nhật ký: {e}")
        finally:
            conn.close()
