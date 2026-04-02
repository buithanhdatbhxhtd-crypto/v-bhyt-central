import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from unidecode import unidecode
import time
from datetime import datetime
from supabase import create_client, Client
import io
import json

# --- 1. CẤU HÌNH TRANG ---
st.set_page_config(
    page_title="V-BHYT Central - Quản trị & Tra cứu",
    page_icon="🏥",
    layout="wide"
)

# --- 2. KẾT NỐI HỆ THỐNG ---

# Khởi tạo Supabase Client (Dùng cho Auth)
@st.cache_resource
def init_supabase():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase: Client = init_supabase()

# Kết nối Postgres trực tiếp (Dùng cho dữ liệu lớn)
def get_db_connection():
    try:
        # Khuyên dùng kết nối Port 6543 của Supabase
        conn = psycopg2.connect(st.secrets["SUPABASE_DB_URL"], connect_timeout=10)
        return conn
    except Exception as e:
        return None

# --- 3. LOGIC XÁC THỰC & PHÂN QUYỀN ---

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
    # Email admin được định nghĩa trong Secrets của Streamlit
    admin_emails = ["admin@example.com", st.secrets.get("ADMIN_EMAIL", "")]
    return st.session_state.user.email in admin_emails

# --- 4. HÀM NHẬT KÝ (AUDIT LOGS) ---

def log_activity(action, details_dict):
    """Ghi lại hoạt động vào bảng audit_logs sử dụng JSONB và Named Placeholders"""
    if not st.session_state.user:
        return
    
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                # Named parameters giúp tránh lỗi tuple index out of range
                sql = "INSERT INTO audit_logs (email, action, details) VALUES (%(email)s, %(action)s, %(details)s)"
                cur.execute(sql, {
                    'email': st.session_state.user.email,
                    'action': action,
                    'details': json.dumps(details_dict, ensure_ascii=False)
                })
            conn.commit()
        except Exception as e:
            print(f"Lỗi ghi nhật ký: {e}")
        finally:
            conn.close()

# --- 5. XỬ LÝ DỮ LIỆU ---

def get_stats():
    """Lấy số liệu thống kê cho Dashboard"""
    conn = get_db_connection()
    if not conn: return None
    stats = {}
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM participants")
            stats['total'] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM participants WHERE han_the < CURRENT_DATE")
            stats['expired'] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM participants WHERE han_the >= CURRENT_DATE AND han_the <= CURRENT_DATE + INTERVAL '30 days'")
            stats['expiring'] = cur.fetchone()[0]
        return stats
    finally:
        conn.close()

def search_data(q_main, q_sub, search_type, status_filter="Tất cả", limit=500):
    """Tìm kiếm linh hoạt với bộ lọc trạng thái"""
    conn = get_db_connection()
    if not conn: return []
    cur = conn.cursor()
    try:
        fields = "ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, dia_chi, sdt, email, han_the"
        where = ""
        params = {'limit': limit}

        if search_type == "Mã BHXH":
            where = "(ma_so_bhxh = %(q)s OR ma_so_bhxh ILIKE %(like_q)s)"
            params.update({'q': q_main.strip(), 'like_q': f"%{q_main.strip()}%"})
        elif search_type == "CCCD":
            where = "(cccd = %(q)s OR cccd ILIKE %(like_q)s)"
            params.update({'q': q_main.strip(), 'like_q': f"%{q_main.strip()}%"})
        else: # Tên & Ngày sinh
            name_norm = unidecode(q_main.strip()).lower()
            dob_clean = q_sub.strip().replace("/", "").replace("-", "").replace(" ", "")
            where = "(ho_ten_unsigned = %(name)s OR ho_ten_unsigned ILIKE %(like_name)s OR (similarity(ho_ten_unsigned, %(name)s) > 0.85))"
            params.update({'name': name_norm, 'like_name': f"%{name_norm}%"})
            if dob_clean:
                if len(dob_clean) == 4:
                    where += " AND TO_CHAR(ngay_sinh, 'YYYY') = %(dob)s"
                else:
                    where += " AND (TO_CHAR(ngay_sinh, 'DDMMYYYY') = %(dob)s OR TO_CHAR(ngay_sinh, 'YYYYMMDD') = %(dob)s)"
                params['dob'] = dob_clean

        # Lọc trạng thái thẻ
        if status_filter == "Đã hết hạn": where += " AND han_the < CURRENT_DATE"
        elif status_filter == "Sắp hết hạn (30 ngày)": where += " AND han_the >= CURRENT_DATE AND han_the <= CURRENT_DATE + INTERVAL '30 days'"
        elif status_filter == "Còn hạn": where += " AND han_the >= CURRENT_DATE"

        query = f"SELECT {fields} FROM participants WHERE {where} ORDER BY ho_ten ASC LIMIT %(limit)s"
        cur.execute(query, params)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()

def export_excel(df):
    """Xuất file Excel chuyên nghiệp với XlsxWriter"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='KetQuaTraCuu')
        workbook = writer.book
        worksheet = writer.sheets['KetQuaTraCuu']
        header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
    return output.getvalue()

def import_db(df):
    """Nạp dữ liệu số lượng lớn (Upsert)"""
    df.columns = [str(c).strip().lower() for c in df.columns]
    mapping = {
        'ma so bhxh': 'ma_so_bhxh', 'mã số bhxh': 'ma_so_bhxh', 'msbhxh': 'ma_so_bhxh',
        'ma the bhyt': 'ma_the_bhyt', 'mã thẻ bhyt': 'ma_the_bhyt',
        'ho ten': 'ho_ten', 'họ tên': 'ho_ten', 'ngay sinh': 'ngay_sinh',
        'cccd': 'cccd', 'socmnd': 'cccd', 'sdt': 'sdt', 'so dien thoai': 'sdt',
        'diachilh': 'dia_chi', 'địa chỉ': 'dia_chi', 'hantheden': 'han_the', 'hạn thẻ': 'han_the', 'email': 'email'
    }
    df = df.rename(columns=mapping)
    target = ['ma_so_bhxh', 'ma_the_bhyt', 'ho_ten', 'ngay_sinh', 'cccd', 'dia_chi', 'sdt', 'email', 'han_the']
    for col in target:
        if col not in df.columns: df[col] = None

    for col in ['ngay_sinh', 'han_the']:
        df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).apply(lambda x: x.date() if pd.notnull(x) else None)

    for col in ['ma_so_bhxh', 'ma_the_bhyt', 'ho_ten', 'cccd', 'sdt', 'dia_chi', 'email']:
        df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        df[col] = df[col].where(~df[col].isin(['nan', 'None', 'NAT', 'NaT', '']), None)

    data = list(df[target].itertuples(index=False, name=None))
    sql = """
        INSERT INTO participants (ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, dia_chi, sdt, email, han_the)
        VALUES %s
        ON CONFLICT (ma_so_bhxh) DO UPDATE SET
            ma_the_bhyt = EXCLUDED.ma_the_bhyt, ho_ten = EXCLUDED.ho_ten, ngay_sinh = EXCLUDED.ngay_sinh,
            cccd = EXCLUDED.cccd, dia_chi = EXCLUDED.dia_chi, sdt = EXCLUDED.sdt,
            email = EXCLUDED.email, han_the = EXCLUDED.han_the, updated_at = NOW();
    """
    batch_size = 5000
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        conn = get_db_connection()
        if not conn: break
        with conn.cursor() as cur:
            execute_values(cur, sql, batch)
            conn.commit()
        conn.close()
        yield min(i + batch_size, len(data))

# --- 6. GIAO DIỆN CHÍNH ---

if 'user' not in st.session_state:
    st.session_state.user = None

if st.session_state.user is None:
    st.markdown("<h1 style='text-align: center; color: #1E88E5;'>🏥 V-BHYT Central</h1>", unsafe_allow_html=True)
    with st.container():
        _, col, _ = st.columns([1, 1.5, 1])
        with col:
            with st.form("login_form"):
                st.info("Đăng nhập bằng tài khoản công vụ")
                email = st.text_input("Email")
                pwd = st.text_input("Mật khẩu", type="password")
                if st.form_submit_button("Đăng nhập hệ thống", use_container_width=True):
                    auth = login_user(email, pwd)
                    if auth:
                        st.session_state.user = auth.user
                        log_activity("LOGIN", {"status": "success"})
                        st.rerun()
    st.stop()

# --- SIDEBAR ---
st.sidebar.title("🏥 V-BHYT Central")
st.sidebar.markdown(f"👤 **{st.session_state.user.email}**")
role_label = "🔴 Quản trị viên" if is_admin() else "🔵 Nhân viên Tra cứu"
st.sidebar.caption(role_label)

menu = ["📊 Dashboard", "🔍 Tra cứu & Xuất dữ liệu"]
if is_admin():
    menu += ["📥 Nhập dữ liệu mới", "📜 Nhật ký hoạt động", "🗑️ Quản lý kho"]

choice = st.sidebar.selectbox("Chức năng chính", menu)
if st.sidebar.button("🚪 Đăng xuất", use_container_width=True):
    log_activity("LOGOUT", {"status": "success"})
    logout_user()

# --- NỘI DUNG ---

if choice == "📊 Dashboard":
    st.header("📊 Tổng quan hệ thống")
    stats = get_stats()
    if stats:
        c1, c2, c3 = st.columns(3)
        c1.metric("Tổng số người tham gia", f"{stats['total']:,}")
        c2.metric("Thẻ đã hết hạn", f"{stats['expired']:,}")
        c3.metric("Sắp hết hạn (30 ngày)", f"{stats['expiring']:,}")
        
        st.write("---")
        st.subheader("🔔 Danh sách cần gia hạn (Gợi ý)")
        warning = search_data("", "", "Tên & Ngày sinh", status_filter="Sắp hết hạn (30 ngày)", limit=10)
        if warning:
            df_w = pd.DataFrame(warning, columns=["Mã BHXH", "Thẻ BHYT", "Họ Tên", "Ngày Sinh", "CCCD", "Địa chỉ", "SĐT", "Email", "Hạn Thẻ"])
            st.table(df_w[["Họ Tên", "SĐT", "Hạn Thẻ"]])
        else:
            st.success("Không có trường hợp sắp hết hạn trong 30 ngày tới.")

elif choice == "🔍 Tra cứu & Xuất dữ liệu":
    st.header("🔍 Tra cứu người tham gia")
    with st.expander("🛠️ Bộ lọc nâng cao", expanded=True):
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1: stype = st.selectbox("Loại tìm kiếm", ["Tên & Ngày sinh", "Mã BHXH", "CCCD"])
        with col2: sfilter = st.selectbox("Trạng thái thẻ", ["Tất cả", "Còn hạn", "Sắp hết hạn (30 ngày)", "Đã hết hạn"])
        with col3: slimit = st.number_input("Giới hạn", 10, 2000, 200)

        c_m, c_s = st.columns([2, 1])
        if stype == "Tên & Ngày sinh":
            with c_m: q_m = st.text_input("Họ tên")
            with c_s: q_s = st.text_input("Ngày/Năm sinh")
        else:
            with c_m: q_m = st.text_input(f"Nhập {stype}")
            q_s = ""

    if st.button("🚀 Thực hiện tra cứu", use_container_width=True):
        res = search_data(q_m, q_s, stype, sfilter, slimit)
        log_activity("SEARCH", {"type": stype, "q": q_m, "dob": q_s, "count": len(res)})
        if res:
            df = pd.DataFrame(res, columns=["Mã BHXH", "Thẻ BHYT", "Họ Tên", "Ngày Sinh", "CCCD", "Địa chỉ", "SĐT", "Email", "Hạn Thẻ"])
            for c in ["Ngày Sinh", "Hạn Thẻ"]:
                df[c] = pd.to_datetime(df[c], errors='coerce').dt.strftime('%d/%m/%Y')
            
            st.success(f"Tìm thấy {len(df)} kết quả.")
            st.download_button("📥 Tải về file Excel", export_excel(df), f"BHYT_TraCuu_{datetime.now().strftime('%Y%m%d')}.xlsx")
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.warning("Không có dữ liệu phù hợp.")

elif choice == "📥 Nhập dữ liệu mới":
    st.header("📥 Nhập dữ liệu hàng loạt")
    f = st.file_uploader("Chọn file Excel (.xlsx, .xlsb)", type=["xlsx", "xlsb"])
    if f:
        df_new = pd.read_excel(f, engine='pyxlsb' if f.name.endswith('.xlsb') else None, dtype=str)
        st.info(f"Phát hiện **{len(df_new):,}** hàng dữ liệu.")
        if st.button("🚀 Bắt đầu cập nhật Database"):
            p = st.progress(0)
            t = st.empty()
            for count in import_db(df_new):
                p.progress(count / len(df_new))
                t.text(f"Đang xử lý: {count:,} / {len(df_new):,} hàng...")
            log_activity("IMPORT", {"file": f.name, "rows": len(df_new)})
            st.success("Cập nhật dữ liệu thành công!")
            st.balloons()

elif choice == "📜 Nhật ký hoạt động":
    st.header("📜 Nhật ký hệ thống (Admin)")
    conn = get_db_connection()
    if conn:
        try:
            df_logs = pd.read_sql("SELECT created_at, email, action, details FROM audit_logs ORDER BY id DESC LIMIT 500", conn)
            df_logs['created_at'] = pd.to_datetime(df_logs['created_at']).dt.strftime('%H:%M:%S %d/%m/%Y')
            df_logs['details'] = df_logs['details'].apply(lambda x: json.dumps(x, ensure_ascii=False))
            df_logs.columns = ["Thời gian", "Người thực hiện", "Hành động", "Chi tiết"]
            st.dataframe(df_logs, use_container_width=True, hide_index=True)
        finally:
            conn.close()

elif choice == "🗑️ Quản lý kho":
    st.header("🗑️ Dọn dẹp dữ liệu")
    st.error("Cảnh báo: Thao tác này sẽ xóa sạch dữ liệu người tham gia BHYT!")
    if st.checkbox("Tôi xác nhận là Admin và muốn xóa toàn bộ."):
        if st.button("🔴 THỰC HIỆN XÓA SẠCH"):
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE participants RESTART IDENTITY")
                conn.commit()
            log_activity("DELETE_ALL", {"scope": "participants"})
            st.success("Đã xóa sạch cơ sở dữ liệu.")
            time.sleep(1)
            st.rerun()
