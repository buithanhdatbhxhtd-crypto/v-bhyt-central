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

# --- 1. CẤU HÌNH & KHỞI TẠO ---
st.set_page_config(
    page_title="V-BHYT Central - Quản trị & Tra cứu",
    page_icon="🏥",
    layout="wide"
)

# Khởi tạo Supabase Client cho Auth (Đăng nhập/Đăng xuất)
@st.cache_resource
def init_supabase():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase: Client = init_supabase()

# Kết nối trực tiếp Database (psycopg2) để xử lý dữ liệu lớn
def get_db_connection():
    try:
        # Sử dụng Connection URL từ Secrets (Khuyên dùng port 6543)
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
    # Kiểm tra email admin trong cấu hình Secrets
    admin_emails = ["admin@example.com", st.secrets.get("ADMIN_EMAIL", "")]
    return st.session_state.user.email in admin_emails

# --- 3. HÀM GHI NHẬT KÝ (AUDIT LOGS) ---

def log_activity(action, details_dict):
    """Ghi lại hoạt động vào bảng audit_logs sử dụng kiểu JSONB an toàn"""
    if not st.session_state.user:
        return
    
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                # Sử dụng Named Parameters để tránh lỗi index out of range
                sql = "INSERT INTO audit_logs (email, action, details) VALUES (%(email)s, %(action)s, %(details)s)"
                cur.execute(sql, {
                    'email': st.session_state.user.email,
                    'action': action,
                    'details': json.dumps(details_dict, ensure_ascii=False)
                })
            conn.commit()
        except Exception as e:
            # Chỉ ghi lỗi ra log hệ thống, không làm phiền người dùng
            print(f"Lưu nhật ký lỗi: {e}")
        finally:
            conn.close()

# --- 4. LOGIC TRUY VẤN DỮ LIỆU ---

def get_dashboard_stats():
    """Lấy số liệu thống kê nhanh cho Dashboard"""
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
            stats['expiring_soon'] = cur.fetchone()[0]
        return stats
    finally:
        conn.close()

def search_participants(q_main, q_sub, search_type, filter_status="Tất cả", limit=500):
    conn = get_db_connection()
    if not conn: return []
    cur = conn.cursor()
    try:
        select_fields = "ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, dia_chi, sdt, email, han_the"
        where_clause = ""
        params = {'limit': limit}

        if search_type == "Mã BHXH":
            where_clause = "(ma_so_bhxh = %(q)s OR ma_so_bhxh ILIKE %(like_q)s)"
            params.update({'q': q_main.strip(), 'like_q': f"%{q_main.strip()}%"})
        elif search_type == "CCCD":
            where_clause = "(cccd = %(q)s OR cccd ILIKE %(like_q)s)"
            params.update({'q': q_main.strip(), 'like_q': f"%{q_main.strip()}%"})
        else: # Tên & Ngày sinh
            name_norm = unidecode(q_main.strip()).lower()
            dob_clean = q_sub.strip().replace("/", "").replace("-", "").replace(" ", "")
            where_clause = "(ho_ten_unsigned = %(name)s OR ho_ten_unsigned ILIKE %(like_name)s OR (similarity(ho_ten_unsigned, %(name)s) > 0.85))"
            params.update({'name': name_norm, 'like_name': f"%{name_norm}%"})
            if dob_clean:
                if len(dob_clean) == 4:
                    where_clause += " AND TO_CHAR(ngay_sinh, 'YYYY') = %(dob)s"
                else:
                    where_clause += " AND (TO_CHAR(ngay_sinh, 'DDMMYYYY') = %(dob)s OR TO_CHAR(ngay_sinh, 'YYYYMMDD') = %(dob)s)"
                params['dob'] = dob_clean

        # Bộ lọc trạng thái thẻ
        if filter_status == "Đã hết hạn": where_clause += " AND han_the < CURRENT_DATE"
        elif filter_status == "Sắp hết hạn (30 ngày)": where_clause += " AND han_the >= CURRENT_DATE AND han_the <= CURRENT_DATE + INTERVAL '30 days'"
        elif filter_status == "Còn hạn": where_clause += " AND han_the >= CURRENT_DATE"

        query = f"SELECT {select_fields} FROM participants WHERE {where_clause} ORDER BY ho_ten ASC LIMIT %(limit)s"
        cur.execute(query, params)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()

# --- 5. XỬ LÝ FILE (XUẤT EXCEL / NHẬP DỮ LIỆU) ---

def export_to_excel(df):
    """Xuất file Excel chuyên nghiệp với định dạng tiêu đề"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Du_Lieu_Tra_Cuu')
        workbook = writer.book
        worksheet = writer.sheets['Du_Lieu_Tra_Cuu']
        header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
    return output.getvalue()

def import_excel_to_db(df):
    """Nạp dữ liệu lớn bằng execute_values (Upsert strategy)"""
    df.columns = [str(c).strip().lower() for c in df.columns]
    mapping = {
        'ma so bhxh': 'ma_so_bhxh', 'mã số bhxh': 'ma_so_bhxh', 'ma the bhyt': 'ma_the_bhyt',
        'ho ten': 'ho_ten', 'họ tên': 'ho_ten', 'ngay sinh': 'ngay_sinh',
        'cccd': 'cccd', 'socmnd': 'cccd', 'sdt': 'sdt', 'so dien thoai': 'sdt',
        'diachilh': 'dia_chi', 'địa chỉ': 'dia_chi', 'hantheden': 'han_the', 'hạn thẻ': 'han_the', 'email': 'email'
    }
    df = df.rename(columns=mapping)
    target_cols = ['ma_so_bhxh', 'ma_the_bhyt', 'ho_ten', 'ngay_sinh', 'cccd', 'dia_chi', 'sdt', 'email', 'han_the']
    for col in target_cols:
        if col not in df.columns: df[col] = None

    # Chuẩn hóa ngày tháng
    for col in ['ngay_sinh', 'han_the']:
        df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).apply(lambda x: x.date() if pd.notnull(x) else None)

    # Chuẩn hóa văn bản
    for col in ['ma_so_bhxh', 'ma_the_bhyt', 'ho_ten', 'cccd', 'sdt', 'dia_chi', 'email']:
        df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        df[col] = df[col].where(~df[col].isin(['nan', 'None', 'NAT', 'NaT', '']), None)

    data_tuples = list(df[target_cols].itertuples(index=False, name=None))
    sql = """
        INSERT INTO participants (ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, dia_chi, sdt, email, han_the)
        VALUES %s
        ON CONFLICT (ma_so_bhxh) DO UPDATE SET
            ma_the_bhyt = EXCLUDED.ma_the_bhyt, ho_ten = EXCLUDED.ho_ten, ngay_sinh = EXCLUDED.ngay_sinh,
            cccd = EXCLUDED.cccd, dia_chi = EXCLUDED.dia_chi, sdt = EXCLUDED.sdt,
            email = EXCLUDED.email, han_the = EXCLUDED.han_the, updated_at = NOW();
    """
    # Xử lý theo lô (batch) 5000 hàng
    for i in range(0, len(data_tuples), 5000):
        batch = data_tuples[i:i + 5000]
        conn = get_db_connection()
        if not conn: break
        with conn.cursor() as cur:
            execute_values(cur, sql, batch)
            conn.commit()
        conn.close()
        yield min(i + 5000, len(data_tuples))

# --- 6. GIAO DIỆN CHÍNH (STREAMLIT) ---

if 'user' not in st.session_state:
    st.session_state.user = None

# Màn hình Đăng nhập
if st.session_state.user is None:
    st.markdown("<h1 style='text-align: center; color: #1E88E5;'>🏥 V-BHYT Central</h1>", unsafe_allow_html=True)
    with st.container():
        col1, col2, col3 = st.columns([1, 1.5, 1])
        with col2:
            with st.form("login_form"):
                st.write("🔒 **Đăng nhập hệ thống**")
                email = st.text_input("Email công vụ")
                password = st.text_input("Mật khẩu", type="password")
                if st.form_submit_button("Đăng nhập", use_container_width=True):
                    auth_res = login_user(email, password)
                    if auth_res:
                        st.session_state.user = auth_res.user
                        log_activity("LOGIN", {"status": "success", "msg": "Đăng nhập thành công"})
                        st.rerun()
    st.stop()

# Thanh Sidebar
st.sidebar.title("🏥 V-BHYT Central")
st.sidebar.write(f"👤 **{st.session_state.user.email}**")
st.sidebar.caption("🔴 Quản trị viên" if is_admin() else "🔵 Nhân viên Tra cứu")

menu = ["📊 Dashboard", "🔍 Tra cứu & Xuất dữ liệu"]
if is_admin():
    menu += ["📥 Nhập dữ liệu mới", "📜 Nhật ký hoạt động", "🗑️ Quản lý kho"]

choice = st.sidebar.selectbox("Chức năng chính", menu)
if st.sidebar.button("🚪 Đăng xuất", use_container_width=True):
    log_activity("LOGOUT", {"status": "success"})
    logout_user()

# --- XỬ LÝ NỘI DUNG TỪNG MENU ---

if choice == "📊 Dashboard":
    st.header("📊 Tổng quan hệ thống")
    stats = get_dashboard_stats()
    if stats:
        c1, c2, c3 = st.columns(3)
        c1.metric("Tổng số người tham gia", f"{stats['total']:,}")
        c2.metric("Thẻ đã hết hạn", f"{stats['expired']:,}")
        c3.metric("Sắp hết hạn (30 ngày)", f"{stats['expiring_soon']:,}")
        
        st.write("---")
        st.subheader("🔔 Gợi ý gia hạn (Sắp hết hạn)")
        # Hiển thị nhanh 5 người sắp hết hạn thẻ
        quick_warn = search_participants("", "", "Tên & Ngày sinh", filter_status="Sắp hết hạn (30 ngày)", limit=5)
        if quick_warn:
            df_q = pd.DataFrame(quick_warn, columns=["Mã BHXH", "Thẻ BHYT", "Họ Tên", "Ngày Sinh", "CCCD", "Địa chỉ", "SĐT", "Email", "Hạn Thẻ"])
            st.table(df_q[["Họ Tên", "SĐT", "Hạn Thẻ"]])
        else:
            st.info("Hiện không có ai sắp hết hạn thẻ.")

elif choice == "🔍 Tra cứu & Xuất dữ liệu":
    st.header("🔍 Tra cứu thông tin")
    with st.expander("🛠️ Bộ lọc nâng cao", expanded=True):
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1: stype = st.selectbox("Tìm kiếm theo", ["Tên & Ngày sinh", "Mã BHXH", "CCCD"])
        with col2: sfilter = st.selectbox("Trạng thái thẻ", ["Tất cả", "Còn hạn", "Sắp hết hạn (30 ngày)", "Đã hết hạn"])
        with col3: limit = st.number_input("Giới hạn", 10, 1000, 200)

        c_m, c_s = st.columns([2, 1])
        if stype == "Tên & Ngày sinh":
            with c_m: q_main = st.text_input("Họ tên")
            with c_s: q_sub = st.text_input("Năm/Ngày sinh (VD: 1988)")
        else:
            with c_m: q_main = st.text_input(f"Nhập {stype}")
            q_sub = ""

    if st.button("🚀 Bắt đầu tra cứu", use_container_width=True):
        data = search_participants(q_main, q_sub, stype, sfilter, limit)
        # Ghi nhật ký tra cứu
        log_activity("SEARCH", {"type": stype, "query": q_main, "dob": q_sub, "count": len(data)})
        
        if data:
            st.success(f"Tìm thấy {len(data)} kết quả.")
            df_res = pd.DataFrame(data, columns=["Mã BHXH", "Thẻ BHYT", "Họ Tên", "Ngày Sinh", "CCCD", "Địa chỉ", "SĐT", "Email", "Hạn Thẻ"])
            for col in ["Ngày Sinh", "Hạn Thẻ"]:
                df_res[col] = pd.to_datetime(df_res[col], errors='coerce').dt.strftime('%d/%m/%Y')
            
            # Nút xuất file Excel
            st.download_button("📥 Tải về file Excel", export_to_excel(df_res), f"KetQua_BHYT_{datetime.now().strftime('%d%m%Y')}.xlsx")
            st.dataframe(df_res, use_container_width=True, hide_index=True)
        else:
            st.warning("Không tìm thấy kết quả phù hợp.")

elif choice == "📥 Nhập dữ liệu mới":
    st.header("📥 Nhập dữ liệu từ Excel")
    file = st.file_uploader("Kéo thả file Excel vào đây (.xlsx, .xlsb)", type=["xlsx", "xlsb"])
    if file:
        df_new = pd.read_excel(file, engine='pyxlsb' if file.name.endswith('.xlsb') else None, dtype=str)
        st.write(f"📦 Phát hiện: **{len(df_new):,}** hàng.")
        if st.button(f"🚀 Bắt đầu nạp dữ liệu"):
            bar = st.progress(0)
            status = st.empty()
            for count in import_excel_to_db(df_new):
                bar.progress(count / len(df_new))
                status.text(f"Đang nạp: {count:,} / {len(df_new):,} hàng...")
            # Ghi nhật ký nhập liệu
            log_activity("IMPORT", {"filename": file.name, "rows": len(df_new)})
            st.success("Cập nhật dữ liệu thành công!")
            st.balloons()

elif choice == "📜 Nhật ký hoạt động":
    st.header("📜 Nhật ký hệ thống (Admin)")
    conn = get_db_connection()
    if conn:
        try:
            # Truy vấn nhật ký từ bảng audit_logs
            query = "SELECT created_at, email, action, details FROM audit_logs ORDER BY id DESC LIMIT 500"
            df_logs = pd.read_sql(query, conn)
            
            # Định dạng hiển thị
            df_logs['created_at'] = pd.to_datetime(df_logs['created_at']).dt.strftime('%H:%M:%S %d/%m/%Y')
            # Chuyển JSON thành chuỗi để đọc dễ hơn trong bảng
            df_logs['details'] = df_logs['details'].apply(lambda x: json.dumps(x, ensure_ascii=False) if x else "")
            
            st.dataframe(df_logs, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"Lỗi truy vấn nhật ký: {e}")
        finally:
            conn.close()

elif choice == "🗑️ Quản lý kho":
    st.header("🗑️ Xóa dữ liệu hệ thống")
    st.warning("Hành động này sẽ xóa toàn bộ danh sách người tham gia. Hãy cẩn trọng!")
    if st.checkbox("Tôi xác nhận muốn xóa toàn bộ dữ liệu."):
        if st.button("🔴 THỰC HIỆN XÓA SẠCH"):
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE participants RESTART IDENTITY")
                conn.commit()
            log_activity("DELETE_ALL", {"target": "participants_table"})
            st.success("Đã xóa sạch dữ liệu.")
            time.sleep(1)
            st.rerun()
