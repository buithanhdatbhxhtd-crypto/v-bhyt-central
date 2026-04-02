import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from unidecode import unidecode
import time
from datetime import datetime, timedelta
from supabase import create_client, Client
import io

# --- 1. CẤU HÌNH & KHỞI TẠO ---
st.set_page_config(
    page_title="V-BHYT Central - Quản trị & Tra cứu",
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

# --- 3. LOGIC XỬ LÝ DỮ LIỆU & THỐNG KÊ ---

def get_dashboard_stats():
    """Lấy số liệu thống kê tổng quan từ Database"""
    conn = get_db_connection()
    if not conn: return None
    stats = {}
    try:
        cur = conn.cursor()
        # Tổng số người
        cur.execute("SELECT COUNT(*) FROM participants")
        stats['total'] = cur.fetchone()[0]
        # Hết hạn
        cur.execute("SELECT COUNT(*) FROM participants WHERE han_the < CURRENT_DATE")
        stats['expired'] = cur.fetchone()[0]
        # Sắp hết hạn (trong 30 ngày tới)
        cur.execute("SELECT COUNT(*) FROM participants WHERE han_the >= CURRENT_DATE AND han_the <= CURRENT_DATE + INTERVAL '30 days'")
        stats['expiring_soon'] = cur.fetchone()[0]
        return stats
    finally:
        cur.close()
        conn.close()

def search_participants(q_main, q_sub, search_type, filter_status="Tất cả", limit=500):
    conn = get_db_connection()
    if not conn: return []
    cur = conn.cursor()
    try:
        select_fields = "ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, dia_chi, sdt, email, han_the"
        where_clause = ""
        params = {'limit': limit}

        # Xử lý loại tìm kiếm
        if search_type == "Mã BHXH":
            where_clause = "(ma_so_bhxh = %(q)s OR ma_so_bhxh ILIKE %(like_q)s)"
            params.update({'q': q_main.strip(), 'like_q': f"%{q_main.strip()}"})
        elif search_type == "CCCD":
            where_clause = "(cccd = %(q)s OR cccd ILIKE %(like_q)s)"
            params.update({'q': q_main.strip(), 'like_q': f"%{q_main.strip()}"})
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

        # Xử lý lọc theo trạng thái
        if filter_status == "Đã hết hạn":
            where_clause += " AND han_the < CURRENT_DATE"
        elif filter_status == "Sắp hết hạn (30 ngày)":
            where_clause += " AND han_the >= CURRENT_DATE AND han_the <= CURRENT_DATE + INTERVAL '30 days'"
        elif filter_status == "Còn hạn":
            where_clause += " AND han_the >= CURRENT_DATE"

        query = f"""
            SELECT {select_fields} FROM participants 
            WHERE {where_clause}
            ORDER BY ho_ten ASC
            LIMIT %(limit)s
        """
        cur.execute(query, params)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()

# --- 4. LOGIC NHẬP/XUẤT DỮ LIỆU ---

def export_to_excel(df):
    """Chuyển đổi DataFrame sang file Excel chuyên nghiệp"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Ket_Qua_Tra_Cuu')
        # Tối ưu giao diện file Excel
        workbook = writer.book
        worksheet = writer.sheets['Ket_Qua_Tra_Cuu']
        header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
    return output.getvalue()

def import_excel_to_db(df):
    df.columns = [str(c).strip().lower() for c in df.columns]
    mapping = {
        'ma so bhxh': 'ma_so_bhxh', 'mã số bhxh': 'ma_so_bhxh', 'msbhxh': 'ma_so_bhxh',
        'ma the bhyt': 'ma_the_bhyt', 'mã thẻ bhyt': 'ma_the_bhyt', 'mathe': 'ma_the_bhyt',
        'ho ten': 'ho_ten', 'họ tên': 'ho_ten', 'ngay sinh': 'ngay_sinh',
        'socmnd': 'cccd', 'cccd': 'cccd', 'sodient': 'sdt', 'so dien thoai': 'sdt',
        'diachilh': 'dia_chi', 'địa chỉ': 'dia_chi', 'hantheden': 'han_the', 'hạn thẻ': 'han_the',
        'email': 'email'
    }
    df = df.rename(columns=mapping)
    target_cols = ['ma_so_bhxh', 'ma_the_bhyt', 'ho_ten', 'ngay_sinh', 'cccd', 'dia_chi', 'sdt', 'email', 'han_the']
    for col in target_cols:
        if col not in df.columns: df[col] = None

    for col in ['ngay_sinh', 'han_the']:
        df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).apply(lambda x: x.date() if pd.notnull(x) else None)

    str_cols = ['ma_so_bhxh', 'ma_the_bhyt', 'ho_ten', 'cccd', 'sdt', 'dia_chi', 'email']
    for col in str_cols:
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
    for i in range(0, len(data_tuples), 5000):
        batch = data_tuples[i:i + 5000]
        conn = get_db_connection()
        if not conn: break
        cur = conn.cursor()
        try:
            execute_values(cur, sql, batch)
            conn.commit()
            yield min(i + 5000, len(data_tuples))
        finally:
            cur.close()
            conn.close()

# --- 5. GIAO DIỆN ĐĂNG NHẬP ---

if 'user' not in st.session_state:
    st.session_state.user = None

if st.session_state.user is None:
    st.markdown("<h1 style='text-align: center; color: #1E88E5;'>🏥 V-BHYT Central</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center;'>Hệ thống quản trị & tra cứu Bảo hiểm y tế tập trung</p>", unsafe_allow_html=True)
    with st.container():
        col1, col2, col3 = st.columns([1, 1.5, 1])
        with col2:
            with st.form("login_form", clear_on_submit=False):
                st.write("🔒 **Đăng nhập hệ thống**")
                email = st.text_input("Email công vụ")
                password = st.text_input("Mật khẩu", type="password")
                if st.form_submit_button("Đăng nhập", use_container_width=True):
                    auth_res = login_user(email, password)
                    if auth_res:
                        st.session_state.user = auth_res.user
                        st.rerun()
    st.stop()

# --- 6. GIAO DIỆN CHÍNH ---

# Sidebar cải tiến
st.sidebar.image("https://img.icons8.com/fluency/96/hospital-room.png", width=80)
st.sidebar.title("V-BHYT Central")
st.sidebar.write(f"👤 **{st.session_state.user.email}**")
role_text = "🔴 Quản trị viên" if is_admin() else "🔵 Nhân viên Tra cứu"
st.sidebar.caption(role_text)

menu = ["📊 Dashboard", "🔍 Tra cứu & Xuất dữ liệu"]
if is_admin():
    menu += ["📥 Nhập dữ liệu mới", "🗑️ Quản lý kho"]

choice = st.sidebar.selectbox("Chức năng chính", menu)
if st.sidebar.button("🚪 Đăng xuất", use_container_width=True):
    logout_user()

# --- NỘI DUNG TỪNG TAB ---

if choice == "📊 Dashboard":
    st.header("📊 Tổng quan hệ thống")
    stats = get_dashboard_stats()
    if stats:
        c1, c2, c3 = st.columns(3)
        c1.metric("Tổng số người tham gia", f"{stats['total']:,}")
        c2.metric("Thẻ đã hết hạn", f"{stats['expired']:,}", delta="-Hành động cần thiết", delta_color="inverse")
        c3.metric("Sắp hết hạn (30 ngày)", f"{stats['expiring_soon']:,}", delta="Cảnh báo", delta_color="off")
        
        # Biểu đồ đơn giản bằng Streamlit
        st.write("---")
        st.subheader("🔔 Danh sách cần gia hạn gấp")
        with st.spinner("Đang lọc danh sách..."):
            warning_data = search_participants("", "", "Tên & Ngày sinh", filter_status="Sắp hết hạn (30 ngày)", limit=10)
            if warning_data:
                df_warn = pd.DataFrame(warning_data, columns=["Mã BHXH", "Thẻ BHYT", "Họ Tên", "Ngày Sinh", "CCCD", "Địa chỉ", "SĐT", "Email", "Hạn Thẻ"])
                st.table(df_warn[["Họ Tên", "SĐT", "Hạn Thẻ"]])
            else:
                st.success("Không có ai sắp hết hạn thẻ trong 30 ngày tới!")

elif choice == "🔍 Tra cứu & Xuất dữ liệu":
    st.header("🔍 Tra cứu thông tin")
    
    with st.expander("🛠️ Bộ lọc tìm kiếm", expanded=True):
        col1, col2, col3 = st.columns([2, 2, 2])
        with col1:
            stype = st.selectbox("Tìm kiếm theo", ["Tên & Ngày sinh", "Mã BHXH", "CCCD"])
        with col2:
            status_filter = st.selectbox("Trạng thái thẻ", ["Tên & Ngày sinh", "Còn hạn", "Sắp hết hạn (30 ngày)", "Đã hết hạn", "Tất cả"])
        with col3:
            limit_res = st.slider("Số lượng kết quả", 50, 1000, 200)

        c_main, c_sub = st.columns([2, 1])
        if stype == "Tên & Ngày sinh":
            with c_main: q_main = st.text_input("Họ tên", placeholder="Ví dụ: Nguyễn Văn A")
            with c_sub: q_sub = st.text_input("Năm/Ngày sinh", placeholder="1990 hoặc 01/01/1990")
        else:
            with c_main: q_main = st.text_input(f"Nhập {stype}")
            q_sub = ""

    if st.button("🚀 Bắt đầu tra cứu", use_container_width=True):
        with st.spinner("Đang truy xuất dữ liệu..."):
            data = search_participants(q_main, q_sub, stype, status_filter, limit_res)
            if data:
                st.success(f"Tìm thấy {len(data)} kết quả phù hợp.")
                cols_display = ["Mã BHXH", "Thẻ BHYT", "Họ Tên", "Ngày Sinh", "CCCD", "Địa chỉ", "SĐT", "Email", "Hạn Thẻ"]
                df_res = pd.DataFrame(data, columns=cols_display)
                
                # Định dạng hiển thị
                for d_col in ["Ngày Sinh", "Hạn Thẻ"]:
                    df_res[d_col] = pd.to_datetime(df_res[d_col], errors='coerce').dt.strftime('%d/%m/%Y')
                
                # Nút Xuất Excel
                excel_data = export_to_excel(df_res)
                st.download_button(
                    label="📥 Tải kết quả về Excel (.xlsx)",
                    data=excel_data,
                    file_name=f"TraCuu_BHYT_{datetime.now().strftime('%Y%M%D_%H%M')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                
                st.dataframe(df_res, use_container_width=True, hide_index=True)
            else:
                st.warning("Không tìm thấy kết quả nào với bộ lọc hiện tại.")

elif choice == "📥 Nhập dữ liệu mới":
    st.header("📥 Nhập dữ liệu hàng loạt")
    st.info("Hệ thống hỗ trợ nạp tối đa 500,000 hàng. Vui lòng đảm bảo các cột: Mã số BHXH, Họ tên, Ngày sinh có dữ liệu.")
    
    uploaded_file = st.file_uploader("Chọn file Excel", type=["xlsx", "xlsb"])
    if uploaded_file:
        engine = 'pyxlsb' if uploaded_file.name.endswith('.xlsb') else None
        df_new = pd.read_excel(uploaded_file, engine=engine, dtype=str)
        st.write(f"📦 Tệp có **{len(df_new):,}** bản ghi.")
        
        if st.button("🚀 Thực hiện Import/Update"):
            p_bar = st.progress(0)
            status = st.empty()
            total = len(df_new)
            for count in import_excel_to_db(df_new):
                p_bar.progress(count / total)
                status.text(f"Đang xử lý: {count:,} / {total:,} hàng...")
            st.success("✅ Đã hoàn tất nạp dữ liệu!")
            st.balloons()

elif choice == "🗑️ Quản lý kho":
    st.header("🗑️ Quản lý & Dọn dẹp")
    st.warning("Hành động xóa toàn bộ sẽ không thể khôi phục dữ liệu.")
    if st.checkbox("Tôi xác nhận là Quản trị viên và muốn xóa dữ liệu."):
        if st.button("🔴 Xóa toàn bộ dữ liệu người tham gia"):
            if delete_all_data():
                st.success("Đã xóa sạch kho dữ liệu.")
                time.sleep(1)
                st.rerun()
