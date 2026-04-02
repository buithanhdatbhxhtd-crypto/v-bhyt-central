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
import plotly.express as px  # Thư viện biểu đồ cao cấp

# --- 1. CẤU HÌNH TRANG ---
st.set_page_config(
    page_title="V-BHYT Central - Quản trị Cao cấp",
    page_icon="🛡️",
    layout="wide"
)

# --- 2. KẾT NỐI HỆ THỐNG ---

@st.cache_resource
def init_supabase():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase: Client = init_supabase()

def get_db_connection():
    try:
        conn = psycopg2.connect(st.secrets["SUPABASE_DB_URL"], connect_timeout=10)
        return conn
    except Exception as e:
        return None

# --- 3. LOGIC XÁC THỰC & QUẢN TRỊ ---

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

def admin_manage_user(target_email, action, new_password=None):
    """Quản lý người dùng nâng cao (Yêu cầu Service Role Key)"""
    try:
        admin_client = create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])
        response = admin_client.auth.admin.list_users()
        all_users = response.users if hasattr(response, 'users') else response
        user = next((u for u in all_users if u.email == target_email), None)
        
        if not user:
            return False, "Không tìm thấy người dùng."

        if action == "RESET_PWD":
            admin_client.auth.admin.update_user_by_id(user.id, {"password": new_password})
            return True, f"Đã đổi mật khẩu cho {target_email}."
        elif action == "DELETE":
            admin_client.auth.admin.delete_user(user.id)
            return True, f"Đã xóa tài khoản {target_email}."
        
        return False, "Hành động không hợp lệ."
    except Exception as e:
        return False, f"Lỗi quản trị: {str(e)}"

# --- 4. HÀM NHẬT KÝ & THỐNG KÊ ---

def log_activity(action, details_dict):
    if not st.session_state.user: return
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                sql = "INSERT INTO audit_logs (email, action, details) VALUES (%(email)s, %(action)s, %(details)s)"
                cur.execute(sql, {
                    'email': st.session_state.user.email,
                    'action': action,
                    'details': json.dumps(details_dict, ensure_ascii=False)
                })
            conn.commit()
        except: pass
        finally: conn.close()

def get_advanced_stats():
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
            cur.execute("SELECT COUNT(*) FROM participants WHERE cccd IS NULL OR cccd = ''")
            stats['incomplete'] = cur.fetchone()[0]
        return stats
    finally: conn.close()

def import_db_logic(df):
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

# --- 5. GIAO DIỆN CHÍNH ---

if 'user' not in st.session_state:
    st.session_state.user = None
if 'threshold' not in st.session_state:
    st.session_state.threshold = 0.85

if st.session_state.user is None:
    st.markdown("<h1 style='text-align: center; color: #1E88E5;'>🏥 V-BHYT Central Pro</h1>", unsafe_allow_html=True)
    with st.container():
        _, col, _ = st.columns([1, 1.5, 1])
        with col:
            with st.form("login_form"):
                st.info("Hệ thống Quản trị & Tra cứu Bảo hiểm Y tế")
                email = st.text_input("Email công vụ")
                pwd = st.text_input("Mật khẩu", type="password")
                if st.form_submit_button("Đăng nhập hệ thống", use_container_width=True):
                    auth = login_user(email, pwd)
                    if auth:
                        st.session_state.user = auth.user
                        log_activity("LOGIN", {"status": "success"})
                        st.rerun()
    st.stop()

# --- SIDEBAR ---
st.sidebar.title("🛡️ V-BHYT PRO")
st.sidebar.markdown(f"👤 **{st.session_state.user.email}**")
role_label = "🔴 Quản trị viên" if is_admin() else "🔵 Nhân viên Tra cứu"
st.sidebar.caption(role_label)

menu = ["📊 Dashboard", "🔍 Tra cứu & Xuất file", "⚙️ Tài khoản"]
if is_admin():
    menu += ["📥 Nhập dữ liệu", "📜 Nhật ký hệ thống", "👥 Quản lý nhân sự", "🔧 Cấu hình", "🗑️ Dọn dẹp"]

choice = st.sidebar.selectbox("Menu Quản lý", menu)
if st.sidebar.button("🚪 Đăng xuất", use_container_width=True):
    log_activity("LOGOUT", {"status": "success"})
    logout_user()

# --- NỘI DUNG ---

if choice == "📊 Dashboard":
    st.header("📊 Phân tích & Thống kê hệ thống")
    stats = get_advanced_stats()
    if stats:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Tổng bản ghi", f"{stats['total']:,}")
        c2.metric("Đã hết hạn", f"{stats['expired']:,}", delta_color="inverse")
        c3.metric("Sắp hết hạn", f"{stats['expiring']:,}")
        c4.metric("Thiếu CCCD", f"{stats['incomplete']:,}", delta="Cần bổ sung", delta_color="off")
        
        st.write("---")
        col_chart1, col_chart2 = st.columns(2)
        with col_chart1:
            st.subheader("📍 Tỷ lệ trạng thái thẻ")
            df_pie = pd.DataFrame({
                "Trạng thái": ["Còn hạn", "Hết hạn", "Sắp hết hạn"],
                "Số lượng": [stats['total'] - stats['expired'] - stats['expiring'], stats['expired'], stats['expiring']]
            })
            fig_pie = px.pie(df_pie, values='Số lượng', names='Trạng thái', hole=0.4, 
                             color_discrete_sequence=px.colors.qualitative.Pastel)
            st.plotly_chart(fig_pie, use_container_width=True)
        with col_chart2:
            st.subheader("🔔 Thông báo hệ thống")
            if stats['expiring'] > 0:
                st.info(f"Phát hiện {stats['expiring']} trường hợp sắp hết hạn. Hãy lên kế hoạch gia hạn sớm.")
            if stats['incomplete'] > 0:
                st.warning(f"Có {stats['incomplete']} bản ghi chưa có mã CCCD. Điều này có thể ảnh hưởng đến tra cứu.")

elif choice == "🔍 Tra cứu & Xuất file":
    st.header("🔍 Tra cứu thông minh")
    with st.expander("🛠️ Bộ lọc nâng cao", expanded=True):
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1: stype = st.selectbox("Loại tìm kiếm", ["Tên & Ngày sinh", "Mã BHXH", "CCCD"])
        with col2: sfilter = st.selectbox("Trạng thái thẻ", ["Tất cả", "Còn hạn", "Sắp hết hạn (30 ngày)", "Đã hết hạn"])
        with col3: slimit = st.number_input("Giới hạn kết quả", 10, 5000, 500)

        c_m, c_s = st.columns([2, 1])
        if stype == "Tên & Ngày sinh":
            with c_m: q_m = st.text_input("Họ tên", help="Tìm kiếm mờ không dấu")
            with c_s: q_s = st.text_input("Ngày/Năm sinh")
        else:
            with c_m: q_m = st.text_input(f"Nhập {stype}")
            q_s = ""

    if st.button("🚀 Thực hiện tra cứu", use_container_width=True):
        conn = get_db_connection()
        if not conn: st.error("Lỗi kết nối CSDL"); st.stop()
        cur = conn.cursor()
        try:
            fields = "ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, dia_chi, sdt, email, han_the"
            where, params = "", {'limit': slimit, 'th': st.session_state.threshold}
            
            if stype == "Mã BHXH":
                where = "(ma_so_bhxh = %(q)s OR ma_so_bhxh ILIKE %(lq)s)"
                params.update({'q': q_m.strip(), 'lq': f"%{q_m.strip()}%"})
            elif stype == "CCCD":
                where = "(cccd = %(q)s OR cccd ILIKE %(lq)s)"
                params.update({'q': q_m.strip(), 'lq': f"%{q_m.strip()}%"})
            else:
                name_n = unidecode(q_m.strip()).lower()
                dob_c = q_s.strip().replace("/", "").replace("-", "")
                where = "(ho_ten_unsigned = %(n)s OR ho_ten_unsigned ILIKE %(ln)s OR similarity(ho_ten_unsigned, %(n)s) > %(th)s)"
                params.update({'n': name_n, 'ln': f"%{name_n}%"})
                if dob_c:
                    if len(dob_c) == 4: where += " AND TO_CHAR(ngay_sinh, 'YYYY') = %(d)s"
                    else: where += " AND (TO_CHAR(ngay_sinh, 'DDMMYYYY') = %(d)s OR TO_CHAR(ngay_sinh, 'YYYYMMDD') = %(d)s)"
                    params['d'] = dob_c

            if sfilter == "Đã hết hạn": where += " AND han_the < CURRENT_DATE"
            elif sfilter == "Sắp hết hạn (30 ngày)": where += " AND han_the >= CURRENT_DATE AND han_the <= CURRENT_DATE + INTERVAL '30 days'"
            elif sfilter == "Còn hạn": where += " AND han_the >= CURRENT_DATE"

            cur.execute(f"SELECT {fields} FROM participants WHERE {where} ORDER BY ho_ten ASC LIMIT %(limit)s", params)
            rows = cur.fetchall()
            log_activity("SEARCH", {"type": stype, "q": q_m, "count": len(rows)})
            
            if rows:
                df = pd.DataFrame(rows, columns=["Mã BHXH", "Thẻ BHYT", "Họ Tên", "Ngày Sinh", "CCCD", "Địa chỉ", "SĐT", "Email", "Hạn Thẻ"])
                for c in ["Ngày Sinh", "Hạn Thẻ"]: df[c] = pd.to_datetime(df[c], errors='coerce').dt.strftime('%d/%m/%Y')
                df['CCCD'] = df['CCCD'].apply(lambda x: f"{str(x)[:3]}****{str(x)[-3:]}" if pd.notnull(x) and len(str(x)) >= 6 else x)
                df = df.fillna("")
                
                st.success(f"Tìm thấy {len(df)} kết quả.")
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df.to_excel(writer, index=False, sheet_name='Data')
                st.download_button("📥 Tải về Excel (.xlsx)", output.getvalue(), f"BHYT_{datetime.now().strftime('%Y%m%d')}.xlsx")
                st.dataframe(df, use_container_width=True, hide_index=True)
            else: st.warning("Không tìm thấy dữ liệu.")
        finally: conn.close()

elif choice == "📥 Nhập dữ liệu":
    st.header("📥 Nhập liệu hàng loạt")
    st.info("Hệ thống tự động đồng bộ dựa trên Mã số BHXH. Dữ liệu mới sẽ cập nhật bản ghi cũ.")
    f = st.file_uploader("Chọn file Excel (.xlsx, .xlsb)", type=["xlsx", "xlsb"])
    if f:
        df_new = pd.read_excel(f, engine='pyxlsb' if f.name.endswith('.xlsb') else None, dtype=str)
        st.write(f"📦 Phát hiện: **{len(df_new):,}** hàng.")
        if st.button("🚀 Bắt đầu cập nhật"):
            p, t = st.progress(0), st.empty()
            total = len(df_new)
            for count in import_db_logic(df_new):
                p.progress(count / total)
                t.text(f"Đang xử lý: {count:,} / {total:,} hàng...")
            log_activity("IMPORT", {"file": f.name, "rows": total})
            st.success("Cập nhật dữ liệu thành công!"); st.balloons()

elif choice == "📜 Nhật ký hệ thống":
    st.header("📜 Nhật ký hoạt động")
    search_q = st.text_input("🔍 Tìm nhanh theo Email hoặc Hành động")
    conn = get_db_connection()
    if conn:
        try:
            query = "SELECT created_at, email, action, details FROM audit_logs ORDER BY id DESC LIMIT 1000"
            df_logs = pd.read_sql(query, conn)
            if not df_logs.empty:
                df_logs['created_at'] = pd.to_datetime(df_logs['created_at']).dt.tz_convert('Asia/Ho_Chi_Minh').dt.strftime('%H:%M:%S %d/%m/%Y')
                df_logs['details'] = df_logs['details'].apply(lambda x: json.dumps(x, ensure_ascii=False))
                df_logs.columns = ["Thời gian", "Người thực hiện", "Hành động", "Chi tiết"]
                if search_q:
                    df_logs = df_logs[df_logs.astype(str).apply(lambda x: search_q.lower() in x.str.lower()).any(axis=1)]
                st.dataframe(df_logs, use_container_width=True, hide_index=True)
            else: st.info("Hệ thống chưa có nhật ký.")
        finally: conn.close()

elif choice == "👥 Quản lý nhân sự":
    st.header("👥 Quản lý tài khoản truy cập")
    col_sel, col_act = st.columns([1, 1])
    with col_sel:
        target_email = st.text_input("Email nhân viên")
        action = st.selectbox("Hành động", ["Đặt lại mật khẩu", "Xóa tài khoản"])
    with col_act:
        if action == "Đặt lại mật khẩu":
            new_pwd = st.text_input("Mật khẩu mới", type="password")
            if st.button("🚀 Cập nhật"):
                if len(new_pwd) < 6: st.warning("Mật khẩu tối thiểu 6 ký tự.")
                else:
                    s, m = admin_manage_user(target_email, "RESET_PWD", new_pwd)
                    if s: st.success(m); log_activity("ADMIN_RESET_PWD", {"target": target_email})
                    else: st.error(m)
        elif action == "Xóa tài khoản":
            st.warning("Hành động này không thể hoàn tác!")
            if st.button("🔴 XÁC NHẬN XÓA"):
                s, m = admin_manage_user(target_email, "DELETE")
                if s: st.success(m); log_activity("ADMIN_DELETE_USER", {"target": target_email})
                else: st.error(m)

elif choice == "🔧 Cấu hình":
    st.header("🔧 Cấu hình hệ thống")
    new_threshold = st.slider("Độ nhạy tìm kiếm tên", 0.5, 0.95, st.session_state.threshold, 0.05)
    if st.button("Lưu cấu hình"):
        st.session_state.threshold = new_threshold
        st.success("Đã cập nhật!"); log_activity("UPDATE_CONFIG", {"threshold": new_threshold})

elif choice == "⚙️ Tài khoản":
    st.header("⚙️ Tài khoản cá nhân")
    with st.form("pwd_form"):
        npwd = st.text_input("Mật khẩu mới", type="password")
        cpwd = st.text_input("Xác nhận", type="password")
        if st.form_submit_button("Đổi mật khẩu"):
            if npwd == cpwd and len(npwd) >= 6:
                supabase.auth.update_user({"password": npwd})
                st.success("Đã đổi mật khẩu!")
            else: st.error("Lỗi mật khẩu.")

elif choice == "🗑️ Dọn dẹp":
    st.header("🗑️ Quản lý kho dữ liệu")
    if st.checkbox("Xác nhận xóa toàn bộ dữ liệu người tham gia"):
        if st.button("🔴 THỰC HIỆN XÓA"):
            conn = get_db_connection()
            with conn.cursor() as cur: cur.execute("TRUNCATE TABLE participants RESTART IDENTITY")
            conn.commit(); conn.close()
            st.success("Đã dọn sạch kho dữ liệu!")
