import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from unidecode import unidecode
import time
from datetime import datetime, date, timedelta
from supabase import create_client, Client
import io
import json
import plotly.express as px
import pdfplumber
import re

# --- 1. CẤU HÌNH & KHỞI TẠO ---
st.set_page_config(
    page_title="V-BHYT Central - Giải pháp Quản trị & Nghiệp vụ",
    page_icon="🛡️",
    layout="wide"
)

# Khởi tạo Supabase cho Auth và Admin API
@st.cache_resource
def init_supabase():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"] # Phải là Service Role Key để dùng Admin API
    return create_client(url, key)

supabase: Client = init_supabase()

# Kết nối Database trực tiếp (Postgres)
def get_db_connection():
    try:
        conn = psycopg2.connect(st.secrets["SUPABASE_DB_URL"], connect_timeout=10)
        return conn
    except Exception:
        return None

# --- 2. LOGIC XÁC THỰC & QUẢN TRỊ ---

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
    """Sử dụng Service Role để quản lý tài khoản nhân viên"""
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
        return False, f"Lỗi: {str(e)}"

# --- 3. HÀM GHI NHẬT KÝ & THỐNG KÊ ---

def log_activity(action, details_dict):
    """Ghi nhận mọi thao tác vào bảng audit_logs"""
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
    """Lấy dữ liệu cho Dashboard"""
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
            cur.execute("SELECT COUNT(*) FROM participants WHERE (cccd IS NULL OR cccd = '') OR (sdt IS NULL OR sdt = '')")
            stats['incomplete'] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM audit_logs WHERE action = 'EXPORT'")
            stats['total_exports'] = cur.fetchone()[0]
        return stats
    finally: conn.close()

# --- 4. LOGIC XỬ LÝ DỮ LIỆU PDF ---

def parse_bhxh_pdf(pdf_file):
    """Trích xuất dữ liệu từ file PDF quá trình đóng BHXH (Mẫu 07/SBH) và lọc trùng"""
    history_data = []
    seen_records = set() 
    msbhxh = ""
    summary_text = ""
    
    with pdfplumber.open(pdf_file) as pdf:
        full_text = ""
        for page in pdf.pages:
            full_text += page.extract_text() + "\n"
        
        match_ms = re.search(r"Mã số BHXH\s*:\s*(\d+)", full_text)
        if match_ms:
            msbhxh = match_ms.group(1).strip()
            
        match_bhxh = re.search(r"Thời gian đóng BHXH vào quỹ hưu trí.*?là\s*(.*?)(?:\n|$)", full_text)
        match_bhtn = re.search(r"Thời gian đóng BHTN vào quỹ BHTN.*?là\s*(.*?)(?:\n|$)", full_text)
        
        sums = []
        if match_bhxh: sums.append(f"BHXH: {match_bhxh.group(1).strip()}")
        if match_bhtn: sums.append(f"BHTN: {match_bhtn.group(1).strip()}")
        summary_text = " | ".join(sums)
            
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    if not row or len(row) < 5 or not row[0]: continue
                    tu_thang = str(row[0]).strip()
                    if not re.match(r"\d{2}/\d{4}", tu_thang): continue
                    
                    try:
                        den_thang = str(row[1]).strip() if row[1] else ""
                        don_vi = str(row[2]).strip().replace('\n', ' ')
                        muc_raw = str(row[3]).replace('.', '').replace(',', '').strip()
                        muc_dong = float(muc_raw) if muc_raw.isdigit() else 0
                        ty_le = str(row[6]).strip() if len(row) > 6 and row[6] else ""
                        loai_bh = "BHTN" if "BẢO HIỂM THẤT NGHIỆP" in don_vi.upper() else "BHXH"
                        
                        record_id = (tu_thang, den_thang, don_vi, muc_dong, loai_bh)
                        if record_id not in seen_records:
                            seen_records.add(record_id)
                            history_data.append((msbhxh, tu_thang, den_thang, don_vi, muc_dong, ty_le, loai_bh))
                    except: continue
                    
    return msbhxh, history_data, summary_text

def save_bhxh_history(msbhxh, data, summary):
    if not msbhxh or not data: return False
    conn = get_db_connection()
    if not conn: return False
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM bhxh_history WHERE ma_so_bhxh = %s", (msbhxh,))
        sql_hist = "INSERT INTO bhxh_history (ma_so_bhxh, tu_thang, den_thang, don_vi_cong_viec, muc_dong, ty_le_dong, loai_bh) VALUES %s"
        execute_values(cur, sql_hist, data)
        if summary:
            cur.execute("UPDATE participants SET tong_thoi_gian_bhxh = %s WHERE ma_so_bhxh = %s", (summary, msbhxh))
        conn.commit()
        return True
    except Exception:
        return False
    finally: conn.close()

def import_db_logic(df):
    """Nạp dữ liệu từ Excel (.xlsb, .xlsx)"""
    df.columns = [str(c).strip().lower() for c in df.columns]
    mapping = {'ma so bhxh': 'ma_so_bhxh', 'mã số bhxh': 'ma_so_bhxh', 'ho ten': 'ho_ten', 'ngay sinh': 'ngay_sinh',
               'cccd': 'cccd', 'sdt': 'sdt', 'diachilh': 'dia_chi', 'hantheden': 'han_the', 'email': 'email'}
    df = df.rename(columns=mapping)
    target = ['ma_so_bhxh', 'ma_the_bhyt', 'ho_ten', 'ngay_sinh', 'cccd', 'dia_chi', 'sdt', 'email', 'han_the']
    for col in target:
        if col not in df.columns: df[col] = None
    for col in ['ngay_sinh', 'han_the']:
        df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).apply(lambda x: x.date() if pd.notnull(x) else None)
    for col in ['ma_so_bhxh', 'ho_ten', 'cccd', 'sdt', 'email']:
        df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        df[col] = df[col].where(~df[col].isin(['nan', 'None', 'NAT', 'NaT', '']), None)
    data = list(df[target].itertuples(index=False, name=None))
    sql = """
        INSERT INTO participants (ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, dia_chi, sdt, email, han_the)
        VALUES %s ON CONFLICT (ma_so_bhxh) DO UPDATE SET
            ma_the_bhyt = EXCLUDED.ma_the_bhyt, ho_ten = EXCLUDED.ho_ten, ngay_sinh = EXCLUDED.ngay_sinh,
            cccd = EXCLUDED.cccd, dia_chi = EXCLUDED.dia_chi, sdt = EXCLUDED.sdt, email = EXCLUDED.email, 
            han_the = EXCLUDED.han_the, updated_at = NOW();
    """
    for i in range(0, len(data), 5000):
        batch = data[i:i + 5000]
        conn = get_db_connection()
        if not conn: break
        with conn.cursor() as cur:
            execute_values(cur, sql, batch)
            conn.commit()
        conn.close()
        yield min(i + 5000, len(data))

# --- 5. GIAO DIỆN CHÍNH ---

if 'user' not in st.session_state: st.session_state.user = None
if 'threshold' not in st.session_state: st.session_state.threshold = 0.85

if st.session_state.user is None:
    st.markdown("<h1 style='text-align: center; color: #1E88E5;'>🏥 V-BHYT Central Pro</h1>", unsafe_allow_html=True)
    with st.container():
        _, col, _ = st.columns([1, 1.5, 1])
        with col:
            with st.form("login_form"):
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

menu = ["📊 Dashboard", "🔍 Tra cứu & Quá trình", "🧮 Tiện ích tính toán", "⚙️ Tài khoản"]
if is_admin():
    menu += ["📥 Nhập dữ liệu", "📜 Nhật ký hệ thống", "👥 Quản lý nhân sự", "🔧 Cấu hình", "🗑️ Dọn dẹp"]

choice = st.sidebar.selectbox("Menu Quản lý", menu)
if st.sidebar.button("🚪 Đăng xuất", use_container_width=True):
    log_activity("LOGOUT", {"status": "success"}); logout_user()

# --- NỘI DUNG TỪNG TAB ---

if choice == "📊 Dashboard":
    st.header("📊 Phân tích & Thống kê hệ thống")
    stats = get_advanced_stats()
    if stats:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Tổng bản ghi", f"{stats['total']:,}")
        c2.metric("Đã hết hạn BHYT", f"{stats['expired']:,}", delta_color="inverse")
        c3.metric("Sắp hết hạn", f"{stats['expiring']:,}")
        c4.metric("Lượt tải dữ liệu", f"{stats.get('total_exports', 0):,}", delta="An ninh", delta_color="off")
        
        st.write("---")
        col_chart1, col_chart2 = st.columns(2)
        with col_chart1:
            st.subheader("📍 Tỷ lệ trạng thái thẻ BHYT")
            df_pie = pd.DataFrame({"Trạng thái": ["Còn hạn", "Hết hạn", "Sắp hết hạn"],
                                   "Số lượng": [stats['total']-stats['expired']-stats['expiring'], stats['expired'], stats['expiring']]})
            st.plotly_chart(px.pie(df_pie, values='Số lượng', names='Trạng thái', hole=0.4, color_discrete_sequence=px.colors.qualitative.Pastel), use_container_width=True)
        with col_chart2:
            st.subheader("🛠️ Chất lượng dữ liệu (PII)")
            df_q = pd.DataFrame({"Loại": ["Đầy đủ", "Thiếu CCCD/SĐT"], "Số lượng": [stats['total']-stats['incomplete'], stats['incomplete']]})
            st.plotly_chart(px.bar(df_q, x="Loại", y="Số lượng", color="Loại", color_discrete_sequence=["#2ecc71", "#e74c3c"]), use_container_width=True)

elif choice == "🔍 Tra cứu & Quá trình":
    st.header("🔍 Tra cứu người tham gia & Lịch sử BHXH")
    with st.expander("🛠️ Bộ lọc nâng cao", expanded=True):
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1: stype = st.selectbox("Loại tìm kiếm", ["Tên & Ngày sinh", "Mã BHXH", "CCCD"])
        with col2: sfilter = st.selectbox("Trạng thái thẻ BHYT", ["Tất cả", "Còn hạn", "Sắp hết hạn (30 ngày)", "Đã hết hạn"])
        with col3: slimit = st.number_input("Giới hạn", 10, 5000, 500)
        c_m, c_s = st.columns([2, 1])
        if stype == "Tên & Ngày sinh":
            with c_m: q_m = st.text_input("Họ tên")
            with c_s: q_s = st.text_input("Ngày/Năm sinh")
        else:
            with c_m: q_m = st.text_input(f"Nhập {stype}")
            q_s = ""

    if st.button("🚀 Thực hiện tra cứu", use_container_width=True):
        conn = get_db_connection()
        if not conn: st.error("Lỗi kết nối CSDL"); st.stop()
        cur = conn.cursor()
        try:
            fields = "ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, dia_chi, sdt, email, han_the, tong_thoi_gian_bhxh"
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
                st.success(f"Tìm thấy {len(rows)} kết quả.")
                for r in rows:
                    with st.container(border=True):
                        # --- CHỈNH SỬA GIAO DIỆN THEO DÒNG (DÒNG 221-255) ---
                        c1, c2, c3, c4 = st.columns([2.5, 2.5, 3, 2])
                        
                        # Cột 1: Thông tin danh tính
                        with c1:
                            st.markdown(f"**{r[2]}**")
                            st.caption(f"🆔 {r[0]} | 🪪 {str(r[4])[:3]}***{str(r[4])[-3:]}")
                            st.caption(f"🎂 {pd.to_datetime(r[3]).strftime('%d/%m/%Y')}")
                        
                        # Cột 2: Địa chỉ & Liên hệ
                        with c2:
                            st.caption(f"📍 {r[5]}")
                            r_sdt = r[6] if r[6] and str(r[6]) not in ['None', 'nan'] else 'Chưa có SĐT'
                            st.markdown(f"📞 `{r_sdt}`")
                        
                        # Cột 3: Tổng quá trình (Hiện ở cuối dòng)
                        with c3:
                            if r[9]:
                                st.success(f"📈 {r[9]}")
                            else:
                                st.info("💡 Chưa nạp PDF quá trình")
                            expiry_str = pd.to_datetime(r[8]).strftime('%d/%m/%Y') if r[8] else 'N/A'
                            st.caption(f"🏥 Hạn thẻ BHYT: {expiry_str}")

                        # Cột 4: Nút tra cứu quá trình (Cuối dòng)
                        with c4:
                            with st.expander("📜 Tra cứu BHXH", expanded=False):
                                cur.execute("""
                                    SELECT tu_thang, den_thang, don_vi_cong_viec, muc_dong, ty_le_dong, loai_bh 
                                    FROM bhxh_history 
                                    WHERE ma_so_bhxh = %s 
                                    ORDER BY to_date(tu_thang, 'MM/YYYY') ASC
                                """, (r[0].strip(),))
                                h_rows = cur.fetchall()
                                if h_rows:
                                    df_h = pd.DataFrame(h_rows, columns=["Từ tháng", "Đến tháng", "Đơn vị/Công việc", "Mức đóng", "Tỷ lệ", "Loại"])
                                    def style_row(row):
                                        color = 'color: #1a73e8; font-weight: bold;' if row['Loại'] == 'BHXH' else 'color: #5f6368;'
                                        return [color] * len(row)
                                    st.dataframe(df_h.style.format({"Mức đóng": "{:,.0f}đ"}).apply(style_row, axis=1), 
                                                 use_container_width=True, hide_index=True)
                                    log_activity("VIEW_HISTORY", {"msbhxh": r[0]})
                                else:
                                    st.warning("Vui lòng nạp PDF Mẫu 07/SBH.")
            else: st.warning("Không tìm thấy dữ liệu.")
        finally: conn.close()

elif choice == "🧮 Tiện ích tính toán":
    st.header("🧮 Công cụ hỗ trợ thu BHYT & BHXH")
    t1, t2, t3 = st.tabs(["🧮 Máy tính nghiệp vụ", "🏥 Tính BHYT Hộ gia đình", "👵 Tính BHXH Tự nguyện"])
    with t1:
        calc_exp = st.text_input("Nhập phép tính (VD: 105300 * 5)", placeholder="Nhấn Enter...")
        if calc_exp:
            try:
                if all(c in set("0123456789+-*/.() ") for c in calc_exp): st.markdown(f"### Kết quả: `{eval(calc_exp):,.2f}`")
            except: st.error("Lỗi tính toán.")
    with t2:
        num = st.number_input("Số người tham gia", 1, 10, 1)
        m1 = 2340000 * 0.045
        prices = [round(m1 if i==1 else m1*0.7 if i==2 else m1*0.6 if i==3 else m1*0.5 if i==4 else m1*0.4) for i in range(1, num+1)]
        df_b = pd.DataFrame({"Người thứ": range(1, num+1), "Mức giảm": (["100%", "70%", "60%", "50%"] + ["40%"]*6)[:num], "Số tiền/12 tháng": [p*12 for p in prices]})
        st.table(df_b.style.format({"Số tiền/12 tháng": "{:,.0f}đ"}))
        st.markdown(f"### 💰 Tổng thu: `{sum(prices)*12:,.0f} VNĐ`")
    with t3:
        income = st.number_input("Mức thu nhập chọn đóng (đ)", 1500000, 36000000, 1500000, 50000)
        support = st.selectbox("Đối tượng hỗ trợ", ["Hộ nghèo (50%)", "Hộ cận nghèo (40%)", "Dân tộc thiểu số (30%)", "Khác (20%)"])
        method = st.selectbox("Phương thức đóng (tháng)", [1, 3, 6, 12])
        s_pct = 0.5 if "hộ nghèo" in support.lower() else 0.4 if "cận nghèo" in support.lower() else 0.3 if "dân tộc" in support.lower() else 0.2
        monthly = (income * 0.22) - (1500000 * 0.22 * s_pct)
        st.markdown(f"### 💰 Thực thu ({method} tháng): `{monthly*method:,.0f} VNĐ`")

elif choice == "📥 Nhập dữ liệu":
    st.header("📥 Nhập liệu hệ thống")
    tx, tp = st.tabs(["📊 File Excel (BHYT)", "📜 File PDF (Quá trình BHXH)"])
    with tx:
        f = st.file_uploader("Chọn file Excel", type=["xlsx", "xlsb"])
        if f:
            df = pd.read_excel(f, dtype=str)
            if st.button("🚀 Bắt đầu nạp BHYT"):
                pb, txt = st.progress(0), st.empty()
                for count in import_db_logic(df):
                    pb.progress(count/len(df)); txt.text(f"Đã nạp {count:,} hàng...")
                st.success("Cập nhật thành công!"); log_activity("IMPORT_EXCEL", {"rows": len(df)})
    with tp:
        st.info("💡 Hệ thống sẽ tự động bóc tách Mã số BHXH và toàn bộ lịch sử đóng từ Mẫu 07/SBH.")
        pdf_f = st.file_uploader("Chọn file PDF (Mẫu 07/SBH)", type=["pdf"])
        if pdf_f and st.button("🔍 Phân tích và Lưu quá trình"):
            with st.spinner("Đang đọc PDF..."):
                ms, data, summary = parse_bhxh_pdf(pdf_f)
                if ms and data:
                    if save_bhxh_history(ms, data, summary):
                        st.success(f"✅ Đã cập nhật thành công giai đoạn đóng và tổng quát cho Mã số: {ms}")
                        log_activity("IMPORT_PDF", {"ms": ms, "rows": len(data)})
                    else: st.error("Lỗi khi lưu vào cơ sở dữ liệu.")
                else: st.error("Không tìm thấy dữ liệu hợp lệ trong file PDF này.")

elif choice == "📜 Nhật ký hệ thống":
    st.header("📜 Nhật ký hoạt động")
    col_f1, col_f2 = st.columns([2, 1])
    with col_f1: search_q = st.text_input("🔍 Tìm theo Email, Hành động")
    with col_f2: dr = st.date_input("Khoảng ngày", value=[date.today()-timedelta(days=7), date.today()])
    conn = get_db_connection()
    if conn:
        df_l = pd.read_sql("SELECT created_at, email, action, details FROM audit_logs ORDER BY id DESC LIMIT 2000", conn)
        if not df_l.empty:
            df_l['created_at'] = pd.to_datetime(df_l['created_at']).dt.tz_convert('Asia/Ho_Chi_Minh')
            if len(dr) == 2:
                df_l = df_l[(df_l['created_at'].dt.date >= dr[0]) & (df_l['created_at'].dt.date <= dr[1])]
            if search_q:
                mask = df_l.astype(str).apply(lambda row: row.str.contains(search_q, case=False).any(), axis=1)
                df_l = df_l[mask]
            df_l['created_at'] = df_l['created_at'].dt.strftime('%H:%M:%S %d/%m/%Y')
            df_l['details'] = df_l['details'].apply(lambda x: json.dumps(x, ensure_ascii=False) if x else "")
            st.dataframe(df_l, use_container_width=True, hide_index=True)
        conn.close()

elif choice == "👥 Quản lý nhân sự":
    st.header("👥 Quản lý nhân sự")
    target = st.text_input("Email nhân viên")
    act = st.selectbox("Hành động", ["Đặt lại mật khẩu", "Xóa tài khoản"])
    if st.button("🚀 Thực thi"):
        if act == "Đặt lại mật khẩu":
            pwd = st.text_input("Mật khẩu mới", type="password", key="np")
            if pwd: s, m = admin_manage_user(target, "RESET_PWD", pwd)
            st.info(m if 'm' in locals() else "Vui lòng nhập mật khẩu.")
        else:
            s, m = admin_manage_user(target, "DELETE"); st.info(m)

elif choice == "🔧 Cấu hình":
    st.header("🔧 Cấu hình")
    st.session_state.threshold = st.slider("Độ nhạy tìm kiếm tên", 0.5, 0.95, st.session_state.threshold)
    if st.button("Lưu"): st.success("Đã lưu cấu hình!")

elif choice == "🗑️ Dọn dẹp":
    st.header("🗑️ Quản lý & Dọn dẹp kho dữ liệu")
    with st.expander("📊 Dọn dẹp dữ liệu BHYT (Excel)", expanded=True):
        if st.checkbox("Tôi xác nhận muốn xóa sạch dữ liệu BHYT", key="chk_del_bhyt"):
            if st.button("🔴 THỰC HIỆN XÓA DỮ LIỆU BHYT", key="btn_del_bhyt"):
                conn = get_db_connection()
                if conn:
                    with conn.cursor() as cur: cur.execute("TRUNCATE TABLE participants RESTART IDENTITY")
                    conn.commit(); conn.close()
                    st.success("Đã dọn sạch!"); time.sleep(1); st.rerun()

    with st.expander("📜 Dọn dẹp dữ liệu BHXH (PDF)", expanded=True):
        if st.checkbox("Tôi xác nhận muốn xóa sạch lịch sử BHXH", key="chk_del_bhxh"):
            if st.button("🔴 THỰC HIỆN XÓA LỊCH SỬ BHXH", key="btn_del_bhxh"):
                conn = get_db_connection()
                if conn:
                    with conn.cursor() as cur:
                        cur.execute("TRUNCATE TABLE bhxh_history RESTART IDENTITY")
                        cur.execute("UPDATE participants SET tong_thoi_gian_bhxh = NULL")
                    conn.commit(); conn.close()
                    st.success("Đã dọn sạch!"); time.sleep(1); st.rerun()

    with st.expander("📜 Dọn dẹp Nhật ký hệ thống", expanded=True):
        if st.checkbox("Tôi xác nhận muốn xóa sạch nhật ký", key="chk_del_logs"):
            if st.button("🔴 THỰC HIỆN XÓA NHẬT KÝ", key="btn_del_logs"):
                conn = get_db_connection()
                if conn:
                    with conn.cursor() as cur: cur.execute("TRUNCATE TABLE audit_logs RESTART IDENTITY")
                    conn.commit(); conn.close()
                    st.success("Đã dọn sạch!"); time.sleep(1); st.rerun()

elif choice == "⚙️ Tài khoản":
    st.header("⚙️ Tài khoản")
    with st.form("p_form"):
        p1, p2 = st.text_input("Mật khẩu mới", type="password"), st.text_input("Xác nhận", type="password")
        if st.form_submit_button("Đổi mật khẩu") and p1 == p2 and len(p1) >= 6:
            supabase.auth.update_user({"password": p1}); st.success("Thành công!")
