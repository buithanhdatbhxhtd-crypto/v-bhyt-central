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
        # Sử dụng URL kết nối trực tiếp đến Postgres
        conn = psycopg2.connect(st.secrets["SUPABASE_DB_URL"], connect_timeout=10)
        return conn
    except Exception as e:
        return None

# --- 3. LOGIC XÁC THỰC & QUẢN TRỊ NGƯỜI DÙNG ---

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
    """Sử dụng Service Role Key để quản lý tài khoản nhân viên"""
    try:
        # Khởi tạo client admin riêng biệt để tránh xung đột session
        admin_client = create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])
        response = admin_client.auth.admin.list_users()
        
        # Kiểm tra cấu trúc trả về của thư viện
        all_users = response.users if hasattr(response, 'users') else response
        user = next((u for u in all_users if u.email == target_email), None)
        
        if not user:
            return False, "Không tìm thấy người dùng có email này."

        if action == "RESET_PWD":
            admin_client.auth.admin.update_user_by_id(user.id, {"password": new_password})
            return True, f"Đã đặt lại mật khẩu thành công cho {target_email}."
        elif action == "DELETE":
            admin_client.auth.admin.delete_user(user.id)
            return True, f"Đã xóa tài khoản {target_email} khỏi hệ thống."
        
        return False, "Hành động quản trị không hợp lệ."
    except Exception as e:
        return False, f"Lỗi quản trị: {str(e)}"

# --- 4. HÀM NHẬT KÝ & THỐNG KÊ CHI SỐ ---

def log_activity(action, details_dict):
    """Lưu vết mọi hành động vào bảng audit_logs"""
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
    """Truy vấn các chỉ số đo lường cho Dashboard"""
    conn = get_db_connection()
    if not conn: return None
    stats = {}
    try:
        with conn.cursor() as cur:
            # Thống kê số lượng bản ghi tham gia
            cur.execute("SELECT COUNT(*) FROM participants")
            stats['total'] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM participants WHERE han_the < CURRENT_DATE")
            stats['expired'] = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM participants WHERE han_the >= CURRENT_DATE AND han_the <= CURRENT_DATE + INTERVAL '30 days'")
            stats['expiring'] = cur.fetchone()[0]
            # Thống kê chất lượng dữ liệu (PII Integrity)
            cur.execute("SELECT COUNT(*) FROM participants WHERE (cccd IS NULL OR cccd = '') OR (sdt IS NULL OR sdt = '')")
            stats['incomplete'] = cur.fetchone()[0]
            # Thống kê an ninh (Số lượt tải dữ liệu)
            cur.execute("SELECT COUNT(*) FROM audit_logs WHERE action = 'EXPORT'")
            stats['total_exports'] = cur.fetchone()[0]
        return stats
    finally: conn.close()

def import_db_logic(df):
    """Nạp dữ liệu hàng loạt sử dụng chiến lược Upsert (ON CONFLICT)"""
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

    # Tiền xử lý dữ liệu trước khi nạp
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
    # Xử lý theo lô 5000 bản ghi để đảm bảo tốc độ và sự ổn định
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

# --- 5. GIAO DIỆN CHÍNH (STREAMLIT) ---

if 'user' not in st.session_state:
    st.session_state.user = None
if 'threshold' not in st.session_state:
    st.session_state.threshold = 0.85

# Màn hình đăng nhập
if st.session_state.user is None:
    st.markdown("<h1 style='text-align: center; color: #1E88E5;'>🏥 V-BHYT Central Pro</h1>", unsafe_allow_html=True)
    with st.container():
        _, col, _ = st.columns([1, 1.5, 1])
        with col:
            with st.form("login_form"):
                st.info("Hệ thống Quản trị & Tra cứu Bảo hiểm Y tế Bảo mật")
                email = st.text_input("Email công vụ")
                pwd = st.text_input("Mật khẩu", type="password")
                if st.form_submit_button("Đăng nhập hệ thống", use_container_width=True):
                    auth = login_user(email, pwd)
                    if auth:
                        st.session_state.user = auth.user
                        log_activity("LOGIN", {"status": "success"})
                        st.rerun()
    st.stop()

# --- SIDEBAR QUẢN LÝ ---
st.sidebar.title("🛡️ V-BHYT PRO")
st.sidebar.markdown(f"👤 **{st.session_state.user.email}**")
role_label = "🔴 Quản trị viên" if is_admin() else "🔵 Nhân viên Tra cứu"
st.sidebar.caption(role_label)

menu = ["📊 Dashboard", "🔍 Tra cứu & Xuất file", "🧮 Tiện ích tính toán", "⚙️ Tài khoản"]
if is_admin():
    menu += ["📥 Nhập dữ liệu", "📜 Nhật ký hệ thống", "👥 Quản lý nhân sự", "🔧 Cấu hình", "🗑️ Dọn dẹp"]

choice = st.sidebar.selectbox("Menu Quản lý", menu)
if st.sidebar.button("🚪 Đăng xuất", use_container_width=True):
    log_activity("LOGOUT", {"status": "success"})
    logout_user()

# --- NỘI DUNG TỪNG TAB ---

if choice == "📊 Dashboard":
    st.header("📊 Phân tích & Thống kê hệ thống")
    stats = get_advanced_stats()
    if stats:
        # Hàng 1: Chỉ số hiệu năng chính
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Tổng bản ghi", f"{stats['total']:,}")
        c2.metric("Đã hết hạn", f"{stats['expired']:,}", delta_color="inverse")
        c3.metric("Sắp hết hạn", f"{stats['expiring']:,}")
        c4.metric("Lượt tải dữ liệu", f"{stats.get('total_exports', 0):,}", delta="Security Log", delta_color="off")
        
        st.write("---")
        # Hàng 2: Biểu đồ phân tích trực quan
        col_chart1, col_chart2 = st.columns(2)
        with col_chart1:
            st.subheader("📍 Tỷ lệ trạng thái thẻ")
            df_pie = pd.DataFrame({
                "Trạng thái": ["Còn hạn", "Hết hạn", "Sắp hết hạn"],
                "Số lượng": [stats['total'] - stats['expired'] - stats['expiring'], stats['expired'], stats['expiring']]
            })
            fig_pie = px.pie(df_pie, values='Số lượng', names='Trạng thái', hole=0.4, 
                             color_discrete_sequence=px.colors.qualitative.Pastel)
            fig_pie.update_layout(margin=dict(t=0, b=0, l=0, r=0))
            st.plotly_chart(fig_pie, use_container_width=True)
            
        with col_chart2:
            st.subheader("🛠️ Chất lượng dữ liệu (PII)")
            df_quality = pd.DataFrame({
                "Loại": ["Dữ liệu đầy đủ", "Thiếu CCCD/SĐT"],
                "Số lượng": [stats['total'] - stats['incomplete'], stats['incomplete']]
            })
            fig_bar = px.bar(df_quality, x="Loại", y="Số lượng", color="Loại", 
                             color_discrete_sequence=["#2ecc71", "#e74c3c"])
            fig_bar.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0))
            st.plotly_chart(fig_bar, use_container_width=True)

elif choice == "🔍 Tra cứu & Xuất file":
    st.header("🔍 Tra cứu thông minh & Bảo mật")
    with st.expander("🛠️ Bộ lọc nâng cao", expanded=True):
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1: stype = st.selectbox("Loại tìm kiếm", ["Tên & Ngày sinh", "Mã BHXH", "CCCD"])
        with col2: sfilter = st.selectbox("Trạng thái thẻ", ["Tất cả", "Còn hạn", "Sắp hết hạn (30 ngày)", "Đã hết hạn"])
        with col3: slimit = st.number_input("Giới hạn kết quả", 10, 5000, 500)

        c_m, c_s = st.columns([2, 1])
        if stype == "Tên & Ngày sinh":
            with c_m: q_m = st.text_input("Họ tên", help="Tìm kiếm mờ thông minh")
            with c_s: q_s = st.text_input("Ngày/Năm sinh")
        else:
            with c_m: q_m = st.text_input(f"Nhập {stype}")
            q_s = ""

    if st.button("🚀 Thực hiện tra cứu", use_container_width=True):
        conn = get_db_connection()
        if not conn: st.error("Lỗi kết nối Cơ sở dữ liệu"); st.stop()
        cur = conn.cursor()
        try:
            fields = "ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, dia_chi, sdt, email, han_the"
            where, params = "", {'limit': slimit, 'th': st.session_state.threshold}
            
            # Logic tìm kiếm linh hoạt
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

            # Lọc trạng thái
            if sfilter == "Đã hết hạn": where += " AND han_the < CURRENT_DATE"
            elif sfilter == "Sắp hết hạn (30 ngày)": where += " AND han_the >= CURRENT_DATE AND han_the <= CURRENT_DATE + INTERVAL '30 days'"
            elif sfilter == "Còn hạn": where += " AND han_the >= CURRENT_DATE"

            cur.execute(f"SELECT {fields} FROM participants WHERE {where} ORDER BY ho_ten ASC LIMIT %(limit)s", params)
            rows = cur.fetchall()
            log_activity("SEARCH", {"type": stype, "q": q_m, "count": len(rows)})
            
            if rows:
                df = pd.DataFrame(rows, columns=["Mã BHXH", "Thẻ BHYT", "Họ Tên", "Ngày Sinh", "CCCD", "Địa chỉ", "SĐT", "Email", "Hạn Thẻ"])
                for c in ["Ngày Sinh", "Hạn Thẻ"]: df[c] = pd.to_datetime(df[c], errors='coerce').dt.strftime('%d/%m/%Y')
                
                # CHUẨN BỊ FILE XUẤT VÀ GHI LOG EXPORT
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df.to_excel(writer, index=False, sheet_name='DuLieuBHYT')
                
                st.success(f"Tìm thấy {len(df)} kết quả.")
                
                # Truy vết xuất dữ liệu khi người dùng bấm tải
                st.download_button(
                    label="📥 Tải về kết quả (.xlsx)", 
                    data=output.getvalue(), 
                    file_name=f"BHYT_Export_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                    on_click=lambda: log_activity("EXPORT", {"rows": len(df), "type": "EXCEL", "query": q_m})
                )
                
                # Hiển thị bảo mật (Che CCCD)
                df_display = df.copy()
                df_display['CCCD'] = df_display['CCCD'].apply(lambda x: f"{str(x)[:3]}****{str(x)[-3:]}" if pd.notnull(x) and len(str(x)) >= 6 else x)
                df_display = df_display.fillna("")
                st.dataframe(df_display, use_container_width=True, hide_index=True)
            else: st.warning("Không tìm thấy dữ liệu phù hợp với yêu cầu.")
        finally: conn.close()

elif choice == "🧮 Tiện ích tính toán":
    st.header("🧮 Công cụ hỗ trợ thu BHYT & BHXH")
    
    t1, t2, t3 = st.tabs(["🧮 Máy tính nghiệp vụ", "🏥 Tính BHYT Hộ gia đình", "👵 Tính BHXH Tự nguyện"])
    
    with t1:
        st.subheader("Máy tính bỏ túi")
        # Sử dụng widget calculator đơn giản bằng input
        calc_exp = st.text_input("Nhập phép tính (VD: 105300 * 5 + 1500000 * 0.22)", placeholder="Nhấn Enter để tính...")
        if calc_exp:
            try:
                # Chỉ cho phép các ký tự an toàn
                allowed = set("0123456789+-*/.() ")
                if all(c in allowed for c in calc_exp):
                    res = eval(calc_exp)
                    st.markdown(f"### Kết quả: `{res:,.2f}`")
                else:
                    st.error("Biểu thức chứa ký tự không hợp lệ.")
            except Exception as e:
                st.error(f"Lỗi tính toán: {e}")
        
        st.info("💡 Bạn có thể thực hiện các phép cộng trừ nhân chia trực tiếp để tính tổng tiền thu của nhiều người.")

    with t2:
        st.subheader("Bảng tính mức đóng BHYT Hộ gia đình")
        st.caption("Áp dụng mức lương cơ sở: 2.340.000đ (Từ 01/07/2024)")
        
        num_members = st.number_input("Số người tham gia trong hộ", 1, 10, 1)
        base_salary = 2340000
        rate = 0.045 # 4.5%
        m1_price = base_salary * rate
        
        prices = []
        for i in range(1, num_members + 1):
            if i == 1: p = m1_price
            elif i == 2: p = m1_price * 0.7
            elif i == 3: p = m1_price * 0.6
            elif i == 4: p = m1_price * 0.5
            else: p = m1_price * 0.4
            prices.append(round(p))
            
        df_bhyt = pd.DataFrame({
            "Thứ tự": [f"Người thứ {i}" for i in range(1, num_members + 1)],
            "Mức giảm": ["100%", "70%", "60%", "50%"] + ["40%"] * (num_members - 4),
            "Số tiền/Tháng": [f"{p:,.0f}đ" for p in prices],
            "Số tiền/12 tháng": [f"{p*12:,.0f}đ" for p in prices]
        })
        
        st.table(df_bhyt)
        total_year = sum(prices) * 12
        st.markdown(f"### 💰 Tổng cộng thu (12 tháng): `{total_year:,.0f} VNĐ`")
        
        if st.button("Lưu nhật ký tư vấn BHYT"):
            log_activity("CALC_BHYT", {"members": num_members, "total": total_year})
            st.toast("Đã lưu hoạt động!")

    with t3:
        st.subheader("Bảng tính mức đóng BHXH Tự nguyện")
        
        col_in, col_support = st.columns(2)
        with col_in:
            chosen_income = st.number_input("Mức thu nhập lựa chọn (đ)", 1500000, 36000000, 1500000, 50000, 
                                           help="Tối thiểu là chuẩn nghèo nông thôn 1.500.000đ")
        with col_support:
            support_type = st.selectbox("Đối tượng hỗ trợ", 
                                        ["Hộ nghèo (50%)", "Hộ cận nghèo (40%)", "Người dân tộc thiểu số (30%)", "Đối tượng khác (20%)"])
        
        method = st.selectbox("Phương thức đóng", 
                              options=[1, 3, 6, 9, 12], 
                              format_func=lambda x: f"Đóng {x} tháng một lần")
        
        # Logic tính toán
        # User defined: Nghèo 50%, Cận nghèo 40%, Dân tộc 30%, Khác 20%
        support_pct = 0.20
        if "nghèo" in support_type.lower():
            support_pct = 0.50 if "hộ nghèo" in support_type.lower() else 0.40
        elif "dân tộc" in support_type.lower():
            support_pct = 0.30
            
        rate_bhxh = 0.22 # 22%
        min_base = 1500000 # Chuẩn nghèo nông thôn làm căn cứ hỗ trợ
        
        monthly_total = chosen_income * rate_bhxh
        monthly_support = min_base * rate_bhxh * support_pct
        monthly_real = monthly_total - monthly_support
        
        total_payment = monthly_real * method
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Mức đóng gốc/Tháng", f"{monthly_total:,.0f}đ")
        c2.metric("NSNN Hỗ trợ/Tháng", f"-{monthly_support:,.0f}đ")
        c3.metric("Thực đóng/Tháng", f"{monthly_real:,.0f}đ")
        
        st.markdown(f"""
        <div style="background-color:#f0f2f6; padding:20px; border-radius:10px; border-left: 5px solid #1E88E5;">
            <h2 style="margin:0; color:#1E88E5;">Tổng tiền thu ({method} tháng): {total_payment:,.0f} VNĐ</h2>
            <p style="margin:5px 0 0 0; color:#555;">Dựa trên mức thu nhập chọn lựa {chosen_income:,.0f}đ</p>
        </div>
        """, unsafe_allow_html=True)

        if st.button("Lưu nhật ký tư vấn BHXH"):
            log_activity("CALC_BHXH", {"income": chosen_income, "method": method, "total": total_payment})
            st.toast("Đã lưu hoạt động!")

elif choice == "📥 Nhập dữ liệu":
    st.header("📥 Nhập liệu hàng loạt (Quản trị)")
    st.info("Hệ thống tự động đồng bộ hóa thông tin dựa trên Mã số BHXH.")
    f = st.file_uploader("Chọn tệp Excel nguồn (.xlsx, .xlsb)", type=["xlsx", "xlsb"])
    if f:
        df_new = pd.read_excel(f, engine='pyxlsb' if f.name.endswith('.xlsb') else None, dtype=str)
        st.write(f"📊 Phát hiện: **{len(df_new):,}** hàng dữ liệu.")
        if st.button("🚀 Bắt đầu đồng bộ hóa Cơ sở dữ liệu"):
            p_bar = st.progress(0)
            p_text = st.empty()
            total = len(df_new)
            for count in import_db_logic(df_new):
                p_bar.progress(count / total)
                p_text.text(f"Đang xử lý: {count:,} / {total:,} hàng...")
            log_activity("IMPORT", {"file": f.name, "rows": total})
            st.success(f"✅ Đã cập nhật thành công {total:,} dòng dữ liệu!"); st.balloons()

elif choice == "📜 Nhật ký hệ thống":
    st.header("📜 Nhật ký hoạt động & An ninh")
    
    col_f1, col_f2 = st.columns([2, 1])
    with col_f1:
        search_q = st.text_input("🔍 Tìm theo Email, Hành động (VD: EXPORT, CALC) hoặc từ khóa")
    with col_f2:
        date_range = st.date_input("Chọn khoảng ngày", value=[date.today() - timedelta(days=7), date.today()])

    conn = get_db_connection()
    if conn:
        try:
            query = "SELECT created_at, email, action, details FROM audit_logs ORDER BY id DESC LIMIT 2000"
            df_logs = pd.read_sql(query, conn)
            if not df_logs.empty:
                # Định dạng thời gian Việt Nam
                df_logs['created_at'] = pd.to_datetime(df_logs['created_at']).dt.tz_convert('Asia/Ho_Chi_Minh')
                
                # Lọc theo dải ngày đã chọn
                if len(date_range) == 2:
                    start_dt = pd.to_datetime(date_range[0]).tz_localize('Asia/Ho_Chi_Minh')
                    end_dt = pd.to_datetime(date_range[1]).tz_localize('Asia/Ho_Chi_Minh') + timedelta(days=1)
                    df_logs = df_logs[(df_logs['created_at'] >= start_dt) & (df_logs['created_at'] < end_dt)]
                
                # Xử lý tìm kiếm không lỗi logic
                if search_q:
                    mask = df_logs.astype(str).apply(lambda row: row.str.contains(search_q, case=False, na=False).any(), axis=1)
                    df_logs = df_logs[mask]

                # Trình bày kết quả
                df_logs['created_at'] = df_logs['created_at'].dt.strftime('%H:%M:%S %d/%m/%Y')
                df_logs['details'] = df_logs['details'].apply(lambda x: json.dumps(x, ensure_ascii=False) if x else "")
                df_logs.columns = ["Thời gian", "Người thực hiện", "Hành động", "Chi tiết hoạt động"]
                st.dataframe(df_logs, use_container_width=True, hide_index=True)
            else: st.info("Hiện chưa có nhật ký hoạt động nào được ghi nhận.")
        finally: conn.close()

elif choice == "👥 Quản lý nhân sự":
    st.header("👥 Quản lý quyền truy cập hệ thống")
    st.warning("Lưu ý: Các thay đổi này tác động trực tiếp đến khả năng đăng nhập của nhân viên.")
    col_sel, col_act = st.columns([1, 1])
    with col_sel:
        target_email = st.text_input("Email nhân viên cần xử lý")
        action = st.selectbox("Chọn hành động", ["Đặt lại mật khẩu", "Xóa tài khoản vĩnh viễn"])
    with col_act:
        if action == "Đặt lại mật khẩu":
            new_pwd = st.text_input("Mật khẩu mới", type="password")
            if st.button("🚀 Cập nhật mật khẩu"):
                if len(new_pwd) < 6: st.warning("Mật khẩu phải từ 6 ký tự.")
                else:
                    s, m = admin_manage_user(target_email, "RESET_PWD", new_pwd)
                    if s: st.success(m); log_activity("ADMIN_RESET_PWD", {"target": target_email})
                    else: st.error(m)
        elif action == "Xóa tài khoản vĩnh viễn":
            st.error("Cảnh báo: Hành động xóa không thể khôi phục!")
            if st.button("🔴 XÁC NHẬN XÓA TÀI KHOẢN"):
                s, m = admin_manage_user(target_email, "DELETE")
                if s: st.success(m); log_activity("ADMIN_DELETE_USER", {"target": target_email})
                else: st.error(m)

elif choice == "🔧 Cấu hình":
    st.header("🔧 Tham số vận hành hệ thống")
    new_threshold = st.slider("Độ nhạy thuật toán tìm kiếm tên (Threshold)", 0.5, 0.95, st.session_state.threshold, 0.05)
    st.info("💡 Mẹo: 0.85 là mức chuẩn. Nếu hạ xuống 0.70, hệ thống sẽ tìm thấy nhiều tên gần giống hơn nhưng tỷ lệ sai lệch cao hơn.")
    if st.button("Lưu cấu hình hệ thống"):
        st.session_state.threshold = new_threshold
        st.success("Đã cập nhật cấu hình thành công!"); log_activity("UPDATE_CONFIG", {"threshold": new_threshold})

elif choice == "⚙️ Tài khoản":
    st.header("⚙️ Cài đặt tài khoản cá nhân")
    st.write(f"Đang đăng nhập: **{st.session_state.user.email}**")
    with st.form("pwd_form"):
        npwd = st.text_input("Mật khẩu mới", type="password")
        cpwd = st.text_input("Nhập lại mật khẩu", type="password")
        if st.form_submit_button("Thay đổi mật khẩu"):
            if npwd == cpwd and len(npwd) >= 6:
                supabase.auth.update_user({"password": npwd})
                st.success("Đã đổi mật khẩu thành công!")
                log_activity("CHANGE_OWN_PASSWORD", {"status": "success"})
            else: st.error("Mật khẩu không khớp hoặc độ dài không đủ.")

elif choice == "🗑️ Dọn dẹp":
    st.header("🗑️ Quản trị Kho dữ liệu")
    st.error("Hành động này sẽ xóa toàn bộ danh sách hơn 200.000 người tham gia BHYT!")
    if st.checkbox("Tôi hiểu và chịu trách nhiệm về việc xóa dữ liệu."):
        if st.button("🔴 THỰC HIỆN XÓA SẠCH KHO"):
            conn = get_db_connection()
            if conn:
                with conn.cursor() as cur: cur.execute("TRUNCATE TABLE participants RESTART IDENTITY")
                conn.commit(); conn.close()
                log_activity("TRUNCATE_DATA", {"scope": "all_participants"})
                st.success("Đã dọn dẹp sạch kho dữ liệu!")
                time.sleep(1); st.rerun()
