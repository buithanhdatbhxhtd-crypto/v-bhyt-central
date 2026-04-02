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
    # Email Admin được cấu hình trong Secrets của Streamlit
    admin_emails = ["admin@example.com", st.secrets.get("ADMIN_EMAIL", "")]
    return st.session_state.user.email in admin_emails

# --- 3. LOGIC TRUY VẤN DỮ LIỆU ---

def search_participants(q_main, q_sub, search_type, limit=100):
    conn = get_db_connection()
    if not conn: return []
    cur = conn.cursor()
    try:
        # Lấy đầy đủ 9 cột thông tin (Đã bao gồm SĐT, Email, Địa chỉ)
        select_fields = "ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, dia_chi, sdt, email, han_the"
        
        if search_type == "Mã BHXH":
            query = f"SELECT {select_fields} FROM participants WHERE ma_so_bhxh = %(q)s OR ma_so_bhxh ILIKE %(like_q)s LIMIT %(limit)s"
            cur.execute(query, {'q': q_main.strip(), 'like_q': f"%{q_main.strip()}", 'limit': limit})
        elif search_type == "CCCD":
            query = f"SELECT {select_fields} FROM participants WHERE cccd = %(q)s OR cccd ILIKE %(like_q)s LIMIT %(limit)s"
            cur.execute(query, {'q': q_main.strip(), 'like_q': f"%{q_main.strip()}", 'limit': limit})
        else: # Tên & Ngày sinh
            name_norm = unidecode(q_main.strip()).lower()
            dob_clean = q_sub.strip().replace("/", "").replace("-", "").replace(" ", "")
            dob_filter = ""
            params = {'name': name_norm, 'like_name': f"%{name_norm}%", 'limit': limit}
            if dob_clean:
                if len(dob_clean) == 4:
                    dob_filter = "AND TO_CHAR(ngay_sinh, 'YYYY') = %(dob)s"
                    params['dob'] = dob_clean
                else:
                    dob_filter = "AND (TO_CHAR(ngay_sinh, 'DDMMYYYY') = %(dob)s OR TO_CHAR(ngay_sinh, 'YYYYMMDD') = %(dob)s)"
                    params['dob'] = dob_clean

            query = f"""
                SELECT {select_fields} FROM participants 
                WHERE (ho_ten_unsigned = %(name)s OR ho_ten_unsigned ILIKE %(like_name)s OR (similarity(ho_ten_unsigned, %(name)s) > 0.85)) {dob_filter}
                ORDER BY (ho_ten_unsigned = %(name)s) DESC, (ho_ten_unsigned ILIKE %(like_name)s) DESC, similarity(ho_ten_unsigned, %(name)s) DESC
                LIMIT %(limit)s
            """
            cur.execute(query, params)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()

# --- 4. LOGIC XỬ LÝ DỮ LIỆU LỚN (ADMIN) ---

def import_excel_to_db(df):
    # Chuẩn hóa tên cột sang chữ thường và xóa khoảng trắng
    df.columns = [str(c).strip().lower() for c in df.columns]
    
    # BỘ TỪ ĐIỂN NHẬN DIỆN CỘT THÔNG MINH
    mapping = {
        'ma so bhxh': 'ma_so_bhxh', 'mã số bhxh': 'ma_so_bhxh', 'msbhxh': 'ma_so_bhxh',
        'ma the bhyt': 'ma_the_bhyt', 'mã thẻ bhyt': 'ma_the_bhyt', 'mathe': 'ma_the_bhyt',
        'ho ten': 'ho_ten', 'họ tên': 'ho_ten', 'họ và tên': 'ho_ten',
        'ngay sinh': 'ngay_sinh', 'ngày sinh': 'ngay_sinh', 'ns': 'ngay_sinh',
        'socmnd': 'cccd', 'cccd': 'cccd', 'số cccd': 'cccd', 'cmnd': 'cccd',
        'sodient': 'sdt', 'so dien thoai': 'sdt', 'số điện thoại': 'sdt', 'sđt': 'sdt', 'số đt': 'sdt', 'phone': 'sdt',
        'diachilh': 'dia_chi', 'địa chỉ': 'dia_chi', 'dia chi': 'dia_chi', 'địa chỉ liên hệ': 'dia_chi', 'địa chỉ cư trú': 'dia_chi',
        'hantheden': 'han_the', 'hạn thẻ': 'han_the', 'hạn thẻ đến': 'han_the', 'hạn dùng': 'han_the',
        'email': 'email', 'thư điện tử': 'email', 'địa chỉ email': 'email'
    }
    
    # Đổi tên các cột dựa trên từ điển
    df = df.rename(columns=mapping)
    
    # Đảm bảo các cột đích luôn tồn tại
    target_cols = ['ma_so_bhxh', 'ma_the_bhyt', 'ho_ten', 'ngay_sinh', 'cccd', 'dia_chi', 'sdt', 'email', 'han_the']
    for col in target_cols:
        if col not in df.columns:
            df[col] = None

    with st.spinner("Đang định dạng dữ liệu..."):
        # Định dạng ngày tháng
        for col in ['ngay_sinh', 'han_the']:
            temp_dt = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
            df[col] = temp_dt.apply(lambda x: x.date() if pd.notnull(x) else None)

        # Định dạng văn bản và số (giữ lại số 0 ở đầu cho SĐT, Mã BHXH, CCCD)
        str_cols = ['ma_so_bhxh', 'ma_the_bhyt', 'ho_ten', 'cccd', 'sdt', 'dia_chi', 'email']
        for col in str_cols:
            df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
            null_values = ['nan', 'None', 'NAT', 'NaT', '<NA>', '', 'NaN', 'NAN', 'null', 'NULL']
            df[col] = df[col].where(~df[col].isin(null_values), None)

    # Chuyển thành danh sách Tuples
    data_tuples = list(df[target_cols].itertuples(index=False, name=None))
    del df

    sql = """
        INSERT INTO participants (ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, dia_chi, sdt, email, han_the)
        VALUES %s
        ON CONFLICT (ma_so_bhxh) DO UPDATE SET
            ma_the_bhyt = EXCLUDED.ma_the_bhyt,
            ho_ten = EXCLUDED.ho_ten,
            ngay_sinh = EXCLUDED.ngay_sinh,
            cccd = EXCLUDED.cccd,
            dia_chi = EXCLUDED.dia_chi,
            sdt = EXCLUDED.sdt,
            email = EXCLUDED.email,
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
                        st.success("Đăng nhập thành công!")
                        time.sleep(0.5)
                        st.rerun()
    st.stop()

# --- 6. GIAO DIỆN CHÍNH ---

st.sidebar.markdown(f"👤 **{st.session_state.user.email}**")
role_label = "🔴 Quản trị viên" if is_admin() else "🔵 Nhân viên Tra cứu"
st.sidebar.markdown(f"Vai trò: {role_label}")

menu_options = ["🔍 Tra cứu dữ liệu"]
if is_admin():
    menu_options += ["📥 Nhập dữ liệu mới", "🗑️ Dọn dẹp dữ liệu"]

choice = st.sidebar.radio("Menu Chức năng", menu_options)

if st.sidebar.button("🚪 Đăng xuất"):
    logout_user()

if choice == "🔍 Tra cứu dữ liệu":
    st.subheader("🔍 Tra cứu người tham gia BHYT")
    
    col_sel, col_empty = st.columns([1, 3])
    with col_sel:
        stype = st.selectbox("Loại tìm kiếm", ["Tên & Ngày sinh", "Mã BHXH", "CCCD"])
    
    if stype == "Tên & Ngày sinh":
        col_name, col_dob = st.columns([2, 1])
        with col_name:
            q_name = st.text_input("Họ tên", placeholder="Ví dụ: Bùi Thành Đạt")
        with col_dob:
            q_dob = st.text_input("Ngày/Năm sinh", placeholder="1988 hoặc 22/01/1988")
        
        search_trigger = q_name 
        q_main = q_name
        q_sub = q_dob
    else:
        q_val = st.text_input(f"Nhập {stype}", placeholder=f"Nhập {stype} cần tìm...")
        search_trigger = q_val
        q_main = q_val
        q_sub = ""

    if search_trigger:
        with st.spinner("Đang truy xuất..."):
            data = search_participants(q_main, q_sub, stype)
            if data:
                st.success(f"Tìm thấy {len(data)} kết quả.")
                cols_display = ["Mã BHXH", "Thẻ BHYT", "Họ Tên", "Ngày Sinh", "CCCD", "Địa chỉ", "SĐT", "Email", "Hạn Thẻ"]
                df_res = pd.DataFrame(data, columns=cols_display)
                
                # Định dạng ngày hiển thị
                for date_col in ["Ngày Sinh", "Hạn Thẻ"]:
                    df_res[date_col] = pd.to_datetime(df_res[date_col], errors='coerce').dt.strftime('%d/%m/%Y').replace('NaT', '')
                
                # Che bớt số CCCD để bảo mật
                df_res['CCCD'] = df_res['CCCD'].apply(lambda x: f"{x[:3]}****{x[-3:]}" if x and len(str(x)) > 6 else x)
                
                st.dataframe(df_res, use_container_width=True, hide_index=True)
            else:
                st.warning("Không tìm thấy kết quả phù hợp.")

elif choice == "📥 Nhập dữ liệu mới":
    st.subheader("📥 Nhập dữ liệu hàng loạt (Admin)")
    uploaded_file = st.file_uploader("Chọn tệp Excel (.xlsx, .xlsb)", type=["xlsx", "xlsb"])
    if uploaded_file:
        try:
            engine = 'pyxlsb' if uploaded_file.name.endswith('.xlsb') else None
            df_preview = pd.read_excel(uploaded_file, engine=engine, dtype=str)
            st.write(f"📊 Phát hiện: **{len(df_preview):,}** hàng.")
            
            st.info("💡 Hệ thống sẽ tự động nhận diện các cột: Họ tên, Số điện thoại, Email, Địa chỉ, Hạn thẻ...")
            
            if st.button("🚀 Bắt đầu nạp dữ liệu"):
                progress_bar = st.progress(0)
                status_text = st.empty()
                total = len(df_preview)
                success_count = 0
                for count in import_excel_to_db(df_preview):
                    progress_bar.progress(count / total)
                    status_text.text(f"Đang xử lý: {count:,} / {total:,} hàng...")
                    success_count = count
                if success_count > 0:
                    st.success(f"✅ Thành công! Đã cập nhật {success_count:,} dòng.")
                    st.balloons()
        except Exception as e:
            st.error(f"Lỗi khi đọc file: {e}")

elif choice == "🗑️ Dọn dẹp dữ liệu":
    st.subheader("🗑️ Xóa dữ liệu (Admin)")
    confirm = st.checkbox("Xác nhận xóa sạch dữ liệu hệ thống.")
    if st.button("Xóa toàn bộ", disabled=not confirm):
        if delete_all_data():
            st.success("Dữ liệu đã được dọn dẹp sạch sẽ.")
