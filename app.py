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

st.set_page_config(
    page_title="V-BHYT Central - Giải pháp Quản trị & Nghiệp vụ",
    page_icon="🛡️",
    layout="wide"
)

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
    except Exception:
        return None

@st.cache_data(ttl=600)
def perform_search(stype, q_m, q_s, sfilter, slimit, threshold):
    """
    Tra cứu tối ưu hóa:
    - Hỗ trợ cả Mã BHXH 10 số (cũ) và 12 số (đã đồng bộ CCCD mới).
    - Tự động tra cứu chéo giữa Mã số BHXH và CCCD để không bỏ sót người tham gia.
    """
    conn = get_db_connection()
    if not conn: 
        return []
    try:
        with conn.cursor() as cur:
            fields = "ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, dia_chi, sdt, email, han_the, tong_thoi_gian_bhxh"
            where, params = "", {'limit': slimit, 'th': threshold}
            q_m_clean = q_m.strip()
            
            if stype == "Mã BHXH":
                # Tìm kiếm linh hoạt: khớp mã BHXH hoặc quét chéo sang CCCD nếu đã chuyển đổi
                where = "(ma_so_bhxh = %(q)s OR ma_so_bhxh ILIKE %(lq)s OR cccd = %(q)s)"
                params.update({'q': q_m_clean, 'lq': f"{q_m_clean}%"})
            elif stype == "CCCD":
                # Tìm theo CCCD hoặc mã số BHXH nếu đã chuyển thành 12 số
                where = "(cccd = %(q)s OR cccd ILIKE %(lq)s OR ma_so_bhxh = %(q)s)"
                params.update({'q': q_m_clean, 'lq': f"{q_m_clean}%"})
            else:
                name_n = unidecode(q_m_clean).lower()
                dob_c = q_s.strip().replace("/", "").replace("-", "")
                where = "(ho_ten_unsigned LIKE %(ln)s OR similarity(ho_ten_unsigned, %(n)s) > %(th)s)"
                params.update({'n': name_n, 'ln': f"%{name_n}%"})
                if dob_c:
                    if len(dob_c) == 4: 
                        where += " AND TO_CHAR(ngay_sinh, 'YYYY') = %(d)s"
                    else: 
                        where += " AND (TO_CHAR(ngay_sinh, 'DDMMYYYY') = %(d)s OR TO_CHAR(ngay_sinh, 'YYYYMMDD') = %(d)s)"
                    params['d'] = dob_c

            if sfilter == "Đã hết hạn": 
                where += " AND han_the < CURRENT_DATE"
            elif sfilter == "Sắp hết hạn (30 ngày)": 
                where += " AND han_the >= CURRENT_DATE AND han_the <= CURRENT_DATE + INTERVAL '30 days'"
            elif sfilter == "Còn hạn": 
                where += " AND han_the >= CURRENT_DATE"

            query = f"SELECT {fields} FROM participants WHERE {where} ORDER BY ho_ten ASC LIMIT %(limit)s"
            cur.execute(query, params)
            return cur.fetchall()
    except Exception:
        return []
    finally:
        conn.close()

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
    try:
        admin_client = create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])
        response = admin_client.auth.admin.list_users()
        all_users = response.users if hasattr(response, 'users') else response
        user = next((u for u in all_users if u.email == target_email), None)
        
        if not user: 
            return False, "Không tìm thấy người dùng có email này."
        
        if action == "RESET_PWD":
            admin_client.auth.admin.update_user_by_id(user.id, {"password": new_password})
            return True, f"Đã đổi mật khẩu thành công cho {target_email}."
        elif action == "DELETE":
            admin_client.auth.admin.delete_user(user.id)
            return True, f"Đã xóa tài khoản {target_email} khỏi hệ thống."
        return False, "Hành động không hợp lệ."
    except Exception as e: 
        return False, f"Lỗi quản trị: {str(e)}"

def log_activity(action, details_dict):
    if not st.session_state.user: 
        return
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                sql = "INSERT INTO audit_logs (email, action, details) VALUES (%(email)s, %(action)s, %(details)s)"
                cur.execute(sql, {'email': st.session_state.user.email, 'action': action, 'details': json.dumps(details_dict, ensure_ascii=False)})
            conn.commit()
        except Exception: 
            pass
        finally: 
            conn.close()

def get_advanced_stats():
    conn = get_db_connection()
    if not conn: 
        return None
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
    finally: 
        conn.close()

@st.cache_data(ttl=300)
def get_missing_cccd_data():
    """
    Thống kê và phân loại người tham gia có Mã số BHXH 10 số nhưng thiếu số CCCD:
    - Nhóm 1 (active_rows): Còn hạn thẻ BHYT (han_the >= CURRENT_DATE) -> Giữ lại để phối hợp UBND/Công an xã.
    - Nhóm 2 (expired_rows): Đã hết hạn thẻ BHYT hoặc không có hạn thẻ -> Dữ liệu lỗi/ảo cần kiểm tra và xóa.
    """
    conn = get_db_connection()
    if not conn:
        return [], []
    try:
        with conn.cursor() as cur:
            fields = "ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, dia_chi, sdt, email, han_the, tong_thoi_gian_bhxh"
            base_cond = """
                LENGTH(TRIM(ma_so_bhxh)) <= 10
                AND (cccd IS NULL OR TRIM(cccd) = '' OR LOWER(TRIM(cccd)) IN ('nan', 'none', 'null', 'nat'))
            """
            # 1. Nhóm còn hạn thẻ BHYT (Không xóa - Xuất phối hợp UBND xã / Công an xã)
            cur.execute(f"""
                SELECT {fields} FROM participants
                WHERE {base_cond} AND han_the >= CURRENT_DATE
                ORDER BY dia_chi ASC, ho_ten ASC
            """)
            active_rows = cur.fetchall()

            # 2. Nhóm đã hết hạn thẻ hoặc không có hạn thẻ (Dữ liệu lỗi - Cho phép xuất kiểm tra & xóa)
            cur.execute(f"""
                SELECT {fields} FROM participants
                WHERE {base_cond} AND (han_the < CURRENT_DATE OR han_the IS NULL)
                ORDER BY han_the DESC NULLS LAST, ho_ten ASC
            """)
            expired_rows = cur.fetchall()
            return active_rows, expired_rows
    except Exception:
        return [], []
    finally:
        conn.close()

def delete_expired_missing_cccd():
    """
    Chỉ xóa các bản ghi có Mã BHXH 10 số, thiếu CCCD VÀ đã hết hạn thẻ (hoặc không có hạn thẻ).
    Bảo vệ tuyệt đối các trường hợp còn hạn thẻ BHYT (han_the >= CURRENT_DATE).
    """
    conn = get_db_connection()
    if not conn:
        return False, 0, "Không thể kết nối cơ sở dữ liệu."
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM participants
                WHERE LENGTH(TRIM(ma_so_bhxh)) <= 10
                  AND (cccd IS NULL OR TRIM(cccd) = '' OR LOWER(TRIM(cccd)) IN ('nan', 'none', 'null', 'nat'))
                  AND (han_the < CURRENT_DATE OR han_the IS NULL)
            """)
            deleted_count = cur.rowcount
        conn.commit()
        return True, deleted_count, f"Đã xóa thành công {deleted_count:,} bản ghi lỗi (10 số, thiếu CCCD, hết hạn thẻ)."
    except Exception as e:
        conn.rollback()
        return False, 0, f"Lỗi khi xóa dữ liệu: {e}"
    finally:
        conn.close()

def format_participants_df(rows):
    """Chuẩn hóa danh sách người tham gia sang DataFrame sạch để hiển thị và xuất Excel"""
    df = pd.DataFrame(rows, columns=[
        "Mã số BHXH", "Mã thẻ BHYT", "Họ tên", "Ngày sinh", "CCCD", 
        "Địa chỉ", "Số điện thoại", "Email", "Hạn thẻ", "Tổng quá trình"
    ])
    def clean_cell(x):
        if pd.isna(x) or str(x).strip().lower() in ['none', 'nan', 'nat', 'null', '']:
            return ""
        return str(x).strip()
    for col in df.columns:
        df[col] = df[col].apply(clean_cell)
    for d_col in ["Ngày sinh", "Hạn thẻ"]:
        df[d_col] = pd.to_datetime(df[d_col], errors='coerce').dt.strftime('%d/%m/%Y').fillna("")
    return df

def convert_df_to_excel(df, sheet_name="Danh_Sach"):
    """Chuyển đổi DataFrame sang file Excel (.xlsx) trong bộ nhớ để tải về"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()

def parse_bhxh_pdf(pdf_file):
    """Trích xuất dữ liệu từ file PDF quá trình đóng BHXH (Mẫu 07/SBH) và lọc trùng"""
    history_data, seen_records = [], set()
    msbhxh, summary_text = "", ""
    with pdfplumber.open(pdf_file) as pdf:
        full_text = ""
        for page in pdf.pages: 
            full_text += (page.extract_text() or "") + "\n"
        match_ms = re.search(r"Mã số BHXH\s*:\s*(\d+)", full_text)
        if match_ms: 
            msbhxh = match_ms.group(1).strip()
        m_bhxh = re.search(r"Thời gian đóng BHXH vào quỹ hưu trí.*?là\s*(.*?)(?:\n|$)", full_text)
        m_bhtn = re.search(r"Thời gian đóng BHTN vào quỹ BHTN.*?là\s*(.*?)(?:\n|$)", full_text)
        sums = []
        if m_bhxh: 
            sums.append(f"BHXH: {m_bhxh.group(1).strip()}")
        if m_bhtn: 
            sums.append(f"BHTN: {m_bhtn.group(1).strip()}")
        summary_text = " | ".join(sums)
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    if not row or len(row) < 5 or not row[0]: 
                        continue
                    tu_thang = str(row[0]).strip()
                    if not re.match(r"\d{2}/\d{4}", tu_thang): 
                        continue
                    try:
                        den_thang = str(row[1]).strip() if row[1] else ""
                        don_vi = str(row[2]).strip().replace('\n', ' ')
                        muc_raw = str(row[3]).replace('.', '').replace(',', '').strip()
                        muc_dong = float(muc_raw) if muc_raw.isdigit() else 0
                        ty_le = str(row[6]).strip() if len(row) > 6 and row[6] else ""
                        loai_bh = "BHTN" if "BẢO HIỂM THẤT NGHIỆP" in don_vi.upper() else "BHXH"
                        rec_id = (tu_thang, den_thang, don_vi, muc_dong, loai_bh)
                        if rec_id not in seen_records:
                            seen_records.add(rec_id)
                            history_data.append((msbhxh, tu_thang, den_thang, don_vi, muc_dong, ty_le, loai_bh))
                    except Exception: 
                        continue
    return msbhxh, history_data, summary_text

def save_bhxh_history(msbhxh, data, summary):
    if not msbhxh or not data: 
        return False
    conn = get_db_connection()
    if not conn: 
        return False
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM bhxh_history WHERE ma_so_bhxh = %s", (msbhxh,))
        execute_values(cur, "INSERT INTO bhxh_history (ma_so_bhxh, tu_thang, den_thang, don_vi_cong_viec, muc_dong, ty_le_dong, loai_bh) VALUES %s", data)
        if summary: 
            # Cập nhật cho cả trường hợp mã BHXH là 10 số hoặc đã đồng bộ sang 12 số CCCD
            cur.execute("UPDATE participants SET tong_thoi_gian_bhxh = %s WHERE ma_so_bhxh = %s OR cccd = %s", (summary, msbhxh, msbhxh))
        conn.commit()
        return True
    except Exception: 
        return False
    finally: 
        conn.close()

def safe_clean_str(val, max_len=None):
    """Hàm chuẩn hóa chuỗi an toàn tuyệt đối, sửa lỗi 'float' object is not subscriptable"""
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip()
    if s.endswith('.0'):
        s = s[:-2]
    s = s.lstrip("'").strip()
    if s.lower() in ['nan', 'none', 'nat', 'null', '']:
        return None
    return s[:max_len] if max_len else s

def safe_clean_code(val, max_len=50):
    """Chuẩn hóa Mã số BHXH (10 số hoặc 12 số CCCD) và số CCCD, tự động khôi phục số 0 ở đầu nếu bị Excel làm mất"""
    s = safe_clean_str(val, max_len)
    if not s:
        return None
    if s.isdigit():
        if len(s) == 9:
            s = '0' + s  # Khôi phục số 0 đầu cho Mã BHXH 10 số
        elif len(s) == 11:
            s = '0' + s  # Khôi phục số 0 đầu cho Mã BHXH / CCCD 12 số
    return s

def safe_clean_phone(val, max_len=100):
    """Chuẩn hóa Số điện thoại, khôi phục số 0 ở đầu nếu bị định dạng số làm mất"""
    s = safe_clean_str(val, max_len)
    if not s:
        return None
    if s.isdigit() and len(s) == 9:
        s = '0' + s
    return s

def safe_parse_date(val):
    """Chuyển đổi ngày tháng an toàn cho cả chuỗi DD/MM/YYYY và số serial date của file .xlsb"""
    if pd.isna(val) or val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    s = str(val).strip()
    if s.endswith('.0'):
        s = s[:-2]
    if s.lower() in ['nan', 'none', 'nat', 'null', '']:
        return None
    # Xử lý trường hợp pyxlsb trả về số serial date của Excel (VD: 46146)
    if s.isdigit():
        num = int(s)
        if 10000 <= num <= 70000:
            return (datetime(1899, 12, 30) + timedelta(days=num)).date()
        elif len(s) == 4 and 1900 <= num <= 2100:
            return date(num, 1, 1)
    try:
        dt = pd.to_datetime(s, errors='coerce', dayfirst=True)
        return dt.date() if pd.notnull(dt) else None
    except Exception:
        return None

def ensure_db_schema():
    """Tự động nâng cấp cấu trúc bảng trên Supabase để hỗ trợ Mã BHXH 12 số (CCCD) và tránh lỗi giới hạn ký tự"""
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE participants 
                    ALTER COLUMN ma_so_bhxh TYPE VARCHAR(50),
                    ALTER COLUMN ma_the_bhyt TYPE VARCHAR(50),
                    ALTER COLUMN cccd TYPE VARCHAR(50),
                    ALTER COLUMN sdt TYPE VARCHAR(100);
                ALTER TABLE participants DROP CONSTRAINT IF EXISTS participants_cccd_key;
                ALTER TABLE participants ADD COLUMN IF NOT EXISTS tong_thoi_gian_bhxh TEXT;
                CREATE INDEX IF NOT EXISTS idx_cccd ON participants(cccd);
                ALTER TABLE IF EXISTS bhxh_history 
                    ALTER COLUMN ma_so_bhxh TYPE VARCHAR(50);
            """)
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        conn.close()

def import_db_logic(df, import_mode="MERGE"):
    """
    Nạp dữ liệu bảo vệ thông tin cũ và đồng bộ Mã BHXH 10 số <-> 12 số (CCCD):
    - Xử lý đồng thời Mã số BHXH 10 số và 12 số (CCCD).
    - Tự động nhận diện người cũ đã có mã 10 số khi nạp mã 12 số (CCCD) để cập nhật thay vì tạo trùng.
    - Cơ chế MERGE thông minh (không ghi đè ô rỗng lên thông tin cũ).
    """
    ensure_db_schema()
    df.columns = [str(c).strip().lower() for c in df.columns]
    
    mapping = {
        'ma so bhxh': 'ma_so_bhxh', 
        'mã số bhxh': 'ma_so_bhxh',
        'masobhxh': 'ma_so_bhxh',
        'ma the bhyt': 'ma_the_bhyt',
        'mã thẻ bhyt': 'ma_the_bhyt',
        'mathebhyt': 'ma_the_bhyt',
        'ho ten': 'ho_ten', 
        'họ tên': 'ho_ten',
        'hoten': 'ho_ten',
        'ngay sinh': 'ngay_sinh',
        'ngày sinh': 'ngay_sinh',
        'ngaysinh': 'ngay_sinh',
        'cccd': 'cccd', 
        'socmnd': 'cccd',        
        'so cccd': 'cccd',
        'số cccd': 'cccd',
        'sdt': 'sdt', 
        'sodienthoai': 'sdt',    
        'so dien thoai': 'sdt',
        'số điện thoại': 'sdt',
        'diachilh': 'dia_chi',   
        'dia chi': 'dia_chi',
        'địa chỉ': 'dia_chi',
        'hantheden': 'han_the',  
        'han the': 'han_the',
        'hạn thẻ': 'han_the',
        'email': 'email',
        'vss_email': 'email'     
    }
    df = df.rename(columns=mapping)
    # Nếu file có nhiều cột trùng tên sau khi map, chỉ giữ cột đầu tiên
    df = df.loc[:, ~df.columns.duplicated()]
    target = ['ma_so_bhxh', 'ma_the_bhyt', 'ho_ten', 'ngay_sinh', 'cccd', 'dia_chi', 'sdt', 'email', 'han_the']
    
    for col in target:
        if col not in df.columns: 
            df[col] = None
    
    # Định dạng ngày tháng an toàn (hỗ trợ cả chuỗi và số serial Excel)
    for col in ['ngay_sinh', 'han_the']:
        df[col] = df[col].apply(safe_parse_date)
    
    # Chuẩn hóa mã BHXH, CCCD, SĐT và các chuỗi
    df['ma_so_bhxh'] = df['ma_so_bhxh'].apply(lambda x: safe_clean_code(x, 50))
    df['ma_the_bhyt'] = df['ma_the_bhyt'].apply(lambda x: safe_clean_str(x, 50))
    df['ho_ten'] = df['ho_ten'].apply(lambda x: safe_clean_str(x))
    df['cccd'] = df['cccd'].apply(lambda x: safe_clean_code(x, 50))
    df['dia_chi'] = df['dia_chi'].apply(lambda x: safe_clean_str(x))
    df['sdt'] = df['sdt'].apply(lambda x: safe_clean_phone(x, 100))
    df['email'] = df['email'].apply(lambda x: safe_clean_str(x))
    
    # Nếu Mã số BHXH đã chuyển thành 12 số (CCCD) mà cột CCCD đang trống thì tự động điền sang cột CCCD
    mask_12_digits = df['ma_so_bhxh'].notnull() & (df['ma_so_bhxh'].str.len() == 12) & df['cccd'].isnull()
    df.loc[mask_12_digits, 'cccd'] = df.loc[mask_12_digits, 'ma_so_bhxh']
    
    # Bỏ qua các dòng không có Mã số BHXH hoặc Họ tên
    df = df[df['ma_so_bhxh'].notnull() & df['ho_ten'].notnull()]
    df = df.drop_duplicates(subset=['ma_so_bhxh'], keep='first')

    conn = get_db_connection()
    if not conn: 
        return

    data = list(df[target].itertuples(index=False, name=None))
    if not data:
        conn.close()
        return

    try:
        cur = conn.cursor()

        # Câu lệnh đồng bộ thông minh:
        # Nếu trong DB đã có người tham gia với Mã BHXH 10 số (cũ) và số CCCD trùng với Mã BHXH 12 số (mới),
        # hệ thống tự động nâng cấp Mã BHXH của người đó sang 12 số để gộp dữ liệu mà không tạo bản ghi trùng.
        sync_10_to_12_sql = """
            UPDATE participants p
            SET ma_so_bhxh = t.ma_so_bhxh
            FROM temp_import t
            WHERE LENGTH(t.ma_so_bhxh) = 12
              AND p.id = (
                  SELECT p3.id FROM participants p3
                  WHERE p3.cccd = t.ma_so_bhxh
                    AND LENGTH(p3.ma_so_bhxh) = 10
                    AND lower(unaccent(p3.ho_ten)) = lower(unaccent(t.ho_ten))
                  LIMIT 1
              )
              AND NOT EXISTS (
                  SELECT 1 FROM participants p2 WHERE p2.ma_so_bhxh = t.ma_so_bhxh
              );
        """

        if import_mode == "INSERT_ONLY":
            sql = """
                INSERT INTO participants (ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, dia_chi, sdt, email, han_the)
                SELECT 
                    t.ma_so_bhxh, t.ma_the_bhyt, t.ho_ten, t.ngay_sinh,
                    t.cccd, t.dia_chi, t.sdt, t.email, t.han_the
                FROM temp_import t
                WHERE NOT EXISTS (
                    SELECT 1 FROM participants p 
                    WHERE p.ma_so_bhxh = t.ma_so_bhxh
                       OR (t.cccd IS NOT NULL AND p.ma_so_bhxh = t.cccd)
                )
                ON CONFLICT (ma_so_bhxh) DO NOTHING;
            """
        else:
            sql = """
                INSERT INTO participants (ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, dia_chi, sdt, email, han_the)
                SELECT 
                    t.ma_so_bhxh, t.ma_the_bhyt, t.ho_ten, t.ngay_sinh,
                    t.cccd, t.dia_chi, t.sdt, t.email, t.han_the
                FROM temp_import t
                ON CONFLICT (ma_so_bhxh) DO UPDATE SET
                    ma_the_bhyt = COALESCE(EXCLUDED.ma_the_bhyt, participants.ma_the_bhyt),
                    ho_ten = COALESCE(EXCLUDED.ho_ten, participants.ho_ten),
                    ngay_sinh = COALESCE(EXCLUDED.ngay_sinh, participants.ngay_sinh),
                    cccd = COALESCE(EXCLUDED.cccd, participants.cccd),
                    dia_chi = COALESCE(EXCLUDED.dia_chi, participants.dia_chi),
                    sdt = COALESCE(EXCLUDED.sdt, participants.sdt),
                    email = COALESCE(EXCLUDED.email, participants.email),
                    han_the = COALESCE(EXCLUDED.han_the, participants.han_the),
                    updated_at = NOW();
            """

        total_rows = len(data)
        for i in range(0, total_rows, 5000):
            batch = data[i:i + 5000]
            # Tạo bảng tạm cho từng lô 5.000 dòng vì ON COMMIT DROP sẽ tự xóa bảng sau mỗi lần commit()
            cur.execute("""
                CREATE TEMP TABLE IF NOT EXISTS temp_import (
                    ma_so_bhxh VARCHAR(50), ma_the_bhyt VARCHAR(50), ho_ten TEXT, 
                    ngay_sinh DATE, cccd VARCHAR(50), dia_chi TEXT, 
                    sdt VARCHAR(100), email TEXT, han_the DATE
                ) ON COMMIT DROP;
                TRUNCATE TABLE temp_import;
            """)
            execute_values(cur, "INSERT INTO temp_import VALUES %s", batch)
            if import_mode == "MERGE":
                cur.execute(sync_10_to_12_sql)
            cur.execute(sql)
            conn.commit()
            yield min(i + 5000, total_rows), total_rows
    except Exception as e:
        conn.rollback()
        st.error(f"Lỗi nạp dữ liệu: {e}")
    finally:
        conn.close()

if 'user' not in st.session_state: 
    st.session_state.user = None
if 'threshold' not in st.session_state: 
    st.session_state.threshold = 0.85
if 'search_results' not in st.session_state: 
    st.session_state.search_results = None

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

st.sidebar.title("🛡️ V-BHYT PRO")
st.sidebar.markdown(f"👤 **{st.session_state.user.email}**")
role_label = "🔴 Quản trị viên" if is_admin() else "🔵 Nhân viên Tra cứu"
st.sidebar.caption(role_label)

menu_options = ["📊 Dashboard", "🔍 Tra cứu & Quá trình", "🧹 Rà soát thiếu CCCD", "🧮 Tiện ích tính toán", "⚙️ Tài khoản"]
if is_admin():
    menu_options += ["📥 Nhập dữ liệu", "📜 Nhật ký hệ thống", "👥 Quản lý nhân sự", "🔧 Cấu hình", "🗑️ Dọn dẹp"]

choice = st.sidebar.radio("Danh mục quản lý", menu_options, label_visibility="collapsed")

st.sidebar.markdown("---")
if st.sidebar.button("🚪 Đăng xuất", use_container_width=True):
    st.cache_data.clear()
    st.session_state.clear()
    logout_user()

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
        col1, col2 = st.columns(2)
        with col1: 
            st.plotly_chart(
                px.pie(
                    pd.DataFrame({
                        "Trạng thái": ["Còn hạn", "Hết hạn", "Sắp hạn"], 
                        "Số lượng": [stats['total']-stats['expired']-stats['expiring'], stats['expired'], stats['expiring']]
                    }), 
                    values='Số lượng', names='Trạng thái', hole=0.4, title="📍 Tỷ lệ trạng thái thẻ BHYT"
                ), 
                use_container_width=True
            )
        with col2: 
            st.plotly_chart(
                px.bar(
                    pd.DataFrame({
                        "Loại": ["Đầy đủ", "Thiếu PII"], 
                        "Số lượng": [stats['total']-stats['incomplete'], stats['incomplete']]
                    }), 
                    x="Loại", y="Số lượng", color="Loại", title="🛠️ Chất lượng dữ liệu"
                ), 
                use_container_width=True
            )

elif choice == "🔍 Tra cứu & Quá trình":
    st.header("🔍 Tra cứu người tham gia & Lịch sử BHXH")
    with st.expander("🛠️ Bộ lọc nâng cao", expanded=True):
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1: 
            stype = st.selectbox("Loại tìm kiếm", ["Tên & Ngày sinh", "Mã BHXH", "CCCD"])
        with col2: 
            sfilter = st.selectbox("Trạng thái thẻ BHYT", ["Tất cả", "Còn hạn", "Sắp hết hạn (30 ngày)", "Đã hết hạn"])
        with col3: 
            slimit = st.number_input("Giới hạn", 10, 5000, 100)
        c_m, c_s = st.columns([2, 1])
        if stype == "Tên & Ngày sinh":
            q_m = st.text_input("Họ tên")
            q_s = st.text_input("Ngày/Năm sinh")
        else:
            q_m = st.text_input(f"Nhập {stype} (hỗ trợ mã 10 số hoặc CCCD 12 số)")
            q_s = ""

    if st.button("🚀 Thực hiện tra cứu", use_container_width=True):
        st.session_state.search_results = perform_search(stype, q_m, q_s, sfilter, slimit, st.session_state.threshold)
        log_activity("SEARCH", {"type": stype, "q": q_m, "count": len(st.session_state.search_results)})

    if st.session_state.search_results:
        rows = st.session_state.search_results
        df_display = pd.DataFrame(rows, columns=[
            "Mã số BHXH", "Mã thẻ BHYT", "Họ tên", "Ngày sinh", "CCCD", 
            "Địa chỉ", "Số điện thoại", "Email", "Hạn thẻ", "Tổng quá trình"
        ])
        
        def clean_fmt(x):
            if pd.isna(x) or str(x).lower() in ['none', 'nan', 'nat', '']: 
                return ""
            return str(x)
            
        for col in df_display.columns: 
            df_display[col] = df_display[col].apply(clean_fmt)
        
        for d_col in ["Ngày sinh", "Hạn thẻ"]:
            df_display[d_col] = pd.to_datetime(df_display[d_col], errors='coerce').dt.strftime('%d/%m/%Y').fillna("N/A")
            
        st.success(f"Tìm thấy {len(rows)} kết quả.")
        st.dataframe(df_display, use_container_width=True, hide_index=True)
        
        st.write("---")
        st.subheader("📜 Xem quá trình BHXH chi tiết")
        options = [f"{r[2]} ({r[0]})" for r in rows]
        sel = st.selectbox("Chọn người tham gia cần xem lịch sử:", options=["-- Mời chọn --"] + options, key="selected_bhxh_person")
        if sel != "-- Mời chọn --":
            ms_sel = re.search(r"\((.*?)\)", sel).group(1)
            # Lấy thêm CCCD của người được chọn để tra cứu chéo lịch sử BHXH nếu mã BHXH đã đổi sang 12 số
            cccd_sel = next((str(r[4]).strip() for r in rows if str(r[0]).strip() == ms_sel and r[4]), "")
            conn = get_db_connection()
            if conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT tu_thang, den_thang, don_vi_cong_viec, muc_dong, ty_le_dong, loai_bh 
                        FROM bhxh_history 
                        WHERE ma_so_bhxh = %s OR (ma_so_bhxh = %s AND %s <> '')
                        ORDER BY to_date(tu_thang, 'MM/YYYY') ASC
                    """, (ms_sel, cccd_sel, cccd_sel))
                    h_rows = cur.fetchall()
                    if h_rows:
                        st.table(pd.DataFrame(h_rows, columns=["Từ tháng", "Đến tháng", "Đơn vị", "Mức đóng", "Tỷ lệ", "Loại"]).style.format({"Mức đóng": "{:,.0f}đ"}))
                        log_activity("VIEW_DETAIL", {"msbhxh": ms_sel})
                    else: 
                        st.warning("Chưa có lịch sử chi tiết.")
                conn.close()
    elif st.session_state.search_results == []:
        st.warning("Không tìm thấy dữ liệu phù hợp.")

elif choice == "🧹 Rà soát thiếu CCCD":
    st.header("🧹 Rà soát Mã BHXH 10 số thiếu CCCD & Làm sạch dữ liệu")
    st.markdown(
        "Công cụ giúp thống kê các trường hợp **có Mã số BHXH (10 số) nhưng chưa có số CCCD**, "
        "tự động phân tách riêng nhóm **Còn hạn thẻ BHYT** (để phối hợp UBND xã, Công an xã) "
        "và nhóm **Đã hết hạn thẻ / Dữ liệu lỗi** (để kiểm tra và xóa khỏi hệ thống)."
    )

    if st.button("🔄 Làm mới số liệu thống kê", use_container_width=False):
        st.cache_data.clear()

    with st.spinner("Đang rà soát và phân loại kho dữ liệu..."):
        active_missing_rows, expired_missing_rows = get_missing_cccd_data()

    total_missing = len(active_missing_rows) + len(expired_missing_rows)
    c1, c2, c3 = st.columns(3)
    c1.metric("Tổng Mã 10 số thiếu CCCD", f"{total_missing:,} người")
    c2.metric(
        "🟢 Còn hạn thẻ BHYT (Giữ lại)", 
        f"{len(active_missing_rows):,} người", 
        delta="Phối hợp UBND / Công an xã", 
        delta_color="normal"
    )
    c3.metric(
        "🔴 Hết hạn / Không hạn thẻ (Dữ liệu lỗi)", 
        f"{len(expired_missing_rows):,} người", 
        delta="Cần kiểm tra & xóa", 
        delta_color="inverse"
    )

    st.write("---")
    tab_keep, tab_clean = st.tabs([
        f"🟢 1. Còn hạn thẻ BHYT - Phối hợp Xã/Công an ({len(active_missing_rows):,})",
        f"🔴 2. Dữ liệu lỗi / Đã hết hạn thẻ - Kiểm tra & Xóa ({len(expired_missing_rows):,})"
    ])

    with tab_keep:
        st.info(
            "🛡️ **NGUYÊN TẮC BẢO VỆ:** Những trường hợp dưới đây chưa có số CCCD nhưng **Hạn thẻ BHYT vẫn còn hiệu lực**. "
            "Hệ thống **KHÔNG XÓA** những người này. Bạn hãy xuất file Excel để gửi UBND xã, Công an xã phối hợp rà soát và bổ sung số định danh cá nhân (CCCD)."
        )
        if active_missing_rows:
            df_active_missing = format_participants_df(active_missing_rows)
            excel_active = convert_df_to_excel(df_active_missing, sheet_name="ConHan_ThieuCCCD")
            st.download_button(
                label=f"📥 Xuất Excel danh sách phối hợp UBND & Công an xã ({len(active_missing_rows):,} người)",
                data=excel_active,
                file_name=f"DS_ConHanBHYT_ThieuCCCD_PhoiHopXa_{date.today().strftime('%d%m%Y')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                on_click=lambda: log_activity("EXPORT", {"type": "ACTIVE_MISSING_CCCD", "count": len(active_missing_rows)})
            )
            st.dataframe(df_active_missing, use_container_width=True, hide_index=True)
        else:
            st.success("🎉 Tuyệt vời! Không có trường hợp nào đang còn hạn thẻ BHYT mà bị thiếu số CCCD.")

    with tab_clean:
        st.warning(
            "⚠️ **DỮ LIỆU LỖI / THẺ HẾT HẠN:** Đây là các bản ghi có Mã BHXH 10 số, **không có số CCCD** và **hạn thẻ BHYT đã hết hạn (hoặc trống)**. "
            "Bạn nên bấm **Xuất Excel** để lưu kiểm tra trước, sau đó có thể xóa khỏi hệ thống để kho dữ liệu chỉ còn lại người tham gia hợp lệ."
        )
        if expired_missing_rows:
            df_expired_missing = format_participants_df(expired_missing_rows)
            excel_expired = convert_df_to_excel(df_expired_missing, sheet_name="DuLieuLoi_ThieuCCCD")
            st.download_button(
                label=f"📥 Bước 1: Xuất Excel dữ liệu lỗi để kiểm tra ({len(expired_missing_rows):,} bản ghi)",
                data=excel_expired,
                file_name=f"DS_DuLieuLoi_10So_KhongCCCD_HetHan_{date.today().strftime('%d%m%Y')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                on_click=lambda: log_activity("EXPORT", {"type": "EXPIRED_MISSING_CCCD", "count": len(expired_missing_rows)})
            )
            st.dataframe(df_expired_missing, use_container_width=True, hide_index=True)

            if is_admin():
                st.write("---")
                st.subheader("🗑️ Bước 2: Xóa dữ liệu lỗi khỏi hệ thống")
                st.markdown(
                    f"- **Số bản ghi sẽ bị xóa:** `{len(expired_missing_rows):,}` bản ghi (Mã 10 số + Không có CCCD + Đã hết hạn thẻ / Không có hạn thẻ).\n"
                    f"- **Số bản ghi được giữ nguyên an toàn:** Toàn bộ người đã có CCCD và `{len(active_missing_rows):,}` người thiếu CCCD nhưng **còn hạn thẻ BHYT**."
                )
                confirm_clean = st.checkbox(
                    f"Tôi đã xuất file Excel kiểm tra và xác nhận xóa {len(expired_missing_rows):,} bản ghi dữ liệu lỗi này khỏi hệ thống",
                    key="confirm_del_missing_cccd"
                )
                if confirm_clean and st.button("🔴 XÓA DỮ LIỆU LỖI (THIẾU CCCD & HẾT HẠN THẺ)", use_container_width=True):
                    ok, deleted_cnt, msg = delete_expired_missing_cccd()
                    if ok:
                        log_activity("CLEAN_INVALID_NO_CCCD", {"deleted_rows": deleted_cnt, "kept_active_rows": len(active_missing_rows)})
                        st.cache_data.clear()
                        st.success(f"✅ {msg}")
                        time.sleep(1.5)
                        st.rerun()
                    else:
                        st.error(msg)
        else:
            st.success("🎉 Hệ thống đã sạch sẽ! Không còn bản ghi lỗi (Mã 10 số thiếu CCCD và đã hết hạn thẻ) nào trong kho dữ liệu.")

elif choice == "👥 Quản lý nhân sự":
    st.header("👥 Quản lý nhân sự & Phân quyền")
    tab_manage, tab_add = st.tabs(["🔧 Xử lý tài khoản", "➕ Thêm nhân viên mới"])
    
    with tab_manage:
        target_email = st.text_input("Nhập chính xác Email nhân viên")
        act_type = st.selectbox("Hành động", ["Đặt lại mật khẩu", "Xóa tài khoản"])
        
        if act_type == "Đặt lại mật khẩu":
            new_p = st.text_input("Nhập mật khẩu mới cho nhân viên", type="password")
            if st.button("🚀 Thực thi đổi mật khẩu"):
                if target_email and new_p:
                    s, m = admin_manage_user(target_email, "RESET_PWD", new_p)
                    if s: 
                        st.success(m)
                        log_activity("ADMIN_RESET_PWD", {"target": target_email})
                    else: 
                        st.error(m)
                else: 
                    st.warning("Vui lòng điền đầy đủ email và mật khẩu mới.")
        else:
            if st.button("🔴 Xác nhận XÓA tài khoản"):
                if target_email:
                    s, m = admin_manage_user(target_email, "DELETE")
                    if s: 
                        st.success(m)
                        log_activity("ADMIN_DELETE_USER", {"target": target_email})
                    else: 
                        st.error(m)
                else: 
                    st.warning("Vui lòng nhập email cần xóa.")
                
    with tab_add:
        with st.form("add_user_form"):
            st.subheader("Tạo tài khoản công vụ mới")
            n_email = st.text_input("Email nhân viên (ví dụ: nhanvien@vbhyt.vn)")
            n_pwd = st.text_input("Mật khẩu tạm thời (ít nhất 6 ký tự)", type="password")
            if st.form_submit_button("Tạo tài khoản"):
                if n_email and len(n_pwd) >= 6:
                    try:
                        admin_client = create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_KEY"])
                        admin_client.auth.admin.create_user({
                            "email": n_email,
                            "password": n_pwd,
                            "email_confirm": True
                        })
                        st.success(f"✅ Đã tạo thành công tài khoản cho: {n_email}")
                        log_activity("ADMIN_CREATE_USER", {"target": n_email})
                    except Exception as e: 
                        st.error(f"Lỗi: {e}")
                else: 
                    st.warning("Vui lòng kiểm tra lại email và độ dài mật khẩu.")

elif choice == "🧮 Tiện ích tính toán":
    st.header("🧮 Công cụ hỗ trợ thu BHYT & BHXH")
    t1, t2, t3 = st.tabs(["🧮 Máy tính nghiệp vụ", "🏥 Tính BHYT Hộ gia đình", "👵 Tính BHXH Tự nguyện"])
    with t1:
        calc_exp = st.text_input("Nhập phép tính (VD: 105300 * 5)", placeholder="Nhấn Enter...")
        if calc_exp:
            try:
                if all(c in set("0123456789+-*/.() ") for c in calc_exp): 
                    st.markdown(f"### Kết quả: `{eval(calc_exp):,.2f}`")
            except Exception: 
                st.error("Lỗi tính toán.")
    with t2:
        num = st.number_input("Số người tham gia", 1, 10, 1)
        m1 = 2340000 * 0.045
        prices = [round(m1 if i==1 else m1*0.7 if i==2 else m1*0.6 if i==3 else m1*0.5 if i==4 else m1*0.4) for i in range(1, num+1)]
        st.table(pd.DataFrame({"Người thứ": range(1, num+1), "Mức giảm": (["100%", "70%", "60%", "50%"] + ["40%"]*6)[:num], "Số tiền/12 tháng": [p*12 for p in prices]}).style.format({"Số tiền/12 tháng": "{:,.0f}đ"}))
        st.markdown(f"### 💰 Tổng thu: `{sum(prices)*12:,.0f} VNĐ`")
    with t3:
        income = st.number_input("Mức thu nhập chọn đóng (đ)", 1500000, 36000000, 1500000, 50000)
        support = st.selectbox("Đối tượng hỗ trợ", ["Hộ nghèo (50%)", "Hộ cận nghèo (40%)", "Dân tộc thiểu số (30%)", "Khác (20%)"])
        method = st.selectbox("Phương thức đóng (tháng)", [1, 3, 6, 12])
        s_pct = 0.5 if "hộ nghèo" in support.lower() else 0.4 if "cận nghèo" in support.lower() else 0.3 if "dân tộc" in support.lower() else 0.2
        monthly = (income * 0.22) - (1500000 * 0.22 * s_pct)
        st.markdown(f"### 💰 Thực thu ({method} tháng): `{monthly*method:,.0f} VNĐ`")

elif choice == "📥 Nhập dữ liệu":
    st.header("📥 Nhập liệu hệ thống (Nạp cộng dồn an toàn)")
    tx, tp = st.tabs(["📊 File Excel (BHYT)", "📜 File PDF (Quá trình BHXH)"])
    with tx:
        st.info("💡 **Hỗ trợ đầy đủ:** File Excel chứa Mã số BHXH 10 số (cũ) hoặc 12 số (đã tích hợp CCCD mới). Dữ liệu cũ được bảo vệ nguyên vẹn.")
        mode_label = st.radio(
            "Chọn phương thức xử lý khi gặp Mã số BHXH đã có sẵn trong hệ thống:",
            [
                "🔄 Bổ sung & Cập nhật thông minh (Thêm người mới; nếu trùng Mã BHXH thì cập nhật thông tin mới nhưng giữ nguyên dữ liệu cũ nếu ô mới bị trống)",
                "➕ Chỉ thêm người mới (Chỉ nạp các Mã BHXH chưa có trong hệ thống, bỏ qua hoàn toàn nếu đã tồn tại)"
            ]
        )
        import_mode = "MERGE" if "🔄" in mode_label else "INSERT_ONLY"
        
        f = st.file_uploader("Chọn file Excel", type=["xlsx", "xlsb"])
        if f and st.button("🚀 Bắt đầu nạp BHYT"):
            engine = 'pyxlsb' if f.name.endswith('.xlsb') else 'openpyxl'
            try:
                with st.spinner("Đang đọc và chuẩn hóa file Excel..."):
                    df = pd.read_excel(f, dtype=str, engine=engine)
                pb, txt = st.progress(0), st.empty()
                processed = 0
                for count, total in import_db_logic(df, import_mode=import_mode):
                    processed = count
                    pb.progress(min(processed / total, 1.0))
                    txt.text(f"⏳ Đang nạp và đồng bộ: {processed:,} / {total:,} bản ghi...")
                if processed > 0:
                    st.cache_data.clear()
                    st.success(f"✅ Hoàn tất! Đã xử lý và nạp thành công {processed:,} bản ghi vào hệ thống.")
                    log_activity("IMPORT_EXCEL", {"rows": len(df), "mode": import_mode})
            except Exception as e: 
                st.error(f"Lỗi đọc file: {e}")
    with tp:
        pdf_f = st.file_uploader("Chọn file PDF (Mẫu 07/SBH)", type=["pdf"])
        if pdf_f and st.button("🔍 Phân tích và Lưu"):
            with st.spinner("Đang đọc PDF..."):
                ms, data, summ = parse_bhxh_pdf(pdf_f)
                if ms and data and save_bhxh_history(ms, data, summ):
                    st.cache_data.clear()
                    st.success(f"✅ Thành công Mã số: {ms}")
                    log_activity("IMPORT_PDF", {"ms": ms, "rows": len(data)})
                else: 
                    st.error("Lỗi hoặc không tìm thấy dữ liệu.")

elif choice == "📜 Nhật ký hệ thống":
    st.header("📜 Nhật ký hoạt động")
    col1, col2 = st.columns([2, 1])
    with col1: 
        search_q = st.text_input("🔍 Tìm nhanh")
    with col2: 
        dr = st.date_input("Khoảng ngày", value=[date.today()-timedelta(days=7), date.today()])
    conn = get_db_connection()
    if conn:
        df_l = pd.read_sql("SELECT created_at, email, action, details FROM audit_logs ORDER BY id DESC LIMIT 2000", conn)
        if not df_l.empty:
            df_l['created_at'] = pd.to_datetime(df_l['created_at']).dt.tz_convert('Asia/Ho_Chi_Minh')
            if len(dr) == 2: 
                df_l = df_l[(df_l['created_at'].dt.date >= dr[0]) & (df_l['created_at'].dt.date <= dr[1])]
            if search_q: 
                df_l = df_l[df_l.astype(str).apply(lambda row: row.str.contains(search_q, case=False).any(), axis=1)]
            df_l['created_at'] = df_l['created_at'].dt.strftime('%H:%M:%S %d/%m/%Y')
            st.dataframe(df_l, use_container_width=True, hide_index=True)
        conn.close()

elif choice == "🔧 Cấu hình":
    st.header("🔧 Cấu hình")
    st.session_state.threshold = st.slider("Độ nhạy tìm kiếm tên", 0.5, 0.95, st.session_state.threshold)
    if st.button("Lưu"): 
        st.cache_data.clear()
        st.success("Đã lưu!")

elif choice == "🗑️ Dọn dẹp":
    st.header("🗑️ Quản lý & Dọn dẹp kho dữ liệu")
    with st.expander("📊 Dọn dẹp dữ liệu BHYT (Excel)", expanded=True):
        if st.checkbox("Tôi xác nhận xóa BHYT", key="del_bhyt") and st.button("🔴 XÓA BHYT"):
            conn = get_db_connection()
            with conn.cursor() as cur: 
                cur.execute("TRUNCATE TABLE participants RESTART IDENTITY")
            conn.commit()
            st.cache_data.clear()
            st.success("Xong!")
            time.sleep(1)
            st.rerun()
    with st.expander("📜 Dọn dẹp dữ liệu BHXH (PDF)", expanded=True):
        if st.checkbox("Tôi xác nhận xóa lịch sử BHXH", key="del_bhxh") and st.button("🔴 XÓA LỊCH SỬ"):
            conn = get_db_connection()
            with conn.cursor() as cur: 
                cur.execute("TRUNCATE TABLE bhxh_history RESTART IDENTITY")
                cur.execute("UPDATE participants SET tong_thoi_gian_bhxh = NULL")
            conn.commit()
            st.cache_data.clear()
            st.success("Xong!")
            time.sleep(1)
            st.rerun()
    with st.expander("📜 Dọn dẹp Nhật ký", expanded=True):
        if st.checkbox("Tôi xác nhận xóa nhật ký", key="del_logs") and st.button("🔴 XÓA NHẬT KÝ"):
            conn = get_db_connection()
            with conn.cursor() as cur: 
                cur.execute("TRUNCATE TABLE audit_logs RESTART IDENTITY")
            conn.commit()
            st.success("Xong!")
            time.sleep(1)
            st.rerun()

elif choice == "⚙️ Tài khoản":
    st.header("⚙️ Tài khoản")
    with st.form("p_form"):
        p1, p2 = st.text_input("Mật khẩu mới", type="password"), st.text_input("Xác nhận", type="password")
        if st.form_submit_button("Đổi mật khẩu") and p1 == p2 and len(p1) >= 6:
            supabase.auth.update_user({"password": p1})
            st.success("Thành công!")
