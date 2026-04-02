import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from unidecode import unidecode
import time

# --- 1. CẤU HÌNH TRANG ---
st.set_page_config(
    page_title="Hệ thống Quản lý & Tra cứu BHYT",
    page_icon="🏥",
    layout="wide"
)

# --- 2. KẾT NỐI DATABASE ---
def get_db_connection():
    try:
        # LƯU Ý: Phải sử dụng URI của Connection Pooler (Port 6543)
        # Ví dụ: postgresql://postgres.[ID]:[PASS]@aws-0-ap-southeast-1.pooler.supabase.com:6543/postgres
        conn = psycopg2.connect(st.secrets["SUPABASE_DB_URL"], connect_timeout=10)
        return conn
    except Exception as e:
        return None

# --- 3. LOGIC TRUY VẤN DỮ LIỆU ---
def search_participants(search_query, search_type, limit=100):
    conn = get_db_connection()
    if not conn: 
        st.error("Không thể kết nối cơ sở dữ liệu. Hãy kiểm tra Secrets (Port 6543).")
        return []
    
    cur = conn.cursor()
    try:
        q_clean = search_query.strip()
        
        if search_type == "Mã BHXH":
            query = """
                SELECT ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, sdt, han_the 
                FROM participants 
                WHERE TRIM(ma_so_bhxh) = %s OR ma_so_bhxh ILIKE %s
                LIMIT %s
            """
            cur.execute(query, (q_clean, f"%{q_clean}%", limit))
            
        elif search_type == "CCCD":
            query = """
                SELECT ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, sdt, han_the 
                FROM participants 
                WHERE TRIM(cccd) = %s OR cccd ILIKE %s
                LIMIT %s
            """
            cur.execute(query, (q_clean, f"%{q_clean}%", limit))
            
        else: # Tìm kiếm theo Tên
            q_norm = unidecode(q_clean).lower()
            query = """
                SELECT ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, sdt, han_the 
                FROM participants 
                WHERE 
                    ho_ten_unsigned ILIKE %s 
                    OR (similarity(ho_ten_unsigned, %s) > 0.7)
                ORDER BY 
                    (ho_ten_unsigned = %s) DESC,
                    similarity(ho_ten_unsigned, %s) DESC
                LIMIT %s
            """
            cur.execute(query, (f"%{q_norm}%", q_norm, q_norm, q_norm, limit))
        
        return cur.fetchall()
    except Exception as e:
        st.error(f"Lỗi khi tìm kiếm: {e}")
        return []
    finally:
        cur.close()
        conn.close()

# --- 4. LOGIC NHẬP DỮ LIỆU TỐI ƯU HÓA CAO (CHO 500K DÒNG) ---
def import_excel_to_db(df):
    # Bước 1: Chuẩn hóa tên cột
    df.columns = [str(c).strip().lower() for c in df.columns]
    mapping = {
        'ma so bhxh': 'ma_so_bhxh', 'ma the bhyt': 'ma_the_bhyt',
        'ho ten': 'ho_ten', 'ngay sinh': 'ngay_sinh',
        'socmnd': 'cccd', 'sodient': 'sdt',
        'diachilh': 'dia_chi', 'hantheden': 'han_the'
    }
    df = df.rename(columns=mapping)
    
    # Bước 2: Tiền xử lý véc-tơ hóa
    with st.spinner("Đang chuẩn hóa dữ liệu..."):
        for col in ['ngay_sinh', 'han_the']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

        str_cols = ['ma_so_bhxh', 'ma_the_bhyt', 'ho_ten', 'cccd', 'sdt', 'dia_chi']
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
                df[col] = df[col].replace(['nan', 'None', 'NAT', '<NA>'], '')
            else:
                df[col] = ''

    # Chuyển đổi sang List of Tuples
    data_tuples = list(df[['ma_so_bhxh', 'ma_the_bhyt', 'ho_ten', 'ngay_sinh', 'cccd', 'sdt', 'dia_chi', 'han_the']].itertuples(index=False, name=None))
    del df # Giải phóng bộ nhớ

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
    
    # Nạp theo lô (Batch)
    batch_size = 3000 # Giảm nhẹ để tránh nghẽn kết nối
    total_len = len(data_tuples)
    
    for i in range(0, total_len, batch_size):
        batch = data_tuples[i:i + batch_size]
        
        # Thử kết nối lại cho mỗi batch để tránh timeout
        conn = get_db_connection()
        if not conn:
            st.error(f"Mất kết nối tại dòng {i:,}. Đang thử lại...")
            time.sleep(2)
            conn = get_db_connection()
            if not conn: break

        cur = conn.cursor()
        try:
            execute_values(cur, sql, batch)
            conn.commit()
            yield min(i + batch_size, total_len)
        except Exception as e:
            st.error(f"Lỗi tại lô {i:,}: {e}")
            conn.rollback()
            break
        finally:
            cur.close()
            conn.close()
        
        # Nghỉ ngắn để DB "thở"
        time.sleep(0.1)

# --- 5. GIAO DIỆN CHÍNH ---
def main():
    st.title("🏥 Hệ thống Quản lý & Tra cứu BHYT")
    st.sidebar.header("🛡️ Bảng điều khiển")
    mode = st.sidebar.radio("Chọn chức năng", ["Tra cứu dữ liệu", "Quản trị viên (Nhập dữ liệu)"])

    if mode == "Tra cứu dữ liệu":
        st.subheader("🔍 Tìm kiếm người tham gia")
        col1, col2 = st.columns([3, 1])
        with col1:
            q = st.text_input("Nhập nội dung tìm kiếm...", placeholder="Tên, Mã BHXH hoặc CCCD")
        with col2:
            stype = st.selectbox("Loại tìm kiếm", ["Tên", "Mã BHXH", "CCCD"])

        if q:
            with st.spinner("Đang truy xuất..."):
                start_time = time.time()
                data = search_participants(q, stype)
                duration = time.time() - start_time
                
                if data:
                    st.success(f"Tìm thấy {len(data)} bản ghi trong {duration:.3f} giây.")
                    df_res = pd.DataFrame(data, columns=["Mã BHXH", "Thẻ BHYT", "Họ Tên", "Ngày Sinh", "CCCD", "SĐT", "Hạn Thẻ"])
                    df_res['CCCD'] = df_res['CCCD'].apply(lambda x: f"{x[:3]}****{x[-3:]}" if x and len(str(x)) > 6 else x)
                    st.dataframe(df_res, use_container_width=True, hide_index=True)
                else:
                    st.warning("Không tìm thấy kết quả phù hợp.")

    else: # Quản trị viên
        st.subheader("📥 Nhập dữ liệu lớn (Tối đa 500.000 hàng)")
        uploaded_file = st.file_uploader("Chọn tệp Excel (.xlsx, .xlsb)", type=["xlsx", "xlsb"])
        if uploaded_file:
            try:
                engine = 'pyxlsb' if uploaded_file.name.endswith('.xlsb') else None
                with st.spinner("Đang đọc file..."):
                    df_preview = pd.read_excel(uploaded_file, engine=engine, dtype=str)
                
                st.write(f"📊 Phát hiện: **{len(df_preview):,}** hàng.")
                
                if st.button("🚀 Bắt đầu nạp vào hệ thống"):
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    total = len(df_preview)
                    success_count = 0
                    start_import = time.time()
                    
                    for count in import_excel_to_db(df_preview):
                        percent = count / total
                        progress_bar.progress(percent)
                        status_text.text(f"Đang xử lý: {count:,} / {total:,} dòng...")
                        success_count = count
                    
                    if success_count > 0:
                        st.success(f"✅ Thành công! Đã nạp {success_count:,} dòng trong {int(time.time() - start_import)} giây.")
                        st.balloons()
            except Exception as e:
                st.error(f"Lỗi: {e}")

if __name__ == "__main__":
    main()
