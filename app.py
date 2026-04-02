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
        # Sử dụng Connection Pooler (Port 6543) là bắt buộc cho dữ liệu lớn
        conn = psycopg2.connect(st.secrets["SUPABASE_DB_URL"])
        return conn
    except Exception as e:
        return None

# --- 3. LOGIC TRUY VẤN DỮ LIỆU ---
def search_participants(search_query, search_type, limit=100):
    conn = get_db_connection()
    if not conn: 
        st.error("Không thể kết nối cơ sở dữ liệu. Vui lòng kiểm tra lại cấu hình trong Secrets.")
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

# --- 4. LOGIC NHẬP DỮ LIỆU TỐI ƯU HÓA (CHO 500K DÒNG) ---
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
    
    # Bước 2: Kiểm tra cột bắt buộc
    if 'ma_so_bhxh' not in df.columns or 'ho_ten' not in df.columns:
        st.error(f"File thiếu cột bắt buộc. Cột hiện có: {list(df.columns)}")
        return

    # Bước 3: Tiền xử lý véc-tơ hóa (CỰC NHANH)
    with st.spinner("Đang chuẩn hóa định dạng dữ liệu..."):
        # Xử lý Ngày tháng cho toàn bộ cột một lần
        for col in ['ngay_sinh', 'han_the']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
            else:
                df[col] = None

        # Xử lý Chuỗi (giữ số 0, xóa .0 thừa, trim khoảng trắng) cho toàn bộ cột
        str_cols = ['ma_so_bhxh', 'ma_the_bhyt', 'ho_ten', 'cccd', 'sdt', 'dia_chi']
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
                df[col] = df[col].replace(['nan', 'None', 'NAT'], '')
            else:
                df[col] = ''

    # Bước 4: Chuyển sang danh sách Tuple để nạp (itertuples nhanh hơn iterrows)
    data_tuples = list(df[['ma_so_bhxh', 'ma_the_bhyt', 'ho_ten', 'ngay_sinh', 'cccd', 'sdt', 'dia_chi', 'han_the']].itertuples(index=False, name=None))
    
    conn = get_db_connection()
    if not conn: 
        st.error("Không thể kết nối cơ sở dữ liệu. Hãy đảm bảo bạn đang dùng Connection Pooler (Port 6543).")
        return
    
    cur = conn.cursor()
    try:
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
        
        # Tăng batch_size lên 5000 để giảm thời gian giao tiếp DB
        batch_size = 5000
        total_len = len(data_tuples)
        
        for i in range(0, total_len, batch_size):
            batch = data_tuples[i:i + batch_size]
            execute_values(cur, sql, batch)
            conn.commit()
            yield min(i + batch_size, total_len)
            
    except Exception as e:
        st.error(f"Lỗi SQL khi nạp: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

# --- 5. GIAO DIỆN CHÍNH ---
def main():
    st.title("🏥 Hệ thống Quản lý & Tra cứu BHYT")
    st.sidebar.header("🛡️ Bảng điều khiển")
    mode = st.sidebar.radio("Chọn chức năng", ["Tra cứu dữ liệu", "Quản trị viên (Nhập dữ liệu)"])

    if mode == "Tra cứu dữ liệu":
        st.subheader("🔍 Tìm kiếm người tham gia")
        st.caption("Gợi ý: Tìm tên không dấu, mã BHXH hoặc CCCD.")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            q = st.text_input("Nhập nội dung tìm kiếm...", placeholder="Nhập thông tin tại đây")
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
        st.subheader("📥 Nhập dữ liệu hàng loạt (Hỗ trợ tối đa 500.000 hàng)")
        st.warning("⚠️ Hệ thống đã được tối ưu hóa để nạp dữ liệu lớn. Vui lòng không tắt trình duyệt khi đang xử lý.")
        
        uploaded_file = st.file_uploader("Chọn tệp Excel (.xlsx, .xlsb)", type=["xlsx", "xlsb"])
        if uploaded_file:
            try:
                engine = 'pyxlsb' if uploaded_file.name.endswith('.xlsb') else None
                # Đọc file (Dùng dtype=str để giữ số 0)
                with st.spinner("Đang đọc file Excel..."):
                    df_preview = pd.read_excel(uploaded_file, engine=engine, dtype=str)
                
                st.write(f"📊 Phát hiện: **{len(df_preview):,}** hàng dữ liệu.")
                st.dataframe(df_preview.head(5)) 
                
                if st.button("🚀 Bắt đầu thực hiện nạp"):
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    total = len(df_preview)
                    success_count = 0
                    
                    start_import = time.time()
                    for count in import_excel_to_db(df_preview):
                        percent = count / total
                        progress_bar.progress(percent)
                        status_text.text(f"Đang nạp: {count:,} / {total:,} dòng...")
                        success_count = count
                    
                    end_import = time.time()
                    if success_count > 0:
                        st.success(f"✅ Hoàn tất! Đã nạp thành công {success_count:,} bản ghi trong {int(end_import - start_import)} giây.")
                        st.balloons()
            except Exception as e:
                st.error(f"Lỗi hệ thống: {e}")

if __name__ == "__main__":
    main()
