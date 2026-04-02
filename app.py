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
        # Lấy URI từ Secrets. 
        # KHUYÊN DÙNG: Sử dụng URI của Connection Pooler (Port 6543) thay vì Port 5432
        conn = psycopg2.connect(st.secrets["SUPABASE_DB_URL"])
        return conn
    except Exception as e:
        st.error(f"Lỗi kết nối cơ sở dữ liệu: {e}")
        return None

# --- 3. LOGIC TRUY VẤN DỮ LIỆU ---
def search_participants(search_query, search_type, limit=50):
    conn = get_db_connection()
    if not conn: return []
    
    cur = conn.cursor()
    try:
        if search_type == "Mã BHXH":
            query = "SELECT ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, sdt, han_the FROM participants WHERE ma_so_bhxh = %s LIMIT %s"
            cur.execute(query, (search_query, limit))
        elif search_type == "CCCD":
            query = "SELECT ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, sdt, han_the FROM participants WHERE cccd = %s LIMIT %s"
            cur.execute(query, (search_query, limit))
        else: # Tìm kiếm mờ theo tên
            search_norm = unidecode(search_query).lower()
            query = """
                SELECT ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, sdt, han_the 
                FROM participants 
                WHERE ho_ten_unsigned % %s OR ho_ten_unsigned ILIKE %s
                ORDER BY similarity(ho_ten_unsigned, %s) DESC
                LIMIT %s
            """
            cur.execute(query, (search_norm, f"%{search_norm}%", search_norm, limit))
        
        return cur.fetchall()
    except Exception as e:
        st.error(f"Lỗi khi tìm kiếm: {e}")
        return []
    finally:
        cur.close()
        conn.close()

# --- 4. LOGIC NHẬP DỮ LIỆU BATCH (ADMIN) ---
def import_excel_to_db(df):
    # Tự động ánh xạ tên cột từ file của bạn sang database
    mapping = {
        'ma so bhxh': 'ma_so_bhxh',
        'ma the bhyt': 'ma_the_bhyt',
        'ho ten': 'ho_ten',
        'ngay sinh': 'ngay_sinh',
        'soCmnd': 'cccd',
        'soDienT': 'sdt',
        'diaChiLh': 'dia_chi',
        'hanTheDen': 'han_the'
    }
    
    # Đổi tên cột nếu tìm thấy trong file
    df = df.rename(columns=mapping)
    
    conn = get_db_connection()
    if not conn: return
    cur = conn.cursor()
    
    try:
        data = []
        # Kiểm tra các cột tối thiểu cần thiết
        if 'ma_so_bhxh' not in df.columns or 'ho_ten' not in df.columns:
            st.error("File thiếu các cột quan trọng (Mã số BHXH hoặc Họ tên). Hãy kiểm tra lại tiêu đề cột.")
            return

        for _, row in df.iterrows():
            def parse_date(val):
                if pd.isnull(val) or str(val).strip() == "": return None
                try:
                    return pd.to_datetime(val).date()
                except:
                    return None

            def clean_str(val):
                if pd.isnull(val): return ""
                return str(val).split('.')[0].strip()

            data.append((
                clean_str(row['ma_so_bhxh']), 
                clean_str(row.get('ma_the_bhyt', '')), 
                str(row['ho_ten']).strip(),
                parse_date(row.get('ngay_sinh')),
                clean_str(row.get('cccd', '')),
                clean_str(row.get('sdt', '')),
                str(row.get('dia_chi', '')).strip(),
                parse_date(row.get('han_the'))
            ))

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
        
        batch_size = 5000
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            execute_values(cur, sql, batch)
            conn.commit()
            yield min(i + batch_size, len(data))
            
    except Exception as e:
        st.error(f"Lỗi xử lý cơ sở dữ liệu: {e}")
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
        col1, col2 = st.columns([3, 1])
        with col1:
            q = st.text_input("Nhập thông tin (Tên/Mã BHXH/CCCD)", placeholder="Ví dụ: Nguyễn Văn A...")
        with col2:
            stype = st.selectbox("Tìm kiếm theo", ["Tên", "Mã BHXH", "CCCD"])

        if q:
            with st.spinner("Đang tìm kiếm..."):
                start_time = time.time()
                data = search_participants(q, stype)
                duration = time.time() - start_time
                
                if data:
                    st.success(f"Tìm thấy {len(data)} bản ghi trong {duration:.3f} giây.")
                    df_res = pd.DataFrame(data, columns=["Mã BHXH", "Thẻ BHYT", "Họ Tên", "Ngày Sinh", "CCCD", "SĐT", "Hạn Thẻ"])
                    df_res['CCCD'] = df_res['CCCD'].apply(lambda x: f"{x[:3]}****{x[-3:]}" if x and len(str(x)) > 6 else x)
                    df_res['SĐT'] = df_res['SĐT'].apply(lambda x: f"{x[:4]}***{x[-3:]}" if x and len(str(x)) > 7 else x)
                    st.dataframe(df_res, use_container_width=True, hide_index=True)
                else:
                    st.warning("Không tìm thấy dữ liệu phù hợp.")

    else: # Quản trị viên
        st.subheader("📥 Nhập dữ liệu hàng loạt (Hỗ trợ Excel XLSX/XLSB)")
        uploaded_file = st.file_uploader("Chọn tệp Excel (.xlsx hoặc .xlsb)", type=["xlsx", "xlsb"])
        if uploaded_file:
            try:
                engine = 'pyxlsb' if uploaded_file.name.endswith('.xlsb') else None
                df_preview = pd.read_excel(uploaded_file, engine=engine)
                st.write(f"📊 Phát hiện: **{len(df_preview):,}** bản ghi.")
                st.dataframe(df_preview.head(5))
                
                if st.button("🚀 Bắt đầu nạp dữ liệu vào hệ thống"):
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    total = len(df_preview)
                    success = False
                    for count in import_excel_to_db(df_preview):
                        percent = count / total
                        progress_bar.progress(percent)
                        status_text.text(f"Đang xử lý: {count:,} / {total:,} hàng...")
                        success = True # Đánh dấu có ít nhất 1 đợt thành công
                    
                    if success:
                        st.success("✅ Đã hoàn thành nạp dữ liệu thành công!")
                        st.balloons()
            except Exception as e:
                st.error(f"Lỗi: {e}")

if __name__ == "__main__":
    main()
