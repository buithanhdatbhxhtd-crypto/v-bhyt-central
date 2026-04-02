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
        # LƯU Ý QUAN TRỌNG: 
        # Hãy vào Supabase -> Settings -> Database -> Connection Pooler
        # Chọn Mode: Transaction và Copy URI mới (thường có Port là 6543)
        # Dán URI mới này vào Secrets trên Streamlit Cloud thay cho mã cũ.
        conn = psycopg2.connect(st.secrets["SUPABASE_DB_URL"])
        return conn
    except Exception as e:
        # Trả về None thay vì hiện lỗi ngay tại đây để logic bên ngoài xử lý
        return None

# --- 3. LOGIC TRUY VẤN DỮ LIỆU ---
def search_participants(search_query, search_type, limit=50):
    conn = get_db_connection()
    if not conn: 
        st.error("Không thể kết nối cơ sở dữ liệu. Vui lòng kiểm tra cấu hình SUPABASE_DB_URL trong Secrets.")
        return []
    
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
            # SỬA LỖI: Dùng %% để escape dấu % của toán tử similarity trong PostgreSQL
            query = """
                SELECT ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, sdt, han_the 
                FROM participants 
                WHERE ho_ten_unsigned %% %s OR ho_ten_unsigned ILIKE %s
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
    # Chuẩn hóa tên cột (bỏ khoảng trắng, viết thường) để khớp chính xác
    df.columns = [str(c).strip().lower() for c in df.columns]
    
    # Ánh xạ tên cột linh hoạt hơn dựa trên hình ảnh file mẫu của bạn
    mapping = {
        'ma so bhxh': 'ma_so_bhxh',
        'ma the bhyt': 'ma_the_bhyt',
        'ho ten': 'ho_ten',
        'ngay sinh': 'ngay_sinh',
        'socmnd': 'cccd',
        'sodient': 'sdt',
        'diachilh': 'dia_chi',
        'hantheden': 'han_the'
    }
    
    df = df.rename(columns=mapping)
    
    conn = get_db_connection()
    if not conn: 
        st.error("Lỗi kết nối cơ sở dữ liệu khi nạp. Hãy thử dùng Connection Pooler (Port 6543).")
        return
    
    cur = conn.cursor()
    try:
        data = []
        if 'ma_so_bhxh' not in df.columns or 'ho_ten' not in df.columns:
            st.error(f"File thiếu cột bắt buộc. Các cột hiện có: {list(df.columns)}")
            return

        for _, row in df.iterrows():
            def parse_date(val):
                if pd.isnull(val) or str(val).strip() == "": return None
                try: return pd.to_datetime(val).date()
                except: return None

            def clean_str(val):
                if pd.isnull(val): return ""
                s = str(val).strip()
                return s.split('.')[0] if '.' in s else s

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
        
        batch_size = 2000 # Giảm batch size để ổn định hơn trên gói free
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            execute_values(cur, sql, batch)
            conn.commit()
            yield min(i + batch_size, len(data))
            
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
        st.subheader("📥 Nhập dữ liệu hàng loạt (XLSX/XLSB)")
        uploaded_file = st.file_uploader("Chọn tệp Excel", type=["xlsx", "xlsb"])
        if uploaded_file:
            try:
                engine = 'pyxlsb' if uploaded_file.name.endswith('.xlsb') else None
                df_preview = pd.read_excel(uploaded_file, engine=engine)
                st.write(f"📊 Phát hiện: **{len(df_preview):,}** bản ghi.")
                st.dataframe(df_preview.head(5))
                
                if st.button("🚀 Bắt đầu nạp dữ liệu"):
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    total = len(df_preview)
                    has_data = False
                    for count in import_excel_to_db(df_preview):
                        percent = count / total
                        progress_bar.progress(percent)
                        status_text.text(f"Đang nạp: {count:,} / {total:,} hàng...")
                        has_data = True
                    
                    if has_data:
                        st.success("✅ Đã hoàn thành nạp dữ liệu thành công!")
                        st.balloons()
            except Exception as e:
                st.error(f"Lỗi hệ thống: {e}")

if __name__ == "__main__":
    main()
