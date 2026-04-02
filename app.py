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
        # Khuyên dùng Connection Pooler (Port 6543) để tránh lỗi connection
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
        # Làm sạch input tìm kiếm
        q_clean = search_query.strip()
        
        if search_type == "Mã BHXH":
            # Sử dụng ILIKE và % để tìm kiếm linh hoạt hơn (phòng trường hợp mã có khoảng trắng)
            query = """
                SELECT ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, sdt, han_the 
                FROM participants 
                WHERE ma_so_bhxh ILIKE %s OR ma_so_bhxh = %s
                LIMIT %s
            """
            cur.execute(query, (f"%{q_clean}%", q_clean, limit))
            
        elif search_type == "CCCD":
            query = """
                SELECT ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, sdt, han_the 
                FROM participants 
                WHERE cccd ILIKE %s OR cccd = %s
                LIMIT %s
            """
            cur.execute(query, (f"%{q_clean}%", q_clean, limit))
            
        else: # Tìm kiếm theo Tên (Nâng cấp logic)
            # 1. Chuẩn hóa chuỗi tìm kiếm (không dấu, viết thường)
            q_norm = unidecode(q_clean).lower()
            # 2. Chuỗi tìm kiếm không khoảng trắng
            q_no_space = q_norm.replace(" ", "")
            
            query = """
                SELECT ma_so_bhxh, ma_the_bhyt, ho_ten, ngay_sinh, cccd, sdt, han_the 
                FROM participants 
                WHERE 
                    -- Ưu tiên 1: Khớp chính xác cụm từ không dấu
                    ho_ten_unsigned ILIKE %s 
                    -- Ưu tiên 2: Khớp khi loại bỏ toàn bộ khoảng trắng (ví dụ: havanly)
                    OR REPLACE(ho_ten_unsigned, ' ', '') = %s
                    -- Ưu tiên 3: Độ tương đồng cao (similarity > 0.5)
                    OR (similarity(ho_ten_unsigned, %s) > 0.5)
                ORDER BY 
                    (ho_ten_unsigned = %s) DESC, -- Chính xác tuyệt đối lên đầu
                    similarity(ho_ten_unsigned, %s) DESC
                LIMIT %s
            """
            cur.execute(query, (f"%{q_norm}%", q_no_space, q_norm, q_norm, q_norm, limit))
        
        return cur.fetchall()
    except Exception as e:
        st.error(f"Lỗi khi tìm kiếm: {e}")
        return []
    finally:
        cur.close()
        conn.close()

# --- 4. LOGIC NHẬP DỮ LIỆU BATCH ---
def import_excel_to_db(df):
    # Chuẩn hóa tên cột
    df.columns = [str(c).strip().lower() for c in df.columns]
    
    # Ánh xạ tên cột linh hoạt (hỗ trợ file của bạn)
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
        st.error("Lỗi kết nối cơ sở dữ liệu. Hãy đảm bảo bạn đang dùng Connection Pooler (Port 6543).")
        return
    
    cur = conn.cursor()
    try:
        data = []
        # Kiểm tra cột tối thiểu
        if 'ma_so_bhxh' not in df.columns or 'ho_ten' not in df.columns:
            st.error(f"File thiếu cột 'ma so bhxh' hoặc 'ho ten'. Cột hiện có: {list(df.columns)}")
            return

        for _, row in df.iterrows():
            def parse_date(val):
                if pd.isnull(val) or str(val).strip() == "": return None
                try: return pd.to_datetime(val).date()
                except: return None

            def clean_str(val):
                if pd.isnull(val): return ""
                s = str(val).strip()
                # Loại bỏ số thập phân nếu Excel tự hiểu là float (123.0 -> 123)
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
        
        batch_size = 2000
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
        
        # Thêm hướng dẫn tìm kiếm
        st.caption("Mẹo: Bạn có thể tìm tên không dấu hoặc viết liền (ví dụ: havanly)")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            q = st.text_input("Nhập nội dung tìm kiếm...", placeholder="Nhập tên, mã BHXH hoặc số CCCD")
        with col2:
            stype = st.selectbox("Loại tìm kiếm", ["Tên", "Mã BHXH", "CCCD"])

        if q:
            with st.spinner("Đang truy xuất dữ liệu..."):
                start_time = time.time()
                data = search_participants(q, stype)
                duration = time.time() - start_time
                
                if data:
                    st.success(f"Tìm thấy {len(data)} bản ghi trong {duration:.3f} giây.")
                    df_res = pd.DataFrame(data, columns=["Mã BHXH", "Thẻ BHYT", "Họ Tên", "Ngày Sinh", "CCCD", "SĐT", "Hạn Thẻ"])
                    
                    # Data Masking
                    df_res['CCCD'] = df_res['CCCD'].apply(lambda x: f"{x[:3]}****{x[-3:]}" if x and len(str(x)) > 6 else x)
                    
                    st.dataframe(df_res, use_container_width=True, hide_index=True)
                    
                    st.download_button(
                        "📥 Tải kết quả này (CSV)",
                        df_res.to_csv(index=False).encode('utf-8-sig'),
                        f"tra_cuu_{q}.csv",
                        "text/csv"
                    )
                else:
                    st.warning("Không tìm thấy kết quả phù hợp. Hãy kiểm tra lại từ khóa hoặc loại tìm kiếm.")

    else: # Quản trị viên
        st.subheader("📥 Nhập dữ liệu từ Excel (XLSX/XLSB)")
        uploaded_file = st.file_uploader("Chọn tệp dữ liệu", type=["xlsx", "xlsb"])
        if uploaded_file:
            try:
                engine = 'pyxlsb' if uploaded_file.name.endswith('.xlsb') else None
                df_preview = pd.read_excel(uploaded_file, engine=engine)
                st.write(f"📊 Phát hiện: **{len(df_preview):,}** hàng dữ liệu.")
                st.dataframe(df_preview.head(5))
                
                if st.button("🚀 Thực hiện nạp vào hệ thống"):
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    total = len(df_preview)
                    success_count = 0
                    for count in import_excel_to_db(df_preview):
                        percent = count / total
                        progress_bar.progress(percent)
                        status_text.text(f"Đang xử lý: {count:,} / {total:,} hàng...")
                        success_count = count
                    
                    if success_count > 0:
                        st.success(f"✅ Đã nạp thành công {success_count:,} bản ghi!")
                        st.balloons()
            except Exception as e:
                st.error(f"Lỗi: {e}")

if __name__ == "__main__":
    main()
