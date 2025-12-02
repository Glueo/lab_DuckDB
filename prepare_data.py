import duckdb
import glob
import os

def convert_to_csv():
    # 确保输出目录存在
    os.makedirs('data_csv', exist_ok=True)
    
    # 获取 data 目录下的所有 parquet 文件
    parquet_files = glob.glob('data/yellow_tripdata_2019-*.parquet')
    
    if not parquet_files:
        print("错误：在 data/ 目录下没有找到 parquet 文件！请先下载数据。")
        return

    print(f"找到 {len(parquet_files)} 个 Parquet 文件，准备转换为 CSV...")
    
    con = duckdb.connect()
    
    for p_file in parquet_files:
        # 生成对应的 csv 文件名
        filename = os.path.basename(p_file).replace('.parquet', '.csv')
        csv_path = os.path.join('data_csv', filename)
        
        if os.path.exists(csv_path):
            print(f"跳过已存在文件: {csv_path}")
            continue
            
        print(f"正在转换: {p_file} -> {csv_path}")
        # 使用 DuckDB 高效转换
        con.sql(f"COPY (SELECT * FROM '{p_file}') TO '{csv_path}' (HEADER, DELIMITER ',')")
        
    print("所有文件转换完成！")

if __name__ == "__main__":
    convert_to_csv()