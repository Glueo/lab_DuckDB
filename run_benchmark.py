import duckdb
import time
import pandas as pd
import psutil
import os

# === 配置区域 ===
# 你的 CPU 核心数 (根据实际情况修改，比如 4, 8, 16)
MAX_CORES = 6
# 测试的线程数序列 (实验一)
THREADS_LIST = [1, 2, 4, 8]
# 重复运行次数 (取平均值消除抖动)
ITERATIONS = 3
# 结果保存文件
RESULTS_FILE = "benchmark_results.csv"

# === 查询定义 (基于作业要求 Q1-Q3) ===
# {source} 占位符会被替换为具体的文件路径
QUERIES = {
    "Q1_Simple_Agg": """
        SELECT count(*), avg(total_amount)
        FROM {source};
    """,
    "Q2_Filter_Group": """
        SELECT passenger_count, avg(trip_distance) AS avg_dist
        FROM {source}
        WHERE trip_distance > 0 AND total_amount > 0
        GROUP BY passenger_count
        ORDER BY avg_dist DESC;
    """,
    "Q3_Complex_Group": """
        SELECT PULocationID, DOLocationID, count(*) AS trip_count
        FROM {source}
        GROUP BY PULocationID, DOLocationID
        ORDER BY trip_count DESC
        LIMIT 10;
    """,
}

# === 数据源定义 ===
# Parquet: 使用 glob 模式匹配所有文件
SRC_PARQUET_ALL = "'data/yellow_tripdata_2019-*.parquet'"
# Parquet: 仅一个月 (用于实验三：规模测试)
SRC_PARQUET_1M = "'data/yellow_tripdata_2019-01.parquet'"
# CSV: 使用 glob 模式匹配所有 CSV
SRC_CSV_ALL = "read_csv_auto('data_csv/yellow_tripdata_2019-*.csv')"


def clear_cache():
    """尝试清理系统缓存 (需要 sudo 权限，否则只打印提示)"""
    try:
        # 注意：这行命令在普通用户权限下会失败，可以注释掉
        # os.system("sync; echo 3 > /proc/sys/vm/drop_caches")
        pass
    except:
        pass


def run_query(con, sql, threads, label):
    con.execute(f"PRAGMA threads={threads}")

    times = []
    for i in range(ITERATIONS):
        clear_cache()
        start = time.time()
        con.sql(sql).fetchall()  # 执行并获取所有结果以确保计算完成
        end = time.time()
        times.append(end - start)

    avg_time = sum(times) / len(times)
    print(f"  [{label}] Threads={threads}: {avg_time:.4f}s")
    return avg_time


def main():
    results = []
    con = duckdb.connect()

    print(f"检测到物理核心数: {MAX_CORES}, 开始评测...")

    # === 实验一：并行度扩展性 (Parquet, Full Year) ===
    print("\n=== Experiment 1: Parallel Scalability (Parquet) ===")
    for t in THREADS_LIST:
        # 限制线程数不超过物理核心太多，避免过度竞争
        if t > MAX_CORES * 2:
            break

        for q_name, q_sql in QUERIES.items():
            sql = q_sql.format(source=SRC_PARQUET_ALL)
            avg_time = run_query(con, sql, t, q_name)
            results.append(
                {
                    "Experiment": "Exp1_Scalability",
                    "Query": q_name,
                    "Threads": t,
                    "Format": "Parquet",
                    "DataScale": "12_Months",
                    "Time": avg_time,
                }
            )

    # === 实验二：数据格式对比 (Max Threads, Full Year) ===
    print("\n=== Experiment 2: Format Comparison (Parquet vs CSV) ===")
    # 使用最大线程数进行对比
    best_threads = MAX_CORES

    # 1. 测 CSV
    for q_name, q_sql in QUERIES.items():
        sql = q_sql.format(source=SRC_CSV_ALL)
        avg_time = run_query(con, sql, best_threads, f"{q_name}_CSV")
        results.append(
            {
                "Experiment": "Exp2_Format",
                "Query": q_name,
                "Threads": best_threads,
                "Format": "CSV",
                "DataScale": "12_Months",
                "Time": avg_time,
            }
        )

    # 2. 测 Parquet (复用实验一的数据，或者重新跑一次)
    for q_name, q_sql in QUERIES.items():
        sql = q_sql.format(source=SRC_PARQUET_ALL)
        avg_time = run_query(con, sql, best_threads, f"{q_name}_Parquet")
        results.append(
            {
                "Experiment": "Exp2_Format",
                "Query": q_name,
                "Threads": best_threads,
                "Format": "Parquet",
                "DataScale": "12_Months",
                "Time": avg_time,
            }
        )

    # === 实验三：数据规模扩展性 (Parquet, Max Threads) ===
    print("\n=== Experiment 3: Data Scale (1 Month vs 12 Months) ===")
    # 1. 跑 1 个月的数据
    for q_name, q_sql in QUERIES.items():
        sql = q_sql.format(source=SRC_PARQUET_1M)
        avg_time = run_query(con, sql, best_threads, f"{q_name}_1M")
        results.append(
            {
                "Experiment": "Exp3_Scale",
                "Query": q_name,
                "Threads": best_threads,
                "Format": "Parquet",
                "DataScale": "01_Month",
                "Time": avg_time,
            }
        )

    # 12 个月的数据已经在 Exp1/2 中跑过，这里为了图表方便再记录一次逻辑上的条目
    # (或者后期在绘图脚本里处理，这里为了简单重新跑一次或复用)

    con.close()

    # 保存
    df = pd.DataFrame(results)
    df.to_csv(RESULTS_FILE, index=False)
    print(f"\n所有测试完成！结果已保存至 {RESULTS_FILE}")


if __name__ == "__main__":
    main()
