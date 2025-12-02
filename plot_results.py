import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def plot():
    try:
        df = pd.read_csv('benchmark_results.csv')
    except FileNotFoundError:
        print("未找到 benchmark_results.csv，请先运行评测脚本。")
        return

    sns.set_theme(style="whitegrid")

    # === 图1：并行扩展性 (折线图) ===
    exp1 = df[df['Experiment'] == 'Exp1_Scalability']
    if not exp1.empty:
        plt.figure(figsize=(10, 6))
        sns.lineplot(data=exp1, x='Threads', y='Time', hue='Query', marker='o', linewidth=2.5)
        plt.title('DuckDB Parallel Scalability (Yellow Taxi 2019 Parquet)', fontsize=14)
        plt.ylabel('Execution Time (seconds)', fontsize=12)
        plt.xlabel('Number of Threads', fontsize=12)
        plt.xticks(sorted(exp1['Threads'].unique()))
        plt.grid(True, which='both', linestyle='--', linewidth=0.5)
        plt.savefig('fig1_scalability.png', dpi=300)
        print("已生成图1: fig1_scalability.png")

    # === 图2：格式对比 (柱状图) ===
    exp2 = df[df['Experiment'] == 'Exp2_Format']
    if not exp2.empty:
        plt.figure(figsize=(10, 6))
        sns.barplot(data=exp2, x='Query', y='Time', hue='Format', palette="viridis")
        plt.title('Format Comparison: Parquet vs CSV (Full Year)', fontsize=14)
        plt.ylabel('Execution Time (seconds)', fontsize=12)
        plt.yscale('log') # 建议使用对数坐标，因为 CSV 通常比 Parquet 慢很多
        plt.savefig('fig2_format_comparison.png', dpi=300)
        print("已生成图2: fig2_format_comparison.png (注意：使用了对数Y轴)")

    # === 图3：数据规模 (柱状图) ===
    exp3 = df[(df['Experiment'] == 'Exp3_Scale') | 
              ((df['Experiment'] == 'Exp1_Scalability') & (df['Threads'] == df['Threads'].max()))]
    # 简单筛选 1个月 vs 12个月的数据
    # 这里需要你根据实际生成的数据做简单的过滤，如果 Exp1 包含 12个月数据
    
if __name__ == "__main__":
    plot()