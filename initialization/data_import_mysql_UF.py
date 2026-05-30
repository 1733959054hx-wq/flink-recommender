"""
导入用户特征数据到 MySQL 数据库（user_features 表）
"""

import pandas as pd
import os
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

# 配置
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'flinkpy',
    'password': '123456',
    'database': 'recommend_db',
    'charset': 'utf8mb4'
}

# 用户特征配置
FEATURES_FILE = 'split_data/user_features.csv'
FEATURES_TABLE_NAME = 'user_features'

BATCH_SIZE = 1000

# 统一使用 now() 的前一天
DEFAULT_UPDATE_TIME = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')


def create_engine_with_pool():
    """创建带连接池配置的数据库引擎"""
    conn_str = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        f"?charset={DB_CONFIG['charset']}"
    )

    engine = create_engine(
        conn_str,
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=3600,
        pool_pre_ping=True,
        echo=False
    )
    return engine


def create_user_features_table(engine):
    """创建 user_features 表（如果不存在）"""
    create_sql = """
    CREATE TABLE IF NOT EXISTS user_features (
        user_id BIGINT PRIMARY KEY,
        pv_count INT DEFAULT 0,
        buy_count INT DEFAULT 0,
        cart_count INT DEFAULT 0,
        fav_count INT DEFAULT 0,
        buy_rate DECIMAL(8,4) DEFAULT 0.0000,
        active_score DECIMAL(8,4) DEFAULT 0.0000,
        high_freq_category BIGINT DEFAULT NULL,
        update_time DATETIME NULL,
        INDEX idx_buy_rate (buy_rate),
        INDEX idx_active_score (active_score)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(create_sql))
            conn.commit()
        print(f"表 {FEATURES_TABLE_NAME} 创建成功或已存在")
    except Exception as e:
        print(f"创建表 {FEATURES_TABLE_NAME} 失败: {e}")
        raise


def import_user_features_data(engine):
    """导入用户特征数据"""
    features_path = os.path.join(os.path.dirname(__file__), FEATURES_FILE)

    if not os.path.exists(features_path):
        print(f"警告：用户特征文件不存在: {features_path}")
        print("跳过用户特征数据导入")
        return True

    print(f"\n{'='*60}")
    print(f"开始导入: {FEATURES_TABLE_NAME}")
    print(f"{'='*60}")
    print(f"正在读取用户特征: {features_path}")
    print(f"update_time 将统一设为: {DEFAULT_UPDATE_TIME}")

    try:
        df = pd.read_csv(features_path, header=0, low_memory=False)
        print(f"CSV文件列名: {df.columns.tolist()}")
        print(f"共 {len(df):,} 条用户特征数据待导入")

        required_columns = ['user_id', 'pv_count', 'buy_count', 'cart_count',
                            'fav_count', 'buy_rate', 'active_score', 'high_freq_category']

        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"错误：缺少必需的列: {missing_columns}")
            return False

        df = df[required_columns]

        print("正在进行数据类型转换...")
        df['user_id'] = pd.to_numeric(df['user_id'], errors='coerce').fillna(0).astype('int64')
        df['pv_count'] = pd.to_numeric(df['pv_count'], errors='coerce').fillna(0).astype('int')
        df['buy_count'] = pd.to_numeric(df['buy_count'], errors='coerce').fillna(0).astype('int')
        df['cart_count'] = pd.to_numeric(df['cart_count'], errors='coerce').fillna(0).astype('int')
        df['fav_count'] = pd.to_numeric(df['fav_count'], errors='coerce').fillna(0).astype('int')
        df['buy_rate'] = pd.to_numeric(df['buy_rate'], errors='coerce').fillna(0.0)
        df['active_score'] = pd.to_numeric(df['active_score'], errors='coerce').fillna(0.0)
        df['high_freq_category'] = pd.to_numeric(df['high_freq_category'], errors='coerce').fillna(0).astype('int64')

        # 统一设置 update_time 为当前时间的前一天
        df['update_time'] = DEFAULT_UPDATE_TIME
        print(f"已为所有记录设置 update_time = {DEFAULT_UPDATE_TIME}")

        original_count = len(df)
        df = df[df['user_id'] != 0]
        print(f"过滤无效数据后剩余 {len(df):,} 条（移除 {original_count - len(df)} 条）")

        duplicate_count = df['user_id'].duplicated().sum()
        if duplicate_count > 0:
            print(f"发现 {duplicate_count} 条重复的user_id，将保留第一条记录")
            df = df.drop_duplicates(subset=['user_id'], keep='first')
            print(f"去重后剩余 {len(df):,} 条")

    except Exception as e:
        print(f"读取或处理CSV文件失败: {e}")
        import traceback
        traceback.print_exc()
        return False

    # 先删除旧表并重建
    try:
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {FEATURES_TABLE_NAME}"))
            conn.commit()
        print(f"已删除旧表 {FEATURES_TABLE_NAME}")

        create_user_features_table(engine)
    except Exception as e:
        print(f"创建表失败: {e}")
        return False

    # 批量导入
    print("开始批量导入用户特征数据...")
    total = len(df)
    success_count = 0
    error_count = 0

    try:
        for i in range(0, total, BATCH_SIZE):
            batch = df.iloc[i:i+BATCH_SIZE]

            try:
                batch.to_sql(
                    FEATURES_TABLE_NAME,
                    engine,
                    if_exists='append',
                    index=False,
                    method=None,
                    chunksize=100
                )
                success_count += len(batch)

            except Exception as e:
                print(f"批次 {i//BATCH_SIZE + 1} 导入失败: {e}")
                error_count += len(batch)

                print("尝试逐行导入...")
                for idx, row in batch.iterrows():
                    try:
                        row_df = pd.DataFrame([row])
                        row_df.to_sql(
                            FEATURES_TABLE_NAME,
                            engine,
                            if_exists='append',
                            index=False,
                            method=None
                        )
                        success_count += 1
                    except Exception as row_e:
                        print(f"行 {idx} (user_id={row['user_id']}) 导入失败: {row_e}")
                        error_count += 1

            progress = min(i + BATCH_SIZE, total)
            print(f"进度: {progress:,}/{total:,} ({progress/total*100:.1f}%) - 成功: {success_count}, 失败: {error_count}")

            time.sleep(0.1)

    except KeyboardInterrupt:
        print("\n用户中断导入")
    except Exception as e:
        print(f"导入过程发生严重错误: {e}")
        import traceback
        traceback.print_exc()
        return False

    # 验证导入结果
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {FEATURES_TABLE_NAME}"))
            db_count = result.scalar()
            print(f"\n{FEATURES_TABLE_NAME} 导入完成统计:")
            print(f"  预期导入: {total:,} 条")
            print(f"  成功导入: {success_count:,} 条")
            print(f"  失败数量: {error_count:,} 条")
            print(f"  数据库实际记录: {db_count:,} 条")

            if db_count > 0:
                result = conn.execute(text(f"SELECT * FROM {FEATURES_TABLE_NAME} LIMIT 5"))
                print("\n数据库中的前5条数据:")
                for row in result:
                    print(f"  {row}")

                result = conn.execute(text(f"""
                    SELECT 
                        COUNT(*) as total_users,
                        AVG(pv_count) as avg_pv,
                        AVG(buy_count) as avg_buy,
                        AVG(cart_count) as avg_cart,
                        AVG(fav_count) as avg_fav,
                        AVG(buy_rate) as avg_buy_rate,
                        AVG(active_score) as avg_active_score
                    FROM {FEATURES_TABLE_NAME}
                """))
                print("\n用户特征统计:")
                row = result.fetchone()
                print(f"  总用户数: {row[0]:,}")
                print(f"  平均PV: {row[1]:.2f}")
                print(f"  平均购买: {row[2]:.2f}")
                print(f"  平均加购: {row[3]:.2f}")
                print(f"  平均收藏: {row[4]:.2f}")
                print(f"  平均购买率: {row[5]:.4f}")
                print(f"  平均活跃分: {row[6]:.4f}")

    except Exception as e:
        print(f"验证导入结果失败: {e}")

    return error_count == 0


if __name__ == '__main__':
    print("=" * 60)
    print("用户特征数据导入工具")
    print("=" * 60)
    print(f"待导入表: {FEATURES_TABLE_NAME}")

    start_time = time.time()

    try:
        engine = create_engine_with_pool()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("数据库连接成功")
    except Exception as e:
        print(f"数据库连接失败: {e}")
        exit(1)

    success = import_user_features_data(engine)
    engine.dispose()

    elapsed_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"总耗时: {elapsed_time:.1f} 秒")
    if success:
        print("🎉 用户特征数据导入完成！")
    else:
        print("⚠️ 数据导入有部分失败，请检查日志")