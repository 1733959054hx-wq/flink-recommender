"""
导入用户行为训练数据到 MySQL 数据库（user_behavior 表）
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

# 训练集配置
TRAIN_FILE = 'split_data/train.csv'
TRAIN_TABLE_NAME = 'user_behavior'

BATCH_SIZE = 1000

# 统一使用 now() 的前一天
DEFAULT_CREATE_TIME = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')


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


def create_user_behavior_table(engine):
    """创建 user_behavior 表（如果不存在）"""
    create_sql = """
    CREATE TABLE IF NOT EXISTS user_behavior (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id BIGINT NOT NULL,
        item_id BIGINT NOT NULL,
        category_id BIGINT NOT NULL,
        behavior_type VARCHAR(20) NOT NULL,
        behavior_time BIGINT NOT NULL,
        create_time DATETIME NULL,
        INDEX idx_user_id (user_id),
        INDEX idx_item_id (item_id),
        INDEX idx_behavior_type (behavior_type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(create_sql))
            conn.commit()
        print(f"表 {TRAIN_TABLE_NAME} 创建成功或已存在")
    except Exception as e:
        print(f"创建表 {TRAIN_TABLE_NAME} 失败: {e}")
        raise


def import_user_behavior_data(engine):
    """导入用户行为训练数据"""
    train_path = os.path.join(os.path.dirname(__file__), TRAIN_FILE)

    if not os.path.exists(train_path):
        print(f"错误：训练集文件不存在: {train_path}")
        print("请先运行 data_split.py 切分数据")
        return False

    print(f"\n{'='*60}")
    print(f"开始导入: {TRAIN_TABLE_NAME}")
    print(f"{'='*60}")
    print(f"正在读取训练集: {train_path}")
    print(f"create_time 将统一设为: {DEFAULT_CREATE_TIME}")

    try:
        df = pd.read_csv(train_path, header=0, low_memory=False)
        print(f"CSV文件列名: {df.columns.tolist()}")
        print(f"共 {len(df):,} 条数据待导入")

        required_columns = ['user_id', 'item_id', 'category_id', 'behavior_type', 'behavior_time']

        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"错误：缺少必需的列: {missing_columns}")
            return False

        df = df[required_columns]

        print("正在进行数据类型转换...")
        df['user_id'] = pd.to_numeric(df['user_id'], errors='coerce').fillna(0).astype('int64')
        df['item_id'] = pd.to_numeric(df['item_id'], errors='coerce').fillna(0).astype('int64')
        df['category_id'] = pd.to_numeric(df['category_id'], errors='coerce').fillna(0).astype('int64')
        df['behavior_type'] = df['behavior_type'].astype(str)
        df['behavior_time'] = pd.to_numeric(df['behavior_time'], errors='coerce').fillna(0).astype('int64')

        # 统一设置 create_time 为当前时间的前一天
        df['create_time'] = DEFAULT_CREATE_TIME
        print(f"已为所有记录设置 create_time = {DEFAULT_CREATE_TIME}")

        original_count = len(df)
        df = df[(df['user_id'] != 0) & (df['item_id'] != 0)]
        print(f"过滤无效数据后剩余 {len(df):,} 条（移除 {original_count - len(df)} 条）")

    except Exception as e:
        print(f"读取或处理CSV文件失败: {e}")
        import traceback
        traceback.print_exc()
        return False

    # 先删除旧表并重建
    try:
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {TRAIN_TABLE_NAME}"))
            conn.commit()
        print(f"已删除旧表 {TRAIN_TABLE_NAME}")

        create_user_behavior_table(engine)
    except Exception as e:
        print(f"创建表失败: {e}")
        return False

    # 批量导入
    print("开始批量导入数据...")
    total = len(df)
    success_count = 0
    error_count = 0

    try:
        for i in range(0, total, BATCH_SIZE):
            batch = df.iloc[i:i+BATCH_SIZE]

            try:
                batch.to_sql(
                    TRAIN_TABLE_NAME,
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
                            TRAIN_TABLE_NAME,
                            engine,
                            if_exists='append',
                            index=False,
                            method=None
                        )
                        success_count += 1
                    except Exception as row_e:
                        print(f"行 {idx} 导入失败: {row_e}")
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
            result = conn.execute(text(f"SELECT COUNT(*) FROM {TRAIN_TABLE_NAME}"))
            db_count = result.scalar()
            print(f"\n{TRAIN_TABLE_NAME} 导入完成统计:")
            print(f"  预期导入: {total:,} 条")
            print(f"  成功导入: {success_count:,} 条")
            print(f"  失败数量: {error_count:,} 条")
            print(f"  数据库实际记录: {db_count:,} 条")

            if db_count > 0:
                result = conn.execute(text(f"SELECT * FROM {TRAIN_TABLE_NAME} LIMIT 3"))
                print("\n数据库中的前3条数据:")
                for row in result:
                    print(f"  {row}")

                result = conn.execute(text(f"""
                    SELECT behavior_type, COUNT(*) as count 
                    FROM {TRAIN_TABLE_NAME} 
                    GROUP BY behavior_type
                """))
                print("\n行为类型分布:")
                for row in result:
                    print(f"  {row[0]}: {row[1]:,}")

    except Exception as e:
        print(f"验证导入结果失败: {e}")

    return error_count == 0


if __name__ == '__main__':
    print("=" * 60)
    print("用户行为数据导入工具")
    print("=" * 60)
    print(f"待导入表: {TRAIN_TABLE_NAME}")

    start_time = time.time()

    try:
        engine = create_engine_with_pool()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("数据库连接成功")
    except Exception as e:
        print(f"数据库连接失败: {e}")
        exit(1)

    success = import_user_behavior_data(engine)
    engine.dispose()

    elapsed_time = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"总耗时: {elapsed_time:.1f} 秒")
    if success:
        print("🎉 用户行为数据导入完成！")
    else:
        print("⚠️ 数据导入有部分失败，请检查日志")