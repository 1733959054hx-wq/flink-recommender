"""
电商推荐系统 - 推荐在线评测
功能：推荐在线评测 CTR/CVR（推荐结果 + 用户行为）
      v2.0 修复版：加入时间过滤，只统计推荐之后发生的用户行为

后端使用方法：
python test_for_CTR_and_CVR.py "2026-05-29 00:00:00" "2026-05-29 11:16:40"
"""

import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import os
import sys
import logging
import io
from datetime import timezone
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
# ==================== 配置 ====================
DB_URL = 'mysql+pymysql://flinkpy:123456@localhost:3306/recommend_db'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 输出路径
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(LOG_DIR, exist_ok=True)

# ==================== 日志配置 ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'test_for_ctr_cvr.log'), encoding='utf-8'),
    ]
)
logger = logging.getLogger(__name__)


# ==================== 数据库工具 ====================
def get_db_connection():
    engine = create_engine(DB_URL)
    return engine.raw_connection()

def get_engine():
    return create_engine(DB_URL)


# ==================== 时间配置 ====================
def get_eval_time_range():
    """获取评测时间范围"""
    default_start = '2026-05-10 09:00:00'
    default_end = '2026-05-10 11:00:00'

    if len(sys.argv) >= 3:
        return sys.argv[1], sys.argv[2]

    print(f"\n  默认时间范围: {default_start} ~ {default_end}")
    use_default = input(f"  使用默认时间范围？(y/n，直接回车=是): ").strip().lower()

    if use_default == 'n':
        start_input = input(f"  请输入开始时间 (格式: YYYY-MM-DD HH:MM:SS): ").strip()
        end_input = input(f"  请输入结束时间 (格式: YYYY-MM-DD HH:MM:SS): ").strip()
        try:
            datetime.strptime(start_input, '%Y-%m-%d %H:%M:%S')
            datetime.strptime(end_input, '%Y-%m-%d %H:%M:%S')
            return start_input, end_input
        except ValueError:
            print(f"  ⚠️ 时间格式错误，使用默认时间范围")
            return default_start, default_end
    else:
        return default_start, default_end


# ==================== 核心函数 ====================
def load_recommendations(start_time, end_time):
    """从 MySQL 加载推荐结果"""
    logger.info(f"加载推荐结果: {start_time} ~ {end_time}")
    engine = get_engine()
    query = f"""
        SELECT id, user_id, item_id, predict_score, final_score, 
               rank_no, recommend_type, create_time
        FROM recommendations
        WHERE create_time >= '{start_time}'
          AND create_time <= '{end_time}'
    """
    df = pd.read_sql(query, engine)
    logger.info(f"推荐结果: {len(df)} 条, 用户 {df['user_id'].nunique()} 个, 商品 {df['item_id'].nunique()} 个")
    return df


def load_user_behaviors(start_time, end_time):
    """从 MySQL 加载用户行为"""
    logger.info(f"加载用户行为: {start_time} ~ {end_time}")
    engine = get_engine()
    query = f"""
        SELECT user_id, item_id, behavior_type, create_time
        FROM user_behavior
        WHERE create_time >= '{start_time}'
          AND create_time <= '{end_time}'
    """
    df = pd.read_sql(query, engine)

    behavior_dist = df['behavior_type'].value_counts() if len(df) > 0 else pd.Series()
    logger.info(f"用户行为: {len(df)} 条, pv={behavior_dist.get('pv',0)}, buy={behavior_dist.get('buy',0)}, "
                f"cart={behavior_dist.get('cart',0)}, fav={behavior_dist.get('fav',0)}")
    return df


def compute_ctr_cvr(recommend_df, behavior_df):
    """
    计算 CTR/CVR（v2.0 修复版）

    核心改进：加入时间先后判断，只统计"推荐之后发生的用户行为"，
    避免将用于生成推荐的历史行为也计入CTR，解决CTR虚高问题。
    """
    logger.info("计算 CTR/CVR（v2.0 时间过滤版）")
    recommend_count = len(recommend_df)
    if recommend_count == 0:
        logger.warning("推荐数据为空")
        return 0.0, 0.0, 0

    # ===== 步骤1：原始方法（无时间过滤）作为对比 =====
    recommend_df['_key'] = recommend_df['user_id'].astype(str) + '_' + recommend_df['item_id'].astype(str)
    behavior_df['_key'] = behavior_df['user_id'].astype(str) + '_' + behavior_df['item_id'].astype(str)

    old_viewed_set = set(behavior_df[behavior_df['behavior_type'] == 'pv']['_key'])
    old_bought_set = set(behavior_df[behavior_df['behavior_type'] == 'buy']['_key'])

    old_viewed_count = sum(1 for key in recommend_df['_key'] if key in old_viewed_set)
    old_bought_count = sum(1 for key in recommend_df['_key'] if key in old_bought_set)

    old_ctr = old_viewed_count / recommend_count
    old_cvr = old_bought_count / old_viewed_count if old_viewed_count > 0 else 0.0

    # ===== 步骤2（核心修复）：加入时间过滤，只算推荐之后的行为 =====
    # 将 recommendations 与 behaviors 按 user_id + item_id 合并
    merged = pd.merge(
        recommend_df[['id', 'user_id', 'item_id', 'create_time']],
        behavior_df[['user_id', 'item_id', 'behavior_type', 'create_time']],
        on=['user_id', 'item_id'],
        suffixes=['_rec', '_beh'],
        how='left'
    )

    # 关键过滤：只保留行为时间 > 推荐时间的记录
    merged = merged[merged['create_time_beh'] > merged['create_time_rec']]

    # 统计推荐后有pv行为的推荐记录数（推荐被点击数）
    viewed_rec_ids = set(
        merged.loc[merged['behavior_type'] == 'pv', 'id'].unique()
    )
    # 统计推荐后有buy行为的推荐记录数（推荐后购买数）
    bought_rec_ids = set(
        merged.loc[merged['behavior_type'] == 'buy', 'id'].unique()
    )

    new_viewed_count = len(viewed_rec_ids)
    new_bought_count = len(bought_rec_ids)

    new_ctr = new_viewed_count / recommend_count
    new_cvr = new_bought_count / new_viewed_count if new_viewed_count > 0 else 0.0

    # ===== 输出详细对比信息 =====

    logger.info("【对比】旧方法（仅交集，无时间过滤）：")
    logger.info(f"  CTR={old_ctr:.4f} ({old_ctr*100:.2f}%), "
                f"CVR={old_cvr:.4f} ({old_cvr*100:.2f}%), "
                f"被浏览 {old_viewed_count} 条, 被购买 {old_bought_count} 条")
    logger.info("【修复】新方法（仅统计推荐后的行为）：")
    logger.info(f"  CTR={new_ctr:.4f} ({new_ctr*100:.2f}%), "
                f"CVR={new_cvr:.4f} ({new_cvr*100:.2f}%), "
                f"推荐后被浏览 {new_viewed_count} 条, 推荐后被购买 {new_bought_count} 条")

    return new_ctr, new_cvr, recommend_count


def print_ctr_details(recommend_df, behavior_df):
    """打印CTR详细分析（诊断辅助）"""
    merged = pd.merge(
        recommend_df[['id', 'user_id', 'item_id', 'create_time']],
        behavior_df[['user_id', 'item_id', 'behavior_type', 'create_time']],
        on=['user_id', 'item_id'],
        suffixes=['_rec', '_beh'],
        how='left'
    )

    # 行为在推荐之前的记录数
    before_rec = merged[merged['create_time_beh'] <= merged['create_time_rec']]
    # 行为在推荐之后的记录数
    after_rec = merged[merged['create_time_beh'] > merged['create_time_rec']]

    total_behavior_matches = len(merged.dropna(subset=['create_time_beh']))
    before_count = len(before_rec.dropna(subset=['create_time_beh']))
    after_count = len(after_rec.dropna(subset=['create_time_beh']))

    print(f"\n  📋 CTR 诊断分析：")
    print(f"    有匹配行为的推荐记录数: {total_behavior_matches}")
    print(f"    行为发生在推荐之前的:    {before_count} ({before_count/total_behavior_matches*100:.1f}%) ← 历史行为（不应计入）")
    print(f"    行为发生在推荐之后的:    {after_count} ({after_count/total_behavior_matches*100:.1f}%) ← 真实推荐效果")


def run_recommend_evaluation():
    """运行推荐在线评测"""
    start_time, end_time = get_eval_time_range()

    start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    end_dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    duration = end_dt - start_dt

    logger.info("=" * 40)
    logger.info(f"开始推荐在线评测(v2.0): {start_time} ~ {end_time} ({duration})")

    print("=" * 60)
    print("  功能点2：推荐在线评测 CTR/CVR（v2.0 时间过滤版）")
    print(f"  时间范围: {start_time} ~ {end_time} ({duration})")
    print(f"  开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    try:
        recommend_df = load_recommendations(start_time, end_time)
        behavior_df = load_user_behaviors(start_time, end_time)

        # 诊断分析
        print_ctr_details(recommend_df, behavior_df)

        # 核心计算（时间过滤版）
        ctr, cvr, recommend_count = compute_ctr_cvr(recommend_df, behavior_df)

        print(f"\n  📊 真实CTR（推荐后点击率） = {ctr:.4f} ({ctr*100:.2f}%)")
        print(f"  📊 真实CVR（推荐后购买率） = {cvr:.4f} ({cvr*100:.2f}%)")
        print(f"  📊 推荐总数 = {recommend_count}")

        save_metrics(ctr, cvr, recommend_count, start_time, end_time)

        print(f"\n  ✅ 推荐评测完成（v2.0）")
        print(f"  详细日志: {os.path.join(LOG_DIR, 'test_for_ctr_cvr.log')}")
        print(f"  结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    except Exception as e:
        logger.error(f"推荐评测失败: {e}", exc_info=True)
        print(f"\n  ❌ 推荐评测失败: {e}")


def save_metrics(ctr, cvr, recommend_count, start_time, end_time):
    """写入 eval_metrics 表（pr_auc 填 -1）"""
    logger.info("写入评估指标")
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            insert_sql = """
                INSERT INTO eval_metrics 
                    (metric_start_time, metric_end_time, pr_auc, ctr, cvr, recommend_count, create_time)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
            """
            cursor.execute(insert_sql, (
                start_time, end_time,
                -1.0,  # pr_auc 占位
                round(ctr, 4), round(cvr, 4), recommend_count
            ))
        conn.commit()
        logger.info("指标写入成功")
    except Exception as e:
        logger.error(f"写入失败: {e}")
        conn.rollback()
    finally:
        conn.close()


def main():
    run_recommend_evaluation()


if __name__ == "__main__":
    main()
