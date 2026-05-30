"""
电商推荐系统 - 模型离线评测
功能：模型离线评测 PR-AUC（测试集 + PMML模型）
"""

import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine
import os
import sys
import logging
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
# ==================== 配置 ====================
DB_URL = 'mysql+pymysql://flinkpy:123456@localhost:3306/recommend_db'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 模型离线评测配置
PMML_MODEL_PATH = os.path.join(BASE_DIR, "model", "recommend_model_champion.pmml")

# CSV 数据文件路径
DATA_DIR = os.path.join(BASE_DIR, "data")
USER_BEHAVIOR_CSV = os.path.join(DATA_DIR, "user_behavior_for_test.csv")
USER_FEATURES_CSV = os.path.join(DATA_DIR, "user_features_for_test.csv")

# 输出路径
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")
LOG_DIR = os.path.join(BASE_DIR, "logs")

# 确保目录存在
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# ==================== 日志配置 ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'test_for_model.log'), encoding='utf-8'),
    ]
)
logger = logging.getLogger(__name__)


# ==================== 数据库工具 ====================
def get_db_connection():
    engine = create_engine(DB_URL)
    return engine.raw_connection()


# ==================== 核心函数 ====================
def load_test_data():
    """Step 1: 从 CSV 文件加载测试集"""
    logger.info("Step 1: 加载测试集")
    df = pd.read_csv(USER_BEHAVIOR_CSV)
    df['label'] = (df['behavior_type'] == 'buy').astype(int)
    df['user_id'] = df['user_id'].astype(int)
    df['item_id'] = df['item_id'].astype(int)
    df['category_id'] = df['category_id'].astype(str)
    if 'behavior_time' in df.columns:
        df = df.rename(columns={'behavior_time': 'timestamp'})

    logger.info(f"测试集加载完成: 总记录 {len(df)} 条, 正样本 {df['label'].sum()} 条, "
                f"用户 {df['user_id'].nunique()} 个, 商品 {df['item_id'].nunique()} 个")
    return df


def load_user_features():
    """Step 2: 从 CSV 文件加载用户特征"""
    logger.info("Step 2: 加载用户特征")
    df = pd.read_csv(USER_FEATURES_CSV)
    df = df[['user_id', 'buy_rate', 'active_score', 'high_freq_category']].copy()
    df['user_id'] = df['user_id'].astype(int)
    df['high_freq_category'] = df['high_freq_category'].astype(str)

    logger.info(f"用户特征加载完成: {len(df)} 条, buy_rate 均值 {df['buy_rate'].mean():.4f}, "
                f"active_score 均值 {df['active_score'].mean():.2f}")
    return df


def compute_interaction_features(test_df):
    """Step 3: 从训练集历史行为 CSV 计算交互特征"""
    logger.info("Step 3: 计算历史交互特征")
    csv_path = os.path.join(DATA_DIR, "history_user_behavior(from_train).csv")

    user_item_pairs = list(set(zip(test_df['user_id'], test_df['item_id'])))
    logger.info(f"待查询的 user-item 对数: {len(user_item_pairs)}")

    if len(user_item_pairs) == 0:
        return pd.DataFrame(columns=['user_id', 'item_id', 'pv_count_5min', 'is_cart_or_fav'])

    history_df = pd.read_csv(csv_path)
    history_df['user_id'] = history_df['user_id'].astype(int)
    history_df['item_id'] = history_df['item_id'].astype(int)
    history_df = history_df[history_df['behavior_type'].isin(['pv', 'cart', 'fav'])]

    agg_df = history_df.groupby(['user_id', 'item_id']).agg(
        pv_count_5min=('behavior_type', 'count'),
        is_cart_or_fav=('behavior_type', lambda x: 1 if any(t in ('cart', 'fav') for t in x) else 0)
    ).reset_index()
    agg_df['pv_count_5min'] = agg_df['pv_count_5min'].clip(upper=20)

    pairs_df = pd.DataFrame(user_item_pairs, columns=['user_id', 'item_id'])
    df = pairs_df.merge(agg_df, on=['user_id', 'item_id'], how='left')
    df['pv_count_5min'] = df['pv_count_5min'].fillna(0).astype(int)
    df['is_cart_or_fav'] = df['is_cart_or_fav'].fillna(0).astype(int)

    df.to_csv(os.path.join(OUTPUT_DIR, "step3_interaction_features.csv"), index=False)

    has_interaction = int((df['pv_count_5min'] > 0).sum())
    coverage = has_interaction / len(user_item_pairs) * 100
    logger.info(f"交互特征计算完成: 有交互 {has_interaction} 对 ({coverage:.2f}%), "
                f"pv_count_5min 均值 {df['pv_count_5min'].mean():.2f}, "
                f"is_cart_or_fav 占比 {df['is_cart_or_fav'].sum()/len(df)*100:.2f}%")
    return df


def build_features(test_df, user_features_df, interaction_df):
    """Step 4: 特征工程"""
    logger.info("Step 4: 特征工程")
    feature_df = test_df[['user_id', 'item_id', 'category_id', 'label']].copy()

    feature_df = feature_df.merge(
        user_features_df[['user_id', 'buy_rate', 'active_score', 'high_freq_category']],
        on='user_id', how='inner'
    )

    original_count = len(test_df)
    excluded_count = original_count - len(feature_df)
    if excluded_count > 0:
        logger.warning(f"排除冷启动用户: {excluded_count} 条样本")

    if len(feature_df) == 0:
        logger.error("所有用户均为冷启动用户，无可用样本")
        sys.exit(1)

    if len(interaction_df) > 0:
        feature_df = feature_df.merge(
            interaction_df[['user_id', 'item_id', 'pv_count_5min', 'is_cart_or_fav']],
            on=['user_id', 'item_id'], how='left'
        )
    else:
        feature_df['pv_count_5min'] = 0
        feature_df['is_cart_or_fav'] = 0

    feature_df['pv_count_5min'] = feature_df['pv_count_5min'].fillna(0).astype(int)
    feature_df['is_cart_or_fav'] = feature_df['is_cart_or_fav'].fillna(0).astype(int)
    feature_df['category_id'] = feature_df['category_id'].astype(str)
    feature_df['high_freq_category'] = feature_df['high_freq_category'].astype(str)
    feature_df['category_match'] = (
            feature_df['category_id'] == feature_df['high_freq_category']
    ).astype(int)

    feature_df = feature_df[[
        'user_id', 'label', 'buy_rate', 'active_score',
        'category_match', 'pv_count_5min', 'is_cart_or_fav'
    ]]

    feature_df.to_csv(os.path.join(OUTPUT_DIR, "step4_test_features.csv"), index=False)

    logger.info(f"特征工程完成: {len(feature_df)} 样本, 正样本 {feature_df['label'].sum()} 条, "
                f"特征列: {list(feature_df.columns)}")
    return feature_df


def predict_with_pmml(feature_df, pmml_model_path=None):
    """Step 5: 使用 PMML 模型进行预测"""
    if pmml_model_path is None:
        pmml_model_path = PMML_MODEL_PATH

    logger.info(f"Step 5: 模型预测, 模型路径: {pmml_model_path}")

    try:
        from pypmml import Model
    except ImportError:
        logger.error("请先安装 pypmml: pip install pypmml")
        raise

    model = Model.load(pmml_model_path)
    logger.info(f"模型输入字段: {model.inputNames}")

    feature_cols = ['pv_count_5min', 'is_cart_or_fav', 'buy_rate', 'active_score', 'category_match']
    missing_cols = [col for col in feature_cols if col not in feature_df.columns]
    if missing_cols:
        logger.error(f"缺少特征列: {missing_cols}")
        raise ValueError(f"特征列不匹配: {missing_cols}")

    X = feature_df[feature_cols].copy()
    predictions = []
    total = len(X)
    batch_size = 10000

    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        batch = X.iloc[start:end]

        for _, row in batch.iterrows():
            input_data = {
                'pv_count_5min': float(row['pv_count_5min']),
                'is_cart_or_fav': int(row['is_cart_or_fav']),
                'buy_rate': float(row['buy_rate']),
                'active_score': float(row['active_score']),
                'category_match': int(row['category_match'])
            }
            result = model.predict(input_data)
            predictions.append(float(result['probability(1)']))

        print(f"  已预测 {end}/{total} 条...")

    predictions_df = feature_df.copy()
    predictions_df['predict_score'] = predictions

    predictions_df.to_csv(os.path.join(OUTPUT_DIR, "step5_predictions.csv"), index=False)

    logger.info(f"预测完成: {total} 样本, predict_score 范围 [{min(predictions):.4f}, {max(predictions):.4f}], "
                f"均值 {np.mean(predictions):.4f}")
    return predictions_df


def compute_pr_auc(predictions_df):
    """Step 6: 计算 PR-AUC"""
    logger.info("Step 6: 计算 PR-AUC")

    from sklearn.metrics import average_precision_score

    y_true = predictions_df['label'].values
    y_score = predictions_df['predict_score'].values

    pr_auc = average_precision_score(y_true, y_score)

    n_pos = int(y_true.sum())
    n_neg = int((y_true == 0).sum())

    logger.info(f"全量测试集: 正样本 {n_pos}, 负样本 {n_neg}, 比例 1:{n_neg/n_pos:.1f}" if n_pos > 0 else "全量测试集: 无正样本")
    logger.info(f"PR-AUC (average_precision_score): {pr_auc:.4f}")

    return pr_auc


def save_metrics(pr_auc):
    """将 PR-AUC 写入 eval_metrics 表（CTR/CVR 填 -1）"""
    logger.info("写入评估指标到 MySQL")
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            insert_sql = """
                INSERT INTO eval_metrics 
                    (metric_start_time, metric_end_time, pr_auc, ctr, cvr, recommend_count, create_time)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
            """
            cursor.execute(insert_sql, (
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                round(pr_auc, 4),
                -1.0,   # CTR 占位
                -1.0,   # CVR 占位
                -1      # recommend_count 占位
            ))
        conn.commit()

        metrics_df = pd.DataFrame([{
            'metric_start_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'metric_end_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'pr_auc': round(pr_auc, 4),
            'ctr': -1.0,
            'cvr': -1.0,
            'recommend_count': -1
        }])
        metrics_df.to_csv(os.path.join(OUTPUT_DIR, "model_eval_metrics.csv"), index=False)
        logger.info("指标写入成功")
    except Exception as e:
        logger.error(f"写入失败: {e}")
        conn.rollback()
    finally:
        conn.close()


def run_model_evaluation(pmml_model_path=None):
    """运行模型离线评测"""
    if pmml_model_path is None:
        pmml_model_path = PMML_MODEL_PATH

    logger.info("=" * 40)
    logger.info("开始模型离线评测")
    logger.info(f"模型路径: {pmml_model_path}")

    print("=" * 60)
    print("  模型离线评测 PR-AUC")
    print(f"  模型: {pmml_model_path}")
    print(f"  开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    try:
        test_df = load_test_data()
        user_features_df = load_user_features()
        interaction_df = compute_interaction_features(test_df)
        feature_df = build_features(test_df, user_features_df, interaction_df)
        predictions_df = predict_with_pmml(feature_df, pmml_model_path)
        pr_auc = compute_pr_auc(predictions_df)

        save_metrics(pr_auc)

        logger.info(f"模型评测完成, PR-AUC = {pr_auc:.4f}")

        print(f"\n  ✅ 模型评测完成")
        print(f"  PR-AUC = {pr_auc:.4f}")
        print(f"  详细日志: {os.path.join(LOG_DIR, 'test_for_model.log')}")
        print(f"  结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        return pr_auc

    except Exception as e:
        logger.error(f"模型评测失败: {e}", exc_info=True)
        print(f"\n  ❌ 模型评测失败: {e}")
        return None


def main():
    """主函数"""
    pmml_model_path = sys.argv[1] if len(sys.argv) > 1 else None
    run_model_evaluation(pmml_model_path)


if __name__ == "__main__":
    main()