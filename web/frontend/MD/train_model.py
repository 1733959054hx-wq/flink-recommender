import os
import pandas as pd
import warnings
import pymysql
import time
import numpy as np
import tempfile
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import classification_report, average_precision_score
from sklearn.base import clone

from sklearn2pmml.pipeline import PMMLPipeline
from sklearn2pmml import sklearn2pmml


def load_data_from_mysql():
    """
    通过 MySQL 联合查询 user_behavior、user_features、item_features，
    构造 User-Item 级别的交互特征样本。
    """
    query = """
            SELECT uf.buy_rate, \
                   uf.active_score, \
                   LEAST(COUNT(ub.id), 20)                                AS pv_count_5min, \
                   MAX(IF(ub.behavior_type IN ('cart', 'fav'), 1, 0))     AS is_cart_or_fav, \
                   MAX(IF(uf.high_freq_category = itf.category_id, 1, 0)) AS category_match, \
                   MAX(IF(ub.behavior_type = 'buy', 1, 0))                AS buy_label
            FROM user_behavior ub
                     JOIN user_features uf ON ub.user_id = uf.user_id
                     JOIN item_features itf ON ub.item_id = itf.item_id
            GROUP BY ub.user_id, ub.item_id, uf.buy_rate, uf.active_score, uf.high_freq_category, itf.category_id \
            """

    # 采用 with 上下文管理器安全管理数据库连接，确保异常发生时连接能自动释放
    with pymysql.connect(
            host='127.0.0.1',
            user='flinkpy',
            password='123456',
            database='recommend_db'
    ) as connection:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            df = pd.read_sql(query, connection)

    return df


def balance_dataset(df):
    """
    平衡数据集：对负样本进行随机下采样，控制正负比例为 1:3 黄金比例。
    既降低了由于类别极度不平衡带来的模型偏见，也避免了内存溢出的风险。
    """
    positive = df[df['buy_label'] == 1]
    negative = df[df['buy_label'] == 0]

    if len(negative) > len(positive) * 3:
        negative = negative.sample(n=len(positive) * 3, random_state=42)

    return pd.concat([positive, negative]).sample(frac=1, random_state=42).reset_index(drop=True)


def strip_random_state(obj):
    """
    递归遍历并清理 Scikit-Learn Pipeline 模型图谱中的所有 RandomState、Generator 与 mtrand 对象。

    设计背景与学术意义：
    -----------------
    1. 序列化障碍兼容：
       高版本 NumPy (1.22+) 重构了随机数发生器，引入了 LegacyRandomState 以向后兼容。当使用
       joblib 序列化带有随机种子属性的模型时，这些状态对象会被一同打包。
       在导出为 PMML 时，Java 端的 JPMML 转换引擎（pickle 包）预期读取旧版单属性格式，面对
       高版本 NumPy 复杂的对象属性会抛出 net.razorvine.pickle.PickleException 反序列化异常。

    2. 预测无损与确定性：
       random_state 属性仅在模型训练（.fit()）阶段起作用，例如辅助树的分裂选择、样本自助采样等。
       在拟合完成后，决策边界与分裂阈值均已硬编码，预测阶段（.predict_proba()）是完全确定的
       数学计算。因此，在导出前递归将模型各节点的 random_state 引用净化置为 None，
       在保留模型 100% 预测精度与 PR-AUC 分值的前提下，彻底消除了序列化兼容性隐患。

    3. 广度递归防御：
       本函数通过防循环引用的图遍历，安全深度净化包括 Pipeline 步骤、集成学习的基分类器矩阵
       （如 GradientBoosting 的 estimators_ 矩阵）以及 NumPy 对象数组在内的整个对象关系网。
    """
    if obj is None:
        return
    visited = set()

    def _recurse(o):
        if o is None:
            return
        oid = id(o)
        if oid in visited:
            return
        visited.add(oid)

        # 1. 递归处理带有 __dict__ 属性的标准 Python 对象
        if hasattr(o, "__dict__"):
            for key, val in list(o.__dict__.items()):
                val_type_str = str(type(val))
                if (isinstance(val, (np.random.RandomState, np.random.Generator)) or
                        "RandomState" in val_type_str or
                        "Generator" in val_type_str or
                        "mtrand" in val_type_str):
                    o.__dict__[key] = None
                elif key == "random_state" or key == "_random_state":
                    o.__dict__[key] = None
                else:
                    _recurse(val)

        # 2. 递归处理列表、元组或集合容器
        elif isinstance(o, (list, tuple, set)):
            for item in o:
                _recurse(item)

        # 3. 递归处理字典容器
        elif isinstance(o, dict):
            for key, val in list(o.items()):
                val_type_str = str(type(val))
                if (isinstance(val, (np.random.RandomState, np.random.Generator)) or
                        "RandomState" in val_type_str or
                        "Generator" in val_type_str or
                        "mtrand" in val_type_str):
                    o[key] = None
                elif key == "random_state" or key == "_random_state":
                    o[key] = None
                else:
                    _recurse(val)

        # 4. 递归处理 NumPy 的 ndarray 对象数组（针对集成算法子基学习器矩阵）
        elif isinstance(o, np.ndarray):
            if o.dtype == object:
                for item in o.flat:
                    _recurse(item)

    _recurse(obj)


# ==================== 扁平流式主程序 ====================
if __name__ == "__main__":

    # ========== 强制设置纯英文临时目录 ==========
    # 这是解决中文路径问题的核心
    temp_dir = "C:\\temp_ml_work"
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    # 多重保障设置临时目录
    os.environ['TMP'] = temp_dir
    os.environ['TEMP'] = temp_dir
    os.environ['TMPDIR'] = temp_dir
    tempfile.tempdir = temp_dir  # 这行最重要！

    # ========== 设置项目路径（英文） ==========
    project_path = "C:\\temp-data"
    if not os.path.exists(project_path):
        os.makedirs(project_path)
    os.chdir(project_path)

    # joblib 专用的临时文件夹设置
    custom_temp = os.path.join(temp_dir, "joblib_temp")  # 注意这里改为 temp_dir
    if not os.path.exists(custom_temp):
        os.makedirs(custom_temp)

    # 额外的 joblib 环境变量
    os.environ['JOBLIB_TEMP_FOLDER'] = custom_temp

    print(f"📁 临时目录已设置为: {temp_dir}")
    print(f"📁 joblib 专用临时目录: {custom_temp}")

    print("正在从 MySQL 拉取数据并构造 User-Item 样本...")
    df_raw = load_data_from_mysql()

    if len(df_raw) == 0:
        print("❌ 错误：查询结果为空！")
        exit()

    print(f"✅ 原始数据拉取成功！共获取 {len(df_raw)} 条交互样本。")

    feature_cols = ['buy_rate', 'active_score', 'pv_count_5min', 'is_cart_or_fav', 'category_match']
    X_raw = df_raw[feature_cols].astype(float)
    y_raw = df_raw['buy_label'].astype(int)

    X_train_raw, X_test, y_train_raw, y_test = train_test_split(
        X_raw, y_raw, test_size=0.2, random_state=42
    )

    train_df = pd.concat([X_train_raw, y_train_raw], axis=1)
    train_df_balanced = balance_dataset(train_df)

    X_train = train_df_balanced[feature_cols]
    y_train = train_df_balanced['buy_label']

    print(f"⚖️ 仅训练集重采样完毕，参与训练样本数: {len(X_train)}")
    print(f"🔍 测试集保持真实分布，样本数: {len(X_test)}")

    # 3. 模型 Pipeline 配置
    models_to_try = {
        "GradientBoosting": {
            "pipeline": PMMLPipeline([
                ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
                ("classifier", GradientBoostingClassifier(random_state=42))
            ]),
            "params": {
                'classifier__n_estimators': [50, 100],
                'classifier__max_depth': [3, 5],
                'classifier__learning_rate': [0.05, 0.1]
            }
        },
        "RandomForest": {
            "pipeline": PMMLPipeline([
                ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
                ("classifier", RandomForestClassifier(class_weight='balanced', random_state=42))
            ]),
            "params": {
                'classifier__n_estimators': [50, 100],
                'classifier__max_depth': [5, 10]
            }
        },
        "LogisticRegression": {
            "pipeline": PMMLPipeline([
                ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
                ("scaler", StandardScaler()),
                ("poly", PolynomialFeatures(degree=2, interaction_only=True, include_bias=False)),
                ("classifier", LogisticRegression(class_weight='balanced', max_iter=1000, solver='liblinear'))
            ]),
            "params": {
                'classifier__C': [0.1, 1.0, 10.0]
            }
        }
    }

    # 存储所有已经拟合完成的最优候选模型
    trained_models = []

    print("\n🏆 开始多模型 + GridSearch 对比评估 🏆")

    for name, config in models_to_try.items():
        print(f"\n👉 正在训练: {name} ...")

        grid = GridSearchCV(
            config["pipeline"],
            config["params"],
            cv=3,
            scoring='average_precision',
            n_jobs=2
        )

        grid.fit(X_train, y_train)

        print(f"✅ {name} 最佳 PR-AUC: {grid.best_score_:.4f}")
        print(f"📍 选中参数: {grid.best_params_}")

        # 收集训练完毕的最优 Pipeline (由于 GridSearch 带有 refit=True，此时已在 X_train 上完成全量拟合)
        trained_models.append({
            "name": name,
            "score": grid.best_score_,
            "pipeline": grid.best_estimator_
        })

    # 按 PR-AUC 分数从高到低排序
    trained_models.sort(key=lambda x: x["score"], reverse=True)

    # 获取全能冠军模型展示
    champion_model = trained_models[0]
    best_name = champion_model["name"]
    best_score = champion_model["score"]
    best_pipeline = champion_model["pipeline"]

    print("\n" + "=" * 30)
    print(f"🎉 最终冠军模型: {best_name}")
    print(f"🏆 最高 PR-AUC: {best_score:.4f}")
    print("=" * 30 + "\n")

    print("📊 测试集详细评估汇报：")
    print(classification_report(y_test, best_pipeline.predict(X_test)))

    y_pred_proba = best_pipeline.predict_proba(X_test)[:, 1]
    test_pr_auc = average_precision_score(y_test, y_pred_proba)
    print(f"\n🎯 测试集真实分布下的 PR-AUC: {test_pr_auc:.4f}")

    # 4. 🚀 启动级降导出机制（优先尝试高评分模型）
    model_dir = os.path.join(project_path, "model")
    if not os.path.exists(model_dir):
        os.makedirs(model_dir)

    pmml_output_path = os.path.join(model_dir, "recommend_model_champion.pmml")
    print("\n🚀 启动级降导出机制（按评分自高到低尝试）...")

    export_success = False

    for model_info in trained_models:
        name = model_info["name"]
        score = model_info["score"]
        pipeline = model_info["pipeline"]

        print(f"\n🔍 正在尝试导出: {name} (PR-AUC: {score:.4f})...")

        # 导出前执行清理
        print(f"🧹 正在清理 {name} 模型中的 RandomState 状态...")
        strip_random_state(pipeline)

        try:
            sklearn2pmml(pipeline, pmml_output_path, with_repr=False)
            print(f"✅ 成功导出模型: {name} (PR-AUC: {score:.4f})！")
            print(f"📁 PMML 文件绝对路径: {os.path.abspath(pmml_output_path)}")
            export_success = True
            break  # 成功导出了当前可用模型中分数最高的，直接退出循环
        except Exception as e:
            print(f"⚠️ 导出 {name} 异常。错误摘要: {str(e)[:150]}...")
            print("🔄 准备尝试下一个次高分的候选模型...")
            time.sleep(1)

    # 5. 终极保底（只有所有高级模型全部失败时才触发）
    if not export_success:
        print(f"\n❌ 所有高级候选模型均导出失败。启动终极保底预案：使用基础决策树导出...")
        fallback = PMMLPipeline([
            ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
            ("classifier", DecisionTreeClassifier(max_depth=3, class_weight='balanced', random_state=42))
        ])
        fallback.fit(X_train, y_train)
        strip_random_state(fallback)
        try:
            sklearn2pmml(fallback, pmml_output_path, with_repr=False)
            print(f"✅ 终极保底方案 PMML 导出成功！系统流处理不会中断。")
            print(f"📁 PMML 文件绝对路径: {os.path.abspath(pmml_output_path)}")
        except Exception as e_fb:
            print(f"❌ 终极保底方案导出也失败了: {e_fb}")

    print("\n" + "=" * 60)
    print("🎉 模型比对与导出流程结束")
    print("=" * 60)