import pandas as pd
import numpy as np
import lightgbm as lgb
import joblib
import warnings
import time
from pathlib import Path
from sklearn.model_selection import RandomizedSearchCV, train_test_split
from sklearn.metrics import (classification_report, confusion_matrix, recall_score, 
                             precision_score, f1_score, roc_auc_score, average_precision_score)

warnings.filterwarnings('ignore')

# ---------------------------------------------------------
# 1. 数据读取与切分
# ---------------------------------------------------------
def load_and_split_data(file_path):
    if not Path(file_path).exists():
        raise FileNotFoundError(f"未找到文件: {file_path}")
    
    df = pd.read_excel(file_path)
    
    # 按照年份划分
    train_df = df[df["年份"].between(2015, 2022)].copy()
    test_df = df[df["年份"].between(2023, 2025)].copy()
    
    info_cols = ["证券代码", "证券简称", "统计截止日期", "年份", "是否舞弊"]
    feature_cols = [c for c in df.columns if c not in info_cols]
    
    X_train, y_train = train_df[feature_cols], train_df["是否舞弊"]
    X_test, y_test = test_df[feature_cols], test_df["是否舞弊"]
    
    return X_train, y_train, X_test, y_test, test_df

# ---------------------------------------------------------
# 2. 分阶段调参逻辑 (优化目标改为 F1 或 AP)
# ---------------------------------------------------------
def tune_lightgbm(X_train, y_train):
    # 划分验证集用于早停
    X_tr, X_val, y_tr, y_val = train_test_split(
        X_train, y_train, test_size=0.2, stratify=y_train, random_state=42
    )

    # 计算正负样本比例，用于处理不平衡
    ratio = (len(y_train) - sum(y_train)) / sum(y_train)

    base_params = {
        'objective': 'binary',
        'boosting_type': 'gbdt',
        'scale_pos_weight': ratio * 0.8,  # 适当降低权重以控制误报
        'random_state': 42,
        'verbose': -1,
        'device_type': 'cpu' # 如有GPU可改为'gpu'
    }

    start_time = time.time()
    print("开始优化模型性能（平衡 Precision 和 Recall）...")

    # Stage 1: 树结构
    stage1_grid = {
        'max_depth': [6, 8, 10],
        'num_leaves': [31, 63, 127]
    }
    # 注意：这里 scoring 改成了 'f1'，不再只看 recall
    tuner1 = RandomizedSearchCV(
        lgb.LGBMClassifier(**base_params),
        stage1_grid, n_iter=5, scoring='f1', cv=3, n_jobs=-1, random_state=42
    )
    tuner1.fit(X_tr, y_tr)
    
    # Stage 2: 正则化与随机性
    best1 = tuner1.best_params_
    stage2_grid = {
        'feature_fraction': [0.7, 0.8, 0.9],
        'bagging_fraction': [0.7, 0.8, 0.9],
        'bagging_freq': [3, 5],
        'reg_alpha': [0.01, 0.1, 1.0],
        'reg_lambda': [1.0, 4.0, 10.0]
    }
    tuner2 = RandomizedSearchCV(
        lgb.LGBMClassifier(**base_params, **best1),
        stage2_grid, n_iter=10, scoring='f1', cv=3, n_jobs=-1, random_state=42
    )
    tuner2.fit(X_tr, y_tr)
    
    best_params = {**best1, **tuner2.best_params_}
    best_params['learning_rate'] = 0.02
    best_params['n_estimators'] = 1000

    # 最终拟合，带早停
    final_model = lgb.LGBMClassifier(**base_params, **best_params)
    final_model.fit(
        X_tr, y_tr,
        eval_set=[(X_val, y_val)],
        eval_metric='binary_logloss',
        callbacks=[lgb.early_stopping(100)]
    )
    
    print(f"调参完成，耗时: {(time.time()-start_time)/60:.2f} 分钟")
    return final_model

# ---------------------------------------------------------
# 3. 评估函数 (包含自定义阈值)
# ---------------------------------------------------------
def evaluate_model(model, X, y, set_name="测试集", threshold=0.5):
    proba = model.predict_proba(X)[:, 1]
    pred = (proba >= threshold).astype(int)
    
    print(f"\n{'='*20} {set_name} 评估 (阈值: {threshold}) {'='*20}")
    print(f"Recall:    {recall_score(y, pred):.44f}")
    print(f"Precision: {precision_score(y, pred):.4f}")
    print(f"F1 Score:  {f1_score(y, pred):.4f}")
    print(f"AUC-ROC:   {roc_auc_score(y, proba):.4f}")
    print(f"Avg Precision (PR-AUC): {average_precision_score(y, proba):.4f}")
    
    print("\n混淆矩阵:")
    print(confusion_matrix(y, pred))
    print("\n分类报告:")
    print(classification_report(y, pred, target_names=['非舞弊', '舞弊']))

# ---------------------------------------------------------
# 主程序
# ---------------------------------------------------------
if __name__ == "__main__":
    DATA_PATH = "FS_Preprocessed.xlsx"
    
    # 1. 加载数据
    X_train, y_train, X_test, y_test, test_df = load_and_split_data(DATA_PATH)
    
    # 2. 训练模型
    best_model = tune_lightgbm(X_train, y_train)
    
    # 3. 评估 - 观察不同阈值下的表现
    # 默认 0.5 阈值
    evaluate_model(best_model, X_test, y_test, "测试集 (默认阈值)", threshold=0.5)
    
    # 尝试高阈值以减少误报 (这是解决你目前问题的关键)
    evaluate_model(best_model, X_test, y_test, "测试集 (高阈值模式)", threshold=0.8)
    
    # 4. 保存
    joblib.dump(best_model, "improved_lgb_model.pkl")
    print("\n模型已保存为 improved_lgb_model.pkl")