#!/usr/bin/env python3
"""
PXI Model Training with proper export format for Cloudflare Worker inference.
"""

import json
import re
import numpy as np
import pandas as pd
import xgboost as xgb

# Hyperparameters
XGB_PARAMS = {
    'n_estimators': 100,
    'max_depth': 4,
    'learning_rate': 0.1,
    'subsample': 0.8,
    'colsample_bytree': 0.8,
    'random_state': 42,
}

CATEGORIES = ['breadth', 'credit', 'crypto', 'global', 'macro', 'positioning', 'volatility']


def load_data():
    """Load training data from exported JSON files."""
    print("Loading data...")

    with open('/tmp/pxi_scores.json', 'r') as f:
        pxi_df = pd.DataFrame(json.load(f))
    pxi_df['date'] = pd.to_datetime(pxi_df['date'])
    print(f"  PXI scores: {len(pxi_df)} rows")

    with open('/tmp/category_scores.json', 'r') as f:
        cat_df = pd.DataFrame(json.load(f))
    cat_df['date'] = pd.to_datetime(cat_df['date'])
    cat_pivot = cat_df.pivot(index='date', columns='category', values='score')
    cat_pivot.columns = [f'cat_{c}' for c in cat_pivot.columns]
    cat_pivot = cat_pivot.reset_index()
    print(f"  Category scores: {len(cat_pivot)} rows")

    with open('/tmp/indicators.json', 'r') as f:
        ind_df = pd.DataFrame(json.load(f))
    ind_df['date'] = pd.to_datetime(ind_df['date'])
    ind_pivot = ind_df.pivot(index='date', columns='indicator_id', values='value')
    ind_pivot = ind_pivot.reset_index()
    print(f"  Indicators: {len(ind_pivot)} rows")

    # Merge
    df = pxi_df.merge(cat_pivot, on='date', how='left')
    df = df.merge(ind_pivot, on='date', how='left')
    df = df.sort_values('date').reset_index(drop=True)

    return df


def engineer_features(df):
    """Create features matching the Worker's extractMLFeatures function."""

    # Momentum signals
    d7 = df['pxi_delta_7d'].fillna(0)
    df['momentum_7d_signal'] = np.where(d7 > 5, 2, np.where(d7 > 2, 1, np.where(d7 > -2, 0, np.where(d7 > -5, -1, -2))))

    d30 = df['pxi_delta_30d'].fillna(0)
    df['momentum_30d_signal'] = np.where(d30 > 10, 2, np.where(d30 > 4, 1, np.where(d30 > -4, 0, np.where(d30 > -10, -1, -2))))

    # Acceleration
    df['acceleration'] = d7 - (d30 / 4.3)
    df['acceleration_signal'] = np.where(df['acceleration'] > 2, 1, np.where(df['acceleration'] < -2, -1, 0))

    # Category dispersion
    cat_cols = [f'cat_{c}' for c in CATEGORIES if f'cat_{c}' in df.columns]
    df['category_dispersion'] = df[cat_cols].max(axis=1) - df[cat_cols].min(axis=1)
    df['category_std'] = df[cat_cols].std(axis=1)
    df['category_mean'] = df[cat_cols].mean(axis=1)

    # Extreme categories
    df['extreme_low'] = (df[cat_cols] < 20).sum(axis=1)
    df['extreme_high'] = (df[cat_cols] > 80).sum(axis=1)
    df['weak_categories'] = (df[cat_cols] < 40).sum(axis=1)
    df['strong_categories'] = (df[cat_cols] > 60).sum(axis=1)

    # PXI bucket
    df['pxi_bucket'] = pd.cut(df['pxi_score'], bins=[0, 20, 40, 60, 80, 100], labels=[0, 1, 2, 3, 4])

    # Rolling stats
    df['pxi_ma_5'] = df['pxi_score'].rolling(5).mean()
    df['pxi_ma_20'] = df['pxi_score'].rolling(20).mean()
    df['pxi_std_20'] = df['pxi_score'].rolling(20).std()
    df['pxi_vs_ma_20'] = df['pxi_score'] - df['pxi_ma_20']

    # VIX features if available
    if 'vix' in df.columns:
        df['vix_high'] = (df['vix'] > 25).astype(int)
        df['vix_low'] = (df['vix'] < 15).astype(int)

    # Targets
    df['target_7d'] = df['pxi_score'].shift(-7) - df['pxi_score']
    df['target_30d'] = df['pxi_score'].shift(-30) - df['pxi_score']

    return df


def get_feature_columns(df):
    """Get feature columns matching Worker expectations."""
    feature_cols = [
        'pxi_score', 'pxi_delta_1d', 'pxi_delta_7d', 'pxi_delta_30d',
        'momentum_7d_signal', 'momentum_30d_signal',
        'acceleration', 'acceleration_signal',
    ]

    # Category features
    for cat in CATEGORIES:
        col = f'cat_{cat}'
        if col in df.columns:
            feature_cols.append(col)

    # Dispersion features
    feature_cols.extend([
        'category_dispersion', 'category_std', 'category_mean',
        'extreme_low', 'extreme_high', 'weak_categories', 'strong_categories',
        'pxi_bucket',
    ])

    # Rolling features
    feature_cols.extend(['pxi_ma_5', 'pxi_ma_20', 'pxi_std_20', 'pxi_vs_ma_20'])

    # Indicator features
    for ind in ['vix', 'hy_oas', 'ig_oas', 'put_call_ratio', 'aaii_bull_bear']:
        if ind in df.columns:
            feature_cols.append(ind)

    if 'vix' in df.columns:
        feature_cols.extend(['vix_high', 'vix_low'])

    # Filter to existing columns
    feature_cols = [c for c in feature_cols if c in df.columns]

    return feature_cols


def parse_xgb_tree_dump(tree_str, feature_names):
    """Parse XGBoost text dump to nested dict format."""
    lines = tree_str.strip().split('\n')
    nodes = {}

    for line in lines:
        # Parse indentation for depth
        indent = len(line) - len(line.lstrip('\t'))
        line = line.strip()

        # Parse node ID
        match = re.match(r'^(\d+):', line)
        if not match:
            continue
        nodeid = int(match.group(1))
        rest = line[match.end():]

        # Check if leaf
        leaf_match = re.search(r'leaf=([+-]?\d+\.?\d*(?:e[+-]?\d+)?)', rest)
        if leaf_match:
            nodes[nodeid] = {
                'nodeid': nodeid,
                'leaf': float(leaf_match.group(1))
            }
        else:
            # Internal node: [feature<threshold] yes=X,no=Y,missing=Z
            split_match = re.search(r'\[(\w+)<([+-]?\d+\.?\d*(?:e[+-]?\d+)?)\]', rest)
            branch_match = re.search(r'yes=(\d+),no=(\d+),missing=(\d+)', rest)

            if split_match and branch_match:
                feature = split_match.group(1)
                threshold = float(split_match.group(2))
                yes = int(branch_match.group(1))
                no = int(branch_match.group(2))
                missing = int(branch_match.group(3))

                nodes[nodeid] = {
                    'nodeid': nodeid,
                    'depth': indent,
                    'split': feature,  # Keep as f0, f1, etc.
                    'split_condition': threshold,
                    'yes': yes,
                    'no': no,
                    'missing': missing,
                }

    # Build tree structure with children
    def build_tree(nodeid):
        if nodeid not in nodes:
            return None

        node = nodes[nodeid].copy()

        if 'leaf' in node:
            return node

        children = []
        if node['yes'] in nodes:
            child = build_tree(node['yes'])
            if child:
                children.append(child)
        if node['no'] in nodes:
            child = build_tree(node['no'])
            if child:
                children.append(child)

        if children:
            node['children'] = children

        return node

    return build_tree(0)


def export_xgboost(model, feature_names):
    """Export XGBoost model to Worker-compatible JSON format."""
    booster = model.get_booster()
    tree_dumps = booster.get_dump()

    trees = []
    for tree_str in tree_dumps:
        tree = parse_xgb_tree_dump(tree_str, feature_names)
        if tree:
            trees.append(tree)

    # Get actual base_score from model config
    config = json.loads(booster.save_config())
    base_score = float(config['learner']['learner_model_param']['base_score'])

    return {
        'b': base_score,
        't': trees,
    }


def train_and_export():
    """Main training and export function."""
    print("=" * 50)
    print("PXI Model Training")
    print("=" * 50)

    # Load data
    df = load_data()

    # Engineer features
    df = engineer_features(df)

    # Get feature columns
    feature_cols = get_feature_columns(df)
    print(f"\nFeatures ({len(feature_cols)}):")
    for i, f in enumerate(feature_cols):
        print(f"  f{i}: {f}")

    # Prepare training data
    df_train = df.dropna(subset=['target_7d', 'target_30d'])
    X = df_train[feature_cols].fillna(0).values
    y_7d = df_train['target_7d'].values
    y_30d = df_train['target_30d'].values

    print(f"\nTraining samples: {len(X)}")

    # Train XGBoost
    print("\n=== Training XGBoost ===")

    print("Training 7-day model...")
    model_7d = xgb.XGBRegressor(**XGB_PARAMS)
    model_7d.fit(X, y_7d, verbose=False)

    print("Training 30-day model...")
    model_30d = xgb.XGBRegressor(**XGB_PARAMS)
    model_30d.fit(X, y_30d, verbose=False)

    # Export
    print("\n=== Exporting Models ===")

    model_json = {
        'v': '2.1',
        'f': feature_cols,
        'm': {
            '7d': export_xgboost(model_7d, feature_cols),
            '30d': export_xgboost(model_30d, feature_cols),
        }
    }

    with open('pxi_model.json', 'w') as f:
        json.dump(model_json, f, indent=2)

    with open('pxi_model_compact.json', 'w') as f:
        json.dump(model_json, f, separators=(',', ':'))

    import os
    print(f"  pxi_model.json: {os.path.getsize('pxi_model.json') / 1024:.1f} KB")
    print(f"  pxi_model_compact.json: {os.path.getsize('pxi_model_compact.json') / 1024:.1f} KB")

    # Verify export
    print("\n=== Verification ===")
    print(f"  7d trees: {len(model_json['m']['7d']['t'])}")
    print(f"  30d trees: {len(model_json['m']['30d']['t'])}")

    # Test prediction on last row
    test_features = {f'f{i}': v for i, v in enumerate(X[-1])}

    def predict_tree(node, features):
        if 'leaf' in node:
            return node['leaf']
        feat_val = features.get(node['split'], 0)
        go_left = feat_val < node['split_condition']
        target_id = node['yes'] if go_left else node['no']
        for child in node.get('children', []):
            if child['nodeid'] == target_id:
                return predict_tree(child, features)
        return 0

    pred_7d = model_json['m']['7d']['b']
    for tree in model_json['m']['7d']['t']:
        pred_7d += predict_tree(tree, test_features)

    sklearn_pred = model_7d.predict(X[-1:])
    print(f"  Test prediction (7d): exported={pred_7d:.4f}, sklearn={sklearn_pred[0]:.4f}")

    print("\n" + "=" * 50)
    print("Training complete!")
    print("=" * 50)


if __name__ == '__main__':
    train_and_export()
