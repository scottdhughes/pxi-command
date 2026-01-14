#!/usr/bin/env python3
"""
PXI SPY Return Prediction Model

Trains XGBoost models to predict 7-day and 30-day SPY forward returns
based on PXI score, category scores, and engineered features.

This is the ALPHA model - predicting actual market returns, not PXI changes.
"""

import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import xgboost as xgb
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

# Configuration
API_URL = os.getenv('PXI_API_URL', 'https://pxi-api.novoamorx1.workers.dev')
API_KEY = os.getenv('WRITE_API_KEY', '')

CATEGORIES = ['breadth', 'credit', 'crypto', 'global', 'liquidity', 'macro', 'positioning', 'volatility']


def fetch_training_data() -> pd.DataFrame:
    """Load training data from local file or API."""
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'training_data.json')

    # Try local file first
    if os.path.exists(local_path):
        print(f"Loading training data from local file: {local_path}")
        with open(local_path, 'r') as f:
            data = json.load(f)
        print(f"Loaded {data['count']} records with {data.get('spy_data_points', 0)} SPY data points")
    else:
        # Fall back to API
        print("Fetching training data from API...")
        headers = {'Authorization': f'Bearer {API_KEY}'} if API_KEY else {}
        response = requests.get(f'{API_URL}/api/export/training-data', headers=headers)

        if response.status_code != 200:
            raise Exception(f"Failed to fetch training data: {response.status_code} - {response.text}")

        data = response.json()
        print(f"Fetched {data['count']} records with {data.get('spy_data_points', 0)} SPY data points")

    # Convert to DataFrame
    records = []
    for row in data['data']:
        record = {
            'date': row['date'],
            'spy_return_7d': row.get('spy_return_7d'),
            'spy_return_30d': row.get('spy_return_30d'),
            'pxi_score': row['pxi_score'],
            'pxi_delta_1d': row.get('pxi_delta_1d'),
            'pxi_delta_7d': row.get('pxi_delta_7d'),
            'pxi_delta_30d': row.get('pxi_delta_30d'),
        }

        # Add category scores
        for cat in CATEGORIES:
            record[f'cat_{cat}'] = row['categories'].get(cat)

        # Add key indicators
        indicators = row.get('indicators', {})
        record['vix'] = indicators.get('vix')
        record['hy_spread'] = indicators.get('hy_oas')
        record['ig_spread'] = indicators.get('ig_oas')
        record['breadth_ratio'] = indicators.get('rsp_spy_ratio')
        record['yield_curve'] = indicators.get('yield_curve_2s10s')
        record['dxy'] = indicators.get('dxy')
        record['btc_vs_200d'] = indicators.get('btc_vs_200d')

        records.append(record)

    df = pd.DataFrame(records)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Engineer features for return prediction."""
    print("Engineering features...")

    # PXI bucket (numeric)
    df['pxi_bucket'] = pd.cut(
        df['pxi_score'],
        bins=[0, 20, 40, 60, 80, 100],
        labels=[0, 1, 2, 3, 4],
        include_lowest=True
    ).astype(float)

    # Momentum signals
    df['momentum_7d_signal'] = df['pxi_delta_7d'].apply(
        lambda x: 2 if x > 5 else (1 if x > 2 else (0 if x > -2 else (-1 if x > -5 else -2))) if pd.notna(x) else 0
    )
    df['momentum_30d_signal'] = df['pxi_delta_30d'].apply(
        lambda x: 2 if x > 10 else (1 if x > 4 else (0 if x > -4 else (-1 if x > -10 else -2))) if pd.notna(x) else 0
    )

    # Acceleration
    df['acceleration'] = df['pxi_delta_7d'] - (df['pxi_delta_30d'] / 4.3)
    df['acceleration_signal'] = df['acceleration'].apply(
        lambda x: 1 if x > 2 else (-1 if x < -2 else 0) if pd.notna(x) else 0
    )

    # Category statistics
    cat_cols = [f'cat_{c}' for c in CATEGORIES]
    df['category_mean'] = df[cat_cols].mean(axis=1)
    df['category_max'] = df[cat_cols].max(axis=1)
    df['category_min'] = df[cat_cols].min(axis=1)
    df['category_dispersion'] = df['category_max'] - df['category_min']
    df['category_std'] = df[cat_cols].std(axis=1)
    df['strong_categories'] = (df[cat_cols] > 70).sum(axis=1)
    df['weak_categories'] = (df[cat_cols] < 30).sum(axis=1)

    # VIX features (handle missing data)
    df['vix'] = pd.to_numeric(df['vix'], errors='coerce')
    df['vix_high'] = (df['vix'] > 25).fillna(0).astype(int)
    df['vix_low'] = (df['vix'] < 15).fillna(0).astype(int)
    df['vix_ma_20'] = df['vix'].rolling(20, min_periods=1).mean()
    df['vix_vs_ma'] = df['vix'] - df['vix_ma_20']

    # PXI rolling features
    df['pxi_ma_5'] = df['pxi_score'].rolling(5, min_periods=1).mean()
    df['pxi_ma_20'] = df['pxi_score'].rolling(20, min_periods=1).mean()
    df['pxi_std_20'] = df['pxi_score'].rolling(20, min_periods=1).std()
    df['pxi_vs_ma_20'] = df['pxi_score'] - df['pxi_ma_20']

    # Regime features
    df['above_50'] = (df['pxi_score'] > 50).astype(int)
    df['extreme_low'] = (df['pxi_score'] < 25).astype(int)
    df['extreme_high'] = (df['pxi_score'] > 75).astype(int)

    # Credit spread features (handle missing data)
    if df['hy_spread'].notna().sum() > 5:
        df['spread_widening'] = df['hy_spread'].diff(5) > 0
        df['spread_widening'] = df['spread_widening'].fillna(0).astype(int)
    else:
        df['spread_widening'] = 0

    return df


def prepare_features(df: pd.DataFrame) -> Tuple[np.ndarray, List[str]]:
    """Prepare feature matrix for training."""

    feature_cols = [
        # PXI features
        'pxi_score', 'pxi_delta_1d', 'pxi_delta_7d', 'pxi_delta_30d',
        'pxi_bucket', 'momentum_7d_signal', 'momentum_30d_signal',
        'acceleration', 'acceleration_signal',

        # Category features
        'cat_breadth', 'cat_credit', 'cat_crypto', 'cat_global',
        'cat_liquidity', 'cat_macro', 'cat_positioning', 'cat_volatility',
        'category_mean', 'category_dispersion', 'category_std',
        'strong_categories', 'weak_categories',

        # Indicator features
        'vix', 'hy_spread', 'ig_spread', 'breadth_ratio',
        'yield_curve', 'dxy', 'btc_vs_200d',

        # Derived features
        'vix_high', 'vix_low', 'vix_ma_20', 'vix_vs_ma',
        'pxi_ma_5', 'pxi_ma_20', 'pxi_std_20', 'pxi_vs_ma_20',
        'above_50', 'extreme_low', 'extreme_high',
        'spread_widening',
    ]

    available_cols = [c for c in feature_cols if c in df.columns]
    print(f"Using {len(available_cols)} features")

    X = df[available_cols].values
    return X, available_cols


def train_model(
    X: np.ndarray,
    y: np.ndarray,
    feature_names: List[str],
    target_name: str,
    n_splits: int = 5
) -> Tuple[xgb.XGBRegressor, Dict]:
    """Train XGBoost model with time series cross-validation."""

    print(f"\nTraining model for {target_name}...")

    # Remove rows with missing targets
    valid_mask = ~np.isnan(y)
    X_valid = X[valid_mask]
    y_valid = y[valid_mask]

    print(f"  Valid samples: {len(y_valid)} / {len(y)}")

    if len(y_valid) < 100:
        print(f"  WARNING: Insufficient data for {target_name}")
        return None, {}

    # Time series cross-validation
    tscv = TimeSeriesSplit(n_splits=n_splits)
    cv_scores = {'mse': [], 'mae': [], 'r2': [], 'direction_acc': []}

    for fold, (train_idx, val_idx) in enumerate(tscv.split(X_valid)):
        X_train, X_val = X_valid[train_idx], X_valid[val_idx]
        y_train, y_val = y_valid[train_idx], y_valid[val_idx]

        model = xgb.XGBRegressor(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=5,
            random_state=42,
            n_jobs=-1,
        )

        model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            verbose=False
        )

        y_pred = model.predict(X_val)

        cv_scores['mse'].append(mean_squared_error(y_val, y_pred))
        cv_scores['mae'].append(mean_absolute_error(y_val, y_pred))
        cv_scores['r2'].append(r2_score(y_val, y_pred))

        # Direction accuracy (positive vs negative return)
        direction_correct = np.sign(y_pred) == np.sign(y_val)
        cv_scores['direction_acc'].append(np.mean(direction_correct) * 100)

    print(f"  CV MSE: {np.mean(cv_scores['mse']):.4f} (+/- {np.std(cv_scores['mse']):.4f})")
    print(f"  CV MAE: {np.mean(cv_scores['mae']):.4f}% (+/- {np.std(cv_scores['mae']):.4f})")
    print(f"  CV R2:  {np.mean(cv_scores['r2']):.4f} (+/- {np.std(cv_scores['r2']):.4f})")
    print(f"  Direction Accuracy: {np.mean(cv_scores['direction_acc']):.1f}% (+/- {np.std(cv_scores['direction_acc']):.1f})")

    # Train final model on all data
    final_model = xgb.XGBRegressor(
        n_estimators=100,
        max_depth=4,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        random_state=42,
        n_jobs=-1,
    )
    final_model.fit(X_valid, y_valid, verbose=False)

    # Feature importance
    importance = dict(zip(feature_names, final_model.feature_importances_))
    top_features = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:10]
    print("  Top features:")
    for feat, imp in top_features:
        print(f"    {feat}: {imp:.4f}")

    metrics = {
        'cv_mse_mean': float(np.mean(cv_scores['mse'])),
        'cv_mae_mean': float(np.mean(cv_scores['mae'])),
        'cv_r2_mean': float(np.mean(cv_scores['r2'])),
        'cv_direction_acc': float(np.mean(cv_scores['direction_acc'])),
        'n_samples': int(len(y_valid)),
        'feature_importance': {k: float(v) for k, v in top_features},
    }

    return final_model, metrics


def export_model_to_json(model: xgb.XGBRegressor, feature_names: List[str]) -> Dict:
    """Export XGBoost model to JSON format for Worker inference."""

    booster = model.get_booster()
    trees_json = booster.get_dump(dump_format='json')
    trees = [json.loads(t) for t in trees_json]

    return {
        'type': 'xgboost',
        'version': '1.0',
        'n_estimators': len(trees),
        'base_score': float(model.get_params()['base_score'] or 0.5),
        'feature_names': feature_names,
        'trees': trees,
    }


def main():
    # Fetch and prepare data (uses local file if available)
    df = fetch_training_data()
    df = engineer_features(df)

    print(f"\nDataset shape: {df.shape}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"SPY return coverage - 7d: {df['spy_return_7d'].notna().sum()}, 30d: {df['spy_return_30d'].notna().sum()}")

    # Check if we have SPY data
    if df['spy_return_7d'].notna().sum() < 100:
        print("\nERROR: Insufficient SPY return data")
        print("Make sure SPY prices are being fetched in the pipeline")
        sys.exit(1)

    # Prepare features
    X, feature_names = prepare_features(df)

    # Handle missing values
    X = np.nan_to_num(X, nan=0.0)

    # Train models
    models = {}
    metrics = {}

    # 7-day SPY return model
    y_7d = df['spy_return_7d'].values
    model_7d, metrics_7d = train_model(X, y_7d, feature_names, 'spy_return_7d')
    if model_7d:
        models['7d'] = model_7d
        metrics['7d'] = metrics_7d

    # 30-day SPY return model
    y_30d = df['spy_return_30d'].values
    model_30d, metrics_30d = train_model(X, y_30d, feature_names, 'spy_return_30d')
    if model_30d:
        models['30d'] = model_30d
        metrics['30d'] = metrics_30d

    if not models:
        print("\nERROR: No models were trained successfully")
        sys.exit(1)

    # Export models
    output_dir = os.path.dirname(os.path.abspath(__file__))

    export = {
        'created_at': datetime.utcnow().isoformat(),
        'model_type': 'spy_returns',
        'feature_names': feature_names,
        'models': {},
        'metrics': metrics,
    }

    for name, model in models.items():
        export['models'][name] = export_model_to_json(model, feature_names)

    output_path = os.path.join(output_dir, 'spy_return_model.json')
    with open(output_path, 'w') as f:
        json.dump(export, f, indent=2)

    print(f"\n{'='*60}")
    print(f"Model exported to: {output_path}")
    print(f"File size: {os.path.getsize(output_path) / 1024:.1f} KB")

    # Summary
    print(f"\n{'='*60}")
    print("SPY RETURN PREDICTION MODEL SUMMARY")
    print(f"{'='*60}")
    for horizon, m in metrics.items():
        print(f"\n{horizon} Model:")
        print(f"  Direction Accuracy: {m['cv_direction_acc']:.1f}%")
        print(f"  MAE: {m['cv_mae_mean']:.2f}%")
        print(f"  R2: {m['cv_r2_mean']:.3f}")
        print(f"  Samples: {m['n_samples']}")


if __name__ == '__main__':
    main()
