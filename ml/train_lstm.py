#!/usr/bin/env python3
"""
PXI LSTM Model Training Script

Trains an LSTM model to predict 7-day and 30-day PXI changes
using sequences of historical PXI and category scores.

Exports model weights as JSON for edge inference in Cloudflare Workers.
"""

import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import requests

# Try PyTorch first, fall back to manual implementation
try:
    import torch
    import torch.nn as nn
    from torch.utils.data import DataLoader, TensorDataset
    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False
    print("PyTorch not found, using numpy-based training (slower)")

# Configuration
API_URL = os.getenv('PXI_API_URL', 'https://pxi-api.novoamorx1.workers.dev')
API_KEY = os.getenv('WRITE_API_KEY', '')

# Model hyperparameters
SEQUENCE_LENGTH = 20  # Use 20 days of history
HIDDEN_SIZE = 32      # LSTM hidden units
NUM_LAYERS = 1        # Single LSTM layer for simplicity
BATCH_SIZE = 32
EPOCHS = 100
LEARNING_RATE = 0.001

# Features to use in sequences
FEATURE_COLS = [
    'pxi_score',
    'pxi_delta_7d',
    'cat_breadth', 'cat_credit', 'cat_crypto', 'cat_global',
    'cat_liquidity', 'cat_macro', 'cat_positioning', 'cat_volatility',
    'vix', 'category_dispersion'
]


def fetch_training_data() -> pd.DataFrame:
    """Fetch training data from the API."""
    print("Fetching training data from API...")

    headers = {'Authorization': f'Bearer {API_KEY}'} if API_KEY else {}
    response = requests.get(f'{API_URL}/api/export/training-data', headers=headers)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch: {response.status_code} - {response.text}")

    data = response.json()
    print(f"Fetched {data['count']} records")

    records = []
    for row in data['data']:
        record = {
            'date': row['date'],
            'pxi_score': row['pxi_score'],
            'pxi_delta_1d': row['pxi_delta_1d'],
            'pxi_delta_7d': row['pxi_delta_7d'],
            'pxi_delta_30d': row['pxi_delta_30d'],
        }

        # Category scores
        for cat in ['breadth', 'credit', 'crypto', 'global', 'liquidity', 'macro', 'positioning', 'volatility']:
            record[f'cat_{cat}'] = row['categories'].get(cat, 0)

        # Key indicators
        indicators = row.get('indicators', {})
        record['vix'] = indicators.get('vix', 20)

        records.append(record)

    df = pd.DataFrame(records)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    # Calculate targets (forward returns)
    df['target_7d'] = df['pxi_score'].shift(-7) - df['pxi_score']
    df['target_30d'] = df['pxi_score'].shift(-30) - df['pxi_score']

    # Category dispersion
    cat_cols = [c for c in df.columns if c.startswith('cat_')]
    df['category_dispersion'] = df[cat_cols].max(axis=1) - df[cat_cols].min(axis=1)

    return df


def normalize_features(df: pd.DataFrame, feature_cols: List[str]) -> Tuple[pd.DataFrame, Dict]:
    """Normalize features and return normalization params."""
    norm_params = {}
    df_norm = df.copy()

    for col in feature_cols:
        if col in df.columns:
            mean = df[col].mean()
            std = df[col].std()
            if std == 0:
                std = 1
            df_norm[col] = (df[col] - mean) / std
            norm_params[col] = {'mean': float(mean), 'std': float(std)}

    return df_norm, norm_params


def create_sequences(df: pd.DataFrame, feature_cols: List[str], target_col: str,
                     seq_length: int) -> Tuple[np.ndarray, np.ndarray]:
    """Create sequences for LSTM training."""
    features = df[feature_cols].fillna(0).values
    targets = df[target_col].values

    X, y = [], []
    for i in range(len(df) - seq_length):
        if not np.isnan(targets[i + seq_length]):
            X.append(features[i:i + seq_length])
            y.append(targets[i + seq_length])

    return np.array(X, dtype=np.float32), np.array(y, dtype=np.float32)


class LSTMModel(nn.Module):
    """Simple LSTM model for time series prediction."""

    def __init__(self, input_size: int, hidden_size: int, num_layers: int):
        super().__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers

        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=0 if num_layers == 1 else 0.2
        )
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x):
        # x: (batch, seq_len, input_size)
        lstm_out, _ = self.lstm(x)
        # Take last timestep
        last_out = lstm_out[:, -1, :]
        return self.fc(last_out).squeeze(-1)


def train_pytorch_model(X_train: np.ndarray, y_train: np.ndarray,
                        X_val: np.ndarray, y_val: np.ndarray,
                        input_size: int, target_name: str) -> Tuple[nn.Module, Dict]:
    """Train LSTM model using PyTorch."""
    print(f"\nTraining LSTM for {target_name}...")
    print(f"  Train: {len(X_train)}, Val: {len(X_val)}")

    # Create datasets
    train_dataset = TensorDataset(
        torch.FloatTensor(X_train),
        torch.FloatTensor(y_train)
    )
    val_dataset = TensorDataset(
        torch.FloatTensor(X_val),
        torch.FloatTensor(y_val)
    )

    train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=BATCH_SIZE)

    # Initialize model
    model = LSTMModel(input_size, HIDDEN_SIZE, NUM_LAYERS)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=LEARNING_RATE)

    best_val_loss = float('inf')
    best_state = None
    patience = 10
    patience_counter = 0

    for epoch in range(EPOCHS):
        # Training
        model.train()
        train_loss = 0
        for X_batch, y_batch in train_loader:
            optimizer.zero_grad()
            y_pred = model(X_batch)
            loss = criterion(y_pred, y_batch)
            loss.backward()
            optimizer.step()
            train_loss += loss.item()

        train_loss /= len(train_loader)

        # Validation
        model.eval()
        val_loss = 0
        val_preds = []
        val_actuals = []
        with torch.no_grad():
            for X_batch, y_batch in val_loader:
                y_pred = model(X_batch)
                val_loss += criterion(y_pred, y_batch).item()
                val_preds.extend(y_pred.numpy())
                val_actuals.extend(y_batch.numpy())

        val_loss /= len(val_loader)

        # Early stopping
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            best_state = model.state_dict().copy()
            patience_counter = 0
        else:
            patience_counter += 1

        if (epoch + 1) % 10 == 0:
            print(f"  Epoch {epoch+1}: train_loss={train_loss:.4f}, val_loss={val_loss:.4f}")

        if patience_counter >= patience:
            print(f"  Early stopping at epoch {epoch+1}")
            break

    # Load best model
    model.load_state_dict(best_state)

    # Calculate metrics
    model.eval()
    val_preds = []
    val_actuals = []
    with torch.no_grad():
        for X_batch, y_batch in val_loader:
            val_preds.extend(model(X_batch).numpy())
            val_actuals.extend(y_batch.numpy())

    val_preds = np.array(val_preds)
    val_actuals = np.array(val_actuals)

    mse = np.mean((val_preds - val_actuals) ** 2)
    mae = np.mean(np.abs(val_preds - val_actuals))
    ss_res = np.sum((val_actuals - val_preds) ** 2)
    ss_tot = np.sum((val_actuals - np.mean(val_actuals)) ** 2)
    r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0

    print(f"  Final MSE: {mse:.4f}, MAE: {mae:.4f}, R2: {r2:.4f}")

    metrics = {
        'mse': float(mse),
        'mae': float(mae),
        'r2': float(r2),
        'best_epoch': EPOCHS - patience_counter,
    }

    return model, metrics


def export_model_weights(model: nn.Module, norm_params: Dict, target: str) -> Dict:
    """Export PyTorch model weights to JSON-serializable format."""
    state = model.state_dict()

    # Extract LSTM weights
    # PyTorch LSTM has weight_ih_l0, weight_hh_l0, bias_ih_l0, bias_hh_l0
    # Shape: weight_ih is (4*hidden_size, input_size) - gates are [i, f, g, o]
    # Shape: weight_hh is (4*hidden_size, hidden_size)

    weights = {
        'lstm': {
            'weight_ih': state['lstm.weight_ih_l0'].numpy().tolist(),
            'weight_hh': state['lstm.weight_hh_l0'].numpy().tolist(),
            'bias_ih': state['lstm.bias_ih_l0'].numpy().tolist(),
            'bias_hh': state['lstm.bias_hh_l0'].numpy().tolist(),
        },
        'fc': {
            'weight': state['fc.weight'].numpy().tolist(),
            'bias': state['fc.bias'].numpy().tolist(),
        },
        'config': {
            'input_size': state['lstm.weight_ih_l0'].shape[1],
            'hidden_size': HIDDEN_SIZE,
            'seq_length': SEQUENCE_LENGTH,
        },
        'normalization': norm_params,
    }

    return weights


def main():
    if not HAS_TORCH:
        print("ERROR: PyTorch is required for LSTM training")
        print("Install with: pip install torch")
        sys.exit(1)

    # Fetch data
    df = fetch_training_data()
    print(f"\nDataset: {len(df)} rows, {df['date'].min()} to {df['date'].max()}")

    # Normalize features
    df_norm, norm_params = normalize_features(df, FEATURE_COLS)

    # Available features (filter to those in df)
    available_features = [f for f in FEATURE_COLS if f in df_norm.columns]
    print(f"Using {len(available_features)} features: {available_features}")

    # Train/val split (time-based)
    split_idx = int(len(df_norm) * 0.8)

    models = {}
    metrics = {}

    for target in ['target_7d', 'target_30d']:
        # Create sequences
        X, y = create_sequences(df_norm, available_features, target, SEQUENCE_LENGTH)
        print(f"\n{target}: {len(X)} sequences")

        if len(X) < 100:
            print(f"  Skipping {target} - insufficient data")
            continue

        # Split
        X_train, X_val = X[:split_idx-SEQUENCE_LENGTH], X[split_idx-SEQUENCE_LENGTH:]
        y_train, y_val = y[:split_idx-SEQUENCE_LENGTH], y[split_idx-SEQUENCE_LENGTH:]

        # Train
        model, model_metrics = train_pytorch_model(
            X_train, y_train, X_val, y_val,
            input_size=len(available_features),
            target_name=target
        )

        target_key = '7d' if '7d' in target else '30d'
        models[target_key] = model
        metrics[target_key] = model_metrics

    # Export models
    output_dir = os.path.dirname(os.path.abspath(__file__))

    export = {
        'type': 'lstm',
        'version': '1.0',
        'created_at': datetime.utcnow().isoformat(),
        'config': {
            'sequence_length': SEQUENCE_LENGTH,
            'hidden_size': HIDDEN_SIZE,
            'feature_names': available_features,
        },
        'normalization': norm_params,
        'models': {},
        'metrics': metrics,
    }

    for target_key, model in models.items():
        export['models'][target_key] = export_model_weights(model, norm_params, target_key)

    output_path = os.path.join(output_dir, 'pxi_lstm_model.json')
    with open(output_path, 'w') as f:
        json.dump(export, f, indent=2)

    print(f"\nModel exported to: {output_path}")
    print(f"File size: {os.path.getsize(output_path) / 1024:.1f} KB")

    # Compact version
    compact = {
        'v': '1.0',
        'c': {
            's': SEQUENCE_LENGTH,
            'h': HIDDEN_SIZE,
            'f': available_features,
        },
        'n': norm_params,
        'm': {
            k: {
                'lstm': v['lstm'],
                'fc': v['fc'],
            }
            for k, v in export['models'].items()
        },
    }

    compact_path = os.path.join(output_dir, 'pxi_lstm_compact.json')
    with open(compact_path, 'w') as f:
        json.dump(compact, f, separators=(',', ':'))

    print(f"Compact model: {compact_path}")
    print(f"Compact size: {os.path.getsize(compact_path) / 1024:.1f} KB")


if __name__ == '__main__':
    main()
