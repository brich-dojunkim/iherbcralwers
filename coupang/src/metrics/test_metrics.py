#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.data_manager import DataManager
from src.metrics import MetricsManager
from config.settings import Config


def test_single_snapshot():
    """단일 스냅샷 테스트"""
    dm = DataManager(Config.INTEGRATED_DB_PATH)
    metrics = MetricsManager(dm)
    
    df = metrics.get_view(
        metric_groups=['core', 'action'],
        n_latest=1
    )
    
    print("✅ 단일 스냅샷 테스트")
    print(f"   행: {len(df)}")
    print(f"   컬럼: {list(df.columns)[:5]}...")


def test_dual_snapshot():
    """이중 스냅샷 + Δ 테스트"""
    dm = DataManager(Config.INTEGRATED_DB_PATH)
    metrics = MetricsManager(dm)
    
    df = metrics.get_view(
        metric_groups=['core', 'action', 'performance_snapshot'],
        n_latest=2,
        compute_deltas=True,
        delta_metrics=['iherb_sales_quantity']
    )
    
    print("\n✅ 이중 스냅샷 테스트")
    print(f"   행: {len(df)}")
    
    # Δ 컬럼 확인
    delta_cols = [c for c in df.columns if 'delta' in c]
    print(f"   Δ 컬럼: {delta_cols}")


def test_multi_snapshot():
    """다중 스냅샷 테스트"""
    dm = DataManager(Config.INTEGRATED_DB_PATH)
    metrics = MetricsManager(dm)
    
    df = metrics.get_view(
        metric_groups=['core', 'performance_snapshot'],
        n_latest=3,
        compute_deltas=False
    )
    
    print("\n✅ 다중 스냅샷 테스트")
    print(f"   행: {len(df)}")
    
    # 시계열 컬럼 확인
    time_cols = [c for c in df.columns if '__' in c]
    print(f"   시계열 컬럼 예시: {time_cols[:3]}...")


if __name__ == "__main__":
    test_single_snapshot()
    test_dual_snapshot()
    test_multi_snapshot()