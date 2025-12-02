#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
metrics.core
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Metrics Layer 표준 인터페이스
"""

from typing import List, Optional, Sequence
import pandas as pd

from .schema import METRIC_GROUPS
from .temporal import build_snapshot_panel, compute_multiple_deltas, _sanitize_label


class MetricsManager:
    """
    시맨틱 레이어: DB → Analysis 사이의 표준화된 데이터 제공
    
    Usage:
        dm = DataManager(db_path)
        metrics = MetricsManager(dm)
        
        # 단일 스냅샷 뷰
        df = metrics.get_view(
            metric_groups=['core', 'action'],
            n_latest=1
        )
        
        # 복수 스냅샷 시계열 뷰
        df = metrics.get_view(
            metric_groups=['core', 'action', 'performance_snapshot'],
            n_latest=3,
            compute_deltas=True
        )
    """
    
    def __init__(self, data_manager):
        """
        Args:
            data_manager: DataManager 인스턴스
        """
        self.dm = data_manager
    
    def get_view(
        self,
        metric_groups: Sequence[str] = ['all'],
        snapshot_ids: Optional[List[int]] = None,
        n_latest: int = 1,
        include_unmatched: bool = True,
        compute_deltas: bool = False,
        delta_metrics: Optional[List[str]] = None,
        as_pct: bool = False,
    ) -> pd.DataFrame:
        """
        표준화된 분석용 데이터프레임 생성
        
        Args:
            metric_groups:
                포함할 컬럼 그룹 리스트
                - 'core': 매칭상태, 품번 등
                - 'action': 할인전략 지표
                - 'performance_snapshot': 가격/판매량/재고
                - 'performance_rolling_7d': 최근 7일 지표
                - 'meta': 제품명/카테고리/링크
                - 'all': 모든 컬럼
                예: ['core', 'action', 'performance_snapshot']
            
            snapshot_ids:
                명시적 스냅샷 ID 리스트 (None이면 n_latest 사용)
            
            n_latest:
                최신 N개 스냅샷 (snapshot_ids가 None일 때만 사용)
            
            include_unmatched:
                아이허브 미매칭 상품 포함 여부
            
            compute_deltas:
                시점 간 변화량(Δ) 계산 여부
                - True: metric_delta_t0_t1 컬럼 추가
            
            delta_metrics:
                Δ 계산할 메트릭 리스트 (None이면 performance_snapshot 전체)
            
            as_pct:
                Δ를 퍼센트로 계산 (True: %, False: 절대값)
        
        Returns:
            표준화된 DataFrame
            
            [단일 스냅샷 (n_latest=1)]
            - metric 컬럼들만 포함
            
            [복수 스냅샷 (n_latest>=2)]
            - metric__t0, metric__t1, ... (시점별)
            - metric_delta_t0_t1 (compute_deltas=True인 경우)
        """
        
        # 1. 필요한 컬럼 목록 결정
        selected_metrics = self._resolve_metric_groups(metric_groups)
        
        # 2. 스냅샷 로드
        if n_latest == 1:
            # 단일 스냅샷
            df = self._get_single_snapshot_view(
                snapshot_ids, 
                include_unmatched,
                selected_metrics
            )
            return df
        
        else:
            # 복수 스냅샷 → wide 패널
            df_panel = self._get_panel_view(
                snapshot_ids,
                n_latest,
                include_unmatched,
                selected_metrics,
                compute_deltas,
                delta_metrics,
                as_pct
            )
            return df_panel
    
    def _resolve_metric_groups(self, groups: Sequence[str]) -> List[str]:
        """그룹명 → 실제 컬럼 리스트 변환"""
        if 'all' in groups:
            return METRIC_GROUPS['all']
        
        metrics = []
        for g in groups:
            if g in METRIC_GROUPS:
                metrics.extend(METRIC_GROUPS[g])
            else:
                print(f"⚠️ 알 수 없는 메트릭 그룹: {g}")
        
        # 중복 제거 (순서 유지)
        seen = set()
        unique = []
        for m in metrics:
            if m not in seen:
                seen.add(m)
                unique.append(m)
        
        return unique
    
    def _get_single_snapshot_view(
        self,
        snapshot_ids: Optional[List[int]],
        include_unmatched: bool,
        selected_metrics: List[str]
    ) -> pd.DataFrame:
        """단일 스냅샷 뷰"""
        
        if snapshot_ids:
            sid = snapshot_ids[0]
            df = self.dm.get_snapshot_view(
                snapshot_id=sid,
                include_unmatched=include_unmatched
            )
        else:
            df = self.dm.get_snapshot_view(
                include_unmatched=include_unmatched
            )
        
        if df.empty:
            return df
        
        # 필요한 컬럼만 선택 (존재하는 것만)
        available = [c for c in selected_metrics if c in df.columns]
        return df[available].copy()
    
    def _get_panel_view(
        self,
        snapshot_ids: Optional[List[int]],
        n_latest: int,
        include_unmatched: bool,
        selected_metrics: List[str],
        compute_deltas: bool,
        delta_metrics: Optional[List[str]],
        as_pct: bool
    ) -> pd.DataFrame:
        """복수 스냅샷 wide 패널 뷰"""
        
        # 1. 패널 데이터 로드
        panels = self.dm.get_panel_views(
            snapshot_ids=snapshot_ids,
            n_latest=n_latest,
            include_unmatched=include_unmatched
        )
        
        if not panels:
            return pd.DataFrame()
        
        print(f"\n📦 패널 로드 완료:")
        for i, p in enumerate(panels):
            print(f"   [{i}] {p['snapshot_date']}: {len(p['df']):,}개")
        
        # 2. 날짜 기반 라벨 생성
        labels = []
        for p in panels:
            date = p['snapshot_date']
            if date:
                labels.append(date)
            else:
                labels.append(f"t{len(labels)}")
        
        print(f"\n🏷️  라벨: {labels}")
        
        # 3. Key 컬럼 결정 (조인 기준)
        key_cols = ['iherb_vendor_id']  # 기본 키
        
        # 🔥 핵심 수정: Key 컬럼을 selected_metrics에 자동 추가
        metrics_with_keys = list(key_cols) + [m for m in selected_metrics if m not in key_cols]
        
        # 4. Wide 패널 생성
        dfs = [p['df'] for p in panels]
        
        print(f"\n🔗 Wide 패널 생성 중...")
        panel_df = build_snapshot_panel(
            dfs=dfs,
            key_cols=key_cols,
            metric_cols=metrics_with_keys,  # 🔥 Key 포함된 리스트 사용
            labels=labels,
            how='left'  # 최신 스냅샷 기준
        )
        
        print(f"   ✅ 최종 패널: {len(panel_df):,}행 × {len(panel_df.columns)}열")
        
        # 5. Δ 계산 (옵션)
        if compute_deltas and len(labels) >= 2:
            if delta_metrics is None:
                # 기본: performance_snapshot 전체
                delta_metrics = METRIC_GROUPS['performance_snapshot']
            
            print(f"\n📊 Δ 계산 중 ({labels[0]} vs {labels[1]}):")
            
            # 실제 존재하는 메트릭만 필터링
            available_delta = []
            for m in delta_metrics:
                col_new = f"{m}__{_sanitize_label(labels[0])}"
                col_old = f"{m}__{_sanitize_label(labels[1])}"
                if col_new in panel_df.columns and col_old in panel_df.columns:
                    available_delta.append(m)
            
            print(f"   대상 메트릭: {available_delta}")
            
            if available_delta:
                panel_df = compute_multiple_deltas(
                    panel=panel_df,
                    metrics=available_delta,
                    newer_label=labels[0],
                    older_label=labels[1],
                    as_pct=as_pct
                )
                
                delta_cols = [c for c in panel_df.columns if 'delta' in c]
                print(f"   ✅ 생성된 Δ 컬럼: {len(delta_cols)}개")
            else:
                print(f"   ⚠️ Δ 계산 가능한 메트릭이 없습니다.")
        
        return panel_df