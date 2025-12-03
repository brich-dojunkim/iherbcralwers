#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Excel Renderer
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Excel 렌더링 엔진 (성능 최적화)
"""

import pandas as pd
from pathlib import Path
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

from .types import ExcelConfig
from .constants import COLOR_SCHEMES


class ExcelRenderer:
    """Excel 렌더러"""
    
    def __init__(self, output_path: str, sheet_name: str = 'Sheet1'):
        self.output_path = Path(output_path)
        self.sheet_name = sheet_name
        self.wb = None
        self.ws = None
    
    def render(self, df: pd.DataFrame, config: ExcelConfig) -> dict:
        """DataFrame → Excel 렌더링
        
        Returns:
            {'success': bool, 'path': str, 'rows': int, 'cols': int, 'error': str}
        """
        try:
            print(f"[RENDERER] 1/8 데이터 쓰기... ({len(df):,}행)")
            # 1. 데이터 쓰기
            with pd.ExcelWriter(self.output_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=self.sheet_name, index=False, header=False)
            print(f"[RENDERER] 1/8 완료")
            
            print(f"[RENDERER] 2/8 파일 로드...")
            # 2. 파일 로드
            self.wb = load_workbook(self.output_path)
            self.ws = self.wb[self.sheet_name]
            self.ws.insert_rows(1, 3)
            print(f"[RENDERER] 2/8 완료")
            
            print(f"[RENDERER] 3/8 헤더 렌더링...")
            # 3. 헤더
            self._render_headers(config.groups)
            print(f"[RENDERER] 3/8 완료")
            
            print(f"[RENDERER] 4/8 컬럼 너비...")
            # 4. 컬럼 너비
            self._set_column_widths(config.columns)
            print(f"[RENDERER] 4/8 완료")
            
            print(f"[RENDERER] 5/8 데이터 영역 스타일...")
            # 5. 데이터 영역
            self._style_data_area(config.columns)
            print(f"[RENDERER] 5/8 완료")
            
            print(f"[RENDERER] 6/8 조건부 서식... ({len(config.conditional_rules)}개 규칙)")
            # 6. 조건부 서식
            if config.conditional_rules:
                self._apply_conditional_rules(config.conditional_rules)
            print(f"[RENDERER] 6/8 완료")
            
            print(f"[RENDERER] 7/8 링크 처리...")
            # 7. 링크
            self._apply_links()
            print(f"[RENDERER] 7/8 완료")
            
            print(f"[RENDERER] 8/8 UI 설정...")
            # 8. UI
            if config.freeze_panes:
                self.ws.freeze_panes = self.ws.cell(*config.freeze_panes)
            
            if config.auto_filter:
                self.ws.auto_filter.ref = (
                    f"A3:{get_column_letter(self.ws.max_column)}{self.ws.max_row}"
                )
            
            # 저장
            self.wb.save(self.output_path)
            print(f"[RENDERER] 8/8 완료")
            
            return {
                'success': True,
                'path': str(self.output_path),
                'rows': len(df),
                'cols': len(df.columns),
                'error': None
            }
        
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'path': str(self.output_path),
                'rows': 0,
                'cols': 0,
                'error': str(e)
            }
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Private Methods
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    def _render_headers(self, groups):
        """3단 헤더 렌더링"""
        border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        
        col_pos = 1
        
        for group in groups:
            colors = COLOR_SCHEMES[group.color_scheme]
            total_span = sum(len(sg.columns) for sg in group.sub_groups)
            
            # 1단: 그룹 헤더
            for i in range(col_pos, col_pos + total_span):
                cell = self.ws.cell(1, i)
                cell.fill = PatternFill(start_color=colors["top"], end_color=colors["top"], fill_type="solid")
                cell.font = Font(color="FFFFFF", bold=True, size=11)
                cell.alignment = Alignment(horizontal='center', vertical='center')
                cell.border = border
            
            if total_span > 1:
                self.ws.merge_cells(start_row=1, start_column=col_pos, end_row=1, end_column=col_pos + total_span - 1)
            
            self.ws.cell(1, col_pos).value = group.name
            
            # 양 끝 강조
            self.ws.cell(1, col_pos).border = Border(
                left=Side(style='medium'), right=Side(style='thin'),
                top=Side(style='thin'), bottom=Side(style='thin')
            )
            self.ws.cell(1, col_pos + total_span - 1).border = Border(
                left=Side(style='thin'), right=Side(style='medium'),
                top=Side(style='thin'), bottom=Side(style='thin')
            )
            
            # 2단 & 3단
            for sub in group.sub_groups:
                sub_span = len(sub.columns)
                
                # 2단
                for i in range(col_pos, col_pos + sub_span):
                    cell = self.ws.cell(2, i)
                    cell.fill = PatternFill(start_color=colors["mid"], end_color=colors["mid"], fill_type="solid")
                    cell.font = Font(color="FFFFFF", bold=True, size=11)
                    cell.alignment = Alignment(horizontal='center', vertical='center')
                    cell.border = border
                
                if sub_span > 1:
                    self.ws.merge_cells(start_row=2, start_column=col_pos, end_row=2, end_column=col_pos + sub_span - 1)
                
                self.ws.cell(2, col_pos).value = sub.name
                
                # 3단
                for i, col_name in enumerate(sub.columns):
                    cell = self.ws.cell(3, col_pos + i)
                    cell.value = col_name
                    cell.fill = PatternFill(start_color=colors["bottom"], end_color=colors["bottom"], fill_type="solid")
                    cell.font = Font(color="000000", bold=True, size=10)
                    cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
                    cell.border = border
                
                col_pos += sub_span
    
    def _set_column_widths(self, columns):
        """컬럼 너비 설정"""
        for col_idx, col_spec in enumerate(columns, 1):
            self.ws.column_dimensions[get_column_letter(col_idx)].width = col_spec.width
    
    def _style_data_area(self, columns):
        """데이터 영역 스타일 (테두리, 서식, 정렬)"""
        border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        
        for row_idx in range(4, self.ws.max_row + 1):
            for col_idx, col_spec in enumerate(columns, 1):
                cell = self.ws.cell(row_idx, col_idx)
                cell.border = border
                cell.number_format = col_spec.number_format
                cell.alignment = Alignment(
                    horizontal=col_spec.alignment,
                    vertical='center',
                    wrap_text=False
                )
    
    def _apply_conditional_rules(self, rules):
        """조건부 서식 적용 (최적화: 컬럼별 한 번만 순회)"""
        # 컬럼별로 규칙 그룹핑
        rules_by_column = {}
        for rule in rules:
            if rule.column not in rules_by_column:
                rules_by_column[rule.column] = []
            rules_by_column[rule.column].append(rule)
        
        # 컬럼별 처리
        for col_name, col_rules in rules_by_column.items():
            col_idx = self._find_column(col_name)
            if not col_idx:
                continue
            
            # 해당 컬럼만 한 번 순회
            for row_idx in range(4, self.ws.max_row + 1):
                cell = self.ws.cell(row_idx, col_idx)
                value = cell.value
                
                # 모든 규칙 체크 (첫 번째 매칭되는 것 적용)
                for rule in col_rules:
                    try:
                        if rule.condition(value):
                            if rule.fill_color:
                                cell.fill = PatternFill(
                                    start_color=rule.fill_color,
                                    end_color=rule.fill_color,
                                    fill_type="solid"
                                )
                            if rule.font_color:
                                cell.font = Font(color=rule.font_color)
                            break  # 첫 매칭만
                    except:
                        pass
    
    def _apply_links(self):
        """하이퍼링크 처리"""
        # 3행에서 링크 컬럼 찾기
        link_columns = []
        for col_idx in range(1, self.ws.max_column + 1):
            col_name = self.ws.cell(3, col_idx).value
            if col_name and ('링크' in str(col_name) or 'url' in str(col_name).lower()):
                link_columns.append(col_idx)
        
        # 링크 처리
        for col_idx in link_columns:
            for row_idx in range(4, self.ws.max_row + 1):
                cell = self.ws.cell(row_idx, col_idx)
                url = cell.value
                
                if url and str(url).startswith('http'):
                    cell.hyperlink = str(url)
                    cell.value = "Link"
                    cell.font = Font(color="0563C1", underline="single")
                    cell.alignment = Alignment(horizontal='center', vertical='center')
    
    def _find_column(self, col_name: str) -> int:
        """컬럼명으로 인덱스 찾기 (3행 기준)"""
        for col_idx in range(1, self.ws.max_column + 1):
            if self.ws.cell(3, col_idx).value == col_name:
                return col_idx
        return None