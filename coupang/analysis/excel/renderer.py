#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Excel Renderer
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Excel 렌더링 (매핑 + 렌더링 통합)
"""

import pandas as pd
from pathlib import Path
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.formatting.rule import DataBarRule

from src.metrics.schema import METRIC_TYPES
from .styles import ExcelConfig, FORMATS


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
            # 1. 데이터 쓰기
            with pd.ExcelWriter(self.output_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=self.sheet_name, index=False, header=False)
            
            # 2. 헤더 공간 확보
            self.wb = load_workbook(self.output_path)
            self.ws = self.wb[self.sheet_name]
            self.ws.insert_rows(1, 3)
            
            # 3. 헤더 스타일
            self._apply_header_styles(config.header_groups)
            
            # 4. 컬럼 너비
            self._set_column_widths(config.column_widths)
            
            # 5. 데이터 영역 스타일
            self._apply_data_styles(config.column_formats or {})
            
            # 6. 조건부 서식
            if config.conditional_formats:
                self._apply_conditional_formats(config.conditional_formats)
            
            # 7. 데이터바
            if config.databar_columns:
                self._apply_databars(config.databar_columns)
            
            # 8. 하이퍼링크
            if config.link_columns:
                self._apply_links(config.link_columns)
            
            # 9. UI
            if config.freeze_panes:
                self.ws.freeze_panes = self.ws.cell(*config.freeze_panes)
            
            if config.auto_filter:
                self.ws.auto_filter.ref = (
                    f"A3:{get_column_letter(self.ws.max_column)}{self.ws.max_row}"
                )
            
            # 10. 저장
            self.wb.save(self.output_path)
            
            return {
                'success': True,
                'path': str(self.output_path),
                'rows': len(df),
                'cols': len(df.columns),
                'error': None
            }
        
        except Exception as e:
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
    
    def _apply_header_styles(self, groups):
        """3단 헤더 스타일"""
        border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        
        col_pos = 1
        
        for group in groups:
            total_span = sum(len(sg.cols) for sg in group.sub_groups)
            
            # 1단: 그룹 헤더
            for i in range(col_pos, col_pos + total_span):
                cell = self.ws.cell(1, i)
                cell.fill = PatternFill(start_color=group.color_top, end_color=group.color_top, fill_type="solid")
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
                sub_span = len(sub.cols)
                
                # 2단
                for i in range(col_pos, col_pos + sub_span):
                    cell = self.ws.cell(2, i)
                    cell.fill = PatternFill(start_color=group.color_mid, end_color=group.color_mid, fill_type="solid")
                    cell.font = Font(color="FFFFFF", bold=True, size=11)
                    cell.alignment = Alignment(horizontal='center', vertical='center')
                    cell.border = border
                
                if sub_span > 1:
                    self.ws.merge_cells(start_row=2, start_column=col_pos, end_row=2, end_column=col_pos + sub_span - 1)
                
                self.ws.cell(2, col_pos).value = sub.name
                
                # 3단
                for i, col_name in enumerate(sub.cols):
                    cell = self.ws.cell(3, col_pos + i)
                    cell.value = col_name
                    cell.fill = PatternFill(start_color=group.color_bottom, end_color=group.color_bottom, fill_type="solid")
                    cell.font = Font(color="000000", bold=True, size=10)
                    cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
                    cell.border = border
                
                col_pos += sub_span
    
    def _set_column_widths(self, widths):
        """컬럼 너비"""
        for col_idx in range(1, self.ws.max_column + 1):
            col_name = self.ws.cell(3, col_idx).value
            width = widths.get(col_name, 12.0)
            self.ws.column_dimensions[get_column_letter(col_idx)].width = width
    
    def _apply_data_styles(self, formats):
        """데이터 영역 스타일"""
        border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        
        for row_idx in range(4, self.ws.max_row + 1):
            for col_idx in range(1, self.ws.max_column + 1):
                cell = self.ws.cell(row_idx, col_idx)
                cell.border = border
                cell.alignment = Alignment(vertical='center', wrap_text=False)
                
                col_name = self.ws.cell(3, col_idx).value
                if col_name in formats:
                    cell.number_format = formats[col_name]
    
    def _apply_conditional_formats(self, formats):
        """조건부 서식"""
        for fmt in formats:
            col_idx = self._find_column(fmt.column)
            if not col_idx:
                continue
            
            for row_idx in range(4, self.ws.max_row + 1):
                cell = self.ws.cell(row_idx, col_idx)
                try:
                    if fmt.condition(cell.value):
                        cell.fill = PatternFill(start_color=fmt.color, end_color=fmt.color, fill_type="solid")
                except:
                    pass
    
    def _apply_databars(self, columns):
        """데이터바"""
        for col_name in columns:
            col_idx = self._find_column(col_name)
            if not col_idx:
                continue
            
            col_letter = get_column_letter(col_idx)
            rule = DataBarRule(
                start_type='num', start_value=0,
                end_type='num', end_value=100,
                color="63C384"
            )
            self.ws.conditional_formatting.add(f'{col_letter}4:{col_letter}{self.ws.max_row}', rule)
    
    def _apply_links(self, columns):
        """하이퍼링크"""
        for col_name in columns:
            col_idx = self._find_column(col_name)
            if not col_idx:
                continue
            
            for row_idx in range(4, self.ws.max_row + 1):
                cell = self.ws.cell(row_idx, col_idx)
                url = cell.value
                
                if url and str(url).startswith('http'):
                    cell.hyperlink = str(url)
                    cell.value = "Link"
                    cell.font = Font(color="0563C1", underline="single")
                    cell.alignment = Alignment(horizontal='center', vertical='center')
    
    def _find_column(self, col_name: str) -> int:
        """컬럼명으로 인덱스 찾기"""
        for col_idx in range(1, self.ws.max_column + 1):
            if self.ws.cell(3, col_idx).value == col_name:
                return col_idx
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 헬퍼 함수: 메트릭 메타데이터 → Excel 서식 자동 매핑
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def infer_format(metric_key: str) -> str:
    """메트릭 타입 → Excel 서식 자동 추론"""
    metric_type = METRIC_TYPES.get(metric_key)
    
    if metric_type == "currency":
        return FORMATS["currency"]
    elif metric_type == "percentage":
        return FORMATS["percentage"]
    elif metric_type == "integer":
        return FORMATS["integer"]
    elif metric_type == "float":
        return FORMATS["float"]
    elif metric_type == "url":
        return FORMATS["text"]
    
    return None