import csv
import json
import logging
import os
from datetime import datetime
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

# Нативный менеджер памяти проекта
from db_manager import DBManager

logger = logging.getLogger("Piluitus.PnLExporter")

class PnLExporter:
    def __init__(self, output_dir: str = "reports"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    async def _load_closed_trades_from_redis(self, db: DBManager) -> list:
        """
        Извлекает снапшот журнала сделок напрямую из Redis ОЗУ.
        Исключает I/O Bottleneck жесткого диска во время торгов.
        """
        try:
            # Получаем словарь активных/закрытых позиций из Redis
            raw_data = await db.get_active_positions()
            if not raw_data:
                logger.warning("Журнал сделок в Redis пуст.")
                return []
            
            # Поддерживаем структуру как списка, так и словаря
            raw_trades = raw_data if isinstance(raw_data, list) else list(raw_data.values())
            
            # Отбираем строго закрытые сделки, у которых рассчитан финансовый результат
            closed_trades = [
                t for t in raw_trades 
                if isinstance(t, dict) and t.get("status") == "CLOSED" and "exit_price" in t
            ]
            return closed_trades
        except Exception as e:
            logger.error(f"Ошибка при извлечении журнала сделок из Redis: {e}")
            return []

    async def export_to_csv(self, db: DBManager) -> str:
        """Экспорт сделок из ОЗУ в стандартный плоский CSV-файл."""
        trades = await self._load_closed_trades_from_redis(db)
        if not trades:
            logger.warning("Нет закрытых сделок в ОЗУ для экспорта в CSV.")
            return ""

        filename = f"pnl_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        filepath = os.path.join(self.output_dir, filename)

        headers = [
            "ID Сделки", "Тикер", "Направление", "Кол-во лотов", 
            "Цена входа", "Цена выхода", "Результат %", "Исход", "Время входа"
        ]

        try:
            with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f, delimiter=";")
                writer.writerow(headers)
                
                for t in trades:
                    writer.writerow([
                        t.get("id", "N/A"),
                        t.get("ticker", "N/A"),
                        t.get("side", "N/A"),
                        t.get("lots", 0),
                        t.get("entry_price", 0.0),
                        t.get("exit_price", 0.0),
                        f"{t.get('pnl_percent', 0.0):.2f}%",
                        t.get("final_outcome", t.get("exit_reason", "N/A")),
                        t.get("entry_time", "N/A")
                    ])
            logger.info(f"CSV отчет успешно сохранен из ОЗУ: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Ошибка асинхронной записи CSV: {e}")
            return ""

    async def export_to_xlsx(self, db: DBManager) -> str:
        """Генерация XLSX отчета с форматированием и цветовой разметкой PnL на основе данных Redis."""
        trades = await self._load_closed_trades_from_redis(db)
        if not trades:
            logger.warning("Нет закрытых сделок в ОЗУ для экспорта в Excel.")
            return ""

        filename = f"pnl_analytics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        filepath = os.path.join(self.output_dir, filename)

        wb = Workbook()
        ws = wb.active
        if ws is None:
            ws = wb.create_sheet(title="Журнал PnL")
        else:
            ws.title = "Журнал PnL"
            
        # Корректное включение сетки таблицы для всех версий openpyxl
        ws.sheet_view.showGridLines = True

        # Стилизация элементов оформления таблицы
        font_title = Font(name="Calibri", size=14, bold=True, color="FFFFFF")
        font_header = Font(name="Calibri", size=11, bold=True, color="FFFFFF")
        font_bold = Font(name="Calibri", size=11, bold=True)
        
        fill_title = PatternFill(start_color="1F497D", end_color="1F497D", fill_type="solid")
        fill_header = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
        fill_green = PatternFill(start_color="E2EFDA", end_color="E2EFDA", fill_type="solid")
        fill_red = PatternFill(start_color="FCE4D6", end_color="FCE4D6", fill_type="solid")

        border_thin = Border(
            left=Side(style='thin', color='D9D9D9'), right=Side(style='thin', color='D9D9D9'),
            top=Side(style='thin', color='D9D9D9'), bottom=Side(style='thin', color='D9D9D9')
        )

        # Рендеринг главной шапки
        ws.merge_cells("A1:I1")
        ws["A1"] = f"ОТЧЕТ ЭФФЕКТИВНОСТИ РОБОТА PILUITUS — {datetime.now().strftime('%d.%m.%Y')}"
        ws["A1"].font = font_title
        ws["A1"].fill = fill_title
        ws["A1"].alignment = Alignment(horizontal="center", vertical="center")
        ws.row_dimensions[1].height = 40

        headers = [
            "ID СДЕЛКИ", "ТИКЕР", "НАПРАВЛЕНИЕ", "ЛОТЫ", 
            "ЦЕНА ВХОДА", "ЦЕНА ВЫХОДА", "ПРОФИТ (%)", "СТАТУС ИСХОДА", "ВРЕМЯ ВХОДА"
        ]
        
        ws.append([]) 
        ws.append(headers)
        ws.row_dimensions[3].height = 25

        for col_idx in range(1, 10):
            cell = ws.cell(row=3, column=col_idx)
            cell.font = font_header
            cell.fill = fill_header
            cell.alignment = Alignment(horizontal="center", vertical="center")

        current_row = 4
        total_pnl = 0.0

        for t in trades:
            pnl = float(t.get("pnl_percent", 0.0))
            total_pnl += pnl
            
            row_data = [
                t.get("id", "N/A"),
                t.get("ticker", "N/A"),
                t.get("side", "N/A"),
                t.get("lots", 0),
                t.get("entry_price", 0.0),
                t.get("exit_price", 0.0),
                pnl / 100.0, 
                t.get("final_outcome", t.get("exit_reason", "N/A")),
                t.get("entry_time", "N/A")[:19].replace("T", " ")
            ]
            ws.append(row_data)
            ws.row_dimensions[current_row].height = 20

            # Поячеистая подгонка форматов под стандарты Мосбиржи
            for col_idx in range(1, 10):
                cell = ws.cell(row=current_row, column=col_idx)
                cell.border = border_thin
                
                if col_idx in (1, 2, 3, 8):
                    cell.alignment = Alignment(horizontal="center", vertical="center")
                
                if col_idx in (5, 6):
                    cell.number_format = '#,##0.0000' # Храним точность до 4 знаков для копеечных акций
                    
                if col_idx == 7:
                    cell.number_format = '0.00%'
                    cell.fill = fill_green if pnl >= 0 else fill_red

            current_row += 1

        # Строка финансового итога сессии
        ws.cell(row=current_row, column=6, value="ИТОГОВЫЙ PnL:").font = font_bold
        pnl_total_cell = ws.cell(row=current_row, column=7, value=total_pnl / 100.0)
        pnl_total_cell.font = font_bold
        pnl_total_cell.number_format = '0.00%'
        pnl_total_cell.fill = fill_green if total_pnl >= 0 else fill_red
        pnl_total_cell.border = Border(bottom=Side(style='double', color='000000'), top=Side(style='thin', color='000000'))

        # Динамический автоподбор ширины ячеек через чистый перебор столбцов
        for col_idx, col in enumerate(ws.columns, start=1):
            col_letter = get_column_letter(col_idx)
            max_len = max(len(str(cell.value or '')) for cell in col)
            ws.column_dimensions[col_letter].width = max(max_len + 3, 12)

        try:
            wb.save(filepath)
            logger.info(f"Excel аналитика успешно сгенерирована из ОЗУ: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Ошибка сохранения Excel: {e}")
            return ""

