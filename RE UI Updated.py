
import os
import sys
import logging
from PyQt5.QtWidgets import (QApplication, QCheckBox, QMainWindow, QPushButton, QLineEdit,
                             QLabel, QFileDialog, QMessageBox, QVBoxLayout, QWidget,
                             QRadioButton, QGridLayout, QComboBox, QDateEdit)
from PyQt5.QtCore import QDate
from Data_Loader import DataLoader
from Clean_And_Process2 import process_and_load_data
from data_analysis import analyze_real_estate_data

class DateRangePicker(QWidget):
    def __init__(self):
        super().__init__()
        self.layout = QVBoxLayout(self)
        self.rangeType = QComboBox()
        self.rangeType.addItems(["Custom", "Monthly", "Quarterly", "Annually"])
        self.rangeType.currentIndexChanged.connect(self.updateDatePickers)
        self.startDate = QDateEdit(calendarPopup=True)
        self.endDate = QDateEdit(calendarPopup=True)
        self.startDate.setDate(QDate.currentDate())
        self.endDate.setDate(QDate.currentDate())
        self.layout.addWidget(self.rangeType)
        self.layout.addWidget(self.startDate)
        self.layout.addWidget(self.endDate)

    def set_dates(self, start_date, end_date):
        self.startDate.setDate(QDate.fromString(start_date, "yyyy-MM-dd"))
        self.endDate.setDate(QDate.fromString(end_date, "yyyy-MM-dd"))

    def updateDatePickers(self, index):
        current_date = QDate.currentDate()
        first_day_of_month = current_date.addDays(-current_date.day() + 1)
        if self.rangeType.currentText() == "Monthly":
            self.startDate.setDate(first_day_of_month)
            self.endDate.setDate(first_day_of_month.addMonths(1).addDays(-1))
        elif self.rangeType.currentText() == "Quarterly":
            quarter = (current_date.month() - 1) // 3
            start_month = quarter * 3 + 1
            end_month = start_month + 2
            self.startDate.setDate(QDate(current_date.year(), start_month, 1))
            self.endDate.setDate(QDate(current_date.year(), end_month, QDate(current_date.year(), end_month, 1).daysInMonth()))
        elif self.rangeType.currentText() == "Annually":
            self.startDate.setDate(QDate(current_date.year(), 1, 1))
            self.endDate.setDate(QDate(current_date.year(), 12, 31))

class RealEstateAnalysisUI(QMainWindow):
    statistic_functions = {
        "New Listings": "new_listings",
        "Closed Listings": "closed_listings",
        "Avg Price Per Ft": "avg_sold_price_per_foot",
        "Avg Days on Market": "avg_days_on_market",
        "Total Volume": "total_dollar_volume",
        "Pending Listings": "pending_listings",
        "List to Sold Ratio": "list_price_to_sold_price_ratio",
        "Active Inventory": "active_inventory",
        "Month Supply of Inventory": "msi",
        "% of Cash Sales": "percent_cash_sales"
    }

    def __init__(self):
        super().__init__()
        self.title = "Real Estate Analysis Tool"
        self.left = 100
        self.top = 100
        self.width = 640
        self.height = 480
        self.data_loader = DataLoader('path_to_database.db')
        self.init_ui()
        self.init_dropdowns()
        self.setup_layout()

    def init_ui(self):
        self.setWindowTitle(self.title)
        self.setGeometry(self.left, self.top, self.width, self.height)

    def init_dropdowns(self):
        self.city_menu = QComboBox()
        self.subdivision_menu = QComboBox()
        self.type_menu = QComboBox()
        self.city_menu.addItem("All")
        self.subdivision_menu.addItem("All")
        self.type_menu.addItem("All")
        self.update_dropdowns()

    def setup_layout(self):
        main_layout = QVBoxLayout()
        self.setup_radio_buttons(main_layout)
        self.setup_file_selection(main_layout)
        self.setup_process_button(main_layout)
        self.setup_dropdown_menus(main_layout)
        self.setup_date_picker(main_layout)
        self.setup_statistic_checkboxes(main_layout)
        self.setup_analyze_button(main_layout)
        container = QWidget()
        container.setLayout(main_layout)
        self.setCentralWidget(container)

    def setup_radio_buttons(self, layout):
        self.create_db_radio = QRadioButton("Create New Database")
        self.add_data_radio = QRadioButton("Add Data to Existing Database")
        self.use_db_radio = QRadioButton("Use Existing Database", self)
        self.use_db_radio.setChecked(True)
        layout.addWidget(self.create_db_radio)
        layout.addWidget(self.add_data_radio)
        layout.addWidget(self.use_db_radio)

    def setup_file_selection(self, layout):
        self.db_label = QLabel("Database Path:")
        self.db_line_edit = QLineEdit()
        self.db_line_edit.setToolTip("Please select a database file.")
        self.select_db_button = QPushButton("Select Database")
        self.select_db_button.clicked.connect(self.select_database)
        self.csv_label = QLabel("Select CSV File (Optional):")
        self.csv_line_edit = QLineEdit()
        self.csv_line_edit.setToolTip("Please select a CSV file to process.")
        self.select_csv_button = QPushButton("Select CSV")
        self.select_csv_button.clicked.connect(self.select_csv)
        layout.addWidget(self.db_label)
        layout.addWidget(self.db_line_edit)
        layout.addWidget(self.select_db_button)
        layout.addWidget(self.csv_label)
        layout.addWidget(self.csv_line_edit)
        layout.addWidget(self.select_csv_button)

   def setup_process_button(self, layout):
        self.process_button = QPushButton("Process")
        self.process_button.clicked.connect(self.process_data)
        layout.addWidget(self.process_button)

    def setup_dropdown_menus(self, layout):
        layout.addWidget(QLabel("Select City:"))
        layout.addWidget(self.city_menu)
        layout.addWidget(QLabel("Select Subdivision:"))
        layout.addWidget(self.subdivision_menu)
        layout.addWidget(QLabel("Select Building Type:"))
        layout.addWidget(self.type_menu)

   def setup_date_picker(self, layout):
        result = self.data_loader.get_min_max_dates('listing_details')
        if result is not None and not result.empty:
            min_date = result.iloc[0]['min_date']
            max_date = result.iloc[0]['max_date']
            self.dateRangePicker = DateRangePicker()
            self.dateRangePicker.set_dates(min_date, max_date)
            layout.addWidget(self.dateRangePicker)
        else:
            logging.error("Failed to fetch date ranges or no data available.")


    def setup_analyze_button(self, layout):
        self.analyze_button = QPushButton("Analyze")
        self.analyze_button.clicked.connect(self.perform_analysis)
        layout.addWidget(self.analyze_button)

    def display_results(self, results):
        result_text = "\n".join([f"{key}: {value}" for key, value in results.items()])
        QMessageBox.information(self, "Analysis Results", result_text)

 def fetch_and_prepare_data(self):
        if not self.db_line_edit.text():
            QMessageBox.warning(self, "Error", "Database path is not set.")
            return None

        params = {
            'city': self.city_menu.currentText() if self.city_menu.currentText() != "All" else None,
            'subdivision': self.subdivision_menu.currentText() if self.subdivision_menu.currentText() != "All" else None,
            'building_type': self.type_menu.currentText() if self.type_menu.currentText() != "All" else None,
            'start_date': self.dateRangePicker.startDate.date().toString("yyyy-MM-dd"),
            'end_date': self.dateRangePicker.endDate.date().toString("yyyy-MM-dd")
        }

        # Fetching the filtered data from the data loader
        filtered_data = self.data_loader.fetch_filtered_data(params)

        if filtered_data.empty:
            QMessageBox.information(self,
    def select_database(self):
        db_filename, _ = QFileDialog.getOpenFileName(self, "Select Database File", "", "Database files (*.db)")
        if db_filename:
            self.db_line_edit.setText(db_filename)
            self.data_loader = DataLoader(db_filename)
            self.update_dropdowns()

    def select_csv(self):
        csv_file, _ = QFileDialog.getOpenFileName(self, "Open CSV File", "", "CSV files (*.csv)")
        if csv_file:
            self.csv_line_edit.setText(csv_file)

    def update_dropdowns(self):
        if self.db_line_edit.text():
            self.data_loader = DataLoader(self.db_line_edit.text())
            self.city_menu.addItems(['All'] + self.data_loader.fetch_unique_values('location', 'city'))
            self.subdivision_menu.addItems(['All'] + self.data_loader.fetch_unique_values('location', 'subdivision'))
            self.type_menu.addItems(['All'] + self.data_loader.fetch_unique_values('properties', 'type'))

    def process_data(self):
        db_filename = self.db_line_edit.text()
        csv_filename = self.csv_line_edit.text()
        if not db_filename or not csv_filename:
            QMessageBox.warning(self, "Error", "Please select both database and CSV files.")
            return

        try:
            # Assuming process_and_load_data takes a list of CSV files, database filename, and a boolean to create a new database
            process_and_load_data([csv_filename], db_filename, self.create_db_radio.isChecked())
            
            # Update the date range picker after processing data
            result = self.data_loader.get_min_max_dates('listing_details')
            if result is not None and not result.empty:
                min_date = result.iloc[0]['min_date']
                max_date = result.iloc[0]['max_date']
                self.dateRangePicker.set_dates(min_date, max_date)
            QMessageBox.information(self, "Success", "Data cleaned and loaded into the database.")
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Processing failed: {str(e)}")

    def perform_analysis(self):
        filtered_data = self.fetch_and_prepare_data()
        if filtered_data is None:
            return

        stats_to_calculate = self.gather_statistics_to_calculate()

        results = analyze_real_estate_data(filtered_data, {
            'timeframe': self.dateRangePicker.rangeType.currentText().lower(),
            'start_date': self.dateRangePicker.startDate.date().toString("yyyy-MM-dd"),
            'end_date': self.dateRangePicker.endDate.date().toString("yyyy-MM-dd"),
            'stats_to_calculate': stats_to_calculate
        })

        self.display_results(results)



    def fetch_and_prepare_data(self):
        if not self.db_line_edit.text():
            QMessageBox.warning(self, "Error", "Database path is not set.")
            return None

        params = {
            'city': self.city_menu.currentText() if self.city_menu.currentText() != "All" else,
            'subdivision': self.subdivision_menu.currentText() if self.subdivision_menu.currentText() != "All" else None,
            'building_type': self.type_menu.currentText() if self.type_menu.currentText() != "All" else None,
            'start_date': self.dateRangePicker.startDate.date().toString("yyyy-MM-dd"),
            'end_date': self.dateRangePicker.endDate.date().toString("yyyy-MM-dd")
        }

        # Fetching the filtered data from the data loader
        filtered_data = self.data_loader.fetch_filtered_data(params)

        if filtered_data.empty:
            QMessageBox.information(self, "No Data", "No data matches the selected criteria.")
            return None

        return filtered_data


       def gather_statistics_to_calculate(self):
        stats_to_calculate = []
        for stat_name, function_key in self.statistic_functions.items():
            checkbox = self.checkboxes.get(function_key)
            if checkbox and checkbox.isChecked():
                stats_to_calculate.append(function_key)
        return stats_to_calculate

    def perform_analysis(self):
        filtered_data = self.fetch_and_prepare_data()
        if filtered_data is None:
            return

        stats_to_calculate = self.gather_statistics_to_calculate()

        params = {
            'timeframe': self.dateRangePicker.rangeType.currentText().lower(),
            'start_date': self.dateRangePicker.startDate.date().toString("yyyy-MM-dd"),
            'end_date': self.dateRangePicker.endDate.date().toString("yyyy-MM-dd"),
            'stats_to_calculate': stats_to_calculate
        }

        results = analyze_real_estate_data(filtered_data, params)

        self.display_results(results)

    def process_data(self):
        db_filename = self.db_line_edit.text()
        csv_filename = self.csv_line_edit.text()
        if not db_filename or not csv_filename:
            QMessageBox.warning(self, "Error", "Please select both database and CSV files.")
            return

        try:
            process_and_load_data([csv_filename], db_filename, self.create_db_radio.isChecked())
            
            # Update the date range picker after processing data
            result = self.data_loader.get_min_max_dates('listing_details')
            if result is not None and not result.empty:
                min_date = result.iloc[0]['min_date']
                max_date = result.iloc[0]['max_date']
                self.dateRangePicker.set_dates(min_date, max_date)
            QMessageBox.information(self, "Success", "Data cleaned and loaded into the database.")
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Processing failed: {str(e)}")

    def select_database(self):
        db_filename, _ = QFileDialog.getOpenFileName(self, "Select Database File", "", "Database files (*.db)")
        if db_filename:
            self.db_line_edit.setText(db_filename)
            self.data_loader = DataLoader(db_filename)
            self.update_dropdowns()

    def select_csv(self):
        csv_file, _ = QFileDialog.getOpenFileName(self, "Open CSV File", "", "CSV files (*.csv)")
        if csv_file:
            self.csv_line_edit.setText(csv_file)

    def update_dropdowns(self):
        self.city_menu.clear()
        self.subdivision_menu.clear()
        self.type_menu.clear()
        
        self.city_menu.addItem("All")
        self.subdivision_menu.addItem("All")
        self.type_menu.addItem("All")
        
        if self.db_line_edit.text():
            self.data_loader = DataLoader(self.db_line_edit.text())
            self.city_menu.addItems(['All'] + self.data_loader.fetch_unique_values('location', 'city'))
            self.subdivision_menu.addItems(['All'] + self.data_loader.fetch_unique_values('location', 'subdivision'))
            self.type_menu.addItems(['All'] + self.data_loader.fetch_unique_values('properties', 'type'))

if __name__ == "__main__":
    app = QApplication(sys.argv)
    ex = RealEstateAnalysisUI()
    ex.show()
    sys.exit(app.exec_())