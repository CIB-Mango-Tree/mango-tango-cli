from csv import Sniffer
from typing import Callable, Optional

import polars as pl
from pydantic import BaseModel

import terminal_tools.prompts as prompts

from .importer import Importer, ImporterSession


class CSVImporter(Importer["CsvImportSession"]):
    @property
    def name(self) -> str:
        return "CSV"

    def suggest(self, input_path: str) -> bool:
        return input_path.endswith(".csv")

    def init_session(self, input_path: str):
        with open(input_path, "r", encoding="utf8") as file:
            dialect = Sniffer().sniff(file.read(65536))

        return CsvImportSession(
            input_file=input_path,
            separator=dialect.delimiter,
            quote_char=dialect.quotechar,
            has_header=True,
        )

    def manual_init_session(self, input_path: str):
        separator = self._separator_option(None)
        if separator is None:
            return None

        quote_char = self._quote_char_option(None)
        if quote_char is None:
            return None

        has_header = self._header_option(None)
        if has_header is None:
            return None

        return CsvImportSession(
            input_file=input_path,
            separator=separator,
            quote_char=quote_char,
            has_header=has_header,
        )

    def modify_session(
        self,
        input_path: str,
        import_session: "CsvImportSession",
        reset_screen: Callable[[], None],
    ):
        is_first_time = True
        while True:
            reset_screen(import_session)
            action = prompts.list_input(
                "What would you like to change?",
                choices=[
                    ("Column separator", "separator"),
                    ("Quote character", "quote_char"),
                    ("Header", "header"),
                    ("Done. Use these options.", "done"),
                ],
                default=None if is_first_time else "done",
            )
            is_first_time = False
            if action is None:
                return None

            if action == "done":
                return import_session

            if action == "separator":
                separator = self._separator_option(import_session.separator)
                if separator is None:
                    continue
                import_session.separator = separator

            if action == "quote_char":
                quote_char = self._quote_char_option(import_session.quote_char)
                if quote_char is None:
                    continue
                import_session.quote_char = quote_char

            if action == "header":
                has_header = self._header_option(import_session.has_header)
                if has_header is None:
                    continue
                import_session.has_header = has_header

    @staticmethod
    def _separator_option(previous_value: Optional[str]) -> Optional[str]:
        input: Optional[str] = prompts.list_input(
            "Select the column separator",
            choices=[
                ("comma (,)", ","),
                ("semicolon (;)", ";"),
                ("Pipe (|)", "|"),
                ("Tab", "\t"),
                ("Other", "other"),
            ],
            default=(
                previous_value
                if previous_value in [",", ";", "\t"]
                else "other" if previous_value is not None else None
            ),
        )
        if input is None:
            return None
        if input != "other":
            return input

        input = prompts.text("Enter the separator")
        if input is None:
            return None
        input = input.strip()
        if len(input) == 0:
            return None

    @staticmethod
    def _quote_char_option(previous_value: Optional[str]) -> Optional[str]:
        input: Optional[str] = prompts.list_input(
            "Select the quote character",
            choices=[
                ('Double quote (")', '"'),
                ("Single quote (')", "'"),
                ("Other", "other"),
            ],
            default=(
                previous_value
                if previous_value in ['"', "'"]
                else "other" if previous_value is not None else None
            ),
        )
        if input is None:
            return None
        if input != "other":
            return input

        input = prompts.text("Enter the quote character")
        if input is None:
            return None
        input = input.strip()
        if len(input) == 0:
            return None

    def _header_option(self, previous_value: Optional[bool]) -> Optional[bool]:
        return prompts.list_input(
            "Does the file have a header?",
            choices=[
                ("Yes", True),
                ("No", False),
            ],
            default=previous_value,
        )


class CsvImportSession(ImporterSession, BaseModel):
    input_file: str
    separator: str
    quote_char: str
    has_header: bool = True

    def print_config(self):
        def present_separator(value: str) -> str:
            if value == "\t":
                return "(Tab)"
            if value == " ":
                return "(Space)"
            if value == ",":
                return "(Comma ,)"
            if value == ";":
                return "(Semicolon ;)"
            if value == "'":
                return "(Single quote ')"
            if value == '"':
                return '(Double quote ")'
            if value == "|":
                return "(Pipe |)"
            return value

        print(f"- Column separator: {present_separator(self.separator)}")
        print(f"- Quote character: {present_separator(self.quote_char)}")
        print(f"- First row is header: {'yes' if self.has_header else 'no'}")

    def load_preview(self, n_records: int) -> pl.DataFrame:
        return pl.read_csv(
            self.input_file,
            separator=self.separator,
            quote_char=self.quote_char,
            has_header=self.has_header,
            n_rows=n_records,
            truncate_ragged_lines=True,
            ignore_errors=True,
        )

    def import_as_parquet(self, output_path: str) -> None:
        lazyframe = pl.scan_csv(
            self.input_file,
            separator=self.separator,
            quote_char=self.quote_char,
            has_header=self.has_header,
            truncate_ragged_lines=True,
            ignore_errors=True,
        )
        lazyframe.sink_parquet(output_path)
