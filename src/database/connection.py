import sqlite3

from pathlib import Path
from typing import Any


class DatabaseConnection:
    """Синхронное подключение к SQLite базе данных."""

    def __init__(self, database_path: str | Path, check_same_thread: bool = False, **kwargs: Any):
        """
        Инициализация синхронного подключения к базе данных.

        Args:
            database_path: Путь к файлу базы данных SQLite
            check_same_thread: Разрешить использование в разных потоках
            **kwargs: Дополнительные параметры для sqlite3.connect()
        """
        self.database_path = Path(database_path)
        self.check_same_thread = check_same_thread
        self.connection: sqlite3.Connection | None = None
        self.kwargs = kwargs

    def connect(self) -> sqlite3.Connection:
        """Открывает подключение к базе данных."""
        if self.connection is None:
            self.connection = sqlite3.connect(
                str(self.database_path), check_same_thread=self.check_same_thread, **self.kwargs
            )
        return self.connection

    def close(self) -> None:
        """Закрывает подключение к базе данных."""
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def commit(self) -> None:
        if self.connection is None:
            raise RuntimeError("Connection is not open")
        self.connection.commit()

    def rollback(self) -> None:
        if self.connection is None:
            raise RuntimeError("Connection is not open")
        self.connection.rollback()

    def execute(self, query: str, parameters: tuple | dict | None = None) -> sqlite3.Cursor:
        """
        Выполняет SQL запрос.

        Args:
            query: SQL запрос
            parameters: Параметры запроса

        Returns:
            Курсор для получения результатов
        """
        if self.connection is None:
            self.connect()
        return self.connection.execute(query, parameters or ())

    def executemany(self, query: str, parameters: list[tuple | dict]) -> sqlite3.Cursor:
        """
        Выполняет SQL запрос с множеством параметров.

        Args:
            query: SQL запрос
            parameters: Список параметров запроса

        Returns:
            Курсор для получения результатов
        """
        if self.connection is None:
            self.connect()
        return self.connection.executemany(query, parameters)

    def __enter__(self) -> "DatabaseConnection":
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()
        self.close()
