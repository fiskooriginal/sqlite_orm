import inspect

from pathlib import Path
from typing import Any, ClassVar, get_type_hints

from src.database.connection import DatabaseConnection
from src.database.field_types import from_db_value, get_field_type, to_db_value
from src.database.query import QueryBuilder


class ModelMeta(type):
    """Мета-класс для Model, обрабатывающий аннотации и Meta конфигурацию."""

    def __new__(mcs, name: str, bases: tuple[type, ...], namespace: dict[str, Any]) -> type:
        cls = super().__new__(mcs, name, bases, namespace)

        # Обработка Meta класса
        meta_class = namespace.get("Meta")
        if meta_class is None:
            # Создаем базовый Meta класс, если его нет
            meta_class = type("Meta", (), {})
            cls.Meta = meta_class

        # Наследование Meta от родительских классов
        for base in bases:
            if hasattr(base, "Meta") and isinstance(base.Meta, type):
                # Копируем атрибуты из родительского Meta
                for attr_name in dir(base.Meta):
                    if not attr_name.startswith("_") and not hasattr(meta_class, attr_name):
                        setattr(meta_class, attr_name, getattr(base.Meta, attr_name))

        # Устанавливаем значения по умолчанию для Meta
        if not hasattr(meta_class, "abstract"):
            meta_class.abstract = False

        if not hasattr(meta_class, "db_table"):
            meta_class.db_table = None

        cls.Meta = meta_class

        # Пропускаем обработку для абстрактных моделей
        if meta_class.abstract:
            return cls

        # Определение имени таблицы
        if meta_class.db_table is None:
            meta_class.db_table = name.lower()

        # Парсинг аннотаций полей
        type_hints = get_type_hints(cls, include_extras=True)
        fields: dict[str, Any] = {}

        for field_name, annotation in type_hints.items():
            # Пропускаем служебные атрибуты
            if field_name.startswith("_") or field_name in ("Meta", "id"):
                continue

            # Пропускаем методы и класс-методы
            if inspect.ismethod(getattr(cls, field_name, None)) or inspect.isfunction(getattr(cls, field_name, None)):
                continue

            # Проверяем, что тип поддерживается
            field_type = get_field_type(annotation)
            if field_type is not None:
                fields[field_name] = {
                    "annotation": annotation,
                    "sqlite_type": field_type.sqlite_type,
                    "python_type": field_type.python_type,
                }

        # Сохраняем метаданные модели
        cls._meta = {
            "table_name": meta_class.db_table,
            "fields": fields,
            "abstract": meta_class.abstract,
        }

        return cls


class Model(metaclass=ModelMeta):
    """Базовый класс для всех моделей ORM."""

    id: int | None = None

    # Глобальное подключение к базе данных (настраивается через configure_db)
    _db: ClassVar[DatabaseConnection | None] = None

    class Meta:
        """Конфигурация модели."""

        abstract: bool = False
        db_table: str | None = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Инициализация экземпляра модели."""
        self.id = kwargs.pop("id", None)

        # Обработка позиционных аргументов
        if hasattr(self, "_meta"):
            field_names = list(self._meta["fields"].keys())

            # Если есть позиционные аргументы, сопоставляем их с полями по порядку
            for i, arg_value in enumerate(args):
                if i < len(field_names):
                    field_name = field_names[i]
                    kwargs[field_name] = arg_value

        # Устанавливаем значения полей из kwargs
        if hasattr(self, "_meta"):
            for field_name in self._meta["fields"]:
                if field_name in kwargs:
                    setattr(self, field_name, kwargs[field_name])
                else:
                    setattr(self, field_name, None)

        # Устанавливаем остальные атрибуты
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __repr__(self) -> str:
        """Строковое представление модели."""
        class_name = self.__class__.__name__
        if self.id is not None:
            return f"<{class_name}(id={self.id})>"
        return f"<{class_name}>"

    @classmethod
    def get_table_name(cls) -> str:
        """Возвращает имя таблицы для модели."""
        if not hasattr(cls, "_meta"):
            raise RuntimeError(f"Model {cls.__name__} is abstract or not properly initialized")
        return cls._meta["table_name"]

    @classmethod
    def get_fields(cls) -> dict[str, Any]:
        """Возвращает словарь полей модели с их метаданными."""
        if not hasattr(cls, "_meta"):
            raise RuntimeError(f"Model {cls.__name__} is abstract or not properly initialized")
        return cls._meta["fields"]

    @classmethod
    def is_abstract(cls) -> bool:
        """Проверяет, является ли модель абстрактной."""
        if not hasattr(cls, "_meta"):
            return True
        return cls._meta.get("abstract", False)

    @classmethod
    def configure_db(cls, database_path: str | Path, check_same_thread: bool = False, **kwargs: Any) -> None:
        """
        Настраивает глобальное подключение к базе данных для всех моделей.

        Args:
            database_path: Путь к файлу базы данных SQLite
            check_same_thread: Разрешить использование в разных потоках
            **kwargs: Дополнительные параметры для sqlite3.connect()
        """
        cls._db = DatabaseConnection(database_path, check_same_thread=check_same_thread, **kwargs)

    @classmethod
    def _ensure_db_connection(cls) -> DatabaseConnection:
        """Проверяет наличие подключения к БД и возвращает его."""
        if cls._db is None:
            raise RuntimeError(
                "Database connection is not configured. Call Model.configure_db(database_path) before using models."
            )
        return cls._db

    @classmethod
    def _table_exists(cls, db: DatabaseConnection, table_name: str) -> bool:
        """Проверяет существование таблицы в базе данных."""
        cursor = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        return cursor.fetchone() is not None

    @classmethod
    def _create_table_sql(cls) -> str:
        """
        Генерирует SQL запрос для создания таблицы на основе метаданных модели.

        Returns:
            SQL запрос CREATE TABLE
        """
        if not hasattr(cls, "_meta"):
            raise RuntimeError(f"Model {cls.__name__} is abstract or not properly initialized")

        if cls.is_abstract():
            raise RuntimeError(f"Cannot create table for abstract model {cls.__name__}")

        table_name = cls.get_table_name()
        fields = cls.get_fields()

        # Начинаем с поля id
        columns = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]

        # Добавляем поля из аннотаций
        for field_name, field_meta in fields.items():
            sqlite_type = field_meta["sqlite_type"]
            columns.append(f"{field_name} {sqlite_type}")

        # Создаем SQL запрос
        return f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"

    @classmethod
    def create_table(cls, check_if_exists: bool = True) -> None:
        """
        Создает таблицу для модели в базе данных.

        Args:
            check_if_exists: Проверять существование таблицы перед созданием

        Raises:
            RuntimeError: Если модель абстрактная или подключение не настроено
        """
        if cls.is_abstract():
            raise RuntimeError(f"Cannot create table for abstract model {cls.__name__}")

        db = cls._ensure_db_connection()
        table_name = cls.get_table_name()

        if check_if_exists and cls._table_exists(db, table_name):
            return

        sql = cls._create_table_sql()
        db.execute(sql)
        db.commit()

    @classmethod
    def _ensure_table(cls) -> None:
        """Проверяет существование таблицы и создает её при необходимости."""
        if cls.is_abstract():
            return

        cls.create_table(check_if_exists=True)

    def _validate(self) -> None:
        """Вызывает метод validate() если он определен пользователем."""
        if hasattr(self, "validate") and callable(self.validate):
            self.validate()

    def _to_db_dict(self) -> dict[str, Any]:
        """Преобразует экземпляр модели в словарь для сохранения в БД."""
        if not hasattr(self, "_meta"):
            raise RuntimeError(f"Model {self.__class__.__name__} is abstract or not properly initialized")

        db_dict: dict[str, Any] = {}
        fields = self.get_fields()

        for field_name, field_meta in fields.items():
            value = getattr(self, field_name, None)
            annotation = field_meta["annotation"]
            db_dict[field_name] = to_db_value(value, annotation)

        return db_dict

    @classmethod
    def _from_db_row(cls, row: tuple[Any, ...], column_names: list[str]) -> "Model":
        """Создает экземпляр модели из строки результата БД."""
        if not hasattr(cls, "_meta"):
            raise RuntimeError(f"Model {cls.__name__} is abstract or not properly initialized")

        fields = cls.get_fields()
        data: dict[str, Any] = {}

        for i, column_name in enumerate(column_names):
            if column_name == "id":
                data["id"] = row[i]
            elif column_name in fields:
                field_meta = fields[column_name]
                annotation = field_meta["annotation"]
                data[column_name] = from_db_value(row[i], annotation)

        return cls(**data)

    @classmethod
    def create(cls, **kwargs: Any) -> "Model":
        """
        Создает новую запись в базе данных.

        Args:
            **kwargs: Значения полей модели

        Returns:
            Экземпляр модели с установленным id

        Raises:
            RuntimeError: Если модель абстрактная или подключение не настроено
            ValueError: Если валидация не прошла
        """
        if cls.is_abstract():
            raise RuntimeError(f"Cannot create instance of abstract model {cls.__name__}")

        instance = cls(**kwargs)
        instance._validate()
        instance._ensure_table()

        db = cls._ensure_db_connection()
        table_name = cls.get_table_name()

        db_dict = instance._to_db_dict()
        field_names = list(db_dict.keys())
        placeholders = ", ".join(["?" for _ in field_names])
        columns = ", ".join(field_names)

        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        values = tuple(db_dict.values())

        cursor = db.execute(sql, values)
        db.commit()

        instance.id = cursor.lastrowid
        return instance

    def save(self) -> int | None:
        """
        Сохраняет или обновляет запись в базе данных.

        Returns:
            id сохраненной записи

        Raises:
            RuntimeError: Если модель абстрактная или подключение не настроено
            ValueError: Если валидация не прошла
        """
        if self.__class__.is_abstract():
            raise RuntimeError(f"Cannot save instance of abstract model {self.__class__.__name__}")

        self._validate()
        self.__class__._ensure_table()

        db = self.__class__._ensure_db_connection()
        table_name = self.__class__.get_table_name()

        db_dict = self._to_db_dict()

        if self.id is None:
            field_names = list(db_dict.keys())
            placeholders = ", ".join(["?" for _ in field_names])
            columns = ", ".join(field_names)

            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            values = tuple(db_dict.values())

            cursor = db.execute(sql, values)
            db.commit()
            self.id = cursor.lastrowid
        else:
            field_names = list(db_dict.keys())
            set_clause = ", ".join([f"{name} = ?" for name in field_names])
            sql = f"UPDATE {table_name} SET {set_clause} WHERE id = ?"
            values = (*tuple(db_dict.values()), self.id)

            db.execute(sql, values)
            db.commit()

        return self.id

    def delete(self) -> None:
        """
        Удаляет запись из базы данных.

        Raises:
            RuntimeError: Если модель абстрактная, подключение не настроено или id не установлен
        """
        if self.__class__.is_abstract():
            raise RuntimeError(f"Cannot delete instance of abstract model {self.__class__.__name__}")

        if self.id is None:
            raise RuntimeError("Cannot delete instance without id")

        db = self.__class__._ensure_db_connection()
        table_name = self.__class__.get_table_name()

        sql = f"DELETE FROM {table_name} WHERE id = ?"
        db.execute(sql, (self.id,))
        db.commit()

        self.id = None

    @classmethod
    def get(cls, id: int) -> "Model | None":
        """
        Получает одну запись по id.

        Args:
            id: Идентификатор записи

        Returns:
            Экземпляр модели или None, если запись не найдена

        Raises:
            RuntimeError: Если модель абстрактная или подключение не настроено
        """
        if cls.is_abstract():
            raise RuntimeError(f"Cannot get instance of abstract model {cls.__name__}")

        cls._ensure_table()

        db = cls._ensure_db_connection()
        table_name = cls.get_table_name()
        fields = cls.get_fields()

        column_names = ["id", *list(fields.keys())]
        columns = ", ".join(column_names)

        sql = f"SELECT {columns} FROM {table_name} WHERE id = ?"
        cursor = db.execute(sql, (id,))
        row = cursor.fetchone()

        if row is None:
            return None

        return cls._from_db_row(row, column_names)

    @classmethod
    def all(cls) -> list["Model"]:
        """
        Получает все записи из таблицы.

        Returns:
            Список экземпляров модели

        Raises:
            RuntimeError: Если модель абстрактная или подключение не настроено
        """
        if cls.is_abstract():
            raise RuntimeError(f"Cannot get instances of abstract model {cls.__name__}")

        cls._ensure_table()

        db = cls._ensure_db_connection()
        table_name = cls.get_table_name()
        fields = cls.get_fields()

        column_names = ["id", *list(fields.keys())]
        columns = ", ".join(column_names)

        sql = f"SELECT {columns} FROM {table_name}"
        cursor = db.execute(sql)
        rows = cursor.fetchall()

        return [cls._from_db_row(row, column_names) for row in rows]

    @classmethod
    def filter(cls, **kwargs: Any) -> list["Model"]:
        """
        Фильтрует записи из таблицы по заданным условиям.

        Поддерживаемые операторы:
            - field__exact - точное совпадение (field = ?)
            - field__gt - больше (field > ?)
            - field__lt - меньше (field < ?)
            - field__like - поиск по шаблону (field LIKE ?)
            - field - по умолчанию точное совпадение (field = ?)

        Args:
            **kwargs: Условия фильтрации в формате field__operator=value или field=value

        Returns:
            Список экземпляров модели, соответствующих условиям

        Raises:
            RuntimeError: Если модель абстрактная или подключение не настроено
            ValueError: Если указан неподдерживаемый оператор фильтрации

        Examples:
            >>> User.filter(age__gt=18, is_active=True)
            >>> User.filter(name__like='%John%')
            >>> User.filter(email__exact='john@example.com')
        """
        if cls.is_abstract():
            raise RuntimeError(f"Cannot filter instances of abstract model {cls.__name__}")

        cls._ensure_table()

        db = cls._ensure_db_connection()
        table_name = cls.get_table_name()
        fields = cls.get_fields()

        # Валидация полей в фильтрах
        where_clause, values = QueryBuilder.parse_filters(**kwargs)

        # Проверяем, что все поля в фильтрах существуют
        for key in kwargs:
            field_name, _ = QueryBuilder.parse_filter_key(key)
            if field_name not in fields and field_name != "id":
                raise ValueError(f"Field '{field_name}' does not exist in model {cls.__name__}")

        column_names = ["id", *list(fields.keys())]
        columns = ", ".join(column_names)

        sql = f"SELECT {columns} FROM {table_name} WHERE {where_clause}"
        cursor = db.execute(sql, tuple(values))
        rows = cursor.fetchall()

        return [cls._from_db_row(row, column_names) for row in rows]
