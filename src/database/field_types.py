from typing import Any, get_args, get_origin


class FieldType:
    def __init__(self, sqlite_type: str, python_type: type[Any]):
        self.sqlite_type = sqlite_type
        self.python_type = python_type

    def to_db_value(self, value: Any) -> Any:
        if value is None:
            return None

        if self.python_type is bool:
            return 1 if value else 0

        return value

    def from_db_value(self, value: Any) -> Any:
        if value is None:
            return None

        if self.python_type is bool:
            return bool(value)

        return value


# Маппинг Python типов на SQLite типы
TYPE_MAPPING = {
    str: FieldType("TEXT", str),
    int: FieldType("INTEGER", int),
    float: FieldType("REAL", float),
    bool: FieldType("INTEGER", bool),
}


def get_field_type(annotation: Any) -> FieldType | None:
    """
    Определяет тип поля на основе аннотации типа Python.

    Args:
        annotation: Аннотация типа из класса

    Returns:
        FieldType или None, если тип не поддерживается
    """
    if annotation is None:
        return None

    # Обработка Optional (Union[T, None])
    origin = get_origin(annotation)
    if origin is not None:
        args = get_args(annotation)

        # Optional[T] это Union[T, None]
        if len(args) == 2:
            # Проверяем наличие None в args
            has_none = any(arg is type(None) if isinstance(arg, type) else arg is None for arg in args)
            if has_none:
                # Получаем реальный тип из Union
                non_none_type = next((arg for arg in args if arg is not type(None)), None)
                if non_none_type is not None:
                    annotation = non_none_type
        elif len(args) > 0:
            # Union без None (обычно не используется для полей)
            # Берем первый тип из Union
            annotation = args[0]

    # Прямое сопоставление типа
    return TYPE_MAPPING.get(annotation)


def python_to_sqlite_type(annotation: Any) -> str | None:
    """
    Преобразует Python тип в тип SQLite колонки.

    Args:
        annotation: Аннотация типа из класса

    Returns:
        Строка с типом SQLite (TEXT, INTEGER, REAL) или None
    """
    field_type = get_field_type(annotation)
    return field_type.sqlite_type if field_type else None


def to_db_value(value: Any, annotation: Any) -> Any:
    """
    Преобразует значение Python в значение для базы данных на основе аннотации типа.

    Args:
        value: Значение Python
        annotation: Аннотация типа из класса

    Returns:
        Значение, готовое для сохранения в БД
    """
    field_type = get_field_type(annotation)
    if field_type is None:
        return value

    return field_type.to_db_value(value)


def from_db_value(value: Any, annotation: Any) -> Any:
    """
    Преобразует значение из базы данных в значение Python на основе аннотации типа.

    Args:
        value: Значение из БД
        annotation: Аннотация типа из класса

    Returns:
        Значение Python
    """
    field_type = get_field_type(annotation)
    if field_type is None:
        return value

    return field_type.from_db_value(value)
