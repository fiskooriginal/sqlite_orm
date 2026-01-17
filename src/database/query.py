from typing import Any, ClassVar


class QueryBuilder:
    """Построитель SQL запросов для фильтрации моделей."""

    # Поддерживаемые операторы фильтрации
    OPERATORS: ClassVar[dict[str, str]] = {
        "__exact": "=",
        "__gt": ">",
        "__lt": "<",
        "__like": "LIKE",
    }

    @classmethod
    def parse_filters(cls, **kwargs: Any) -> tuple[str, list[Any]]:
        """
        Парсит фильтры из kwargs и возвращает SQL WHERE clause и список значений.

        Args:
            **kwargs: Фильтры в формате field__operator=value или field=value

        Returns:
            Кортеж (WHERE clause, список значений для подстановки)

        Examples:
            >>> QueryBuilder.parse_filters(age__gt=18, name__exact='John')
            ('age > ? AND name = ?', [18, 'John'])

            >>> QueryBuilder.parse_filters(name='John')
            ('name = ?', ['John'])
        """
        conditions: list[str] = []
        values: list[Any] = []

        for key, value in kwargs.items():
            field_name, operator = cls.parse_filter_key(key)

            if operator not in cls.OPERATORS:
                raise ValueError(f"Unsupported filter operator: {operator}")

            sql_operator = cls.OPERATORS[operator]
            conditions.append(f"{field_name} {sql_operator} ?")
            values.append(value)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        return where_clause, values

    @classmethod
    def parse_filter_key(cls, key: str) -> tuple[str, str]:
        """
        Парсит ключ фильтра и извлекает имя поля и оператор.

        Args:
            key: Ключ фильтра (например, "age__gt" или "name")

        Returns:
            Кортеж (имя поля, оператор)

        Examples:
            >>> QueryBuilder.parse_filter_key("age__gt")
            ('age', '__gt')

            >>> QueryBuilder.parse_filter_key("name")
            ('name', '__exact')
        """
        if "__" not in key:
            return key, "__exact"

        parts = key.rsplit("__", 1)

        if len(parts) == 2:
            field_name, operator = parts
            operator = f"__{operator}"

            if operator in cls.OPERATORS:
                return field_name, operator

        return key, "__exact"
