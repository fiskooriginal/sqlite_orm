import tempfile

from pathlib import Path

from src.base_model import Model


# Определение модели
class User(Model):
    name: str
    email: str
    age: int = 18

    class Meta:
        db_table = "users"


def test_basic_orm():
    with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as test_db:
        test_db_path = Path(test_db.name)

    Model.configure_db(test_db_path)

    try:
        # 1. Создание таблицы
        User.create_table()

        # 2. Создание записи
        user1 = User("Alice", "alice@mail.com", 25)
        user1_id = user1.save()
        assert user1_id is not None

        # 3. Получение записи
        user1_fetched = User.get(user1_id)
        assert user1_fetched.name == "Alice"
        assert user1_fetched.email == "alice@mail.com"

        # 4. Обновление записи
        user1_fetched.name = "Alice Smith"
        user1_fetched.save()

        user1_updated = User.get(user1_id)
        assert user1_updated.name == "Alice Smith"

        # 5. Получение всех записей
        User("Bob", "bob@mail.com").save()
        all_users = User.all()
        assert len(all_users) == 2

        # 6. Фильтрация
        adults = User.filter(age__gt=18)
        assert len(adults) >= 1

        # 7. Удаление
        user1_updated.delete()
        assert User.get(user1_id) is None

        print("All tests passed!")
    finally:
        # Очистка тестовой БД
        if test_db_path.exists():
            test_db_path.unlink()


if __name__ == "__main__":
    test_basic_orm()
