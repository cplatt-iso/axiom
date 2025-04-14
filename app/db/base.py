# app/db/base.py
from sqlalchemy.orm import DeclarativeBase, declared_attr
from sqlalchemy import Column, Integer
import re

class Base(DeclarativeBase):
    """
    Base class for all SQLAlchemy models.

    Includes an auto-generated __tablename__ and a primary key `id`.
    """
    id: Column[int] = Column(Integer, primary_key=True, index=True)

    # Generate __tablename__ automatically based on class name
    # Converts CamelCase class names to snake_case table names
    @declared_attr.directive
    def __tablename__(cls) -> str:
        # Simple conversion: ExampleClassName -> example_class_name
        name = re.sub(r'(?<!^)(?=[A-Z])', '_', cls.__name__).lower()
        # Optional: Add 's' for pluralization (simple version)
        if not name.endswith('s'):
            name += 's'
        return name
