# src/moo/load/schemas/base.py
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, DateTime, String
from sqlalchemy.sql import func
from datetime import datetime
from typing import Any
import json

Base = declarative_base()


class TimestampMixin:
    """
    Mixin that adds creation and update timestamps to models.
    """

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )


class AuditMixin:
    """
    Mixin that adds audit fields to track changes in models.
    """

    created_by = Column(String(50))
    updated_by = Column(String(50))
    is_deleted = Column(Boolean, default=False)
    deleted_at = Column(DateTime, nullable=True)
    deleted_by = Column(String(50), nullable=True)


class JsonSerializableMixin:
    """
    Mixin that provides JSON serialization capabilities.
    """

    def to_dict(self) -> dict:
        """Convert model instance to dictionary."""
        result = {}
        for column in self.__table__.columns:
            value = getattr(self, column.name)
            if isinstance(value, datetime):
                value = value.isoformat()
            result[column.name] = value
        return result

    def to_json(self) -> str:
        """Convert model instance to JSON string."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict) -> Any:
        """Create model instance from dictionary."""
        return cls(**data)


class BaseModel(Base, TimestampMixin, AuditMixin, JsonSerializableMixin):
    """
    Base model class that includes all common functionality.
    """

    __abstract__ = True

    id = Column(Integer, primary_key=True, autoincrement=True)

    def __repr__(self) -> str:
        """String representation of the model."""
        return f"<{self.__class__.__name__}(id={self.id})>"

    @classmethod
    def get_table_name(cls) -> str:
        """Get the table name for the model."""
        return cls.__tablename__

    @classmethod
    def get_columns(cls) -> list:
        """Get list of column names for the model."""
        return [column.name for column in cls.__table__.columns]

    def update(self, **kwargs) -> None:
        """Update model attributes with provided values."""
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                raise AttributeError(
                    f"'{self.__class__.__name__}' has no attribute '{key}'"
                )


class VersionedModel(BaseModel):
    """
    Base model class that includes versioning capabilities.
    """

    __abstract__ = True

    version = Column(Integer, default=1, nullable=False)
    is_current = Column(Boolean, default=True, nullable=False)
    valid_from = Column(DateTime, default=func.now(), nullable=False)
    valid_to = Column(DateTime, nullable=True)

    def create_new_version(self) -> None:
        """Create a new version of the current record."""
        self.is_current = False
        self.valid_to = func.now()


class ConfigurationModel(BaseModel):
    """
    Base model class for configuration-related tables.
    """

    __abstract__ = True

    key = Column(String(100), unique=True, nullable=False)
    value = Column(String(500), nullable=False)
    description = Column(String(200))
    is_active = Column(Boolean, default=True)

    @classmethod
    def get_active_config(cls, session, key: str) -> Any:
        """Get active configuration value for given key."""
        return session.query(cls).filter(cls.key == key, cls.is_active == True).first()


def create_all_tables(engine):
    """
    Create all database tables.
    """
    Base.metadata.create_all(engine)


def drop_all_tables(engine):
    """
    Drop all database tables.
    """
    Base.metadata.drop_all(engine)
