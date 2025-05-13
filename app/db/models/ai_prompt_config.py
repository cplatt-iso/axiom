# app/db/models/ai_prompt_config.py
from typing import Dict, Any, Optional

from sqlalchemy import String, Text, JSON, Boolean # Keep JSON, JSONB is dialect specific below
from sqlalchemy.dialects.postgresql import JSONB # Only if you are sure about PostgreSQL
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base

class AIPromptConfig(Base):
    # id, created_at, updated_at are inherited from Base
    # __tablename__ will be 'ai_prompt_configs' due to your Base class logic

    name: Mapped[str] = mapped_column(
        String(255), unique=True, index=True, nullable=False,
        comment="Unique, human-readable name for this AI prompt configuration."
    )
    description: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True,
        comment="Optional detailed description of what this prompt config does."
    )
    dicom_tag_keyword: Mapped[str] = mapped_column(
        String(100), nullable=False, index=True,
        comment="DICOM keyword of the tag this configuration targets (e.g., BodyPartExamined, PatientSex)."
    )
    is_enabled: Mapped[bool] = mapped_column(
        Boolean, 
        nullable=False, 
        index=True, 
        default=True, 
        # server_default=sa.true() or text('true') for DB-level default is better for migrations (see below)
        comment="Whether this AI prompt configuraiton is active and can be used."
    )
    prompt_template: Mapped[str] = mapped_column(
        Text, nullable=False,
        comment="The prompt template. Should include '{value}' placeholder. Can also use '{dicom_tag_keyword}'."
    )
    model_identifier: Mapped[str] = mapped_column(
        String(100), nullable=False, default="gemini-1.5-flash-001",
        comment="Identifier for the AI model to be used (e.g., 'gemini-1.5-flash-001')."
    )
    model_parameters: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSONB, nullable=True, # Assuming PostgreSQL. If not, use JSON.
        comment="JSON object for model-specific parameters like temperature, max_output_tokens, top_p, etc."
    )    

    def __repr__(self):
        return f"<AIPromptConfig(id={self.id}, name='{self.name}', tag='{self.dicom_tag_keyword}')>"