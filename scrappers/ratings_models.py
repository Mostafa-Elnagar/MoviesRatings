from typing import Optional
from pydantic import BaseModel, ValidationError, validator
import logging
import datetime

logger = logging.getLogger("scraper")

class Ratings(BaseModel):
    title: str
    year: int
    critic_score: Optional[float] = None
    critic_count: Optional[int] = None
    user_score: Optional[float] = None
    user_count: Optional[int|str] = None


def validate_ratings(ratings: dict) -> dict:
    """
    Validate and coerce a raw ratings dict using the Ratings model.
    Returns the validated dict, or the original dict if validation fails.
    """
    try:
        validated = Ratings(**ratings)
        return validated.model_dump()
    except ValidationError as e:
        logger.error(f"Ratings validation error: {e}")
        return ratings