from pydantic import BaseModel, Field, validator
import time

class EventoValoracion(BaseModel):

    userId: int = Field(..., gt=0, description = "El ID del usuario debe ser positivo")
    movieId: int = Field(..., gt=0, description = "El ID de la película debe ser positivo")
    rating: float = Field(..., ge=0.5, le=5.0, description = "Rating entre 0.5 y 5.0")
    timestamp: float = Field(default_factory=lambda: time.time())

    class Config:
        extra = 'ignore'
