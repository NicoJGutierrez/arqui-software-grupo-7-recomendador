from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.types import JSON as JSONType
from .database import Base


class Property(Base):
    __tablename__ = "properties"

    id = Column(Integer, primary_key=True, index=True)
    external_id = Column(Integer, unique=True, index=True, nullable=True)
    comuna = Column(String, index=True, nullable=True)
    lat = Column(Float, nullable=True)
    lon = Column(Float, nullable=True)
    bedrooms = Column(Integer, nullable=True)
    price = Column(Float, nullable=True)
    raw = Column(JSONType, nullable=True)

    def to_dict(self):
        return {
            "id": self.id,
            "external_id": self.external_id,
            "comuna": self.comuna,
            "lat": self.lat,
            "lon": self.lon,
            "bedrooms": self.bedrooms,
            "price": self.price,
            "raw": self.raw,
        }
