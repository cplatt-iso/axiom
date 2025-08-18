from pydantic import BaseModel

class Sender(BaseModel):
    identifier: str
    name: str
    description: str
