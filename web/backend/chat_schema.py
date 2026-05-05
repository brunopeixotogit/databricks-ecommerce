"""Pydantic models for the chat endpoints."""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class ChatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    session_id: str = Field(min_length=1, max_length=128)
    user_id: str | None = Field(default=None, max_length=128)
    message: str = Field(min_length=1, max_length=2000)


class ChatProduct(BaseModel):
    product_id: str
    name: str
    category: str
    price: float
    currency: str


class ChatResponse(BaseModel):
    intent: str
    reply: str
    products: list[ChatProduct]
    query: str | None = None
    max_price: float | None = None


class AddToCartRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    session_id: str = Field(min_length=1, max_length=128)
    user_id: str | None = Field(default=None, max_length=128)
    cart_id: str | None = Field(default=None, max_length=128)
    product_id: str = Field(min_length=1, max_length=64)
    quantity: int = Field(default=1, ge=1, le=20)


class AddToCartResponse(BaseModel):
    status: str
    product: ChatProduct
    quantity: int
    event_path: str
