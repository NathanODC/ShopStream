from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class SourceName(StrEnum):
    clickstream = "clickstream"
    customer_support = "customer_support"
    product_catalog = "product_catalog"
    sales_transactions = "sales_transactions"


ALL_SOURCES = list(SourceName)

PROFILE_VOLUMES = {
    "small": {
        SourceName.product_catalog: 10,
        SourceName.sales_transactions: 20,
        SourceName.clickstream: 100,
        SourceName.customer_support: 5,
    },
    "demo": {
        SourceName.product_catalog: 50,
        SourceName.sales_transactions: 100,
        SourceName.clickstream: 500,
        SourceName.customer_support: 20,
    },
    "load": {
        SourceName.product_catalog: 500,
        SourceName.sales_transactions: 1_000,
        SourceName.clickstream: 5_000,
        SourceName.customer_support: 200,
    },
    "edge_cases": {
        SourceName.product_catalog: 20,
        SourceName.sales_transactions: 50,
        SourceName.clickstream: 250,
        SourceName.customer_support: 20,
    },
}


class GenerationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    profile: Literal["small", "demo", "load", "edge_cases"] = "demo"
    seed: int = Field(default=42, ge=0, le=2_147_483_647)
    sources: list[SourceName] | None = None
    volumes: dict[SourceName, Annotated[int, Field(ge=1, le=10_000)]] = Field(
        default_factory=dict
    )
    pubsub_topics: dict[str, str]
    log_env: Literal["dev", "stg", "prd"] = "dev"

    @model_validator(mode="after")
    def validate_sources(self):
        if self.sources is not None and not self.sources:
            raise ValueError("sources must be omitted or contain at least one source")
        if self.sources and len(self.sources) != len(set(self.sources)):
            raise ValueError("sources must not contain duplicates")
        total_records = sum(self.volume_for(source) for source in ALL_SOURCES)
        if total_records > 10_000:
            raise ValueError("the generated dataset must not exceed 10000 records")
        return self

    @property
    def selected_sources(self) -> list[SourceName]:
        return self.sources or ALL_SOURCES

    def volume_for(self, source: SourceName) -> int:
        return self.volumes.get(source, PROFILE_VOLUMES[self.profile][source])


class Product(BaseModel):
    product_id: str
    name: str
    category: str
    subcategory: str
    price: float = Field(gt=0)
    cost: float = Field(gt=0)
    stock_quantity: int = Field(ge=0)
    supplier_id: str
    last_updated: datetime
    attributes: dict[str, str]
    tags: list[str]


class TransactionItem(BaseModel):
    product_id: str
    quantity: int = Field(ge=1, le=10)
    price: float = Field(gt=0)


class SalesTransaction(BaseModel):
    transaction_id: str
    user_id: str
    timestamp: datetime
    amount: float = Field(gt=0)
    currency: Literal["USD", "BRL"]
    payment_method: Literal["card", "pix", "wallet"]
    status: Literal["completed", "pending", "refunded"]
    items: list[TransactionItem] = Field(min_length=1)
    shipping_address: str
    billing_country: Literal["US", "BR"]


class ClickstreamEvent(BaseModel):
    event_id: str
    user_id: str
    session_id: str
    timestamp: datetime
    event_type: Literal["page_view", "product_view", "add_to_cart", "purchase"]
    page_url: str
    user_agent: str
    ip_address: str
    referrer: str
    device_type: Literal["mobile", "desktop", "tablet"]
    country: Literal["US", "BR"]
    properties: dict[str, str | int | float]


class SupportTicket(BaseModel):
    ticket_id: str
    user_id: str
    created_at: datetime
    category: Literal["delivery", "payment", "product", "return"]
    priority: Literal["low", "medium", "high"]
    status: Literal["open", "in_progress", "resolved"]
    subject: str
    description: str
    sentiment_score: float = Field(ge=-1, le=1)
    agent_id: str
    resolution_time_hours: float = Field(ge=0)
    satisfaction_score: int = Field(ge=1, le=5)
