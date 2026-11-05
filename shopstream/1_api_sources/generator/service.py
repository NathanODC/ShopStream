import random
from datetime import datetime, timedelta, timezone

from faker import Faker

from generator.models import (
    ClickstreamEvent,
    GenerationRequest,
    Product,
    SalesTransaction,
    SourceName,
    SupportTicket,
    TransactionItem,
)


CATEGORY_DATA = {
    "footwear": ("Running", ["size", "color"]),
    "apparel": ("Sportswear", ["size", "material"]),
    "electronics": ("Accessories", ["color", "warranty"]),
    "home": ("Living", ["material", "color"]),
}


class SyntheticDataGenerator:
    def __init__(self, config: GenerationRequest):
        self.config = config
        self.random = random.Random(config.seed)
        self.fake = Faker(["en_US", "pt_BR"])
        self.fake.seed_instance(config.seed)
        self.base_time = datetime(2026, 1, 1, 9, tzinfo=timezone.utc)
        transaction_count = config.volume_for(SourceName.sales_transactions)
        click_count = config.volume_for(SourceName.clickstream)
        support_count = config.volume_for(SourceName.customer_support)
        self.user_count = max(
            10, transaction_count // 2, click_count // 8, support_count
        )

    def generate(self):
        products = self._products()
        transactions = self._transactions(products)
        return {
            SourceName.product_catalog: products,
            SourceName.sales_transactions: transactions,
            SourceName.clickstream: self._clickstream(products, transactions),
            SourceName.customer_support: self._support(transactions),
        }

    def _products(self) -> list[Product]:
        products = []
        for index in range(1, self.config.volume_for(SourceName.product_catalog) + 1):
            category = self.random.choice(list(CATEGORY_DATA))
            subcategory, attribute_names = CATEGORY_DATA[category]
            price = round(self.random.uniform(10, 400), 2)
            attributes = {
                name: self.random.choice(["blue", "black", "M", "cotton", "1 year"])
                for name in attribute_names
            }
            products.append(
                Product(
                    product_id=f"prod_{index:06d}",
                    name=f"{self.fake.word().title()} {subcategory}",
                    category=category,
                    subcategory=subcategory,
                    price=price,
                    cost=round(price * self.random.uniform(0.35, 0.75), 2),
                    stock_quantity=self.random.randint(0, 500),
                    supplier_id=f"supplier_{self.random.randint(1, 20):03d}",
                    last_updated=self.base_time + timedelta(minutes=index),
                    attributes=attributes,
                    tags=[category, self.fake.word()],
                )
            )
        return products

    def _transactions(self, products: list[Product]) -> list[SalesTransaction]:
        transactions = []
        for index in range(
            1, self.config.volume_for(SourceName.sales_transactions) + 1
        ):
            selected = self.random.sample(
                products, k=min(len(products), self.random.randint(1, 3))
            )
            items = [
                TransactionItem(
                    product_id=product.product_id,
                    quantity=self.random.randint(1, 3),
                    price=product.price,
                )
                for product in selected
            ]
            country = self.random.choice(["US", "BR"])
            transactions.append(
                SalesTransaction(
                    transaction_id=f"txn_{index:07d}",
                    user_id=f"user_{self.random.randint(1, self.user_count):06d}",
                    timestamp=self.base_time + timedelta(minutes=index * 3),
                    amount=round(sum(item.price * item.quantity for item in items), 2),
                    currency="BRL" if country == "BR" else "USD",
                    payment_method=self.random.choice(["card", "pix", "wallet"]),
                    status=self.random.choices(
                        ["completed", "pending", "refunded"], weights=[90, 7, 3]
                    )[0],
                    items=items,
                    shipping_address=self.fake.address().replace("\n", ", "),
                    billing_country=country,
                )
            )
        return transactions

    def _clickstream(
        self, products: list[Product], transactions: list[SalesTransaction]
    ) -> list[ClickstreamEvent]:
        events = []
        event_types = ["page_view", "product_view", "add_to_cart", "purchase"]
        for index in range(1, self.config.volume_for(SourceName.clickstream) + 1):
            product = self.random.choice(products)
            transaction = self.random.choice(transactions)
            events.append(
                ClickstreamEvent(
                    event_id=f"evt_{index:08d}",
                    user_id=transaction.user_id,
                    session_id=f"session_{self.random.randint(1, max(1, len(transactions))):07d}",
                    timestamp=max(
                        self.base_time,
                        transaction.timestamp
                        - timedelta(minutes=self.random.randint(1, 120)),
                    ),
                    event_type=self.random.choices(
                        event_types, weights=[45, 35, 15, 5]
                    )[0],
                    page_url=f"/{product.category}/{product.product_id}",
                    user_agent=self.fake.user_agent(),
                    ip_address=self.fake.ipv4_public(),
                    referrer=self.random.choice(["/", "/search", "/campaign"]),
                    device_type=self.random.choice(["mobile", "desktop", "tablet"]),
                    country=transaction.billing_country,
                    properties={
                        "product_id": product.product_id,
                        "category": product.category,
                        "price": product.price,
                    },
                )
            )
        return events

    def _support(self, transactions: list[SalesTransaction]) -> list[SupportTicket]:
        tickets = []
        for index in range(1, self.config.volume_for(SourceName.customer_support) + 1):
            transaction = self.random.choice(transactions)
            category = self.random.choice(["delivery", "payment", "product", "return"])
            status = self.random.choice(["open", "in_progress", "resolved"])
            tickets.append(
                SupportTicket(
                    ticket_id=f"ticket_{index:07d}",
                    user_id=transaction.user_id,
                    created_at=transaction.timestamp
                    + timedelta(hours=self.random.randint(1, 72)),
                    category=category,
                    priority=self.random.choice(["low", "medium", "high"]),
                    status=status,
                    subject=f"Question about {category}",
                    description=self.fake.sentence(nb_words=12),
                    sentiment_score=round(self.random.uniform(-1, 1), 2),
                    agent_id=f"agent_{self.random.randint(1, 25):03d}",
                    resolution_time_hours=(
                        round(self.random.uniform(0.25, 72), 2)
                        if status == "resolved"
                        else 0
                    ),
                    satisfaction_score=self.random.randint(1, 5),
                )
            )
        return tickets
