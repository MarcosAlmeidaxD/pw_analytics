"""
Producer de transações de e-commerce de tabacaria.
Publica mensagens JSON no tópico Kafka 'transactions' em tempo real.
Logs em formato JSON estruturado (um objeto por linha).
"""

import json
import logging
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
_logger = logging.getLogger(__name__)

fake = Faker("pt_BR")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")


def log(event: str, **kwargs) -> None:
    _logger.info(json.dumps(
        {"ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
         "service": "producer", "event": event, **kwargs},
        ensure_ascii=False,
    ))


# ─── Catálogo de produtos ────────────────────────────────────────────────────
# (nome, categoria, marca, preco_unitario)
PRODUCTS = [
    # Cigarros
    ("Marlboro Red Maço", "cigarros", "Marlboro", 12.50),
    ("Marlboro Gold Maço", "cigarros", "Marlboro", 12.50),
    ("L&M Azul Maço", "cigarros", "L&M", 10.00),
    ("Camel Filtro Maço", "cigarros", "Camel", 11.00),
    ("Dunhill Switch Maço", "cigarros", "Dunhill", 13.00),
    ("Lucky Strike Maço", "cigarros", "Lucky Strike", 11.50),
    ("Free Mentol Maço", "cigarros", "Free", 8.50),
    ("Derby Suave Maço", "cigarros", "Derby", 7.00),
    ("Hollywood Basic Maço", "cigarros", "Hollywood", 9.00),
    ("Parliament Night Maço", "cigarros", "Parliament", 14.00),
    # Charutos
    ("Cohiba Siglo I", "charutos", "Cohiba", 85.00),
    ("Montecristo No. 4", "charutos", "Montecristo", 75.00),
    ("Romeo y Julieta Churchill", "charutos", "Romeo y Julieta", 90.00),
    ("Davidoff Grand Cru No. 2", "charutos", "Davidoff", 120.00),
    ("Arturo Fuente 8-5-8", "charutos", "Arturo Fuente", 65.00),
    ("H. Upmann Coronas", "charutos", "H. Upmann", 55.00),
    # Narguilé
    ("Al Fakher Menta 50g", "narguile", "Al Fakher", 25.00),
    ("Al Fakher Duplo Maçã 50g", "narguile", "Al Fakher", 25.00),
    ("Al Fakher Morango 250g", "narguile", "Al Fakher", 80.00),
    ("Adalya Love 66 50g", "narguile", "Adalya", 28.00),
    ("Starbuzz Blue Mist 250g", "narguile", "Starbuzz", 85.00),
    ("Azure Blueberry 250g", "narguile", "Azure", 75.00),
    ("Fumari Ambrosia 100g", "narguile", "Fumari", 65.00),
    # Papel e Filtros
    ("RAW Classic King Size", "papel_filtros", "RAW", 8.00),
    ("RAW Organic Hemp KS", "papel_filtros", "RAW", 9.50),
    ("OCB Organic Hemp KS", "papel_filtros", "OCB", 9.00),
    ("Smoking Brown KS", "papel_filtros", "Smoking", 7.50),
    ("RAW Filter Tips 50un", "papel_filtros", "RAW", 6.00),
    # Isqueiros
    ("BIC Classic", "isqueiros", "BIC", 5.00),
    ("BIC Maxi", "isqueiros", "BIC", 8.00),
    ("Clipper Fancy Estampado", "isqueiros", "Clipper", 12.00),
    ("Zippo Classic Chrome", "isqueiros", "Zippo", 150.00),
    ("Zippo Brushed Chrome", "isqueiros", "Zippo", 165.00),
    # Acessórios
    ("Estufa Xikar 25 Charutos", "acessorios", "Xikar", 280.00),
    ("Cinzeiro Cristal com Tampa", "acessorios", "Genérico", 45.00),
    ("Cortador Dupla Lâmina Xikar", "acessorios", "Xikar", 95.00),
    ("Umidor Viagem 10 Charutos", "acessorios", "Adorini", 120.00),
    ("Charuto Tube Alumínio 3un", "acessorios", "Genérico", 35.00),
    # Cachimbo
    ("Peterson Irish Smooth Bent", "cachimbo", "Peterson", 250.00),
    ("Stanwell Relief Billiard", "cachimbo", "Stanwell", 185.00),
    ("Fumo Captain Black Regular 42g", "cachimbo", "Captain Black", 35.00),
    ("Fumo Borkum Riff Bourbon 50g", "cachimbo", "Borkum Riff", 42.00),
]

PAYMENT_METHODS = ["pix", "cartao_credito", "cartao_debito", "boleto"]
PAYMENT_WEIGHTS = [40, 35, 15, 10]

PAYMENT_STATUSES = ["aprovado", "recusado", "pendente", "estornado"]
STATUS_WEIGHTS   = [80, 10, 5, 5]

STATES    = ["SP", "RJ", "MG", "RS", "PR", "BA", "SC", "CE", "GO", "PE", "DF", "ES", "AM", "PA"]
S_WEIGHTS = [35, 15, 12, 8, 7, 5, 4, 3, 3, 3, 2, 1, 1, 1]

DEVICES   = ["mobile", "desktop", "tablet"]
D_WEIGHTS = [60, 30, 10]


def generate_transaction() -> dict:
    name, category, brand, base_price = random.choice(PRODUCTS)
    unit_price   = round(base_price * random.uniform(0.95, 1.05), 2)
    quantity     = random.choices([1, 2, 3, 4, 5], weights=[60, 20, 10, 7, 3])[0]
    total_amount = round(unit_price * quantity, 2)
    payment_method = random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0]
    payment_status = random.choices(PAYMENT_STATUSES, weights=STATUS_WEIGHTS)[0]
    if payment_method == "boleto":
        payment_status = random.choices(["aprovado", "pendente"], weights=[70, 30])[0]
    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp":      datetime.utcnow().isoformat(),
        "customer_id":    f"C{random.randint(1000, 9999)}",
        "product_name":   name,
        "category":       category,
        "brand":          brand,
        "quantity":       quantity,
        "unit_price":     unit_price,
        "total_amount":   total_amount,
        "payment_method": payment_method,
        "payment_status": payment_status,
        "state":          random.choices(STATES, weights=S_WEIGHTS)[0],
        "device":         random.choices(DEVICES, weights=D_WEIGHTS)[0],
    }


def connect_producer() -> KafkaProducer:
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks="all",
                retries=5,
            )
            log("connected", broker=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC)
            return producer
        except NoBrokersAvailable:
            log("kafka_unavailable", broker=KAFKA_BOOTSTRAP_SERVERS, retry_in=3)
            time.sleep(3)


def main():
    producer = connect_producer()
    count = 0
    t_start = time.time()

    while True:
        tx = generate_transaction()
        producer.send(KAFKA_TOPIC, value=tx)
        count += 1

        if count % 20 == 0:
            elapsed = round(time.time() - t_start)
            rate = round(count / elapsed, 2) if elapsed > 0 else 0
            log("progress",
                total_sent=count,
                elapsed_s=elapsed,
                rate_per_s=rate,
                last_product=tx["product_name"],
                last_amount=tx["total_amount"],
                last_status=tx["payment_status"],
                last_method=tx["payment_method"])

        time.sleep(random.uniform(0.2, 1.5))


if __name__ == "__main__":
    main()
