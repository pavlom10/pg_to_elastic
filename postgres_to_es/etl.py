import psycopg2
import logging
import time

from typing import Optional, List
from pydantic import BaseModel
from psycopg2.extras import DictCursor
from elasticsearch import Elasticsearch
from elasticsearch import helpers

import pg_extractor
import storage
from backoff import backoff


class DSNSettings(BaseModel):
    host: str
    port: int
    dbname: str
    password: str
    user: str


class PostgresSettings(BaseModel):
    dsn: DSNSettings
    limit: Optional[int]
    order_field: List[str]
    state_field: List[str]
    fetch_delay: Optional[float]
    state_file_path: Optional[str]
    sql_query: str


class Config(BaseModel):
    film_work_pg: PostgresSettings


def load_data_to_elasticsearch(data: dict) -> None:
    """Загружает заранее подготовленные данные в Elasticsearch."""
    es = Elasticsearch()
    actions = [
        {
            "_index": "movies",
            "_type": "_doc",
            "_id": id,
            "_source": source
        }
        for id, source in data.items()
    ]
    helpers.bulk(es, actions)

    es.transport.close()


@backoff(logging)
def do_extract(dsn: dict, state: storage.State, limit: int = 100, delay: float = 0.1) -> None:
    """
    Основная функция, выполняющая загрузку данных.
    Запрашивает обновленные данные в Postgres и загружает их в Elasticsearch.
    """
    pg_conn = psycopg2.connect(**dsn, cursor_factory=DictCursor)

    data = pg_extractor.get_updated_film_data(pg_conn, state, limit)
    if data:
        load_data_to_elasticsearch(data)

    time.sleep(delay)

    pg_conn.close()


if __name__ == '__main__':

    # Параметры подключения к Postgres
    config = Config.parse_file('postgres_to_es/config.json')
    dsn = dict(config.film_work_pg.dsn)

    # Стейт для хранения состояния
    json_storage = storage.JsonFileStorage(config.film_work_pg.state_file_path)
    state = storage.State(json_storage)

    # Лимит загрузки данных за 1 раз и пауза между обращениями к бд
    limit = config.film_work_pg.limit
    delay = config.film_work_pg.fetch_delay

    while True:
        do_extract(dsn, state, limit, delay)
