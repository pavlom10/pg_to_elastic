import psycopg2
import logging
import time

from typing import Optional, List
from pydantic import BaseModel
from psycopg2.extras import DictCursor
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from functools import wraps

import pg_extractor
import storage


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


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor)
    до граничного времени ожидания (border_sleep_time).
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            t = start_sleep_time
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logging.exception(e)
                    time.sleep(t)
                    t = t * factor if t < border_sleep_time else border_sleep_time
        return inner

    return func_wrapper


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


@backoff()
def do_extract(dsn: dict, state: storage.State, limit: int = 100, delay: float = 0.1) -> None:
    """
    Основная функция, выполняющая загрузку данных.
    Последовательно запрашивает новых персон, жанры и фильмы и обновляет данные в Elasticsearch.
    Между запросами выполняется задержка delay, ошибки перехыватываются декоратором @backoff
    """
    pg_conn = psycopg2.connect(**dsn, cursor_factory=DictCursor)

    data = pg_extractor.get_data_with_updated_persons(pg_conn, state, limit)
    if data:
        load_data_to_elasticsearch(data)

    time.sleep(delay)

    data = pg_extractor.get_data_with_updated_genres(pg_conn, state, limit)
    if data:
        load_data_to_elasticsearch(data)

    time.sleep(delay)

    data = pg_extractor.get_data_with_updated_films(pg_conn, state, limit)
    if data:
        load_data_to_elasticsearch(data)

    time.sleep(delay)

    pg_conn.close()


if __name__ == '__main__':

    # Параметры подключения к Postgres
    config = Config.parse_file('config.json')
    dsn = dict(config.film_work_pg.dsn)

    # Стейт для хранения состояния
    json_storage = storage.JsonFileStorage(config.film_work_pg.state_file_path)
    state = storage.State(json_storage)

    # Лимит загрузки данных за 1 раз и пауза между обращениями к бд
    limit = config.film_work_pg.limit
    delay = config.film_work_pg.fetch_delay

    while True:
        do_extract(dsn, state, limit, delay)



