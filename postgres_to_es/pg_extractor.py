from psycopg2.extensions import connection as _connection


def get_raw_data_about_films(conn: _connection, fw_ids: tuple) -> dict:
    """Извлекает по списку id все данные о фильмах, в том числе персон и жанры."""
    cur = conn.cursor()

    cur.execute("""
        SELECT
        fw.id as fw_id,
        fw.title,
        fw.description,
        fw.rating,
        fw.type,
        fw.created_at,
        fw.updated_at,
        pfw.role,
        p.id as p_id,
        p.full_name,
        g.name as genre
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.id IN %s;
    """, (tuple(fw_ids), ))

    return cur.fetchall()


def transform_data_for_elasticsearch(rows: dict) -> dict:
    """Подготавливает данные из Postgres к записи в Elasticsearch."""
    result = {}
    genres = {}
    persons = {}

    for row in rows:
        id = row['fw_id']
        if id not in result:
            result[id] = {
                'id': id,
                'imdb_rating': row['rating'],
                'title': row['title'],
                'description': row['description'],
            }
            genres[id] = []
            persons[id] = {
                'actor': {},
                'director': {},
                'writer': {}
            }

        if row['genre'] is not None:
            genres[id].append(row['genre'])

        p_id = row['p_id']
        if p_id is not None:
            persons[id][row['role']][p_id] = row['full_name']

    for id, row in result.items():
        result[id]['genre'] = ', '.join(set(genres[id]))
        result[id]['actors_names'] = [name for name in persons[id]['actor'].values()]
        result[id]['actors'] = [{'id': p_id, 'name': name} for p_id, name in persons[id]['actor'].items()]
        result[id]['writers_names'] = [name for name in persons[id]['writer'].values()]
        result[id]['writers'] = [{'id': p_id, 'name': name} for p_id, name in persons[id]['writer'].items()]
        result[id]['director'] = [name for name in persons[id]['director'].values()]

    return result


def get_data_with_updated_persons(conn: _connection, state, limit: int = 1000) -> list:
    """Получает подготовленные данные о фильмах в которых изменились актеры."""

    cur = conn.cursor()
    updated_at = state.get_state('person_updated_at')

    cur.execute("""
        SELECT id, updated_at
        FROM content.person
        WHERE updated_at > %s
        ORDER BY updated_at;
    """, (updated_at, ))
    rows = cur.fetchmany(limit)

    if len(rows) == 0:
        return None

    person_ids = [row['id'] for row in rows]

    new_updated_at = rows[-1]['updated_at']
    state.set_state('person_updated_at', str(new_updated_at))

    cur.execute("""
        SELECT fw.id, fw.updated_at
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        WHERE pfw.person_id IN %s
        ORDER BY fw.updated_at;
    """, (tuple(person_ids), ))

    rows = cur.fetchmany(limit)
    fw_ids = [row['id'] for row in rows]

    raw_data = get_raw_data_about_films(conn, fw_ids)
    data = transform_data_for_elasticsearch(raw_data)

    return data


def get_data_with_updated_genres(conn: _connection, state, limit: int = 1000) -> list:
    """Получает подготовленные данные о фильмах в которых изменились жанры."""

    cur = conn.cursor()
    updated_at = state.get_state('genre_updated_at')

    cur.execute("""
        SELECT id, updated_at
        FROM content.genre
        WHERE updated_at > %s
        ORDER BY updated_at;
    """, (updated_at, ))
    rows = cur.fetchmany(limit)

    if len(rows) == 0:
        return None

    person_ids = [row['id'] for row in rows]

    new_updated_at = rows[-1]['updated_at']
    state.set_state('genre_updated_at', str(new_updated_at))

    cur.execute("""
        SELECT fw.id, fw.updated_at
        FROM content.film_work fw
        LEFT JOIN content.genre_film_work pfw ON pfw.film_work_id = fw.id
        WHERE pfw.genre_id IN %s
        ORDER BY fw.updated_at;
    """, (tuple(person_ids), ))

    rows = cur.fetchmany(limit)
    fw_ids = [row['id'] for row in rows]

    raw_data = get_raw_data_about_films(conn, fw_ids)
    data = transform_data_for_elasticsearch(raw_data)

    return data


def get_data_with_updated_films(conn: _connection, state, limit: int = 1000) -> list:
    """Получает подготовленные данные о обновленных фильмах."""

    cur = conn.cursor()
    updated_at = state.get_state('film_updated_at')

    cur.execute("""
        SELECT fw.id, fw.updated_at
        FROM content.film_work fw
        WHERE updated_at > %s
        ORDER BY fw.updated_at;
    """, (updated_at, ))
    rows = cur.fetchmany(limit)

    if len(rows) == 0:
        return None

    new_updated_at = rows[-1]['updated_at']
    state.set_state('film_updated_at', str(new_updated_at))

    fw_ids = [row['id'] for row in rows]

    raw_data = get_raw_data_about_films(conn, fw_ids)
    data = transform_data_for_elasticsearch(raw_data)

    return data

