# flake8: noqa

import sqlite3
import psycopg2 as ps
from dataclasses import dataclass
from typing import List
import uuid
import datetime
import json
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class FilmWork:
    id: uuid.UUID
    title: str
    description: str
    creation_date: str
    certification: str
    file_path: str
    rating: float
    type: str
    created_at: datetime.datetime
    updated_at: datetime.datetime


@dataclass
class Genre:
    id: uuid.UUID
    name: str
    description: str
    created_at: datetime.datetime
    updated_at: datetime.datetime


@dataclass
class Person:
    id: uuid.UUID
    full_name: str
    birth_date: str
    created_at: datetime.datetime
    updated_at: datetime.datetime


@dataclass
class FilmWorkGenre:
    id: uuid.UUID
    film_work_id: uuid.UUID
    genre_id: uuid.UUID
    created_at: datetime.datetime


@dataclass
class FilmWorkPerson:
    id: uuid.UUID
    film_work_id: uuid.UUID
    person_id: uuid.UUID
    role: str
    created_at: datetime.datetime


class ETL:
    def __init__(self, sqlite_db_path: str, pg_config: dict):
        self.sqlite_conn = sqlite3.connect(sqlite_db_path)
        self.sqlite_conn.row_factory = sqlite3.Row
        self.pg_conn = ps.connect(**pg_config)

    def extract_film_works_directors_and_genres(self) -> None:
        cursor = self.sqlite_conn.cursor()
        cursor.execute("SELECT * FROM movies;")
        rows = cursor.fetchall()
        logger.info(f"Извлечено {len(rows)} фильмов из SQLite")

        film_works = [(
            FilmWork(
                id=uuid.uuid4(),
                title=row['title'],
                description=row['plot'],
                creation_date=datetime.datetime.now().isoformat(),
                certification="",
                file_path="",
                rating=row['ratings'],
                type="",
                created_at=datetime.datetime.now().isoformat(),
                updated_at=datetime.datetime.now().isoformat(),
            ), str(row['id']))
            for row in rows
        ]

        genres = [
            (Genre(
                id=uuid.uuid4(),
                name=row['genre'],
                description="",
                created_at=datetime.datetime.now().isoformat(),
                updated_at=datetime.datetime.now().isoformat(),
            ), str(row['id']))
            for row in rows
        ]

        directors = []
        director_movies = []
        for row in rows:
            if row['director']:
                for name in row['director'].split(','):
                    name = name.strip()
                    if name:
                        director = Person(
                            id=uuid.uuid4(),
                            full_name=name,
                            birth_date=datetime.datetime.now().isoformat(),
                            created_at=datetime.datetime.now().isoformat(),
                            updated_at=datetime.datetime.now().isoformat(),
                        )
                        directors.append(director)
                        director_movies.append((director, str(row['id'])))

        self.film_works = film_works
        self.directors = directors
        self.director_movies = director_movies
        self.genres = genres
        logger.info(f"Извлечено: {len(film_works)} фильмов, {len(genres)} жанров, {len(directors)} режиссёров")
        return

    def extract_actors(self) -> None:
        cursor = self.sqlite_conn.cursor()
        cursor.execute("""
            SELECT a.name, m.id AS movie_id
            FROM movie_actors ma
            JOIN actors a ON ma.actor_id = a.id
            JOIN movies m ON ma.movie_id = m.id
        """)
        rows = cursor.fetchall()
        logger.info(f"Извлечено {len(rows)} актёрских связей")

        actors = []
        actors_film_works = []
        for row in rows:
            actor = Person(
                id=uuid.uuid4(),
                full_name=row['name'],
                birth_date=datetime.datetime.now().isoformat(),
                created_at=datetime.datetime.now().isoformat(),
                updated_at=datetime.datetime.now().isoformat(),
            )
            actors.append(actor)
            actors_film_works.append((actor, str(row['movie_id'])))

        self.actors = actors
        self.actors_film_works = actors_film_works
        logger.info(f"Извлечено: {len(actors)} актёров")
        return None

    def extract_writers(self) -> None:
        cursor = self.sqlite_conn.cursor()

        cursor.execute("SELECT id, name FROM writers;")
        writer_id_to_name = {}
        for row in cursor.fetchall():
            writer_id_to_name[str(row['id'])] = row['name']
        logger.info(f"Загружено {len(writer_id_to_name)} имён сценаристов из таблицы writers")

        cursor.execute("SELECT id, writers FROM movies WHERE writers IS NOT NULL;")
        rows = cursor.fetchall()
        logger.info(f"Обработка {len(rows)} фильмов со сценаристами")

        writers_data = {}

        for row in rows:
            try:
                writers_list = json.loads(row['writers'])
            except (json.JSONDecodeError, TypeError):
                continue

            if not isinstance(writers_list, list):
                continue

            movie_id = str(row['id'])
            for writer_entry in writers_list:
                writer_id = writer_entry.get('id')
                if not writer_id:
                    continue
                writer_id = str(writer_id)
                writer_name = writer_id_to_name.get(writer_id)
                if not writer_name:
                    continue

                if writer_name not in writers_data:
                    writers_data[writer_name] = {
                        'person': Person(
                            id=uuid.uuid4(),
                            full_name=writer_name,
                            birth_date=datetime.datetime.now().isoformat(),
                            created_at=datetime.datetime.now().isoformat(),
                            updated_at=datetime.datetime.now().isoformat(),
                        ),
                        'movie_ids': []
                    }
                writers_data[writer_name]['movie_ids'].append(movie_id)

        writers = []
        writers_film_works = []
        for data in writers_data.values():
            person = data['person']
            writers.append(person)
            for movie_id in data['movie_ids']:
                writers_film_works.append((person, movie_id))

        self.writers = writers
        self.writers_film_works = writers_film_works
        logger.info(f"Извлечено: {len(writers)} сценаристов")
        return None

    def load_film_works(self):
        cursor = self.pg_conn.cursor()
        insert_query = """
        INSERT INTO content.film_work (id, title, description, creation_date, certificate, file_path, rating, type, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """
        count = 0
        for film_work, _ in self.film_works:
            cursor.execute(insert_query, (
                str(film_work.id), film_work.title, film_work.description,
                film_work.creation_date, film_work.certification,
                film_work.file_path, film_work.rating, film_work.type,
                film_work.created_at, film_work.updated_at
            ))
            count += 1
        self.pg_conn.commit()
        logger.info(f"Загружено {count} фильмов в PostgreSQL")

    def load_directors(self):
        cursor = self.pg_conn.cursor()
        insert_query = """
        INSERT INTO content.person (id, full_name, birth_date, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """
        count = 0
        for director in self.directors:
            cursor.execute(insert_query, (
                str(director.id), director.full_name, director.birth_date,
                director.created_at, director.updated_at
            ))
            count += 1
        self.pg_conn.commit()
        logger.info(f"Загружено {count} режиссёров в PostgreSQL")

    def load_directors_film_works(self):
        cursor = self.pg_conn.cursor()
        insert_query = """
        INSERT INTO content.person_film_work (id, film_work_id, person_id, role, created_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (film_work_id, person_id, role) DO NOTHING;
        """
        count = 0
        for director, old_film_work_id in self.director_movies:
            for film_work, old_id in self.film_works:
                if old_id == old_film_work_id:
                    cursor.execute(insert_query, (
                        str(uuid.uuid4()), str(film_work.id), str(director.id),
                        'director', datetime.datetime.now().isoformat()
                    ))
                    count += 1
        self.pg_conn.commit()
        logger.info(f"Загружено {count} связей 'режиссёр-фильм'")

    def load_genres(self):
        cursor = self.pg_conn.cursor()
        insert_query = """
        INSERT INTO content.genre (id, name, description, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """
        count = 0
        for genre, _ in self.genres:
            cursor.execute(insert_query, (
                str(genre.id), genre.name, genre.description,
                genre.created_at, genre.updated_at
            ))
            count += 1
        self.pg_conn.commit()
        logger.info(f"Загружено {count} жанров")

    def load_genre_film_works(self):
        cursor = self.pg_conn.cursor()
        insert_query = """
        INSERT INTO content.genre_film_work (id, film_work_id, genre_id, created_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (film_work_id, genre_id) DO NOTHING;
        """
        count = 0
        for genre, old_film_work_id in self.genres:
            for film_work, old_id in self.film_works:
                if old_id == old_film_work_id:
                    cursor.execute(insert_query, (
                        str(uuid.uuid4()), str(film_work.id), str(genre.id),
                        datetime.datetime.now().isoformat()
                    ))
                    count += 1
        self.pg_conn.commit()
        logger.info(f"Загружено {count} связей 'жанр-фильм'")

    def load_actors(self):
        cursor = self.pg_conn.cursor()
        insert_query = """
        INSERT INTO content.person (id, full_name, birth_date, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """
        count = 0
        for actor in self.actors:
            cursor.execute(insert_query, (
                str(actor.id), actor.full_name, actor.birth_date,
                actor.created_at, actor.updated_at
            ))
            count += 1
        self.pg_conn.commit()
        logger.info(f"Загружено {count} актёров")

    def load_actors_film_works(self):
        cursor = self.pg_conn.cursor()
        insert_query = """
        INSERT INTO content.person_film_work (id, film_work_id, person_id, role, created_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (film_work_id, person_id, role) DO NOTHING;
        """
        count = 0
        for actor, old_film_work_id in self.actors_film_works:
            for film_work, old_id in self.film_works:
                if old_id == old_film_work_id:
                    cursor.execute(insert_query, (
                        str(uuid.uuid4()), str(film_work.id), str(actor.id),
                        'actor', datetime.datetime.now().isoformat()
                    ))
                    count += 1
        self.pg_conn.commit()
        logger.info(f"Загружено {count} связей 'актёр-фильм'")

    def load_writers(self):
        cursor = self.pg_conn.cursor()
        insert_query = """
        INSERT INTO content.person (id, full_name, birth_date, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """
        count = 0
        for writer in self.writers:
            cursor.execute(insert_query, (
                str(writer.id), writer.full_name, writer.birth_date,
                writer.created_at, writer.updated_at
            ))
            count += 1
        self.pg_conn.commit()
        logger.info(f"Загружено {count} сценаристов")

    def load_writers_film_works(self):
        cursor = self.pg_conn.cursor()
        insert_query = """
        INSERT INTO content.person_film_work (id, film_work_id, person_id, role, created_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (film_work_id, person_id, role) DO NOTHING;
        """
        count = 0
        for writer, old_film_work_id in self.writers_film_works:
            for film_work, old_id in self.film_works:
                if old_id == old_film_work_id:
                    cursor.execute(insert_query, (
                        str(uuid.uuid4()), str(film_work.id), str(writer.id),
                        'writer', datetime.datetime.now().isoformat()
                    ))
                    count += 1
        self.pg_conn.commit()
        logger.info(f"Загружено {count} связей 'сценарист-фильм'")

    def close_connections(self):
        self.sqlite_conn.close()
        self.pg_conn.close()


def run_etl(sqlite_db_path: str, pg_config: dict) -> None:
    logger.info("=== ЗАПУСК ETL ===")
    etl = ETL(sqlite_db_path, pg_config)
    etl.extract_film_works_directors_and_genres()
    etl.extract_actors()
    etl.extract_writers()
    etl.load_film_works()
    etl.load_directors()
    etl.load_directors_film_works()
    etl.load_genres()
    etl.load_genre_film_works()
    etl.load_actors()
    etl.load_actors_film_works()
    etl.load_writers()
    etl.load_writers_film_works()
    etl.close_connections()
    logger.info("=== ETL ЗАВЕРШЁН ===")


if __name__ == "__main__":
    sqlite_db_path = "db.sqlite"
    pg_config = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': '09112009',
        'host': 'localhost',
        'port': '5432',
    }
    run_etl(sqlite_db_path, pg_config)