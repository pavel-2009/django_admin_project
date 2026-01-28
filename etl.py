import sqlite3
import psycopg2 as ps
from dataclasses import dataclass
from typing import List
import uuid
import datetime
import json


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
        self.pg_conn = ps.connect(**pg_config)

    def extract_film_works(self) -> List[FilmWork]:
        cursor = self.sqlite_conn.cursor()
        cursor.execute("SELECT * FROM movies;")
        rows = cursor.fetchall()
        film_works = [
            FilmWork(
                id=uuid.UUID(row[0]),
                title=row[4],
                description=row[5],
                creation_date=row[3],
                certification="",
                file_path="",
                rating=row[6],
                type="",
                created_at=datetime.datetime.now().isoformat(),
                updated_at=datetime.datetime.now().isoformat(),
            )
            for row in rows
        ]
        directors = [
            Person(
                id=uuid.UUID(),
                full_name=row[3],
                birth_date="",
                created_at=datetime.datetime.now().isoformat(),
                updated_at=datetime.datetime.now().isoformat(),
            )
            for row in rows
        ]
        return film_works, directors

    def load_film_works(self, film_works: List[FilmWork]):
        cursor = self.pg_conn.cursor()
        insert_query = """
        INSERT INTO film_work (id, title, description, creation_date, certification, file_path, rating, type, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """
        for fw in film_works:
            cursor.execute(insert_query, (
                str(fw.id), fw.title, fw.description, fw.creation_date,
                fw.certification, fw.file_path, fw.rating, fw.type,
                fw.created_at, fw.updated_at
            ))
        self.pg_conn.commit()

    def load_directors(self, directors: List[Person]):
        cursor = self.pg_conn.cursor()
        insert_query = """
        INSERT INTO person (id, full_name, birth_date, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """
        for director in directors:
            cursor.execute(insert_query, (
                str(director.id), director.full_name, director.birth_date,
                director.created_at, director.updated_at
            ))
        self.pg_conn.commit()

    def extract_actors(self) -> List[Person]:
        cursor = self.sqlite_conn.cursor()
        cursor.execute("SELECT a.id, a.name, ma.id FROM actors a LEFT JOIN movie_actors ma ON a.id = ma.actor_id;")  # noqa
        rows = cursor.fetchall()
        actors = [
            Person(
                id=uuid.UUID(row[0]),
                full_name=row[1],
                birth_date="",
                created_at=datetime.datetime.now().isoformat(),
                updated_at=datetime.datetime.now().isoformat(),
            )
            for row in rows
        ]
        actors_film_works = [
            FilmWorkPerson(
                id=uuid.uuid4(),
                film_work_id=uuid.UUID(row[2]),
                person_id=uuid.UUID(row[0]),
                role="actor",
                created_at=datetime.datetime.now().isoformat(),
            )
            for row in rows if row[2] is not None
        ]
        return actors, actors_film_works

    def load_actors(self, actors: List[Person]):
        cursor = self.pg_conn.cursor()
        insert_query = """
        INSERT INTO person (id, full_name, birth_date, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """
        for actor in actors:
            cursor.execute(insert_query, (
                str(actor.id), actor.full_name, actor.birth_date,
                actor.created_at, actor.updated_at
            ))
        self.pg_conn.commit()

    def load_actors_film_works(self, actors_film_works: List[FilmWorkPerson]):
        cursor = self.pg_conn.cursor()
        insert_query = """
        INSERT INTO film_work_person (id, film_work_id, person_id, role, created_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """
        for afw in actors_film_works:
            cursor.execute(insert_query, (
                str(afw.id), str(afw.film_work_id), str(afw.person_id),
                afw.role, afw.created_at
            ))
        self.pg_conn.commit()

    def extract_writers(self) -> List[Person]:
        cursor = self.sqlite_conn.cursor()
        select_writers_ids_query = "SELECT id, writers FROM movies WHERE writers IS NOT NULL;"
        cursor.execute(select_writers_ids_query)

        rows = cursor.fetchall()

        writers_movies = {}

        for row in rows:
            writers_ids = json.loads(row[1])
            for writer in writers_ids:
                if writer['id'] not in writers_movies:
                    writers_movies[writer['id']] = []
                writers_movies[writer['id']].append(row[0])

        writers = []

        select_writers_query = "SELECT id, name FROM writers;"
        cursor.execute(select_writers_query)

        writers_rows = cursor.fetchall()
        for row in writers_rows:
            writers.append(
                Person(
                    id=uuid.UUID(row[0]),
                    full_name=row[1],
                    birth_date="",
                    created_at=datetime.datetime.now().isoformat(),
                    updated_at=datetime.datetime.now().isoformat(),
                )
            )

        writers_film_works = []

        for writer_id, movie_ids in writers_movies.items():
            for movie_id in movie_ids:
                writers_film_works.append(
                    FilmWorkPerson(
                        id=uuid.uuid4(),
                        film_work_id=uuid.UUID(movie_id),
                        person_id=uuid.UUID(writer_id),
                        role="writer",
                        created_at=datetime.datetime.now().isoformat(),
                    )
                )

        return writers, writers_film_works
