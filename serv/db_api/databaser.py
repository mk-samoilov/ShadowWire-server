import os
from contextlib import contextmanager
from typing import Any, Optional, Sequence, Union, Literal

import psycopg2
import psycopg2.extras


FetchMode = Literal["none", "one", "all", "val"]


class PDB:
	def __init__(
		self,
		dsn: Optional[str] = None,
		host: Optional[str] = None,
		port: Optional[int] = None,
		dbname: Optional[str] = None,
		user: Optional[str] = None,
		password: Optional[str] = None,
		sslmode: Optional[str] = None,
		autocommit: bool = False,
	):
		self._conn = None
		self._conn_kwargs = {
			"dsn": dsn,
			"host": host,
			"port": port,
			"dbname": dbname,
			"user": user,
			"password": password,
			"sslmode": sslmode,
		}
		self._autocommit = bool(autocommit)

	def connect(self):
		if self._conn is not None:
			return self._conn
		conn = psycopg2.connect(**{k: v for k, v in self._conn_kwargs.items() if v is not None}, cursor_factory=psycopg2.extras.DictCursor)
		conn.autocommit = self._autocommit
		self._conn = conn
		return conn

	def close(self):
		if self._conn is not None:
			try:
				self._conn.close()
			except Exception:
				pass
			finally:
				self._conn = None

	@property
	def conn(self):
		return self._conn or self.connect()

	def cursor(self):
		return self.conn.cursor()

	def execute(
		self,
		query: str,
		params: Optional[Union[Sequence[Any], dict]] = None,
		fetch: FetchMode = "none",
		commit: bool = False,
	):
		with self.cursor() as cur:
			cur.execute(query, params)
			result = None
			if fetch == "one":
				row = cur.fetchone()
				result = dict(row) if row is not None else None
			elif fetch == "all":
				rows = cur.fetchall()
				result = [dict(r) for r in rows]
			elif fetch == "val":
				row = cur.fetchone()
				result = (row[0] if row is not None and len(row) > 0 else None)

			if commit or (self._autocommit is False and fetch == "none"):
				self.conn.commit()

			return result

	@contextmanager
	def transaction(self):
		"""Context manager for DB transactions, commit on success, rollback on error"""
		if self._autocommit:
			yield self
			return
		try:
			yield self
			self.conn.commit()
		except Exception:
			self.conn.rollback()
			raise

	def init_schema(self, sql_file_path: Optional[str] = None):
		"""Apply SQL script to initialize schema (if passed and exists)."""
		if not sql_file_path:
			module_dir = os.path.dirname(os.path.abspath(__file__))
			sql_file_path = os.path.join(module_dir, "init_scheme_src.sql")

		if not os.path.exists(sql_file_path):
			return

		with open(sql_file_path, "r", encoding="utf-8") as f:
			sql_src = f.read()

		if not sql_src.strip():
			return

		with self.cursor() as cur:
			cur.execute(sql_src)
			if not self._autocommit:
				self.conn.commit()
