# TrailBase Tutorial

In this tutorial, we'll set up a database with an IMDB test dataset, spin up
TrailBase and write a small program to access the data.

In an effort to demonstrate TrailBase's loose coupling and the possibility of
simply trying out TrailBase with an existing SQLite-based data analysis
project, we will also offer a alternative path to bootstrapping the database
using the vanilla `sqlite3` CLI.

## Create the Schema

By simply starting TrailBase, the migrations in `traildepot/migrations` will be
applied, including `U1728810800__create_table_movies.sql`:

```sql
CREATE TABLE IF NOT EXISTS movies (
  rank         INTEGER PRIMARY KEY,
  name         TEXT NOT NULL,
  year         ANY NOT NULL,
  watch_time   INTEGER NOT NULL,
  rating       REAL NOT NULL,
  metascore    ANY,
  gross        ANY,
  votes        TEXT NOT NULL,
  description  TEXT NOT NULL
) STRICT;
```

Note that the only schema requirement for exposing an API is: `STRICT` typing
and an integer (or UUIDv7) primary key column.

The main benefit of relying on TrailBase to apply the above schema as migrations
over manually applying the schema yourself, is to:
 * document your database's schema alongside your code and
 * even more importantly, letting TrailBase bootstrap from scratch and
   sync-up databases across your dev setup, your colleague's, every time
   integration tests run, QA stages, and in production.

That said, TrailBase will happily work on existing datasets, in which
case it is your responsibility to provide a SQLite database file that
meets expectations expressed as configured TrailBase API endpoints.

Feel free to run:

```bash
$ mkdir traildepot/data
$ sqlite3 traildepot/data/main.db < traildepot/migrations/U1728810800__create_table_movies.sql
```

before starting TrailBase the first time, if you prefer bootstrapping the
database yourself.

## Importing the Data

After creating the schema above, either manually or starting TrailBase to apply
migrations, we're ready to import the IMDB test dataset.
We could now expose an API endpoint and write a small program to first read the
CSV file to then write movie database records... and we'll do that in a little
later.
For now, let's start by harnessing the fact that SQLite databases are simply a
local file and import the data using the `sqlite3` CLI side-stepping TrailBase:

```
$ sqlite3 traildepot/data/main.db
sqlite> .mode csv
sqlite> .import ./data/Top_1000_IMDb_movies_New_version.csv movies
```

There will be a warning for the first line of the CSV, which contains textual
table headers rather than data matching our schema. That's expected.
We can validate that we successfully imported 1000 movies by running:

```sql
sqlite> SELECT COUNT(*) FROM movies;
1000
```

## Accessing the Data

With TrailBase up and running (`trail run`), the easiest way to explore your
data is go to the admin dashboard under
[http://localhost:4000](http://localhost:4000)
and log in with the admin credentials provided to you in the terminal upon
first start (you can also use the `trail` CLI to reset the password `trail user
reset-password admin@localhost`).

In this tutorial we want to explore more programmatic access and using
TrailBase record APIs.

```textproto
record_apis: [
  # ...
  {
    name: "movies"
    table_name: "movies"
    acl_world: [READ]
    acl_authenticated: [CREATE, READ, UPDATE, DELETE]
  }
]
```

By adding the above snippet to your configuration (which is already the case
for the checked-in configuration) you expose a world-readable API. We're using
the config here but you can also configure the API using the admin dashboard
via the
[tables view](http://localhost:4000/_/admin/tables?pageIndex=0&pageSize=20&table=movies)
and the "Record API" settings in the top right.

Let's try it out by querying the top-3 ranked movies with less than 120min
watch time:

```bash
curl -g 'localhost:4000/api/records/v1/movies?limit=3&order=rank&watch_time[lt]=120'
```

You can also use your browser. Either way, you should see some JSON output with
the respective movies.

## Type-safe APIs and Mutations

Finally, let's authenticate and use privileged APIs to first delete all movies
and then add them pack using type-safe APIs rather than `sqlite3`.

Let's first create the JSON Schema type definitions from the database schema we
added above. Note, that the type definition for creation, reading, and updating
are all different. Creating a new record requires values for all `NOT NULL`
columns w/o a default value, while reads guarantees values for all `NOT NULL`
columns, and updates only require values for columns that are being updated.
In this tutorial we'll "cheat" by using the same type definition for reading
existing and creating new records, since our schema doesn't define any default
values (except implicitly for the primary key), they're almost identical.

In preparation for deleting and re-adding the movies, let's run:

```bash
$ trail schema movies --mode insert
```

This will output a standard JSON schema type definition file. There's quite a few
code-generators you can use to generate bindings for your favorite language.
For this example we'll use *quicktype* to generate *TypeScript* definitions,
which also happens to support some other ~25 languages. You can install it, but
for the tutorial we'll stick with the [browser](https://app.quicktype.io/)
version and copy&paste the JSON schema from above.

With the generated types, we can use the TrailBase TypeScript client to write
the following program:

```ts
# scripts/src/fill.ts
```
