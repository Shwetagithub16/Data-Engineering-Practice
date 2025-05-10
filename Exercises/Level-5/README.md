
The script does the following ...
1. Examine each `csv` file in `data` folder. Design a `CREATE` statement for each file.
2. Ensure you have indexes, primary and forgein keys.
3. Use `psycopg2` to connect to `Postgres` on `localhost` and the default `port`.
4. Create the tables against the database.
5. Ingest the `csv` files into the tables you created, also using `psycopg2`.
