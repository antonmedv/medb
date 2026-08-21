# MeDB

I was building a simple Go server and kept users in a map. I wanted to save them to disk, but a full database felt like
too much, so I just wrote the map to a JSON file. It worked, but I wanted the same simplicity with durability. A map in memory, stored as JSON on disk, without losing
records if the process crashes.

That is how this database was born. MeDB is a small embedded in memory database that persists data to JSON files on disk.

1. Writes are durable: acknowledged writes are fsynced.
2. Data is stored as JSON files.
3. Only one process can open the database at a time.
4. Concurrent reads/writes are safe.

## License

[MIT](LICENSE)
