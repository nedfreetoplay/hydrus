# Hydrus core: аудит SQL-запросов

Отчёт построен автоматически по `hydrus/core/**/*.py` по вызовам `execute`/`executemany`/`_Execute`.

## 1) Файлы, где есть работа с БД

- `hydrus/core/HydrusDB.py`
- `hydrus/core/HydrusDBBase.py`
- `hydrus/core/HydrusDBModule.py`
- `hydrus/core/HydrusRatingArchive.py`
- `hydrus/core/HydrusTagArchive.py`
- `hydrus/core/files/HydrusClipHandling.py`

## 2) Карта операций по файлам

### `hydrus/core/HydrusDB.py`
Сводка: CREATE=1, READ=5, DDL=2, OTHER=24
#### CREATE
- L129 `ReadLargeIdQueryInSeparateChunks` via `execute`: `INSERT INTO  ( temp_id ) <dynamic>`
#### READ
- L143 `ReadLargeIdQueryInSeparateChunks` via `execute`: `SELECT temp_id FROM  WHERE job_id BETWEEN ? AND ?; <dynamic>`
- L404 `__init__` via `_Execute`: `SELECT version FROM version;`
- L474 `__init__` via `_Execute`: `SELECT version FROM version;`
- L714 `_InitDB` via `_Execute`: `SELECT 1 FROM sqlite_master WHERE type = ? AND name = ?;`
- L803 `_InitDBConnection` via `_Execute`: `SELECT * FROM {}.sqlite_master; <dynamic>`
#### DDL
- L127 `ReadLargeIdQueryInSeparateChunks` via `execute`: `CREATE TEMPORARY TABLE  ( job_id INTEGER PRIMARY KEY AUTOINCREMENT, temp_id INTEGER ); <dynamic>`
- L151 `ReadLargeIdQueryInSeparateChunks` via `execute`: `DROP TABLE ; <dynamic>`
#### OTHER
- L30 `CheckCanVacuumCursor` via `execute`: `PRAGMA page_size;`
- L31 `CheckCanVacuumCursor` via `execute`: `PRAGMA page_count;`
- L32 `CheckCanVacuumCursor` via `execute`: `PRAGMA freelist_count;`
- L69 `CheckCanVacuumIntoCursor` via `execute`: `PRAGMA page_size;`
- L70 `CheckCanVacuumIntoCursor` via `execute`: `PRAGMA page_count;`
- L71 `CheckCanVacuumIntoCursor` via `execute`: `PRAGMA freelist_count;`
- L164 `VacuumDB` via `execute`: `PRAGMA journal_mode = TRUNCATE;`
- L169 `VacuumDB` via `execute`: `PRAGMA page_size;`
- L175 `VacuumDB` via `execute`: `PRAGMA journal_mode = TRUNCATE;`
- L176 `VacuumDB` via `execute`: `PRAGMA page_size = ; <dynamic>`
- L179 `VacuumDB` via `execute`: `PRAGMA auto_vacuum = 0;`
- L181 `VacuumDB` via `execute`: `VACUUM;`
- L183 `VacuumDB` via `execute`: `PRAGMA journal_mode = {}; <dynamic>`
- L225 `VacuumDBInto` via `execute`: `VACUUM INTO ?;`
- L508 `_AttachExternalDatabases` via `_Execute`: `ATTACH ? AS ; <dynamic>`
- L513 `_AttachExternalDatabases` via `_Execute`: `ATTACH ? AS durable_temp;`
- L755 `_InitDBConnection` via `_Execute`: `PRAGMA temp_store = 2;`
- L764 `_InitDBConnection` via `_Execute`: `ATTACH ":memory:" AS mem;`
- L778 `_InitDBConnection` via `_Execute`: `PRAGMA database_list;`
- L785 `_InitDBConnection` via `_Execute`: `PRAGMA {}.cache_size = -{}; <dynamic>`
- L787 `_InitDBConnection` via `_Execute`: `PRAGMA {}.journal_mode = {}; <dynamic>`
- L796 `_InitDBConnection` via `_Execute`: `PRAGMA {}.journal_size_limit = {}; <dynamic>`
- L799 `_InitDBConnection` via `_Execute`: `PRAGMA {}.synchronous = {}; <dynamic>`
- L982 `_ShrinkMemory` via `_Execute`: `PRAGMA shrink_memory;`

### `hydrus/core/HydrusDBBase.py`
Сводка: CREATE=1, DELETE=1, DDL=1, OTHER=27
#### CREATE
- L239 `__enter__` via `executemany`: `INSERT INTO {} ( {} ) VALUES ( ? ); <dynamic>`
#### DELETE
- L281 `__exit__` via `execute`: `DELETE FROM {}; <dynamic>`
#### DDL
- L236 `__enter__` via `execute`: `CREATE TABLE IF NOT EXISTS {} ( {} INTEGER PRIMARY KEY ); <dynamic>`
#### OTHER
- L251 `__enter__` via `execute`: `<dynamic f-string SQL>`
- L266 `__enter__` via `execute`: `<dynamic f-string SQL>`
- L273 `__enter__` via `executemany`: `<dynamic f-string SQL>`
- L384 `_AnalyzeTempTable` via `_Execute`: `ANALYZE {}; <dynamic>`
- L385 `_AnalyzeTempTable` via `_Execute`: `ANALYZE mem.sqlite_master;`
- L442 `_Execute` via `execute`: `EXPLAIN QUERY PLAN {} <dynamic>`
- L470 `_ExecuteMany` via `execute`: `EXPLAIN QUERY PLAN {} <dynamic>`
- L495 `_GetAttachedDatabaseNames` via `_Execute`: `PRAGMA database_list;`
- L548 `_ActualIndexExists` via `_Execute`: `<dynamic f-string SQL>`
- L584 `_IdealIndexExists` via `_Execute`: `<dynamic f-string SQL>`
- L593 `_IdealIndexExists` via `_Execute`: `<dynamic f-string SQL>`
- L653 `_TableExists` via `_Execute`: `<dynamic f-string SQL>`
- L702 `_ZeroJournal` via `_Execute`: `BEGIN IMMEDIATE;`
- L705 `_ZeroJournal` via `_Execute`: `PRAGMA database_list;`
- L709 `_ZeroJournal` via `_Execute`: `PRAGMA {}.journal_size_limit = {}; <dynamic>`
- L712 `_ZeroJournal` via `_Execute`: `COMMIT;`
- L716 `_ZeroJournal` via `_Execute`: `PRAGMA {}.journal_size_limit = {}; <dynamic>`
- L724 `BeginImmediate` via `_Execute`: `BEGIN IMMEDIATE;`
- L725 `BeginImmediate` via `_Execute`: `SAVEPOINT hydrus_savepoint;`
- L746 `Commit` via `_Execute`: `COMMIT;`
- L755 `Commit` via `_Execute`: `PRAGMA wal_checkpoint(TRUNCATE);`
- L761 `Commit` via `_Execute`: `PRAGMA wal_checkpoint(PASSIVE);`
- L769 `Commit` via `_Execute`: `DETACH mem;`
- L770 `Commit` via `_Execute`: `ATTACH ":memory:" AS mem;`
- L847 `Rollback` via `_Execute`: `ROLLBACK TO hydrus_savepoint;`
- L867 `Save` via `_Execute`: `RELEASE hydrus_savepoint;`
- L874 `Save` via `_Execute`: `SAVEPOINT hydrus_savepoint;`

### `hydrus/core/HydrusDBModule.py`
Сводка: READ=2, DDL=1
#### READ
- L52 `_CreateTable` via `_Execute`: `SELECT 1 FROM {} WHERE name = ?; <dynamic>`
- L60 `_CreateTable` via `_Execute`: `SELECT 1 FROM {} WHERE name = ?; <dynamic>`
#### DDL
- L62 `_CreateTable` via `_Execute`: `DROP TABLE {}; <dynamic>`

### `hydrus/core/HydrusRatingArchive.py`
Сводка: CREATE=2, UPDATE=1, DELETE=3, READ=6, DDL=3, OTHER=3
#### CREATE
- L195 `SetHashType` via `execute`: `INSERT INTO hash_type ( hash_type ) VALUES ( ? );`
- L202 `SetNumberOfStars` via `execute`: `INSERT INTO number_of_stars ( number_of_stars ) VALUES ( ? );`
#### UPDATE
- L207 `SetRating` via `execute`: `REPLACE INTO ratings ( hash, rating ) VALUES ( ?, ? );`
#### DELETE
- L88 `DeleteRating` via `execute`: `DELETE FROM ratings WHERE hash = ?;`
- L193 `SetHashType` via `execute`: `DELETE FROM hash_type;`
- L200 `SetNumberOfStars` via `execute`: `DELETE FROM number_of_stars;`
#### READ
- L93 `GetHashType` via `execute`: `SELECT hash_type FROM hash_type;`
- L97 `GetHashType` via `execute`: `SELECT hash FROM hashes;`
- L139 `GetNumberOfStars` via `execute`: `SELECT number_of_stars FROM number_of_stars;`
- L155 `GetRating` via `execute`: `SELECT rating FROM ratings WHERE hash = ?;`
- L171 `HasHash` via `execute`: `SELECT 1 FROM ratings WHERE hash = ?;`
- L185 `IterateRatings` via `execute`: `SELECT hash, rating FROM ratings;`
#### DDL
- L61 `_InitDB` via `execute`: `CREATE TABLE hash_type ( hash_type INTEGER );`
- L63 `_InitDB` via `execute`: `CREATE TABLE number_of_stars ( number_of_stars INTEGER );`
- L65 `_InitDB` via `execute`: `CREATE TABLE ratings ( hash BLOB PRIMARY KEY, rating REAL );`
#### OTHER
- L77 `BeginBigJob` via `execute`: `BEGIN IMMEDIATE;`
- L82 `CommitBigJob` via `execute`: `COMMIT;`
- L83 `CommitBigJob` via `execute`: `VACUUM;`

### `hydrus/core/HydrusTagArchive.py`
Сводка: CREATE=11, DELETE=8, READ=16, DDL=14, OTHER=8
#### CREATE
- L72 `ReadLargeIdQueryInSeparateChunks` via `execute`: `INSERT INTO  ( temp_id ) <dynamic>`
- L113 `_AddMappings` via `executemany`: `INSERT OR IGNORE INTO mappings ( hash_id, tag_id ) VALUES ( ?, ? );`
- L157 `_GetHashId` via `execute`: `INSERT INTO hashes ( hash ) VALUES ( ? );`
- L187 `_GetTagId` via `execute`: `INSERT INTO tags ( tag ) VALUES ( ? );`
- L202 `_GetTagId` via `execute`: `INSERT INTO namespaces ( namespace ) VALUES ( ? );`
- L226 `AddMapping` via `execute`: `INSERT OR IGNORE INTO mappings ( hash_id, tag_id ) VALUES ( ?, ? );`
- L462 `RebuildNamespaces` via `executemany`: `INSERT INTO namespaces ( namespace ) VALUES ( ? );`
- L469 `SetHashType` via `execute`: `INSERT INTO hash_type ( hash_type ) VALUES ( ? );`
- L552 `_GetTagId` via `execute`: `INSERT INTO tags ( tag ) VALUES ( ? );`
- L597 `AddPairs` via `executemany`: `INSERT OR IGNORE INTO pairs ( tag_id_1, tag_id_2 ) VALUES ( ?, ? );`
- L664 `SetPairType` via `execute`: `INSERT INTO pair_type ( pair_type ) VALUES ( ? );`
#### DELETE
- L252 `DeleteMapping` via `execute`: `DELETE FROM mappings WHERE hash_id = ? AND tag_id = ?;`
- L268 `DeleteTags` via `execute`: `DELETE FROM mappings WHERE hash_id = ?;`
- L276 `DeleteNamespaces` via `execute`: `DELETE FROM namespaces;`
- L447 `RebuildNamespaces` via `execute`: `DELETE FROM namespaces;`
- L467 `SetHashType` via `execute`: `DELETE FROM hash_type;`
- L476 `SetMappings` via `execute`: `DELETE FROM mappings WHERE hash_id = ?;`
- L609 `DeletePairs` via `executemany`: `DELETE FROM pairs WHERE tag_id_1 = ? AND tag_id_2 = ?;`
- L662 `SetPairType` via `execute`: `DELETE FROM pair_type;`
#### READ
- L85 `ReadLargeIdQueryInSeparateChunks` via `execute`: `SELECT temp_id FROM  WHERE job_id BETWEEN ? AND ?; <dynamic>`
- L107 `__init__` via `execute`: `SELECT namespace FROM namespaces;`
- L141 `_GetHashes` via `execute`: `SELECT hash FROM mappings NATURAL JOIN hashes WHERE tag_id = ?;`
- L148 `_GetHashId` via `execute`: `SELECT hash_id FROM hashes WHERE hash = ?;`
- L171 `_GetTags` via `execute`: `SELECT tag FROM mappings NATURAL JOIN tags WHERE hash_id = ?;`
- L178 `_GetTagId` via `execute`: `SELECT tag_id FROM tags WHERE tag = ?;`
- L295 `GetHashType` via `execute`: `SELECT hash_type FROM hash_type;`
- L299 `GetHashType` via `execute`: `SELECT hash FROM hashes;`
- L382 `HasHashTypeSet` via `execute`: `SELECT hash_type FROM hash_type;`
- L389 `IterateHashes` via `execute`: `SELECT hash FROM hashes;`
- L405 `IterateMappings` via `execute`: `SELECT hash FROM hashes WHERE hash_id = ?;`
- L423 `IterateMappingsTagFirst` via `execute`: `SELECT tag FROM tags WHERE tag_id = ?;`
- L449 `RebuildNamespaces` via `execute`: `SELECT tag FROM tags;`
- L517 `__init__` via `execute`: `SELECT pair_type FROM pair_type;`
- L548 `_GetTagId` via `execute`: `SELECT tag_id FROM tags WHERE tag = ?;`
- L634 `HasPair` via `execute`: `SELECT 1 FROM pairs WHERE tag_id_1 = ? AND tag_id_2 = ?;`
#### DDL
- L70 `ReadLargeIdQueryInSeparateChunks` via `execute`: `CREATE TEMPORARY TABLE  ( job_id INTEGER PRIMARY KEY AUTOINCREMENT, temp_id INTEGER ); <dynamic>`
- L92 `ReadLargeIdQueryInSeparateChunks` via `execute`: `DROP TABLE ; <dynamic>`
- L118 `_InitDB` via `execute`: `CREATE TABLE hash_type ( hash_type INTEGER );`
- L120 `_InitDB` via `execute`: `CREATE TABLE hashes ( hash_id INTEGER PRIMARY KEY, hash BLOB_BYTES );`
- L121 `_InitDB` via `execute`: `CREATE UNIQUE INDEX hashes_hash_index ON hashes ( hash );`
- L123 `_InitDB` via `execute`: `CREATE TABLE mappings ( hash_id INTEGER, tag_id INTEGER, PRIMARY KEY ( hash_id, tag_id ) );`
- L124 `_InitDB` via `execute`: `CREATE INDEX mappings_hash_id_index ON mappings ( hash_id );`
- L126 `_InitDB` via `execute`: `CREATE TABLE namespaces ( namespace TEXT );`
- L128 `_InitDB` via `execute`: `CREATE TABLE tags ( tag_id INTEGER PRIMARY KEY, tag TEXT );`
- L129 `_InitDB` via `execute`: `CREATE UNIQUE INDEX tags_tag_index ON tags ( tag );`
- L531 `_InitDB` via `execute`: `CREATE TABLE pair_type ( pair_type INTEGER );`
- L533 `_InitDB` via `execute`: `CREATE TABLE pairs ( tag_id_1 INTEGER, tag_id_2 INTEGER, PRIMARY KEY ( tag_id_1, tag_id_2 ) );`
- L535 `_InitDB` via `execute`: `CREATE TABLE tags ( tag_id INTEGER PRIMARY KEY, tag TEXT );`
- L536 `_InitDB` via `execute`: `CREATE UNIQUE INDEX tags_tag_index ON tags ( tag );`
#### OTHER
- L213 `BeginBigJob` via `execute`: `BEGIN IMMEDIATE;`
- L218 `CommitBigJob` via `execute`: `COMMIT;`
- L433 `Optimise` via `execute`: `VACUUM;`
- L434 `Optimise` via `execute`: `ANALYZE;`
- L566 `BeginBigJob` via `execute`: `BEGIN IMMEDIATE;`
- L580 `CommitBigJob` via `execute`: `COMMIT;`
- L656 `Optimise` via `execute`: `VACUUM;`
- L657 `Optimise` via `execute`: `ANALYZE;`

### `hydrus/core/files/HydrusClipHandling.py`
Сводка: READ=4
#### READ
- L16 `ExtractDBPNGToPath` via `execute`: `SELECT ImageData FROM CanvasPreview;`
- L53 `GetClipProperties` via `execute`: `SELECT CanvasWidth, CanvasHeight, CanvasUnit, CanvasResolution FROM Canvas;`
- L55 `GetClipProperties` via `execute`: `SELECT 1 FROM sqlite_master WHERE name = "TimeLine";`
- L59 `GetClipProperties` via `execute`: `SELECT StartFrame, FrameRate, EndFrame from TimeLine;`
