# SWH Differ

Extracting the diff between revisions from the SWH Archive.


## Compiling

```bash
mvn compile assembly:single
```


## Running

First download the revision file that denotes all revisions (and their
timestamp) for which we want to compute the diff

```bash
wget -O revisions.txt "https://seafile.ifi.uzh.ch/f/a5a30f813f6e40f59458/?dl=1"
```

Now the differ can be executed as follows.

```bash
java -Xmx5G -cp target/swhdiff-*.jar \
  ch.uzh.ifi.swhdiff.App <GRAPH_BASEPATH> <REVISION_PATH> <OUTPUT_PATH>
```

Where:
- GRAPH_BASEPATH denotes the path to the compressed SWH graph
- REVISION_PATH denotes the path to the file that contains all revisions and
  their timestamps in the format "<SWH-PID> <timestamp>". This data can be
  computed with query Q1 from below or downloaded directly as shown above.
- OUTPUT_PATH denotes the file to which the dataset is written

## Queries


Q1: Get all revisions (as SWH PID) and their date (as unix timestamp).

```sql
COPY (
  SELECT
    'swh:1:rev:' || encode(id, 'hex') as revision_pid,
    extract(epoch from date) as date
  FROM revision
) TO STDOUT WITH CSV DELIMITER E' ' TO '/tmp/revisions.txt';
```


Q2: Compute all neighbors of a directory node

```sql
SELECT *
FROM (
  (
    SELECT
      'swh:1:dir:' || encode(dd.target, 'hex') as id,
      convert_from(dd.name, 'UTF-8') as name
    FROM
      directory d
      JOIN directory_entry_dir dd ON dd.id = ANY(d.dir_entries)
    WHERE d.id = '\x7a2b28e75af7ed3f6674a8e6e46477bedfdcd439'::sha1_git
  ) UNION (
    SELECT
      'swh:1:cnt:' || encode(df.target, 'hex') as id,
      convert_from(df.name, 'UTF-8') as name
    FROM
      directory d
      JOIN directory_entry_file df ON df.id = ANY(d.file_entries)
    WHERE d.id = '\x7a2b28e75af7ed3f6674a8e6e46477bedfdcd439'::sha1_git
  ) UNION (
    SELECT
      'swh:1:rev:' || encode(dr.target, 'hex') as id,
      convert_from(dr.name, 'UTF-8') as name
    FROM
      directory d
      JOIN directory_entry_rev dr ON dr.id = ANY(d.rev_entries)
    WHERE d.id = '\x7a2b28e75af7ed3f6674a8e6e46477bedfdcd439'::sha1_git
  )
) tmp
ORDER BY id;
```


Q3: Compute the diff for a given (revision, parent revision) pair in SQL:

```sql
WITH RECURSIVE
parent_child_commits(old_dir, new_dir, path, committer_date, commit_id) AS (
  (
    -- all commits that have a parent commit
    SELECT
      r1.directory AS old_dir,
      r2.directory AS new_dir,
      '' COLLATE "C" AS path,
      r2.committer_date,
      r2.id AS commit_id
    FROM
      revision_history rh
      JOIN revision r1 ON r1.id = rh.parent_id
      JOIN revision r2 ON r2.id = rh.id
    WHERE
      r1.id = '\xe4e92cd4b9b2fa59f4add1e928ea09b757cb4212'::sha1_git AND
      r2.id = '\x75f03f3ae06b146e34c4ba4fb2d4b9cfdfffc07d'::sha1_git
  )
),
base(old_dir, new_dir, path, committer_date, commit_id) AS (
  SELECT * FROM parent_child_commits
  UNION ALL (
    -- dirty hack for PostgreSQL since base table may occur only once
    WITH base_inner AS (
      SELECT * FROM base
    )
    -- (old_dir, new_dir) represent the same directory in the two
    -- snapshots and they contain a directory/file that has changed
    SELECT
      ded1.target AS old_dir,
      ded2.target AS new_dir,
      b.path || '/' || convert_from(ded2.Name, 'UTF-8') AS path,
      b.committer_date,
      b.commit_id
    FROM
      base_inner b
      JOIN directory d1 ON d1.id = b.old_dir
      JOIN directory d2 ON d2.id = b.new_dir
      JOIN directory_entry_dir ded1 ON ded1.id = ANY(d1.dir_entries)
      JOIN directory_entry_dir ded2 ON ded2.id = ANY(d2.dir_entries)
    WHERE
      b.old_dir IS NOT NULL
      AND b.new_dir IS NOT NULL
      AND ded1.Name = ded2.Name
      AND ded1.Id != ded2.Id
    UNION ALL
    -- (NULL, new_dir) represents that new_dir was created in this
    -- new commit. Collect all new files.
    SELECT
      NULL::sha1_git AS old_dir,
      ded2.target AS new_dir,
      b.path || '/' || convert_from(ded2.Name, 'UTF-8') AS path,
      b.committer_date,
      b.commit_id
    FROM
      base_inner b
      JOIN directory d2 ON d2.id = b.new_dir
      JOIN directory_entry_dir ded2 ON ded2.id = ANY(d2.dir_entries)
    WHERE
      b.old_dir IS NULL
      OR (b.old_dir IS NOT NULL AND NOT EXISTS (
        SELECT *
        FROM
          directory d1
          JOIN directory_entry_dir ded1 ON ded1.id = ANY(d1.dir_entries)
        WHERE
          d1.id = b.old_dir
          AND ded2.Name = ded1.Name
      ))
    UNION ALL
    -- (old_dir, NULL) represents that old_dir was deleted in this
    -- new commit. Collect all deleted files.
    SELECT
      ded1.target AS old_dir,
      NULL::sha1_git AS new_dir,
      b.path || '/' || convert_from(ded1.Name, 'UTF-8') AS path,
      b.committer_date,
      b.commit_id
    FROM
      base_inner b
      JOIN directory d1 ON d1.id = b.old_dir
      JOIN directory_entry_dir ded1 ON ded1.id = ANY(d1.dir_entries)
    WHERE
      b.new_dir IS NULL
      OR (b.new_dir IS NOT NULL AND NOT EXISTS (
        SELECT *
        FROM
          directory d2
          JOIN directory_entry_dir ded2 ON ded2.id = ANY(d2.dir_entries)
        WHERE
          d2.id = b.new_dir
          AND ded2.Name = ded1.Name
      ))
  )
)
-- select * from base order by path;
-- -- SELECT DISTINCT path, committer_date, commit_id
SELECT DISTINCT *
FROM (
  -- (path, commiter_date, commit_id, 'update') represents that path
  -- was updated in commit_id at commit_date
  SELECT
    base.path || '/' || convert_from(def2.Name, 'UTF-8') AS path,
    base.committer_date,
    base.commit_id,
    'update' as operation
  FROM
    base
    JOIN directory d1 ON d1.id = base.old_dir
    JOIN directory d2 ON d2.id = base.new_dir
    JOIN directory_entry_file def1 ON def1.id = ANY(d1.file_entries)
    JOIN directory_entry_file def2 ON def2.id = ANY(d2.file_entries)
  WHERE
    base.old_dir IS NOT NULL
    AND base.new_dir IS NOT NULL
    AND def1.Name = def2.Name
    AND def1.Id != def2.Id
  UNION ALL
  -- (path, commiter_date, commit_id, 'create') represents that path
  -- was created in commit_id at commit_date
  SELECT
    base.path || '/' || convert_from(def2.Name, 'UTF-8') AS path,
    base.committer_date,
    base.commit_id,
    'create' as operation
  FROM
    base
    JOIN directory d2 ON d2.id = base.new_dir
    JOIN directory_entry_file def2 ON def2.id = ANY(d2.file_entries)
  WHERE
    base.new_dir IS NOT NULL
    AND (
      base.old_dir IS NULL
      OR (base.old_dir IS NOT NULL AND NOT EXISTS (
        SELECT *
        FROM
          directory d1
          JOIN directory_entry_file def1 ON def1.id = ANY(d1.file_entries)
        WHERE
          d1.id = base.old_dir
          AND def1.Name = def2.Name
      ))
    )
  UNION ALL
  -- (path, commiter_date, commit_id, 'delete') represents that path
  -- was deleted in commit_id at commit_date
  SELECT
    base.path || '/' || convert_from(def1.Name, 'UTF-8') AS path,
    base.committer_date,
    base.commit_id,
    'delete' as operation
  FROM
    base
    JOIN directory d1 ON d1.id = base.old_dir
    JOIN directory_entry_file def1 ON def1.id = ANY(d1.file_entries)
  WHERE
    base.old_dir IS NOT NULL
    AND (
      base.new_dir IS NULL
      OR (base.new_dir IS NOT NULL AND NOT EXISTS (
        SELECT *
        FROM
          directory d2
          JOIN directory_entry_file def2 ON def2.id = ANY(d2.file_entries)
        WHERE
          d2.id = base.new_dir
          AND def2.Name = def1.Name
      ))
    )
) tmp
ORDER BY tmp.committer_date, tmp.path;
```


## Scripts

Find the number of revisions in the compressed graph

```bash
xxd -c 30 python3k.pid2node.bin | grep ": 0104" | wc -l
```

returns 4024002
