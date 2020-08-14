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
wget -O revisions.txt "https://seafile.ifi.uzh.ch/f/f666a345b42340eea7bd/?dl=1"
```

Now the differ can be executed as follows.

```bash
java -Xmx5G -cp target/swhdiff-*.jar \
  ch.uzh.ifi.swhdiff.App <GRAPH_BASEPATH> <REVISION_PATH>
```

Where:
- GRAPH_BASEPATH denotes the path to the compressed SWH graph
- REVISION_PATH denotes the path to the file that contains all revisions and
  their timestamps in the format "<SWH-PID> <timestamp>". This data can be
  computed with query Q1 from below or downloaded directly as shown above.


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


## Scripts

Find the number of revisions in the compressed graph

```bash
xxd -c 30 python3k.pid2node.bin | grep ": 0104" | wc -l
```

returns 4024002
