# pgsnap
Fake PostgreSQL proxy for unit testing

## Getting Started
You can download the library in by typing this command

```
go get github.com/egon12/pgsnap
```
and then use it in test
```go
package db

import (
  "database/sql"
  "testing"
  
  "github.com/egon12/pgsnap"
  "github.com/lib/pq"
)

func TestDB_GetProduct(t *testing.T) {
  snap := pgsnap.NewSnap(t, "postgresql://user:password@localhost:5432/dbname")
  defer snap.Finish()
  
  db, _ := sql.Open(snap.Addr())

  p := ProductRepo{DB: db}
  p, err := p.Get(1)

  if err != nil {
    t.Errorf("got error: %v", err)
  }

  if p.ID != 1 {
    t.Error("got wrong product")
  }
}

```

## Why we need this?
The best way to test PostgreSQL is by using real DB. Why, usually what we pass are queries.  And the one that can predict queries is the DB itself. But it comes with a large baggage.
Using DB as testing is quite hard, because we need to maintain the DB content while we 
do the testing.

The snapshot testing is inspired by snapshot testing UI using jest. In jest, to create the 
code at first we will use real UI (browser). And then when we do test, we will create some 
snapshot (tree structure of the UI). And every time we do some test, it will try to matched
with snapshot that we have.


## How does it work?
When we create pgsnap, we will create a postgresql proxy, that will be used to get message that 
send/receive between app and postgres. At first run, or when we don't have the snapshot file,
it will create the snapshot file and save it.

At the second run the proxy would not connect to the real postgres server, and instead read
the snapshot file and response accordingly to the app.

### First run
```mermaid
graph LR

    TestApp --> pgsnap.Proxy --> postgreSQL_Server
    postgreSQL_Server --> pgsnap.Proxy --> TestApp
    pgsnap.Proxy --> snapshot_file
    
```

### Second run (and run in CI/CD environment)
```mermaid
graph LR

    TestApp --> pgsnap.Proxy
    snapshot_file --> pgsnap.Proxy --> TestApp
    
```

### Known Bugs
Inserting values which are different every run won't work. For example `time.Now()` will not work, since it's different every time, which defeats the whole idea of this package. If you need a timestamp, and you can't use PG`s `NOW()` or similar, use a fixed Go time value (`time.Date(2022, 5, 4, ...)`).
