# pgsnap
PostgreSQL replay for unit testing. This library records the PG traffic generated in a test, and can replay the traffic next time you run the test, which saves you running a real PG (and it might also be much faster).


This code comes originally from https://github.com/egon12/pgsnap, but diverged.

## Getting Started

```go
package db

import (
  "database/sql"
  "testing"
  
  "github.com/alicebob/pgsnap"
)

func TestDB_GetProduct(t *testing.T) {
  addr := pgsnap.RunEnv(t, "postgresql://user:password@localhost:5432/dbname")
  
  db, _ := sql.Open(addr)

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

Run this during development with your local postgres, and with `go test`. The traffic will be stored in .txt files. Do commit the .txt files. During CI, or similar situations, you can use the replay when running: `PGREPLAY=1 go test`.


## Why we need this?
The best way to test PostgreSQL is by using real DB. Why, usually what we pass are queries.  And the one that can predict queries is the DB itself. But it comes with a large baggage.
Using DB as testing is quite hard, because we need to maintain the DB content while we 
do the testing.

The snapshot testing is inspired by snapshot testing UI using jest. In jest, to create the 
code at first we will use real UI (browser). And then when we do test, we will create some 
snapshot (tree structure of the UI). And every time we do some test, it will try to matched
with snapshot that we have.


## How does it work?
In a normal test run pgsnap we will create a postgresql proxy, that will be used to record the postgres requests and responses and stores it in a .txt file. If you run the test with `PGREPLAY=1` set in the environment pgsnap will create a fake postgres server, and replay the recorded traffic, erroring if it gets something unexpected. There is no need to have a real postgres around anymore in replay mode.

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


### Known bugs and limitations

Inserting values which are different every run won't work. For example `time.Now()` will not work, since it's different every time, which defeats the whole idea of this package.  
If you need a timestamp, and you can't use PG's `NOW()` or similar, use a fixed Go time value (`time.Date(2022, 5, 4, ...)`).  
For UUIDs you can seed the randomness in test. For example with github.com/google/uuid you can call: `uuid.SetRand(bytes.NewBufferString("helloworld1231231231231312312331"))`.

The .txt files which store the commands are named after the test name. Don't run multiple pgsnap in the same test (subtests are OK).

Multiple connections does not (yet) work.
