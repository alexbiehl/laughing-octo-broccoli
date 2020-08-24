```sh
cargo build
```

Start local redis

```sh
docker run --rm -p 6379:6379 -d redis:latest
```

Executor1 (terminal 1)

```sh
RUN_ID=1 REDIS_HOST=127.0.0.1 REDIS_PORT=6379 REDIS_DB=0 NM_HOST=localhost python python-src/red/executor.py
```

Executor2 (terminal 2)
```sh
RUN_ID=1 REDIS_HOST=127.0.0.1 REDIS_PORT=6379 REDIS_DB=0 NM_HOST=localhost python python-src/red/executor.py
```

Scheduler (terminal3)
```sh
RUN_ID=1 REDIS_HOST=127.0.0.1 REDIS_PORT=6379 REDIS_DB=0 NM_HOST=localhost PYTHONPATH=python-src/:python-src/red/ python test.py
```
