# bendsql
>= v0.8.0
https://github.com/datafuselabs/bendsql

# Config

```
export BENDSQL_DSN="databend://<user>:<pwd>@<host>:443/<database>"
```

# Preapre

[prepare.sql](prepare.sql)

# Run

```
python3 ./bend.py [--warehouse <warehousename>] [--nosuspend]
```
* `--warehouse`: warehouse name, default is fetch from BENDSQL_DSN.
* `--nosuspend`: do not suspend warehouse after a query
