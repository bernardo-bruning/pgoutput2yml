# INTRODUCTION

System that allows you to replicate data through capturing changes in Postgres.

## COMPILE
To compile execute the command:

```
sudo apt install libpq-dev
make
```

## INSTALL

To use the pgoutput2yml is necessary install with command:

```
pgoutput2yml --host $HOST --user $USER --password $PASSWORD --install
```

## USAGE

Running in terminal the command:
```
pgoutput2yml --host $HOST --user $USER --password $PASSWORD
```

## UNINSTALL

To uninstall is necessary remove with command:

```
pgoutput2yml --host $HOST --user $USER --password $PASSWORD --uninstall
```
