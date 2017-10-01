# DDB SQL Server

Simple SQL server with column based engine.

Now it can:

1. Communicate with MySQL client by mysql41 protocol
2. Parse simple SQL Select queries
3. Find records
4. Return records to MySQL client

## How to build

1. Clone project to $GOPATH/src
2. Run script build.sh

```bash
cd $GOPATH/src/

git clone https://github.com/drdreyworld/ddb.git

chmod +x ./build.sh

./build.sh
```

## How to run server

```bash
$GOPATH/src/ddb/bin/server -host 127.0.0.1 -port 3306
```

## How to configure

```
$GOPATH/src/ddb/bin/server -h
Usage of ./server:
  -host string
    	server host (default "127.0.0.1")
  -port string
    	server port (default "3306")
```

## How to generate testdata

Go to example path and run create-table.go file.

It will create test table Users with 10.000.000 rows.

Rows struct:
- Id - Row ID
- FName - First name
- LName - Last name

```bash
cd $GOPATH/src/ddb/example/
go run create-table.go
```