# [OpenM++](http://www.openmpp.org/) Go tools

This repository is a part of [OpenM++](http://www.openmpp.org/) open source microsimulation platform.
It contains oms web-service, dbcopy utility and openM++ Go libraries.

## Build

```
git clone https://github.com/openmpp/go ompp-go
cd ompp-go
go install ./dbcopy
go install ./oms
```

On Windows you may need to use MinGW or similar tools to make sure there is `gcc` in the `PATH`.

By default only SQLite database supported. 
If you want to use other database vendors (Microsoft SQL, MySQL, PostgreSQL, IBM DB2, Oracle) then compile dbcopy with ODBC support:

```
go install -tags odbc ./dbcopy
```

Please visit our [wiki](https://github.com/openmpp/openmpp.github.io/wiki) for more information or e-mail to: _openmpp dot org at gmail dot com_.

**License:** MIT.
