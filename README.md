# boltron
too flexible and rather low level secondary indexes for boltdb

* the actively maintained (by CoreOS) fork of [boltdb](https://github.com/coreos/bbolt) is used.
* tests copied from boltdb - the only part that changed in tests is the import path.

Tests performed using:

```
$ go test -test.short 
```

### todo
* add more boltron specific tests
* add examples
