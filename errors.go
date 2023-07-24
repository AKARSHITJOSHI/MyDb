package main

import "errors"

var writeInsideReadTxErr = errors.New("can't perform a write operation inside a read transaction")
