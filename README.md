# Bun benchmark

To run this benchmark:

```shell
go test -bench=. -benchmem
```

Results:

```
BenchmarkInsert/pg-12 	    7254	    148380 ns/op	     900 B/op	      13 allocs/op
BenchmarkInsert/pgx-12         	    6494	    166391 ns/op	    2076 B/op	      26 allocs/op
BenchmarkSelect/pg-12          	    9100	    132952 ns/op	    1417 B/op	      18 allocs/op
BenchmarkSelect/pgx-12         	    8199	    154920 ns/op	    3679 B/op	      60 allocs/op
```
