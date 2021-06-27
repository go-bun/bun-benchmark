package main_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
)

type Person struct {
	ID         int32
	FirstName  string
	LastName   string
	Sex        string
	BirthDate  time.Time
	Weight     int32
	Height     int32
	UpdateTime time.Time
}

func genPerson() *Person {
	return &Person{
		FirstName:  gofakeit.FirstName(),
		LastName:   gofakeit.LastName(),
		Sex:        gofakeit.Word(),
		BirthDate:  gofakeit.Date(),
		Weight:     int32(gofakeit.Number(40, 200)),
		Height:     int32(gofakeit.Number(100, 200)),
		UpdateTime: time.Now(),
	}
}

func BenchmarkInsert(b *testing.B) {
	ctx := context.Background()
	person := genPerson()

	for _, db := range dbs() {
		err := db.ResetModel(ctx, (*Person)(nil))
		require.NoError(b, err)

		b.ResetTimer()

		b.Run(db.String(), func(b *testing.B) {
			benchmarkInsert(ctx, b, db.DB, person)
		})
	}
}

func benchmarkInsert(ctx context.Context, b *testing.B, db *bun.DB, person *Person) {
	for i := 0; i < b.N; i++ {
		person.ID = 0
		_, err := db.NewInsert().Model(person).Exec(ctx)
		require.NoError(b, err)
	}
}

func BenchmarkSelect(b *testing.B) {
	ctx := context.Background()
	person := genPerson()

	for _, db := range dbs() {
		err := db.ResetModel(ctx, (*Person)(nil))
		require.NoError(b, err)

		_, err = db.NewInsert().Model(person).Exec(ctx)
		require.NoError(b, err)

		b.ResetTimer()

		b.Run(db.String(), func(b *testing.B) {
			benchmarkSelect(ctx, b, db.DB)
		})
	}
}

func benchmarkSelect(ctx context.Context, b *testing.B, db *bun.DB) {
	for i := 0; i < b.N; i++ {
		person := new(Person)
		err := db.NewSelect().Model(person).Limit(1).Scan(ctx)
		require.NoError(b, err)
	}
}

type DB struct {
	*bun.DB
	driver string
}

func (db DB) String() string {
	return db.driver
}

func dbs() []DB {
	return []DB{
		{DB: newPG(), driver: "pg"},
		{DB: newPGX(), driver: "pgx"},
	}
}

func newPG() *bun.DB {
	sqldb := sql.OpenDB(pgdriver.NewConnector(
		pgdriver.WithDSN(dsn()),
		// Disable read/write timeouts, because pgx does not support timeouts.
		pgdriver.WithTimeout(0),
	))
	return bun.NewDB(sqldb, pgdialect.New())
}

func newPGX() *bun.DB {
	config, err := pgx.ParseConfig(dsn())
	if err != nil {
		panic(err)
	}

	// Bun does not work with prepared statements, because it formats query args itself
	// and passes nil args to drivers.
	config.PreferSimpleProtocol = true
	sqldb := stdlib.OpenDB(*config)

	return bun.NewDB(sqldb, pgdialect.New())
}

func dsn() string {
	return "postgres://postgres:@localhost:5432/test?sslmode=disable"
}
