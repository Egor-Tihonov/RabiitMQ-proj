package repository

import (
	"context"
	"log"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	glog "github.com/labstack/gommon/log"
)

type DB struct {
	Pool *pgxpool.Pool
}

func NewConnection() (*DB, error) {
	conndb, err := pgxpool.Connect(context.Background(), "postgresql://postgres:123@localhost:5432/postgres")
	if err != nil {
		return nil, err
	}
	log.Println("create conn with db")
	return &DB{Pool: conndb}, nil
}

func (d *DB) Write(ctx context.Context, batch *pgx.Batch) error {
	result := d.Pool.SendBatch(ctx, batch)
	_, err := result.Exec()
	if err != nil {
		glog.Errorf("database error %e", err)
		return err
	}

	err = result.Close()
	if err != nil {
		glog.Errorf("database error %e", err)
		return err
	}
	return nil
}
