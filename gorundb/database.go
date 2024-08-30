package gorundb

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/jmoiron/sqlx"
	"github.com/jswidler/gorun/errors"
	"github.com/jswidler/gorun/logger"
	_ "github.com/lib/pq"
)

type Db struct {
	JobView JobView

	db *sqlx.DB
}

func New(db *sql.DB) (*Db, error) {
	d := &Db{
		db: sqlx.NewDb(db, "postgres"),
	}

	d.JobView.db = d

	return d, d.MigrateUp()
}

type DatabaseConfig struct {
	User            string `env:"GORUN_DB_USER" envDefault:"postgres"`
	Password        string `env:"GORUN_DB_PASSWORD" envDefault:"postgres"`
	Host            string `env:"GORUN_DB_HOST" envDefault:"localhost"`
	Port            string `env:"GORUN_DB_PORT" envDefault:"5432"`
	DatabaseName    string `env:"GORUN_DB_DATABASE_NAME" envDefault:"postgres"`
	SslMode         string `env:"GORUN_DB_SSL_MODE" envDefault:"require"`
	ApplicationName string `env:"GORUN_DB_APPLICATION_NAME" envDefault:"gorun"`
}

func NewFromEnv() (*Db, error) {
	config := DatabaseConfig{}
	err := env.Parse(&config)
	if err != nil {
		return nil, errors.Wrap(err, errors.WithMessage("failed to read database config from environment"))
	}

	logger.Default().Info().
		Str("dbHost", config.Host).
		Str("dbName", config.DatabaseName).
		Msg("connecting to postgres")

	connString := config.ConnString()
	var db *sql.DB
	for i := 1; i <= 3; i++ {
		var err error
		db, err = sql.Open("postgres", connString)
		if err == nil {
			_, err = db.Query("SELECT 1")
		}
		if err == nil {
			break
		} else if i < 6 {
			logger.Default().Warn().Err(err).Msg("failed to connect to postgres, retrying")
			time.Sleep(time.Second * 5)
		} else {
			return nil, errors.Wrap(err, errors.WithMessage("failed to connect to postgres"))
		}
	}

	return New(db)
}

func (d *Db) Close() error {
	return d.db.Close()
}

func (c DatabaseConfig) ConnString() string {
	// Use alternative format for Google Cloud SQL
	if strings.HasPrefix(c.Host, "/cloudsql") {
		return fmt.Sprintf("user=%s password=%s database=%s host=%s",
			c.User, c.Password, c.DatabaseName, c.Host)
	}
	return fmt.Sprintf("postgresql://%s:%s@%s:%s/%s?connect_timeout=10&sslmode=%s&application_name=%s",
		c.User, c.Password, c.Host, c.Port, c.DatabaseName, c.SslMode, c.ApplicationName)
}
