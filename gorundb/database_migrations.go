package gorundb

import (
	"embed"
	"io/fs"
	"net/http"
	"sort"
	"time"

	"github.com/jswidler/gorun/errors"
	"github.com/jswidler/gorun/logger"
	"github.com/rs/zerolog/log"
	migrate "github.com/rubenv/sql-migrate"
)

const dialect = "postgres"

//go:embed migrations/*
var migrationFiles embed.FS

var (
	ErrDbMigrationFailed = errors.Sentinel("database migration failed")
	ErrDbResetNotAllowed = errors.Sentinel("database reset is not allowed due to settings")
)

// MigrateUp migrates the database to the latest version
func (d *Db) MigrateUp() error {
	logger.Default().Info().Msg("checking database is up to date")
	migrations := getMigrations()
	n, err := migrate.Exec(d.db.DB, dialect, migrations, migrate.Up)
	if err != nil {
		return errors.Wrap(ErrDbMigrationFailed, errors.WithCause(err))
	}

	if n > 0 {
		logger.Default().Info().Int("numMigrations", n).Msgf("applied %d migrations", n)
	} else {
		logger.Default().Info().Msg("database was up to date")
	}

	return nil
}

// MigrateDown will roll back the database at most `max` migrations. USE CAREFULLY!!! Pass 0 for no limit.
func (d *Db) MigrateDown(max int) error {
	migrations := getMigrations()

	logger.Default().Info().Msg("rolling back a database migration")

	n, err := migrate.ExecMax(d.db.DB, dialect, migrations, migrate.Down, max)
	if err != nil {
		return errors.Wrap(ErrDbMigrationFailed, errors.WithCause(err))
	}
	if n > 0 {
		logger.Default().Info().Msgf("rolled back %d migrations", n)
	} else {
		logger.Default().Info().Msg("no migrations to roll back")
	}

	return nil
}

// MigrateStatus will log the status of all known migrations
func (d *Db) MigrateStatus() error {
	// Get all migrations found in the app
	migrations, err := getMigrations().FindMigrations()
	if err != nil {
		return errors.Wrap(ErrDbMigrationFailed, errors.WithCause(err))
	}

	// Get all migrations found in the database
	records, err := migrate.GetMigrationRecords(d.db.DB, dialect)
	if err != nil {
		return errors.Wrap(ErrDbMigrationFailed, errors.WithCause(err))
	}

	// Combine the information - it's possible to find migrations in one but not the other
	// rowList is so we can print in order.  rowMap is so we can find the entry in the list.
	rowMap := make(map[string]*statusRow)
	rowList := make([]*statusRow, 0, len(migrations))

	for _, m := range migrations {
		rowMap[m.Id] = &statusRow{
			ID:       m.Id,
			Migrated: false,
		}
		rowList = append(rowList, rowMap[m.Id])
	}

	for _, r := range records {
		if rowMap[r.Id] == nil {
			rowMap[r.Id] = &statusRow{
				ID:                   r.Id,
				MigrationFileMissing: true,
			}
			rowList = append(rowList, rowMap[r.Id])
		}
		rowMap[r.Id].Migrated = true
		rowMap[r.Id].AppliedAt = r.AppliedAt
	}

	sort.Slice(rowList, func(i, j int) bool {
		return rowList[i].ID < rowList[j].ID
	})

	for _, r := range rowList {
		if r.Migrated {
			if r.MigrationFileMissing {
				log.Info().Msgf("%s: %s (migration file missing)", r.ID, r.AppliedAt.String())
			} else {
				log.Info().Msgf("%s: %s", r.ID, r.AppliedAt.String())
			}
		} else {
			log.Info().Msgf("%s: not applied", r.ID)
		}
	}

	return nil
}

func getMigrations() *migrate.HttpFileSystemMigrationSource {
	fsys, err := fs.Sub(migrationFiles, "migrations")
	if err != nil {
		panic(err)
	}
	return &migrate.HttpFileSystemMigrationSource{
		FileSystem: http.FS(fsys),
	}
}

type statusRow struct {
	ID                   string
	Migrated             bool
	MigrationFileMissing bool
	AppliedAt            time.Time
}
