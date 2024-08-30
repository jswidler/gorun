package logger

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var DisableLogging = false
var disabledLogger = zerolog.New(nil)

func Ctx(ctx context.Context) *zerolog.Logger {
	if DisableLogging {
		return &disabledLogger
	}
	return log.Ctx(ctx)
}

func Default() *zerolog.Logger {
	if DisableLogging {
		return &disabledLogger
	}
	return &log.Logger
}
