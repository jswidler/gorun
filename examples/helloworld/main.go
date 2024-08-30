package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jswidler/gorun"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func init() {
	gorun.RegisterHandler(func(ctx context.Context, args *HelloWorld) (string, error) {
		log.Ctx(ctx).Info().Msgf("Hello from inside the job!: %s", args.Msg)
		return args.Msg, nil
	})
}

type HelloWorld struct {
	Msg string
}

func (HelloWorld) JobType() string {
	return "hello-world"
}

func main() {
	setupLogging()

	gorunner, err := gorun.NewFromEnv(gorun.WithBatchFreq(5 * time.Second))
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	err = gorunner.Start(ctx)
	if err != nil {
		panic(err)
	}
	defer gorunner.Close()

	gorunner.ScheduleRepeatedWithKey(ctx, "myTriggerId", 2*time.Second, HelloWorld{Msg: "Hello, World!"})

	exitSignal := make(chan os.Signal, 1)
	signal.Notify(exitSignal, syscall.SIGINT)
	<-exitSignal
}

func setupLogging() {
	zerolog.ErrorStackMarshaler = func(err error) any {
		return fmt.Sprintf("%+v", err)
	}
	log.Logger = log.With().Stack().Caller().Logger()
	log.Logger = log.Logger.
		Output(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: "15:04:05 MST",
			FormatCaller: func(i interface{}) string {
				var c string
				if cc, ok := i.(string); ok {
					c = cc
				}
				c, after, found := strings.Cut(c, "app/")
				if found {
					c = after
				} else if c, after, found = strings.Cut(c, "jswidler/"); found {
					c = after
				}
				return c
			},
		})
	zerolog.DefaultContextLogger = &log.Logger
}
