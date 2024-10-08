package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
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
	args := os.Args
	if len(args) > 1 && args[1] == "serve" {
		runJobServer()
	} else {
		runJobProducer()
	}
}

func runJobProducer() {
	log.Info().Msg("Starting job producer")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// The settings do not matter much on the producer side as long as it connects to the same database
	gorunner, err := gorun.NewFromEnv()
	if err != nil {
		panic(err)
	}

	// Every 2 seconds, schedule a job with a random number for the payload
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			i := rand.Int63n(100)
			log.Info().Int64("randomNumber", i).Msg("Scheduling job")
			gorunner.ScheduleImmediately(ctx, &HelloWorld{Msg: "The random number is " + strconv.FormatInt(i, 10)})
		}
	}
}

func runJobServer() {
	log.Info().Msg("Starting jobs server")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// We will slow down the batch frequency to 5 seconds to make it easier to read the logs
	gorunner, err := gorun.NewFromEnv(gorun.WithBatchFreq(5 * time.Second))
	if err != nil {
		panic(err)
	}
	err = gorunner.Start(ctx)
	if err != nil {
		panic(err)
	}
	defer gorunner.Close()

	<-ctx.Done()
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
