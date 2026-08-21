package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr, os.LookupEnv); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type envLookup func(string) (string, bool)

func run(ctx context.Context, args []string, stdout, stderr io.Writer, getenv envLookup) error {
	if len(args) == 0 {
		writeUsage(stderr)
		return errors.New("medb: command required")
	}

	switch args[0] {
	case "serve":
		cfg, err := parseServeConfig(args[1:], stderr, getenv)
		if err != nil {
			return err
		}
		return serve(ctx, cfg, stderr, getenv)
	case "token":
		if len(args) == 2 && args[1] == "generate" {
			token, err := newToken()
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(stdout, token)
			return err
		}
		writeUsage(stderr)
		return errors.New("medb: usage: medb token generate")
	case "auth":
		if len(args) >= 2 && args[1] == "recover" {
			cfg, err := parseRecoverConfig(args[2:], stderr)
			if err != nil {
				return err
			}
			return recoverAuth(cfg, stdout, stderr)
		}
		writeUsage(stderr)
		return errors.New("medb: usage: medb auth recover --dir PATH --name NAME")
	default:
		writeUsage(stderr)
		return fmt.Errorf("medb: unknown command %q", args[0])
	}
}

func writeUsage(w io.Writer) {
	_, _ = io.WriteString(w, `Usage:
  medb serve [options]
  medb token generate
  medb auth recover --dir PATH --name NAME
`)
}
