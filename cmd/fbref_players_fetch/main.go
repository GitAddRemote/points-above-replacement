package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"points-above-replacement/internal/fbref"
	"strconv"
	"strings"
	"time"
	//"github.com/you/par/pkg/fbref"
)

var (
	seasonsCSV = flag.String("seasons", "2020,2021,2022,2023,2024", "Comma-separated season START years (YYYY)")
	outPath    = flag.String("out", "data/player_metrics_fbref.csv", "Output CSV path")
	rps        = flag.Float64("rps", 0.8, "Requests per second (be polite)")
	burst      = flag.Int("burst", 1, "Burst tokens")
	timeout    = flag.Duration("timeout", 20*time.Second, "HTTP timeout")
	maxBody    = flag.Int64("max_body", 2<<20, "Max response body bytes")
	userAgent  = flag.String("ua", "par-fetcher/1.0 (+contact: you@example.com)", "HTTP User-Agent")
	destLeague = flag.String("dest_league", "EPL", "Destination league id (output)")
	fromLeague = flag.String("from_league", "EPL", "From league id (output)")
)

func main() {
	flag.Parse()

	years := split(*seasonsCSV)
	if len(years) == 0 {
		die("no seasons provided")
	}
	if err := os.MkdirAll(filepath.Dir(*outPath), 0755); err != nil {
		die("mkdir data: %v", err)
	}
	f, err := os.Create(*outPath)
	if err != nil {
		die("create out: %v", err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()

	// header required by the PAR backtester
	fbref.WriteBacktesterHeader(w)

	client := fbref.NewPoliteClient(*rps, *burst, *timeout, *maxBody, *userAgent)

	totalRows := 0
	for _, ys := range years {
		yr, err := strconv.Atoi(strings.TrimSpace(ys))
		if err != nil {
			die("bad season year %q", ys)
		}
		seasonStr := fbref.SeasonLabel(yr)

		stdURL := fbref.StandardURL(yr)
		shoURL := fbref.ShootingURL(yr)

		stdPage, code, err := client.Get(stdURL)
		if err != nil || code < 200 || code >= 300 {
			die("fetch standard %s: code=%d err=%v", stdURL, code, err)
		}
		shoPage, code, err := client.Get(shoURL)
		if err != nil || code < 200 || code >= 300 {
			die("fetch shooting %s: code=%d err=%v", shoURL, code, err)
		}

		stdTable, err := fbref.ExtractTable(stdPage, "stats_standard")
		if err != nil {
			die("extract standard table: %v", err)
		}
		shoTable, err := fbref.ExtractTable(shoPage, "stats_shooting")
		if err != nil {
			die("extract shooting table: %v", err)
		}

		stdRows, err := fbref.ParsePlayerTable(stdTable, fbref.RequiredStandardCols)
		if err != nil {
			die("parse standard: %v", err)
		}

		shoRows, err := fbref.ParsePlayerTable(shoTable, fbref.RequiredShootingCols)
		if err != nil {
			// fallback if xA shows as xAG
			shoRows, err = fbref.ParsePlayerTable(shoTable, fbref.RequiredShootingColsFallback)
			if err != nil {
				die("parse shooting: %v", err)
			}
		}

		joined := fbref.JoinStandardShooting(seasonStr, stdRows, shoRows, *destLeague, *fromLeague)
		wrote, err := fbref.WriteBacktesterRows(w, joined)
		if err != nil {
			die("csv write: %v", err)
		}
		w.Flush()
		if err := w.Error(); err != nil {
			die("csv flush: %v", err)
		}
		totalRows += wrote
		fmt.Printf("Season %s: wrote %d player rows\n", seasonStr, wrote)
	}
	fmt.Printf("Done. Wrote %d total rows to %s\n", totalRows, *outPath)
}

func split(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func die(f string, a ...any) {
	fmt.Fprintf(os.Stderr, f+"\n", a...)
	os.Exit(1)
}
