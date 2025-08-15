package main

import (
"encoding/csv"
"encoding/json"
"errors"
"flag"
"fmt"
"log"
"net/http"
"os"
"path/filepath"
"strings"
"time"

//"github.com/you/par/pkg/client"
)

var (
	apiBase   = flag.String("api_base", "https://api.football-data.org/v4", "football-data.org base URL")
	apiKey    = flag.String("api_key", os.Getenv("FOOTBALL_DATA_API_KEY"), "API key (or set FOOTBALL_DATA_API_KEY)")
	code      = flag.String("competition", "PL", "Competition code (PL for Premier League)")
	seasonsCSV= flag.String("seasons", "2020,2021,2022,2023,2024", "Comma-separated season start years (YYYY)")
	outPath   = flag.String("out", "data/team_outcomes.csv", "Output CSV path")
	rps       = flag.Float64("rps", 3.0, "Requests per second")
	burst     = flag.Int("burst", 3, "Burst tokens")
	timeout   = flag.Duration("timeout", 15*time.Second, "HTTP timeout")
	maxBody   = flag.Int64("max_body", 1<<20, "Max response body bytes") // 1 MiB
)

type standingsResp struct {
	Season int `json:"season"`
	Standings []struct{
		Type string `json:"type"`
		Table []struct{
			Team struct{
				ID int `json:"id"`
				Name string `json:"name"`
				Tla string `json:"tla"`
			} `json:"team"`
			Position int `json:"position"`
			Points int `json:"points"`
			GoalDifference int `json:"goalDifference"`
		} `json:"table"`
	} `json:"standings"`
	Competition struct {
		Code string `json:"code"`
	} `json:"competition"`
}

func main() {
	flag.Parse()
	if *apiKey == "" { log.Fatal("missing API key: set -api_key or FOOTBALL_DATA_API_KEY") }
	seasons := split(*seasonsCSV)
	if len(seasons) == 0 { log.Fatal("no seasons provided") }

	rc := client.NewRateLimitedClient(*rps, *burst, *timeout, *maxBody)
	if err := os.MkdirAll(filepath.Dir(*outPath), 0755); err != nil {
		log.Fatalf("mkdir data: %v", err)
	}
	f, err := os.Create(*outPath)
	if err != nil { log.Fatalf("create out: %v", err) }
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()

	// header: season,team_id,points,rank,goal_diff
	if err := w.Write([]string{"season","team_id","points","rank","goal_diff"}); err != nil {
		log.Fatal(err)
	}

	for _, y := range seasons {
		url := fmt.Sprintf("%s/competitions/%s/standings?season=%s", strings.TrimRight(*apiBase,"/"), *code, y)
		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Set("X-Auth-Token", *apiKey)

		body, code, _, err := rc.Do(req)
		if err != nil { log.Fatalf("fetch %s: %v", url, err) }
		if code >= 429 {
			log.Fatalf("rate limited or blocked (%d). slow rps/burst and retry later", code)
		}
		if code < 200 || code >= 300 {
			log.Fatalf("bad status %d for %s: %s", code, url, string(body))
		}

		var sr standingsResp
		if err := json.Unmarshal(body, &sr); err != nil {
			log.Fatalf("json: %v\nbody=%s", err, string(body))
		}
		seasonStr := fmt.Sprintf("%s/%s", y, y[2:])
		mainTable, err := pickMainTable(sr.Standings)
		if err != nil { log.Fatalf("standings parse: %v") }

		for _, row := range mainTable.Table {
			teamID := fmt.Sprintf("%d", row.Team.ID) // numeric ID from api
			rec := []string{
				seasonStr,
				teamID,
				fmt.Sprintf("%d", row.Points),
				fmt.Sprintf("%d", row.Position),
				fmt.Sprintf("%d", row.GoalDifference),
			}
			if err := w.Write(rec); err != nil { log.Fatal(err) }
		}
		log.Printf("wrote standings for season %s (%d teams)", seasonStr, len(mainTable.Table))
	}

	w.Flush()
	if err := w.Error(); err != nil { log.Fatal(err) }
}

func pickMainTable(sts []struct{
	Type string `json:"type"`
	Table []struct{
		Team struct{ ID int `json:"id"; Name, Tla string `json:"name","tla"` } `json:"team"`
		Position, Points, GoalDifference int `json:"position","points","goalDifference"`
	} `json:"table"`
}) (struct{
	Type string `json:"type"`
	Table []struct{
		Team struct{ ID int; Name, Tla string } `json:"team"`
		Position, Points, GoalDifference int
	} `json:"table"`
, error) {
	for _, s := range sts {
		if strings.EqualFold(s.Type, "TOTAL") {
			return s, nil
		}
	}
	return struct{
		Type string `json:"type"`
		Table []struct{
			Team struct{ ID int; Name, Tla string } `json:"team"`
			Position, Points, GoalDifference int
		} `json:"table"`
	}{}, errors.New("no TOTAL table in standings")
}

func split(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" { out = append(out, p) }
	}
	return out
}
