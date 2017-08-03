package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"

	"github.com/cockroachdb/cockroach-go/crdb"
)

var verbose = flag.Bool("v", false, "Print verbose debug output")
var drop = flag.Bool("drop", false,
	"Drop the existing table and recreate it to start from scratch")
var load = flag.Bool("load", false,
	"Load data into the database from a file")

var W = flag.Int("W", 10, "Scale factor of benchmark")

// Open the database connection.
func setupDatabase(dbURL string) (*sql.DB, error) {
	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		return nil, err
	}

	if *verbose {
		log.Printf("connecting to db: %s\n", parsedURL.String())
	}

	db, err := sql.Open("postgres", parsedURL.String())
	if err != nil {
		return nil, err
	}

	// Ensure the database exists
	if err = crdb.ExecuteTx(db, func(tx *sql.Tx) error {
		_, inErr := tx.Exec("CREATE DATABASE IF NOT EXISTS tpch")
		return inErr
	}); err != nil {
		if *verbose {
			log.Fatalf("failed to create database: %s\n", err)
		}
	}

	return db, nil
}

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <db URL>\n\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()
	if *verbose {
		log.Printf("Starting TPC-C load generator\n")
	}

	dbURL := "postgresql://root@localhost:26257/tpcc?sslmode=disable"
	if flag.NArg() == 1 {
		dbURL = flag.Arg(0)
	}

  db, err := setupDatabase(dbURL)
	if err != nil {
		log.Fatalf("connecting to database failed: %s\n", err)
	}

	if *load {
		if err = createTables(db); err != nil {
			log.Fatalf("creating tables and indices failed: %s\n", err)
		}

		if *verbose {
			log.Printf("database setup complete. Loading...\n")
		}
	}

	t := makeTerminal(0, 0, 0, 0, 0)
	err = t.NewOrder(db)
	if err != nil {
		log.Fatalf("error creating new order: %s\n", err)
	}
}
