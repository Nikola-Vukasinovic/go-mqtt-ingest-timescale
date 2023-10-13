package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"go.uber.org/zap"
)

// Function for establishing connection to Timescale database
func openDBConnection(logger *zap.Logger, db, user, pass, port string) (*sql.DB, error) {
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s port=%s sslmode=disable", user, pass, db, port)
	dbc, err := sql.Open("postgres", connStr)
	if err != nil {
		logger.Error("Error opening database connection", zap.Error(err))
		return nil, fmt.Errorf("error opening database connection: %v", err)
	}

	// Check the connection
	err = dbc.Ping()
	if err != nil {
		logger.Error("Error connecting to the database", zap.Error(err))
		return nil, fmt.Errorf("error connecting to the database: %v", err)
	}

	return dbc, err
}

func dataOps(logger *zap.Logger) error {
	db, err := sql.Open("postgres", "postgres://admin@localhost/postgres?sslmode=disable")
	if err != nil {
		return err
	}
	defer db.Close()

	schemaName := "your_schema_name"
	tableName := "your_table_name"

	createTableQuery := `
		CREATE SCHEMA IF NOT EXISTS ` + schemaName + `;
		CREATE TABLE IF NOT EXISTS ` + schemaName + `.` + tableName + ` (
			id SERIAL PRIMARY KEY,
			column1 VARCHAR(255),
			column2 INTEGER,
			created_at TIMESTAMP
		);
	`

	if _, err := db.Exec(createTableQuery); err != nil {
		return err
	}

	log.Println("Table created successfully")

	return nil
}

// InsertMessage inserts an MQTT message into the TimescaleDB table.
func insertMessage(logger *zap.Logger, db *sql.DB, topic, payload string) error {
	stmt, err := db.Prepare("INSERT INTO your_table (topic, payload, timestamp) VALUES ($1, $2, $3)")
	if err != nil {
		logger.Error("Error preparing SQL statement", zap.Error(err))
		return fmt.Errorf("error preparing SQL statement: %v", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(topic, payload, time.Now())
	if err != nil {
		logger.Error("Error executing SQL statement", zap.Error(err))
		return fmt.Errorf("error executing SQL statement: %v", err)
	}

	logger.Info("Inserted message into TimescaleDB",
		zap.String("topic", topic),
		zap.String("payload", payload),
		zap.Time("timestamp", time.Now()),
	)

	return err
}
