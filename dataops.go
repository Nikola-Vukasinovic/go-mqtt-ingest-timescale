package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"
)

// Function for establishing connection connection to Timescale DB using context
func connectDb(logger *zap.Logger, ctx context.Context, connectionString string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		logger.Error("Error while parsing config from connection string")
		return nil, err
	}
	pool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		logger.Error("Error while connecting to DB")
		return nil, err
	}
	logger.Info("Succeffully connected to DB")
	return pool, nil
}

// Create table if not exists using pool
func createTableIfNotExists(logger *zap.Logger, db *pgxpool.Pool, tableName string, tableSchema string) error {
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableName, tableSchema)
	if _, err := db.Exec(context.Background(), query); err != nil {
		logger.Error("Failed to create table", zap.String("table_name", tableName), zap.String("table_schema", tableSchema), zap.Error(err))
		return err
	}
	logger.Info("Table created successfully", zap.String("table_name", tableName))
	return nil
}

// Function for inserting data into database using pool
func insertIntoDB(logger *zap.Logger, db *pgxpool.Pool, messageBuffer chan []byte, tableName string, colName string) {
	for {
		select {
		case payload := <-messageBuffer:
			if db != nil {
				query := fmt.Sprintf("INSERT INTO %s (%s) VALUES ($1)", tableName, colName)
				_, err := db.Exec(context.Background(), query, string(payload))
				if err != nil {
					logger.Error("Failed to insert data into the database", zap.Error(err))
				} else {
					logger.Info("Data inserted into the database")
				}
			}
		}
	}
}

// Function to get schema of table
func getTableSchema(db *pgxpool.Pool, tableName string) (string, error) {
	var schema strings.Builder

	query := fmt.Sprintf("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1")
	rows, err := db.Query(context.Background(), query, tableName)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	for rows.Next() {
		var columnName, dataType string
		if err := rows.Scan(&columnName, &dataType); err != nil {
			return "", err
		}
		schema.WriteString(fmt.Sprintf("%s %s, ", columnName, dataType))
	}

	return schema.String(), nil
}

func test_tsdb(user string, pass string, port string, service string, db string) {
	ctx := context.Background()
	connStr := fmt.Sprintf("postgres://%s:%s@%s:5432/%s", user, pass, service, db) // replace with your connection string
	conn, err := pgx.Connect(ctx, connStr)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	// Run a simple query to check the connection
	var greeting string
	err = conn.QueryRow(ctx, "select 'Hello, Timescale!'").Scan(&greeting)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(greeting)
}
