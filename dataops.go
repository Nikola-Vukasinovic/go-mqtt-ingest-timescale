package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"
)

type PQA_Message struct {
	DeviceID    string  `json:"device_id"`
	SlaveID     string  `json:"slave_id"`
	MeasureTime string  `json:"measure_time"`
	Date        string  `json:"full_date"`
	ModbusAdd   string  `json:"modbus_add"`
	Value       float64 `json:"value"`
	Key         string  `json:"var_name"`
}

var dataStore []PQA_Message

// Function for checking if DB exists
func tableExists(db *pgxpool.Pool, tableName string) (bool, error) {
	var exists bool
	err := db.QueryRow(context.Background(), "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)", tableName).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// Function for unmarshalling received data
func unmarshallMsg(logger *zap.Logger, messageBuffer chan []byte, dataBuffer chan PQA_Message) {
	for {
		select {
		case message := <-messageBuffer:
			var msgs []PQA_Message
			if err := json.Unmarshal([]byte(message), &msgs); err != nil {
				logger.Error("Error while unmarshalling received message from broker", zap.Error(err))
				continue
			}
			//Send unmarshalled message to data buffer
			//logger.Info("Unmarshalled message", zap.Any("message", msgs))
			// Append each message individually to dataStore
			for _, msg := range msgs {
				//dataStore = append(dataStore, msg)
				logger.Info("Sending msg to dataBuffer channel from device", zap.String("device id", msg.DeviceID), zap.String("key", msg.Key))
				dataBuffer <- msg
			}
			//dataBuffer <- msg
			//logger.Info("Size of dataStore %d", zap.Int("size", len(dataStore)))
		}
	}
}

// Function for batch insert using local buffer
func batchInsert(logger *zap.Logger, db *pgxpool.Pool, batchSize int, timer int, tableName string) {
	query := fmt.Sprintf("INSERT INTO %s (time, device_id, slave_id, date, address, value, key) VALUES ($1, $2, $3, $4, $5, $6, $7)", tableName)

	for {
		if len(dataStore) >= batchSize {
			logger.Info("Batch inserting data into table %s", zap.String("table", tableName))
			batch := &pgx.Batch{}
			// Extract batchSize elements from dataStore
			data := dataStore[:batchSize]
			numInserts := len(data)

			for i := range data {
				var row PQA_Message
				row = data[i]
				//Convert time
				measureTime, err := strconv.ParseInt(row.MeasureTime, 10, 64)
				if err != nil {
					logger.Error("Error parsing measure time:", zap.Error(err))
					continue
				}
				t := time.Unix(measureTime, 0)
				//Convert date
				layout := "02/01/2006 15:04:05"
				date, err := time.Parse(layout, row.Date)
				if err != nil {
					logger.Error("Error parsing date:", zap.Error(err))
					continue
				}
				batch.Queue(query, t, row.DeviceID, row.SlaveID, date, row.ModbusAdd, row.Value, row.Key)

			}
			br := db.SendBatch(context.Background(), batch)
			defer br.Close()
			//execute statements in batch queue
			for i := 0; i < numInserts; i++ {
				_, err := br.Exec()
				if err != nil {
					logger.Error("Unable to execute statement %d in batch queue", zap.Int("item:", i), zap.Error(err))
				}
			}
			logger.Info("Successfully batch inserted data")
			//TODO Check are the rows inserted
			//batch.Queue("select count(*) from sensor_data")

			// Remove the inserted elements from dataStore
			dataStore = dataStore[batchSize:]

		}
		time.Sleep((time.Duration(timer) * time.Second))
	}
}

// Function for batch insert into TimescaleDB
func batchInsertDb(logger *zap.Logger, db *pgxpool.Pool, dataBuffer chan PQA_Message, batchSize uint, tableName string) {
	//TODO When batch inserting think of what to do when conflict in insert arises currently skip can be UPDATE - ON CONFLICT DO UPDATE
	query := fmt.Sprintf("INSERT INTO %s (time, device_id, slave_id, date, address, value, key) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING", tableName)
	var batchCount uint = 0
	batch := &pgx.Batch{}
	for {
		select {
		case data := <-dataBuffer:

			//Convert time
			measureTime, err := strconv.ParseInt(data.MeasureTime, 10, 64)
			if err != nil {
				logger.Error("Error parsing measure time:", zap.Error(err))
				continue
			}
			t := time.Unix(measureTime, 0)
			//Convert date
			layout := "02/01/2006 15:04:05"
			date, err := time.Parse(layout, data.Date)
			if err != nil {
				logger.Error("Error parsing date:", zap.Error(err))
				continue
			}
			batch.Queue(query, t, data.DeviceID, data.SlaveID, date, data.ModbusAdd, data.Value, data.Key)
			batchCount++

			if batchCount >= batchSize {
				br := db.SendBatch(context.Background(), batch)

				//execute statements in batch queue
				for i := 0; i < int(batchCount); i++ {
					_, err := br.Exec()
					if err != nil {
						logger.Error("Unable to execute statement in batch queue", zap.Int("item:", i), zap.Error(err))
					}
				}

				br.Close()
				logger.Info("Successfully batch inserted data", zap.Int("data len:", int(batchCount)))
				batchCount = 0
				batch = &pgx.Batch{}
			}
		}
	}
}

// Function for creating hypertable from query
func createHyper(logger *zap.Logger, db *pgxpool.Pool, query string) error {
	if logger == nil {
		logger.Error("Error while trying to create hyper table, *zap.Logger is nil")
		return fmt.Errorf("-1")
	}

	if db == nil {
		logger.Error("Error while trying to create hyper table, *pgxpool.Pool is nil")
		return fmt.Errorf("-1")
	}
	_, err := db.Exec(context.Background(), query)
	if err != nil {
		logger.Error("Unable to create hypertable with: %s", zap.String("query:", query))
		return err
	}
	logger.Info("Hypertable created successfully with: %s", zap.String("query", query))
	return nil
}

// Function for establishing connection connection to Timescale DB using context
func connectDb(logger *zap.Logger, ctx *context.Context, connectionString string) (*pgxpool.Pool, error) {
	/*config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		logger.Error("Error while parsing config from connection string")
		return nil, err
	}
	pool, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		logger.Error("Error while connecting to DB", zap.Error(err))
		return nil, err
	}*/
	pool, err := pgxpool.Connect(context.Background(), connectionString)
	if err != nil {
		logger.Error("Error while creating and initiationg connection to DB", zap.Error(err))
		return nil, err
	}
	// Test the connection
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping failed: %v", err)
	}
	logger.Info("Successfully connected to DB")
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

// Function that check if table exists
func pingTable(logger *zap.Logger, db *pgxpool.Pool, tableName string) bool {
	var exists bool
	if db == nil {
		logger.Error("Connection DB is nil, check connection to DB")
		return false
	}
	err := db.QueryRow(context.Background(), "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = $1)", tableName).Scan(&exists)
	if err != nil {
		logger.Error("Failed to check if table exists check connection %s", zap.String("table:", tableName))
	}
	logger.Info("Ping table successful for table %s", zap.String("table", tableName))
	return exists
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
