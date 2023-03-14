package main

import (
	"context"
	"flag"
	"log"
	"math/big"
	"os"
	"os/signal"
	"syscall"

	ion "github.com/amzn/ion-go/ion"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	consumer "github.com/harlow/kinesis-consumer"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

const accessID = "="
const secretKey = "-"

func main() {
	var (
		stream    = flag.String("stream", "people-stream", "Stream name")
		awsRegion = flag.String("region", "us-east-2", "AWS Region")
	)
	flag.Parse()

	os.Setenv("AWS_ACCESS_KEY_ID", accessID)
	os.Setenv("AWS_SECRET_ACCESS_KEY", secretKey)

	// start gorm db
	db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	// Migrate the schema
	db.AutoMigrate(&Transaction{})

	// client
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(*awsRegion),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	var client = kinesis.NewFromConfig(cfg)

	// consumer
	c, err := consumer.New(
		*stream,
		consumer.WithClient(client),
	)
	if err != nil {
		log.Fatalf("consumer error: %v", err)
	}

	// scan
	ctx := trap()
	err = c.Scan(ctx, func(r *consumer.Record) error {
		p := RecordDetail{}

		//	log.Printf("tx2: %+v \n", string(r.Data))

		err := ion.Unmarshal(r.Data, &p)
		if err != nil {
			// TODO: need to check this
			log.Printf("error unmarshaling: %+v \n", err)
			return nil
		}

		if p.Payload.Revision.Data.TxID != "" {
			log.Printf("tx: %+v \n", p)

			insertTx := Transaction{
				TxID:      p.Payload.Revision.Metadata.TxID,
				Nonce:     p.Payload.Revision.Data.Nonce,
				GasFeeCap: p.Payload.Revision.Data.GasFeeCap.String(),
				Gas:       p.Payload.Revision.Data.Gas,
				GasTipCap: p.Payload.Revision.Data.GasTipCap.String(),
				To:        p.Payload.Revision.Data.To,
				From:      p.Payload.Revision.Data.From,
				Value:     p.Payload.Revision.Data.Value.String(),
				Data:      p.Payload.Revision.Data.Data,
			}
			db.Create(&insertTx)
			log.Println("inserted tx: ", insertTx.ID)
		}
		return nil // continue scanning
	})
	if err != nil {
		log.Fatalf("scan error: %v", err)
	}
}

func trap() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		sig := <-sigs
		log.Printf(" %s", sig)
		cancel()
	}()

	return ctx
}

type RecordDetail struct {
	QldbStreamArn string `ion:"qldbStreamArn"`
	RecordType    string `ion:"recordType"`
	Payload       struct {
		TableInfo struct {
			TableName string `ion:"tableName"`
			TableID   string `ion:"tableId"`
		} `json:"tableInfo"`
		Revision struct {
			BlockAddress struct {
				StrandID   string `ion:"strandId"`
				SequenceNo int    `ion:"sequenceNo"`
			} `json:"blockAddress"`
			Hash     interface{}       `ion:"hash"`
			Data     TransactionStream `ion:"data"`
			Metadata struct {
				ID      string `ion:"id"`
				Version int    `ion:"version"`
				TxID    string `ion:"txId"`
			} `json:"metadata"`
		} `json:"revision"`
	} `json:"payload"`
}

type TransactionStream struct {
	TxID      string   `ion:"txID" gorm:"index"`
	Nonce     uint64   `ion:"nonce" gorm:"type:numeric"`
	GasFeeCap *big.Int `ion:"gasFeeCap" gorm:"type:bigint(20)"`
	Gas       uint64   `ion:"gas" gorm:"type:numeric"`
	GasTipCap *big.Int `ion:"gasTipCap" gorm:"type:bigint(20) "`
	To        string   `ion:"to"`
	From      string   `ion:"from"`
	Value     *big.Int `ion:"value" gorm:"type:bigint(20) "`
	Data      []byte   `ion:"data"`
}

type Transaction struct {
	gorm.Model
	TxID      string `ion:"txID" gorm:"index"`
	Nonce     uint64 `ion:"nonce" gorm:"type:numeric"`
	GasFeeCap string `ion:"gasFeeCap"`
	Gas       uint64 `ion:"gas" gorm:"type:numeric"`
	GasTipCap string `ion:"gasTipCap" `
	To        string `ion:"to"`
	From      string `ion:"from"`
	Value     string `ion:"value" `
	Data      []byte `ion:"data"`
}
