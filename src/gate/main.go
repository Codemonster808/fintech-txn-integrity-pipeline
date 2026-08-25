// Idempotency gate: the one synchronous, latency-sensitive hop in the
// pipeline. It does exactly one thing — a conditional DynamoDB PutItem
// keyed by idempotency_key — and returns 200 (accepted, first time seen)
// or 409 (duplicate, already processed). Everything downstream is async.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/gin-gonic/gin"
)

const tableName = "txn-idempotency"
const metricsTableName = "txn-gate-metrics"
const metricsKey = "counters"

type txnEvent struct {
	TxnID          string `json:"txn_id" binding:"required"`
	IdempotencyKey string `json:"idempotency_key" binding:"required"`
	AccountID      string `json:"account_id" binding:"required"`
	AmountCents    int64  `json:"amount_cents"`
	Currency       string `json:"currency"`
	SchemaVersion  int    `json:"schema_version"`
	Timestamp      string `json:"ts"`
}

func newDynamoClient(ctx context.Context) *dynamodb.Client {
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		log.Fatalf("failed to load AWS config: %v", err)
	}

	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// bumpMetric atomically increments a named counter via UpdateItem/ADD —
// never read-modify-write, which would lose increments under concurrent
// requests. This is what makes /metrics/dedup able to see rejected
// duplicate attempts at all: a failed conditional PutItem leaves no trace
// in the idempotency table itself, so the count has to be kept separately.
func bumpMetric(ctx context.Context, client *dynamodb.Client, counter string) {
	_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(metricsTableName),
		Key: map[string]types.AttributeValue{
			"metric_id": &types.AttributeValueMemberS{Value: metricsKey},
		},
		UpdateExpression: aws.String("ADD " + counter + " :inc"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":inc": &types.AttributeValueMemberN{Value: "1"},
		},
	})
	if err != nil {
		log.Printf("warning: failed to bump metric %s: %v", counter, err)
	}
}

func acceptHandler(client *dynamodb.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		var event txnEvent
		if err := c.ShouldBindJSON(&event); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		_, err := client.PutItem(c.Request.Context(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"idempotency_key": &types.AttributeValueMemberS{Value: event.IdempotencyKey},
				"txn_id":          &types.AttributeValueMemberS{Value: event.TxnID},
				"account_id":      &types.AttributeValueMemberS{Value: event.AccountID},
			},
			ConditionExpression: aws.String("attribute_not_exists(idempotency_key)"),
		})

		if err != nil {
			var condFailed *types.ConditionalCheckFailedException
			if errors.As(err, &condFailed) {
				bumpMetric(c.Request.Context(), client, "duplicate_rejections")
				bumpMetric(c.Request.Context(), client, "total_requests")
				c.JSON(http.StatusConflict, gin.H{
					"status":          "duplicate",
					"idempotency_key": event.IdempotencyKey,
				})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		bumpMetric(c.Request.Context(), client, "total_requests")
		c.JSON(http.StatusOK, gin.H{
			"status": "accepted",
			"txn_id": event.TxnID,
		})
	}
}

func healthHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func main() {
	ctx := context.Background()
	client := newDynamoClient(ctx)

	router := gin.Default()
	router.GET("/health", healthHandler)
	router.POST("/accept", acceptHandler(client))

	port := os.Getenv("GATE_PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("idempotency gate listening on :%s", port)
	if err := router.Run(":" + port); err != nil {
		log.Fatal(err)
	}
}
