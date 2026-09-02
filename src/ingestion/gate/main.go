// Idempotency gate: the one synchronous, latency-sensitive hop in the
// pipeline. It does exactly one thing — a conditional DynamoDB PutItem
// keyed by idempotency_key — and returns 200 (accepted, first time seen)
// or 409 (duplicate, already processed). Everything downstream is async.
//
// Measured (see docs/scale-roadmap.md): a serial caller gets ~273
// requests/s from this gate — but that's the SERIAL CALLER's throughput,
// not the gate's capacity, which was never measured until
// src/bench.py's bench_gate_saturation_curve() existed. Every request
// originally did 2-3 synchronous DynamoDB calls (1 conditional PutItem +
// up to 2 metric UpdateItems) — the metrics accounting cost as much as
// the real work. This version keeps /accept's exactly-once guarantee
// completely unchanged (still one conditional PutItem per key, still the
// single source of truth) and adds three things around it: metrics move
// off the request path (atomic counters, periodic flush), a bounded
// in-memory cache serves confirmed-duplicate responses without touching
// DynamoDB, and /accept/batch trades the atomicity of a single
// conditional write for far fewer round-trips when at-least-once is an
// acceptable trade for high-volume ingestion (see its handler for the
// exact race window this introduces, stated rather than hidden).
package main

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

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
const recentKeysCacheCapacity = 50_000
const metricsFlushInterval = 2 * time.Second

type txnEvent struct {
	TxnID          string `json:"txn_id" binding:"required"`
	IdempotencyKey string `json:"idempotency_key" binding:"required"`
	AccountID      string `json:"account_id" binding:"required"`
	AmountCents    int64  `json:"amount_cents"`
	Currency       string `json:"currency"`
	SchemaVersion  int    `json:"schema_version"`
	Timestamp      string `json:"ts"`
}

// --- metrics: atomic counters on the request path, DynamoDB only on a
// periodic flush. A crashed gate loses at most one flush interval's worth
// of counts — the same trade-off already accepted elsewhere in this repo
// (the outbox pattern is at-least-once, not exactly-once, for the same
// reason: exactness here isn't worth a synchronous write per request).

var totalRequests int64
var duplicateRejections int64

func flushMetrics(ctx context.Context, client *dynamodb.Client) {
	total := atomic.SwapInt64(&totalRequests, 0)
	dup := atomic.SwapInt64(&duplicateRejections, 0)
	if total == 0 && dup == 0 {
		return
	}
	_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(metricsTableName),
		Key: map[string]types.AttributeValue{
			"metric_id": &types.AttributeValueMemberS{Value: metricsKey},
		},
		UpdateExpression: aws.String("ADD total_requests :t, duplicate_rejections :d"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":t": &types.AttributeValueMemberN{Value: strconv.FormatInt(total, 10)},
			":d": &types.AttributeValueMemberN{Value: strconv.FormatInt(dup, 10)},
		},
	})
	if err != nil {
		// Don't lose the counts over one failed flush — add them back so
		// the next tick retries. Slightly overcounts if the UpdateItem
		// actually succeeded server-side but the response was lost, which
		// is an acceptable, documented imprecision for a metrics counter.
		atomic.AddInt64(&totalRequests, total)
		atomic.AddInt64(&duplicateRejections, dup)
		log.Printf("warning: failed to flush metrics: %v", err)
	}
}

func startMetricsFlusher(ctx context.Context, client *dynamodb.Client) *time.Ticker {
	ticker := time.NewTicker(metricsFlushInterval)
	go func() {
		for range ticker.C {
			flushMetrics(ctx, client)
		}
	}()
	return ticker
}

// --- recent-keys cache: a bounded LRU that only ever ACCELERATES the
// duplicate path. A cache hit means this key had a successful PutItem at
// some point (only Add()ed after one) — safe to answer 409 without a
// DynamoDB round-trip. A cache miss (including anything evicted, or any
// key from before this process started) always falls through to the real
// conditional PutItem. Eviction can only produce an extra DynamoDB call
// on a duplicate that would have been free; it can never cause a true
// duplicate to be accepted.

type lruCache struct {
	mu       sync.Mutex
	capacity int
	ll       *list.List
	items    map[string]*list.Element
}

func newLRUCache(capacity int) *lruCache {
	return &lruCache{capacity: capacity, ll: list.New(), items: make(map[string]*list.Element)}
}

func (c *lruCache) Contains(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return false
	}
	c.ll.MoveToFront(el)
	return true
}

func (c *lruCache) Add(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		return
	}
	el := c.ll.PushFront(key)
	c.items[key] = el
	if c.ll.Len() > c.capacity {
		oldest := c.ll.Back()
		if oldest != nil {
			c.ll.Remove(oldest)
			delete(c.items, oldest.Value.(string))
		}
	}
}

func newDynamoClient(ctx context.Context) *dynamodb.Client {
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		endpoint = "http://localhost:4581"
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

func acceptHandler(client *dynamodb.Client, recentKeys *lruCache) gin.HandlerFunc {
	return func(c *gin.Context) {
		var event txnEvent
		if err := c.ShouldBindJSON(&event); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if recentKeys.Contains(event.IdempotencyKey) {
			atomic.AddInt64(&duplicateRejections, 1)
			atomic.AddInt64(&totalRequests, 1)
			c.JSON(http.StatusConflict, gin.H{
				"status":          "duplicate",
				"idempotency_key": event.IdempotencyKey,
			})
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
				recentKeys.Add(event.IdempotencyKey)
				atomic.AddInt64(&duplicateRejections, 1)
				atomic.AddInt64(&totalRequests, 1)
				c.JSON(http.StatusConflict, gin.H{
					"status":          "duplicate",
					"idempotency_key": event.IdempotencyKey,
				})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		recentKeys.Add(event.IdempotencyKey)
		atomic.AddInt64(&totalRequests, 1)
		c.JSON(http.StatusOK, gin.H{
			"status": "accepted",
			"txn_id": event.TxnID,
		})
	}
}

type batchResult struct {
	IdempotencyKey string `json:"idempotency_key"`
	Status         string `json:"status"`
}

// acceptBatchHandler trades the single-write atomicity of /accept for
// far fewer round-trips: BatchGetItem (100 keys/call) to see what already
// exists, then BatchWriteItem (25 items/call) for the rest — neither API
// supports ConditionExpression, so this is a two-phase read-then-write,
// not one atomic conditional write.
//
// The race window this introduces, stated plainly: two /accept/batch
// requests containing the same new idempotency_key, in flight at the
// same time, can both see it as absent in phase 1 and both write it in
// phase 2 — both would report "accepted". /accept is unaffected and
// keeps its exact-one-winner guarantee (still the single conditional
// PutItem, still what tests/test_chaos.py::
// test_concurrent_duplicate_requests_only_one_wins exercises). Use
// /accept/batch for high-volume ingestion where at-least-once is an
// acceptable trade, same as the outbox pattern already accepts
// elsewhere in this repo; use /accept where the atomic guarantee matters.
//
// Known simplification, stated rather than hidden: UnprocessedKeys /
// UnprocessedItems (DynamoDB's partial-throttling response) are not
// retried here — matches the same simplification in
// src/curate_incremental.py's partition filter, for the same reason
// (not observed on MiniStack at these volumes; a production version
// against real AWS at sustained throughput would need a retry loop).
func acceptBatchHandler(client *dynamodb.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		var events []txnEvent
		if err := c.ShouldBindJSON(&events); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if len(events) == 0 {
			c.JSON(http.StatusOK, gin.H{"results": []batchResult{}})
			return
		}

		ctx := c.Request.Context()

		// BatchGetItem rejects a request containing duplicate keys (real
		// AWS behavior, not a MiniStack quirk — found by testing a batch
		// with a within-request duplicate, exactly the case
		// seenInThisRequest below is meant to handle downstream). Dedupe
		// the KEYS QUERIED here; seenInThisRequest still needs to run
		// separately below to correctly mark every event sharing a
		// duplicate key, not just the first.
		uniqueKeys := make([]string, 0, len(events))
		seenForQuery := make(map[string]bool)
		for _, e := range events {
			if !seenForQuery[e.IdempotencyKey] {
				seenForQuery[e.IdempotencyKey] = true
				uniqueKeys = append(uniqueKeys, e.IdempotencyKey)
			}
		}

		existing := make(map[string]bool)
		for i := 0; i < len(uniqueKeys); i += 100 {
			end := min(i+100, len(uniqueKeys))
			keys := make([]map[string]types.AttributeValue, 0, end-i)
			for _, k := range uniqueKeys[i:end] {
				keys = append(keys, map[string]types.AttributeValue{
					"idempotency_key": &types.AttributeValueMemberS{Value: k},
				})
			}
			resp, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
				RequestItems: map[string]types.KeysAndAttributes{
					tableName: {Keys: keys},
				},
			})
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			for _, item := range resp.Responses[tableName] {
				if v, ok := item["idempotency_key"].(*types.AttributeValueMemberS); ok {
					existing[v.Value] = true
				}
			}
		}

		results := make([]batchResult, 0, len(events))
		var newEvents []txnEvent
		seenInThisRequest := make(map[string]bool)
		for _, e := range events {
			if existing[e.IdempotencyKey] || seenInThisRequest[e.IdempotencyKey] {
				results = append(results, batchResult{IdempotencyKey: e.IdempotencyKey, Status: "duplicate"})
				continue
			}
			seenInThisRequest[e.IdempotencyKey] = true
			newEvents = append(newEvents, e)
			results = append(results, batchResult{IdempotencyKey: e.IdempotencyKey, Status: "accepted"})
		}

		for i := 0; i < len(newEvents); i += 25 {
			end := min(i+25, len(newEvents))
			writeReqs := make([]types.WriteRequest, 0, end-i)
			for _, e := range newEvents[i:end] {
				writeReqs = append(writeReqs, types.WriteRequest{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"idempotency_key": &types.AttributeValueMemberS{Value: e.IdempotencyKey},
							"txn_id":          &types.AttributeValueMemberS{Value: e.TxnID},
							"account_id":      &types.AttributeValueMemberS{Value: e.AccountID},
						},
					},
				})
			}
			_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]types.WriteRequest{tableName: writeReqs},
			})
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
		}

		atomic.AddInt64(&totalRequests, int64(len(events)))
		atomic.AddInt64(&duplicateRejections, int64(len(events)-len(newEvents)))

		c.JSON(http.StatusOK, gin.H{"results": results})
	}
}

func healthHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func main() {
	ctx := context.Background()
	client := newDynamoClient(ctx)
	recentKeys := newLRUCache(recentKeysCacheCapacity)

	flushTicker := startMetricsFlusher(ctx, client)
	defer flushTicker.Stop()
	defer flushMetrics(ctx, client) // best-effort final flush on graceful exit

	router := gin.Default()
	router.GET("/health", healthHandler)
	router.POST("/accept", acceptHandler(client, recentKeys))
	router.POST("/accept/batch", acceptBatchHandler(client))

	port := 8080
	if raw := os.Getenv("GATE_PORT"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 65535 {
			// GATE_PORT is read once at process startup from the deploy
			// environment, not from any request — there is no attacker path
			// to this value, only an operator typo. gosec's G706 (log
			// injection via taint) still flags echoing it back because it
			// can't see that distinction; #nosec is the correct tool here,
			// not a code contortion to hide a real diagnostic from whoever
			// misconfigured the env var.
			log.Fatalf("invalid GATE_PORT %q: must be an integer in 1-65535", raw) //#nosec G706 -- startup-only, operator-controlled env var, not attacker-reachable
		}
		port = parsed
	}
	log.Printf("idempotency gate listening on :%d", port)
	if err := router.Run(fmt.Sprintf(":%d", port)); err != nil {
		log.Fatal(err)
	}
}
