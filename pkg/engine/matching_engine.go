package engine

import (
	"fmt"
	"math"
	"sync"

	"apextrade/pkg/orderbook"
)

// MatchingEngine manages the matching cycle for a single trading pair.
type MatchingEngine struct {
	Symbol     string
	Book       *orderbook.OrderBook
	trades     []*Trade
	tradeCount int64
	mu         sync.Mutex
}

// NewMatchingEngine initializes a matching engine for a given trading pair symbol.
func NewMatchingEngine(symbol string) *MatchingEngine {
	return &MatchingEngine{
		Symbol: symbol,
		Book:   orderbook.NewOrderBook(symbol),
		trades: make([]*Trade, 0),
	}
}

// ProcessOrder matches an incoming order against the resting order book.
// Returns matched trades and whether the order was fully filled or added to the book.
func (e *MatchingEngine) ProcessOrder(incoming *orderbook.Order) ([]*Trade, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var matchedTrades []*Trade

	if incoming.Side == orderbook.SideBuy {
		matchedTrades = e.matchBuyOrder(incoming)
	} else {
		matchedTrades = e.matchSellOrder(incoming)
	}

	// If remaining unfilled and it's a LIMIT order, add to the resting book
	if incoming.Remaining() > 0 && incoming.Type == orderbook.OrderTypeLimit {
		if err := e.Book.AddOrder(incoming); err != nil {
			return matchedTrades, err
		}
	}

	return matchedTrades, nil
}

func (e *MatchingEngine) matchBuyOrder(buyOrder *orderbook.Order) []*Trade {
	var trades []*Trade

	for buyOrder.Remaining() > 0 {
		bestAsk, exists := e.Book.BestAsk()
		if !exists {
			break // No sellers available
		}

		// Price check: If limit buy price < lowest sell price, no match
		if buyOrder.Type == orderbook.OrderTypeLimit && buyOrder.Price < bestAsk.Price {
			break
		}

		curr := bestAsk.Head
		for curr != nil && buyOrder.Remaining() > 0 {
			next := curr.Next
			fillAmount := math.Min(buyOrder.Remaining(), curr.Remaining())

			e.tradeCount++
			tradeID := fmt.Sprintf("trd-%d", e.tradeCount)
			trade := NewTrade(
				tradeID,
				e.Symbol,
				buyOrder.UserID, // Buyer is the Taker
				curr.UserID,     // Seller is the Maker
				curr.ID,
				buyOrder.ID,
				curr.Price, // Executed at Maker's resting price
				fillAmount,
			)

			trades = append(trades, trade)
			e.trades = append(e.trades, trade)

			buyOrder.Filled += fillAmount
			curr.Filled += fillAmount
			bestAsk.TotalVolume -= fillAmount

			if curr.IsFilled() {
				// Remove fully filled maker order
				_, _ = e.Book.CancelOrder(curr.ID)
			}

			curr = next
		}
	}

	return trades
}

func (e *MatchingEngine) matchSellOrder(sellOrder *orderbook.Order) []*Trade {
	var trades []*Trade

	for sellOrder.Remaining() > 0 {
		bestBid, exists := e.Book.BestBid()
		if !exists {
			break // No buyers available
		}

		// Price check: If limit sell price > highest buy price, no match
		if sellOrder.Type == orderbook.OrderTypeLimit && sellOrder.Price > bestBid.Price {
			break
		}

		curr := bestBid.Head
		for curr != nil && sellOrder.Remaining() > 0 {
			next := curr.Next
			fillAmount := math.Min(sellOrder.Remaining(), curr.Remaining())

			e.tradeCount++
			tradeID := fmt.Sprintf("trd-%d", e.tradeCount)
			trade := NewTrade(
				tradeID,
				e.Symbol,
				curr.UserID,      // Buyer is the Maker
				sellOrder.UserID, // Seller is the Taker
				curr.ID,
				sellOrder.ID,
				curr.Price, // Executed at Maker's resting price
				fillAmount,
			)

			trades = append(trades, trade)
			e.trades = append(e.trades, trade)

			sellOrder.Filled += fillAmount
			curr.Filled += fillAmount
			bestBid.TotalVolume -= fillAmount

			if curr.IsFilled() {
				// Remove fully filled maker order
				_, _ = e.Book.CancelOrder(curr.ID)
			}

			curr = next
		}
	}

	return trades
}

// GetRecentTrades returns the last N trades executed on the engine.
func (e *MatchingEngine) GetRecentTrades(limit int) []*Trade {
	e.mu.Lock()
	defer e.mu.Unlock()

	total := len(e.trades)
	if total == 0 {
		return []*Trade{}
	}

	start := total - limit
	if start < 0 {
		start = 0
	}

	res := make([]*Trade, total-start)
	copy(res, e.trades[start:])

	// Return most recent first
	for i, j := 0, len(res)-1; i < j; i, j = i+1, j-1 {
		res[i], res[j] = res[j], res[i]
	}
	return res
}

// CancelOrder removes an active order from the engine's book.
func (e *MatchingEngine) CancelOrder(orderID string) (*orderbook.Order, error) {
	return e.Book.CancelOrder(orderID)
}
