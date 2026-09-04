package orderbook

import (
	"time"
)

// Side indicates whether an order is buying or selling.
type Side string

const (
	SideBuy  Side = "BUY"  // Bid
	SideSell Side = "SELL" // Ask
)

// OrderType represents the execution type of the order.
type OrderType string

const (
	OrderTypeLimit  OrderType = "LIMIT"  // Resting limit order on the book
	OrderTypeMarket OrderType = "MARKET" // Immediate fill-or-cancel at best available prices
)

// Order represents an active or historical trade order in the book.
type Order struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	Symbol    string    `json:"symbol"` // e.g. "ETH/USDT"
	Side      Side      `json:"side"`   // BUY or SELL
	Type      OrderType `json:"type"`   // LIMIT or MARKET
	Price     float64   `json:"price"`  // Limit price in quote currency (USDT)
	Amount    float64   `json:"amount"` // Total amount of base asset (ETH)
	Filled    float64   `json:"filled"` // Amount already filled
	Timestamp int64     `json:"timestamp"`

	// Intrusive pointers for doubly-linked list (FIFO queue at the exact same price)
	Prev *Order `json:"-"`
	Next *Order `json:"-"`
}

// NewOrder creates an initialized Order instance.
func NewOrder(id, userID, symbol string, side Side, orderType OrderType, price, amount float64) *Order {
	return &Order{
		ID:        id,
		UserID:    userID,
		Symbol:    symbol,
		Side:      side,
		Type:      orderType,
		Price:     price,
		Amount:    amount,
		Filled:    0,
		Timestamp: time.Now().UnixNano(),
	}
}

// Remaining returns the unfilled portion of the order.
func (o *Order) Remaining() float64 {
	rem := o.Amount - o.Filled
	if rem < 0 {
		return 0
	}
	return rem
}

// IsFilled returns true if the order is completely executed.
func (o *Order) IsFilled() bool {
	return o.Filled >= o.Amount
}
