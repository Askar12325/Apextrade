package engine

import "time"

// Trade represents a matched transaction between a Maker (resting order) and a Taker (aggressive order).
type Trade struct {
	ID           string  `json:"id"`
	Symbol       string  `json:"symbol"`
	BuyerID      string  `json:"buyer_id"`
	SellerID     string  `json:"seller_id"`
	MakerOrderID string  `json:"maker_order_id"`
	TakerOrderID string  `json:"taker_order_id"`
	Price        float64 `json:"price"`        // Executed price (Maker's price)
	Amount       float64 `json:"amount"`       // Base asset quantity (e.g., ETH)
	QuoteAmount  float64 `json:"quote_amount"` // Total USDT exchanged (Price * Amount)
	Timestamp    int64   `json:"timestamp"`
}

// NewTrade initializes a Trade record.
func NewTrade(id, symbol, buyerID, sellerID, makerOrderID, takerOrderID string, price, amount float64) *Trade {
	return &Trade{
		ID:           id,
		Symbol:       symbol,
		BuyerID:      buyerID,
		SellerID:     sellerID,
		MakerOrderID: makerOrderID,
		TakerOrderID: takerOrderID,
		Price:        price,
		Amount:       amount,
		QuoteAmount:  price * amount,
		Timestamp:    time.Now().UnixNano(),
	}
}
