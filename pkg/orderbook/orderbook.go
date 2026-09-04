package orderbook

import (
	"errors"
	"sort"
	"sync"
)

var (
	ErrOrderNotFound = errors.New("order not found in book")
	ErrInvalidPrice  = errors.New("price must be greater than 0 for limit orders")
	ErrInvalidAmount = errors.New("amount must be greater than 0")
)

// PriceLevelSnapshot represents an aggregated L2 depth tier for UI/API consumption.
type PriceLevelSnapshot struct {
	Price  float64 `json:"price"`
	Volume float64 `json:"volume"`
	Orders int     `json:"orders"`
}

// OrderBook maintains the live bids and asks ladders for a single trading pair.
type OrderBook struct {
	Symbol     string
	bids       map[float64]*LimitLevel
	asks       map[float64]*LimitLevel
	bidPrices  []float64 // Sorted descending (highest buy price first)
	askPrices  []float64 // Sorted ascending (lowest sell price first)
	orderIndex map[string]*Order
	mu         sync.RWMutex
}

// NewOrderBook creates a blank, thread-safe OrderBook for a given symbol.
func NewOrderBook(symbol string) *OrderBook {
	return &OrderBook{
		Symbol:     symbol,
		bids:       make(map[float64]*LimitLevel),
		asks:       make(map[float64]*LimitLevel),
		bidPrices:  make([]float64, 0),
		askPrices:  make([]float64, 0),
		orderIndex: make(map[string]*Order),
	}
}

// AddOrder inserts a resting limit order into the appropriate price level queue.
func (ob *OrderBook) AddOrder(order *Order) error {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	if order.Price <= 0 && order.Type == OrderTypeLimit {
		return ErrInvalidPrice
	}
	if order.Remaining() <= 0 {
		return ErrInvalidAmount
	}

	ob.orderIndex[order.ID] = order

	if order.Side == SideBuy {
		level, exists := ob.bids[order.Price]
		if !exists {
			level = NewLimitLevel(order.Price)
			ob.bids[order.Price] = level
			ob.bidPrices = append(ob.bidPrices, order.Price)
			// Sort descending: 3000, 2990, 2980...
			sort.Slice(ob.bidPrices, func(i, j int) bool {
				return ob.bidPrices[i] > ob.bidPrices[j]
			})
		}
		level.Append(order)
	} else {
		level, exists := ob.asks[order.Price]
		if !exists {
			level = NewLimitLevel(order.Price)
			ob.asks[order.Price] = level
			ob.askPrices = append(ob.askPrices, order.Price)
			// Sort ascending: 3010, 3020, 3030...
			sort.Slice(ob.askPrices, func(i, j int) bool {
				return ob.askPrices[i] < ob.askPrices[j]
			})
		}
		level.Append(order)
	}

	return nil
}

// CancelOrder removes an order from the book in O(1) time.
func (ob *OrderBook) CancelOrder(orderID string) (*Order, error) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	order, exists := ob.orderIndex[orderID]
	if !exists {
		return nil, ErrOrderNotFound
	}

	delete(ob.orderIndex, orderID)

	if order.Side == SideBuy {
		if level, ok := ob.bids[order.Price]; ok {
			level.Remove(order)
			if level.IsEmpty() {
				delete(ob.bids, order.Price)
				ob.removeBidPrice(order.Price)
			}
		}
	} else {
		if level, ok := ob.asks[order.Price]; ok {
			level.Remove(order)
			if level.IsEmpty() {
				delete(ob.asks, order.Price)
				ob.removeAskPrice(order.Price)
			}
		}
	}

	return order, nil
}

func (ob *OrderBook) removeBidPrice(price float64) {
	for i, p := range ob.bidPrices {
		if p == price {
			ob.bidPrices = append(ob.bidPrices[:i], ob.bidPrices[i+1:]...)
			break
		}
	}
}

func (ob *OrderBook) removeAskPrice(price float64) {
	for i, p := range ob.askPrices {
		if p == price {
			ob.askPrices = append(ob.askPrices[:i], ob.askPrices[i+1:]...)
			break
		}
	}
}

// BestBid returns the highest price buy level currently available.
func (ob *OrderBook) BestBid() (*LimitLevel, bool) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	if len(ob.bidPrices) == 0 {
		return nil, false
	}
	return ob.bids[ob.bidPrices[0]], true
}

// BestAsk returns the lowest price sell level currently available.
func (ob *OrderBook) BestAsk() (*LimitLevel, bool) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	if len(ob.askPrices) == 0 {
		return nil, false
	}
	return ob.asks[ob.askPrices[0]], true
}

// GetDepth returns an aggregated L2 depth view of top bids and asks.
func (ob *OrderBook) GetDepth(maxLevels int) (bids, asks []PriceLevelSnapshot) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	bidLimit := len(ob.bidPrices)
	if bidLimit > maxLevels {
		bidLimit = maxLevels
	}
	for i := 0; i < bidLimit; i++ {
		price := ob.bidPrices[i]
		level := ob.bids[price]
		bids = append(bids, PriceLevelSnapshot{
			Price:  price,
			Volume: level.TotalVolume,
			Orders: level.OrderCount,
		})
	}

	askLimit := len(ob.askPrices)
	if askLimit > maxLevels {
		askLimit = maxLevels
	}
	for i := 0; i < askLimit; i++ {
		price := ob.askPrices[i]
		level := ob.asks[price]
		asks = append(asks, PriceLevelSnapshot{
			Price:  price,
			Volume: level.TotalVolume,
			Orders: level.OrderCount,
		})
	}

	return bids, asks
}
