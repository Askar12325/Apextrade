package orderbook

// LimitLevel represents a single price tier in the order book, holding a FIFO queue of orders.
type LimitLevel struct {
	Price       float64  `json:"price"`
	TotalVolume float64  `json:"total_volume"`
	OrderCount  int      `json:"order_count"`
	Head        *Order   `json:"-"`
	Tail        *Order   `json:"-"`
}

// NewLimitLevel initializes a new price level queue.
func NewLimitLevel(price float64) *LimitLevel {
	return &LimitLevel{
		Price: price,
	}
}

// Append adds a new order to the back (Tail) of this price level queue in O(1) time.
func (l *LimitLevel) Append(order *Order) {
	order.Prev = l.Tail
	order.Next = nil

	if l.Tail != nil {
		l.Tail.Next = order
	} else {
		l.Head = order
	}
	l.Tail = order

	l.TotalVolume += order.Remaining()
	l.OrderCount++
}

// Remove unlinks an order from this price level in O(1) time.
func (l *LimitLevel) Remove(order *Order) {
	if order.Prev != nil {
		order.Prev.Next = order.Next
	} else {
		l.Head = order.Next
	}

	if order.Next != nil {
		order.Next.Prev = order.Prev
	} else {
		l.Tail = order.Prev
	}

	l.TotalVolume -= order.Remaining()
	if l.TotalVolume < 0 {
		l.TotalVolume = 0
	}
	l.OrderCount--

	order.Prev = nil
	order.Next = nil
}

// IsEmpty returns true if there are no remaining orders at this price level.
func (l *LimitLevel) IsEmpty() bool {
	return l.Head == nil || l.OrderCount <= 0
}
