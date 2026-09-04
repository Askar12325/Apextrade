package orderbook_test

import (
	"testing"

	"apextrade/pkg/orderbook"
)

func TestOrderBook_AddAndFIFOQueue(t *testing.T) {
	ob := orderbook.NewOrderBook("ETH/USDT")

	o1 := orderbook.NewOrder("ord-1", "user-1", "ETH/USDT", orderbook.SideBuy, orderbook.OrderTypeLimit, 3000.0, 1.5)
	o2 := orderbook.NewOrder("ord-2", "user-2", "ETH/USDT", orderbook.SideBuy, orderbook.OrderTypeLimit, 3000.0, 2.0)
	o3 := orderbook.NewOrder("ord-3", "user-3", "ETH/USDT", orderbook.SideBuy, orderbook.OrderTypeLimit, 2990.0, 5.0)

	_ = ob.AddOrder(o1)
	_ = ob.AddOrder(o2)
	_ = ob.AddOrder(o3)

	bestBid, exists := ob.BestBid()
	if !exists {
		t.Fatalf("expected best bid to exist")
	}
	if bestBid.Price != 3000.0 {
		t.Fatalf("expected best bid price 3000.0, got %.2f", bestBid.Price)
	}
	if bestBid.TotalVolume != 3.5 {
		t.Fatalf("expected total volume at 3000.0 to be 3.5, got %.2f", bestBid.TotalVolume)
	}
	// Verify FIFO order: o1 must be head, o2 must be next
	if bestBid.Head.ID != "ord-1" {
		t.Fatalf("expected head order to be ord-1 (FIFO), got %s", bestBid.Head.ID)
	}
	if bestBid.Head.Next.ID != "ord-2" {
		t.Fatalf("expected second order to be ord-2 (FIFO), got %s", bestBid.Head.Next.ID)
	}
}

func TestOrderBook_CancelOrder(t *testing.T) {
	ob := orderbook.NewOrderBook("ETH/USDT")

	o1 := orderbook.NewOrder("ord-1", "user-1", "ETH/USDT", orderbook.SideSell, orderbook.OrderTypeLimit, 3100.0, 1.0)
	_ = ob.AddOrder(o1)

	bestAsk, exists := ob.BestAsk()
	if !exists || bestAsk.Price != 3100.0 {
		t.Fatalf("expected best ask 3100.0")
	}

	canceled, err := ob.CancelOrder("ord-1")
	if err != nil {
		t.Fatalf("failed to cancel order: %v", err)
	}
	if canceled.ID != "ord-1" {
		t.Fatalf("expected canceled ID ord-1, got %s", canceled.ID)
	}

	// Book should now be empty on asks
	_, exists = ob.BestAsk()
	if exists {
		t.Fatalf("expected asks to be empty after cancellation")
	}
}
