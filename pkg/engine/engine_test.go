package engine_test

import (
	"testing"

	"apextrade/pkg/engine"
	"apextrade/pkg/orderbook"
)

func TestMatchingEngine_ExactLimitMatch(t *testing.T) {
	eng := engine.NewMatchingEngine("ETH/USDT")

	// 1. Seller places resting ask: Sell 1.0 ETH @ 3000.0
	ask := orderbook.NewOrder("ask-1", "alice", "ETH/USDT", orderbook.SideSell, orderbook.OrderTypeLimit, 3000.0, 1.0)
	trades, err := eng.ProcessOrder(ask)
	if err != nil || len(trades) != 0 {
		t.Fatalf("expected resting ask with 0 trades, got %d trades, err: %v", len(trades), err)
	}

	// 2. Buyer places aggressive bid: Buy 1.0 ETH @ 3000.0
	bid := orderbook.NewOrder("bid-1", "bob", "ETH/USDT", orderbook.SideBuy, orderbook.OrderTypeLimit, 3000.0, 1.0)
	trades, err = eng.ProcessOrder(bid)
	if err != nil || len(trades) != 1 {
		t.Fatalf("expected 1 trade match, got %d", len(trades))
	}

	trade := trades[0]
	if trade.Price != 3000.0 || trade.Amount != 1.0 || trade.QuoteAmount != 3000.0 {
		t.Fatalf("unexpected trade execution: %+v", trade)
	}
	if trade.BuyerID != "bob" || trade.SellerID != "alice" {
		t.Fatalf("unexpected buyer/seller: %+v", trade)
	}
}

func TestMatchingEngine_PartialFill(t *testing.T) {
	eng := engine.NewMatchingEngine("ETH/USDT")

	// Seller has 0.5 ETH @ 3000.0
	ask := orderbook.NewOrder("ask-1", "alice", "ETH/USDT", orderbook.SideSell, orderbook.OrderTypeLimit, 3000.0, 0.5)
	_, _ = eng.ProcessOrder(ask)

	// Buyer wants 1.5 ETH @ 3000.0
	bid := orderbook.NewOrder("bid-1", "bob", "ETH/USDT", orderbook.SideBuy, orderbook.OrderTypeLimit, 3000.0, 1.5)
	trades, _ := eng.ProcessOrder(bid)

	if len(trades) != 1 {
		t.Fatalf("expected 1 trade, got %d", len(trades))
	}
	if trades[0].Amount != 0.5 {
		t.Fatalf("expected fill amount 0.5, got %.2f", trades[0].Amount)
	}

	// The remaining 1.0 ETH must now rest in the Bids book
	bestBid, exists := eng.Book.BestBid()
	if !exists || bestBid.TotalVolume != 1.0 {
		t.Fatalf("expected remaining 1.0 ETH in bids book, got volume: %.2f", bestBid.TotalVolume)
	}
}

func TestMatchingEngine_MarketOrderMultiLevel(t *testing.T) {
	eng := engine.NewMatchingEngine("ETH/USDT")

	// 2 Sellers at different price tiers
	ask1 := orderbook.NewOrder("ask-1", "alice", "ETH/USDT", orderbook.SideSell, orderbook.OrderTypeLimit, 3000.0, 1.0)
	ask2 := orderbook.NewOrder("ask-2", "charlie", "ETH/USDT", orderbook.SideSell, orderbook.OrderTypeLimit, 3010.0, 1.0)
	_, _ = eng.ProcessOrder(ask1)
	_, _ = eng.ProcessOrder(ask2)

	// Market Buyer sweeps 2.0 ETH
	marketBid := orderbook.NewOrder("market-1", "bob", "ETH/USDT", orderbook.SideBuy, orderbook.OrderTypeMarket, 0, 2.0)
	trades, err := eng.ProcessOrder(marketBid)
	if err != nil || len(trades) != 2 {
		t.Fatalf("expected 2 trades for market sweep, got %d trades, err: %v", len(trades), err)
	}

	if trades[0].Price != 3000.0 || trades[1].Price != 3010.0 {
		t.Fatalf("unexpected prices: trade 1 = %.2f, trade 2 = %.2f", trades[0].Price, trades[1].Price)
	}
}
