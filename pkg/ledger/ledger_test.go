package ledger_test

import (
	"testing"

	"apextrade/pkg/ledger"
)

func TestLedger_FundLockingAndUnlock(t *testing.T) {
	led := ledger.NewLedger()
	acc := led.RegisterUser("user-1", "trader1", map[string]float64{
		"USDT": 10000.0,
		"ETH":  5.0,
	})

	if acc.Balances["USDT"].Available != 10000.0 {
		t.Fatalf("expected 10000 USDT available")
	}

	// Lock 3000 USDT for an order
	err := led.LockFunds("user-1", "USDT", 3000.0)
	if err != nil {
		t.Fatalf("failed to lock funds: %v", err)
	}

	if acc.Balances["USDT"].Available != 7000.0 || acc.Balances["USDT"].Locked != 3000.0 {
		t.Fatalf("unexpected balance after lock: %+v", acc.Balances["USDT"])
	}

	// Unlock 3000 USDT
	err = led.UnlockFunds("user-1", "USDT", 3000.0)
	if err != nil {
		t.Fatalf("failed to unlock funds: %v", err)
	}

	if acc.Balances["USDT"].Available != 10000.0 || acc.Balances["USDT"].Locked != 0.0 {
		t.Fatalf("unexpected balance after unlock: %+v", acc.Balances["USDT"])
	}
}

func TestLedger_SettleTrade(t *testing.T) {
	led := ledger.NewLedger()
	buyer := led.RegisterUser("buyer-1", "buyer", map[string]float64{"USDT": 10000.0, "ETH": 0.0})
	seller := led.RegisterUser("seller-1", "seller", map[string]float64{"USDT": 0.0, "ETH": 5.0})

	// Seller places limit ask: 1.0 ETH @ 3000.0 (Locks 1.0 ETH)
	_ = led.LockFunds("seller-1", "ETH", 1.0)

	// Settle 1.0 ETH @ 3000.0 (Seller was maker, Buyer is taker)
	err := led.SettleTrade("buyer-1", "seller-1", "ETH", "USDT", 1.0, 3000.0, false)
	if err != nil {
		t.Fatalf("settle trade failed: %v", err)
	}

	// Buyer should have 7000 USDT, 1.0 ETH
	if buyer.Balances["USDT"].Available != 7000.0 || buyer.Balances["ETH"].Available != 1.0 {
		t.Fatalf("unexpected buyer balances: %+v", buyer.Balances)
	}

	// Seller should have 3000 USDT, 4.0 ETH (0 locked)
	if seller.Balances["USDT"].Available != 3000.0 || seller.Balances["ETH"].Available != 4.0 || seller.Balances["ETH"].Locked != 0.0 {
		t.Fatalf("unexpected seller balances: %+v", seller.Balances)
	}
}
