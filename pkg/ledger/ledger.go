package ledger

import (
	"errors"
	"sync"
)

var (
	ErrInsufficientBalance = errors.New("insufficient available balance")
	ErrUserNotFound        = errors.New("user account not found")
	ErrInvalidAmount       = errors.New("amount must be greater than zero")
)

// Balance tracks available and locked (in resting orders) funds for an asset.
type Balance struct {
	Available float64 `json:"available"`
	Locked    float64 `json:"locked"`
}

// UserAccount represents a trader's portfolio and virtual assets.
type UserAccount struct {
	ID       string              `json:"id"`
	Username string              `json:"username"`
	Balances map[string]*Balance `json:"balances"` // Currency (e.g., "USDT", "ETH", "BTC") -> Balance
}

// NewUserAccount creates a trader account with initial virtual paper-trading funds.
func NewUserAccount(id, username string, initialBalances map[string]float64) *UserAccount {
	balances := make(map[string]*Balance)
	for currency, amt := range initialBalances {
		balances[currency] = &Balance{
			Available: amt,
			Locked:    0,
		}
	}
	return &UserAccount{
		ID:       id,
		Username: username,
		Balances: balances,
	}
}

// Ledger is the central thread-safe accounting and settlement system.
type Ledger struct {
	accounts map[string]*UserAccount
	mu       sync.RWMutex
}

// NewLedger creates a fresh Ledger instance.
func NewLedger() *Ledger {
	return &Ledger{
		accounts: make(map[string]*UserAccount),
	}
}

// RegisterUser adds or retrieves an existing user account with default starting capital.
func (l *Ledger) RegisterUser(id, username string, defaultFunds map[string]float64) *UserAccount {
	l.mu.Lock()
	defer l.mu.Unlock()

	if acc, exists := l.accounts[id]; exists {
		return acc
	}

	acc := NewUserAccount(id, username, defaultFunds)
	l.accounts[id] = acc
	return acc
}

// GetAccount retrieves a user's account portfolio.
func (l *Ledger) GetAccount(userID string) (*UserAccount, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	acc, exists := l.accounts[userID]
	if !exists {
		return nil, ErrUserNotFound
	}
	return acc, nil
}

// LockFunds moves available funds into escrow when placing a limit order.
func (l *Ledger) LockFunds(userID, currency string, amount float64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	acc, exists := l.accounts[userID]
	if !exists {
		return ErrUserNotFound
	}

	bal, exists := acc.Balances[currency]
	if !exists || bal.Available < amount {
		return ErrInsufficientBalance
	}

	bal.Available -= amount
	bal.Locked += amount
	return nil
}

// UnlockFunds returns locked escrow funds back to available (e.g. on order cancellation).
func (l *Ledger) UnlockFunds(userID, currency string, amount float64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	acc, exists := l.accounts[userID]
	if !exists {
		return ErrUserNotFound
	}

	bal, exists := acc.Balances[currency]
	if !exists || bal.Locked < amount {
		return errors.New("cannot unlock more than currently locked amount")
	}

	bal.Locked -= amount
	bal.Available += amount
	return nil
}

// SettleTrade atomically transfers base and quote assets between Buyer and Seller.
func (l *Ledger) SettleTrade(buyerID, sellerID, baseAsset, quoteAsset string, amount, price float64, isBuyerMaker bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	buyer, ok1 := l.accounts[buyerID]
	seller, ok2 := l.accounts[sellerID]
	if !ok1 || !ok2 {
		return ErrUserNotFound
	}

	totalQuote := amount * price

	// Ensure currency balances exist
	if _, ok := buyer.Balances[baseAsset]; !ok {
		buyer.Balances[baseAsset] = &Balance{}
	}
	if _, ok := buyer.Balances[quoteAsset]; !ok {
		buyer.Balances[quoteAsset] = &Balance{}
	}
	if _, ok := seller.Balances[baseAsset]; !ok {
		seller.Balances[baseAsset] = &Balance{}
	}
	if _, ok := seller.Balances[quoteAsset]; !ok {
		seller.Balances[quoteAsset] = &Balance{}
	}

	// 1. Settle Buyer
	if isBuyerMaker {
		// Buyer had quote currency (USDT) locked in resting order
		buyer.Balances[quoteAsset].Locked -= totalQuote
	} else {
		// Buyer was taker (market or aggressive limit)
		buyer.Balances[quoteAsset].Available -= totalQuote
	}
	buyer.Balances[baseAsset].Available += amount // Received ETH

	// 2. Settle Seller
	if isBuyerMaker {
		// Seller is taker
		seller.Balances[baseAsset].Available -= amount
	} else {
		// Seller was maker with base asset (ETH) locked in resting order
		seller.Balances[baseAsset].Locked -= amount
	}
	seller.Balances[quoteAsset].Available += totalQuote // Received USDT

	return nil
}
