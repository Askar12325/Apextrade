package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"apextrade/pkg/engine"
	"apextrade/pkg/ledger"
	"apextrade/pkg/orderbook"
	"apextrade/pkg/websocket"
)

// OrderHistoryItem records completed or canceled user orders.
type OrderHistoryItem struct {
	ID          string              `json:"id"`
	Symbol      string              `json:"symbol"`
	Side        orderbook.Side      `json:"side"`
	Type        orderbook.OrderType `json:"type"`
	Price       float64             `json:"price"`
	Amount      float64             `json:"amount"`
	Filled      float64             `json:"filled"`
	Status      string              `json:"status"` // "FILLED" or "CANCELED"
	CompletedAt time.Time           `json:"completed_at"`
}

type Server struct {
	engine       *engine.MatchingEngine
	ledger       *ledger.Ledger
	hub          *websocket.Hub
	demoUser     *ledger.UserAccount
	userOrders   map[string]*orderbook.Order
	orderHistory []*OrderHistoryItem
	mu           sync.Mutex
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081" // Default port 8081
	}

	symbol := "ETH/USDT"
	matchingEngine := engine.NewMatchingEngine(symbol)
	led := ledger.NewLedger()
	hub := websocket.NewHub()
	go hub.Run()

	// 1. Create Default Demo Trader with $10,000 USDT & 5 ETH
	demoUser := led.RegisterUser("demo-user", "Demo Trader", map[string]float64{
		"USDT": 10000.0,
		"ETH":  5.0,
	})

	server := &Server{
		engine:       matchingEngine,
		ledger:       led,
		hub:          hub,
		demoUser:     demoUser,
		userOrders:   make(map[string]*orderbook.Order),
		orderHistory: make([]*OrderHistoryItem, 0),
	}

	// 2. Start Background Autonomous Market Maker (Liquidity Generator)
	go server.startMarketMakerBot(symbol)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", server.handleTerminalUI)
	mux.HandleFunc("GET /health", server.handleHealth)
	mux.HandleFunc("GET /ws", hub.ServeWS)
	mux.HandleFunc("GET /api/v1/account", server.handleGetAccount)
	mux.HandleFunc("GET /api/v1/orderbook", server.handleGetOrderBook)
	mux.HandleFunc("GET /api/v1/trades", server.handleGetTrades)
	mux.HandleFunc("GET /api/v1/candles", server.handleGetCandles)
	mux.HandleFunc("POST /api/v1/orders", server.handlePlaceOrder)
	mux.HandleFunc("DELETE /api/v1/orders/", server.handleCancelOrder)
	mux.HandleFunc("POST /api/v1/faucet", server.handleFaucet)

	httpServer := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan

		log.Println("Shutting down ApexTrade Exchange...")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	log.Printf("🚀 ApexTrade Matching Engine & Responsive Terminal running on http://localhost:%s", port)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "UP",
		"engine": "ApexTrade High-Speed Matching Engine",
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Server) handleGetAccount(w http.ResponseWriter, r *http.Request) {
	acc, _ := s.ledger.GetAccount(s.demoUser.ID)
	s.mu.Lock()
	var activeOrders []*orderbook.Order
	for _, o := range s.userOrders {
		if !o.IsFilled() {
			activeOrders = append(activeOrders, o)
		}
	}
	historyCopy := make([]*OrderHistoryItem, len(s.orderHistory))
	copy(historyCopy, s.orderHistory)
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"account":       acc,
		"active_orders": activeOrders,
		"order_history": historyCopy,
	})
}

func (s *Server) handleGetOrderBook(w http.ResponseWriter, r *http.Request) {
	bids, asks := s.engine.Book.GetDepth(15)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"symbol": s.engine.Symbol,
		"bids":   bids,
		"asks":   asks,
	})
}

func (s *Server) handleGetTrades(w http.ResponseWriter, r *http.Request) {
	trades := s.engine.GetRecentTrades(30)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(trades)
}

func (s *Server) handleGetCandles(w http.ResponseWriter, r *http.Request) {
	// Generate realistic 1-minute historical candles around 3000.0
	now := time.Now().Unix()
	start := now - (60 * 60) // Last 60 minutes
	var candles []map[string]any
	currPrice := 2990.0

	for t := start; t <= now; t += 60 {
		delta := (rand.Float64() - 0.49) * 4.0
		open := currPrice
		closePrice := currPrice + delta
		high := mathMax(open, closePrice) + rand.Float64()*2.5 + 0.5
		low := mathMin(open, closePrice) - rand.Float64()*2.5 - 0.5
		candles = append(candles, map[string]any{
			"time":  t,
			"open":  open,
			"high":  high,
			"low":   low,
			"close": closePrice,
		})
		currPrice = closePrice
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(candles)
}

func mathMax(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func mathMin(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func (s *Server) handleFaucet(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.demoUser.Balances["USDT"].Available += 5000.0
	s.demoUser.Balances["ETH"].Available += 2.0
	s.mu.Unlock()

	s.broadcastAccountUpdate()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.demoUser)
}

func (s *Server) handlePlaceOrder(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Side   orderbook.Side      `json:"side"` // BUY or SELL
		Type   orderbook.OrderType `json:"type"` // LIMIT or MARKET
		Price  float64             `json:"price"`
		Amount float64             `json:"amount"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Amount <= 0 {
		http.Error(w, "amount must be greater than 0", http.StatusBadRequest)
		return
	}

	orderID := fmt.Sprintf("ord-%d", time.Now().UnixNano()%10000000)
	order := orderbook.NewOrder(orderID, s.demoUser.ID, s.engine.Symbol, req.Side, req.Type, req.Price, req.Amount)

	// 1. Escrow Fund Locking for Limit Orders
	if req.Type == orderbook.OrderTypeLimit {
		if req.Side == orderbook.SideBuy {
			cost := req.Price * req.Amount
			if err := s.ledger.LockFunds(s.demoUser.ID, "USDT", cost); err != nil {
				http.Error(w, fmt.Sprintf("failed to lock funds: %v", err), http.StatusBadRequest)
				return
			}
		} else {
			if err := s.ledger.LockFunds(s.demoUser.ID, "ETH", req.Amount); err != nil {
				http.Error(w, fmt.Sprintf("failed to lock funds: %v", err), http.StatusBadRequest)
				return
			}
		}
	}

	// 2. Execute against Matching Engine
	trades, err := s.engine.ProcessOrder(order)
	if err != nil {
		http.Error(w, fmt.Sprintf("engine error: %v", err), http.StatusInternalServerError)
		return
	}

	// 3. Settle Balances for each matched trade
	for _, trd := range trades {
		isBuyerMaker := trd.MakerOrderID == trd.TakerOrderID
		_ = s.ledger.SettleTrade(trd.BuyerID, trd.SellerID, "ETH", "USDT", trd.Amount, trd.Price, isBuyerMaker)
		s.hub.BroadcastJSON("TRADE_TICKER", trd)
	}

	// 4. Update user active orders and order history
	s.mu.Lock()
	if order.IsFilled() {
		s.orderHistory = append(s.orderHistory, &OrderHistoryItem{
			ID:          order.ID,
			Symbol:      order.Symbol,
			Side:        order.Side,
			Type:        order.Type,
			Price:       order.Price,
			Amount:      order.Amount,
			Filled:      order.Filled,
			Status:      "FILLED",
			CompletedAt: time.Now().UTC(),
		})
	} else if order.Type == orderbook.OrderTypeLimit {
		s.userOrders[order.ID] = order
	}
	s.mu.Unlock()

	// 5. Broadcast live updates to all WebSockets
	s.broadcastOrderBookUpdate()
	s.broadcastAccountUpdate()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"order":  order,
		"trades": trades,
	})
}

func (s *Server) handleCancelOrder(w http.ResponseWriter, r *http.Request) {
	orderID := strings.TrimPrefix(r.URL.Path, "/api/v1/orders/")
	if orderID == "" {
		http.Error(w, "order ID required", http.StatusBadRequest)
		return
	}

	canceled, err := s.engine.CancelOrder(orderID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// Unlock escrow funds
	if canceled.Side == orderbook.SideBuy {
		_ = s.ledger.UnlockFunds(canceled.UserID, "USDT", canceled.Price*canceled.Remaining())
	} else {
		_ = s.ledger.UnlockFunds(canceled.UserID, "ETH", canceled.Remaining())
	}

	s.mu.Lock()
	delete(s.userOrders, orderID)
	s.orderHistory = append(s.orderHistory, &OrderHistoryItem{
		ID:          canceled.ID,
		Symbol:      canceled.Symbol,
		Side:        canceled.Side,
		Type:        canceled.Type,
		Price:       canceled.Price,
		Amount:      canceled.Amount,
		Filled:      canceled.Filled,
		Status:      "CANCELED",
		CompletedAt: time.Now().UTC(),
	})
	s.mu.Unlock()

	s.broadcastOrderBookUpdate()
	s.broadcastAccountUpdate()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(canceled)
}

func (s *Server) broadcastOrderBookUpdate() {
	bids, asks := s.engine.Book.GetDepth(15)
	s.hub.BroadcastJSON("ORDERBOOK_L2", map[string]any{
		"symbol": s.engine.Symbol,
		"bids":   bids,
		"asks":   asks,
	})
}

func (s *Server) broadcastAccountUpdate() {
	acc, _ := s.ledger.GetAccount(s.demoUser.ID)
	s.mu.Lock()
	var activeOrders []*orderbook.Order
	for _, o := range s.userOrders {
		if !o.IsFilled() {
			activeOrders = append(activeOrders, o)
		}
	}
	historyCopy := make([]*OrderHistoryItem, len(s.orderHistory))
	copy(historyCopy, s.orderHistory)
	s.mu.Unlock()

	s.hub.BroadcastJSON("ACCOUNT_UPDATE", map[string]any{
		"account":       acc,
		"active_orders": activeOrders,
		"order_history": historyCopy,
	})
}

// startMarketMakerBot creates continuous depth and fills around the market price
func (s *Server) startMarketMakerBot(symbol string) {
	botID := "bot-market-maker"
	s.ledger.RegisterUser(botID, "Liquidity Provider", map[string]float64{
		"USDT": 10000000.0,
		"ETH":  5000.0,
	})

	basePrice := 3000.0

	// Initial liquidity seed
	for i := 1; i <= 10; i++ {
		bidPrice := basePrice - float64(i)*2.5
		askPrice := basePrice + float64(i)*2.5
		vol := 0.5 + float64(i)*0.4

		bid := orderbook.NewOrder(fmt.Sprintf("bot-bid-%d", i), botID, symbol, orderbook.SideBuy, orderbook.OrderTypeLimit, bidPrice, vol)
		ask := orderbook.NewOrder(fmt.Sprintf("bot-ask-%d", i), botID, symbol, orderbook.SideSell, orderbook.OrderTypeLimit, askPrice, vol)
		_, _ = s.engine.ProcessOrder(bid)
		_, _ = s.engine.ProcessOrder(ask)
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		delta := (rand.Float64() - 0.5) * 4.0
		newMid := basePrice + delta

		side := orderbook.SideBuy
		if rand.Float64() > 0.5 {
			side = orderbook.SideSell
		}

		price := newMid - 1.0
		if side == orderbook.SideSell {
			price = newMid + 1.0
		}

		botOrd := orderbook.NewOrder(fmt.Sprintf("bot-pulse-%d", time.Now().UnixNano()%100000), botID, symbol, side, orderbook.OrderTypeLimit, price, 0.2+rand.Float64()*0.6)
		trades, err := s.engine.ProcessOrder(botOrd)
		if err == nil {
			for _, trd := range trades {
				_ = s.ledger.SettleTrade(trd.BuyerID, trd.SellerID, "ETH", "USDT", trd.Amount, trd.Price, true)
				s.hub.BroadcastJSON("TRADE_TICKER", trd)
			}
			s.broadcastOrderBookUpdate()
			s.broadcastAccountUpdate()
		}
	}
}

func (s *Server) handleTerminalUI(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	html := `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>ApexTrade | Ultra-Fast Crypto DEX & Terminal</title>
    <!-- TradingView Lightweight Charts CDN -->
    <script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>
    <style>
        :root {
            --bg: #0b0e14;
            --surface: #121721;
            --surface-card: #181f2c;
            --border: #232d3f;
            --text: #e6edf3;
            --text-dim: #7d8590;
            --green: #0ecb81;
            --green-dim: rgba(14, 203, 129, 0.15);
            --red: #f6465d;
            --red-dim: rgba(246, 70, 93, 0.15);
            --amber: #fcd535;
            --amber-dim: rgba(252, 213, 53, 0.15);
            --touch-target: 44px;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace;
            background-color: var(--bg);
            color: var(--text);
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            overflow-x: hidden;
        }
        /* Top Navigation Bar */
        .navbar {
            height: 56px;
            background: var(--surface);
            border-bottom: 1px solid var(--border);
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 0 16px;
            flex-shrink: 0;
            z-index: 10;
        }
        .nav-brand {
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 17px;
            font-weight: 800;
            color: #fff;
            white-space: nowrap;
        }
        .nav-brand span { color: var(--amber); }
        .nav-stats {
            display: flex;
            align-items: center;
            gap: 16px;
            font-size: 13px;
        }
        .pair-badge {
            font-size: 16px;
            font-weight: 700;
            color: var(--green);
            display: flex;
            align-items: center;
            gap: 4px;
        }
        
        /* WebSocket Resilient Indicator */
        .ws-badge {
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 11px;
            font-weight: 700;
            display: inline-flex;
            align-items: center;
            gap: 6px;
            min-height: 32px;
            cursor: pointer;
            transition: 0.2s;
        }
        .ws-badge.connected { background: var(--green-dim); color: var(--green); }
        .ws-badge.reconnecting { background: var(--amber-dim); color: var(--amber); }
        .ws-badge.disconnected { background: var(--red-dim); color: var(--red); }
        .ws-dot { width: 8px; height: 8px; border-radius: 50%; }
        .ws-dot.connected { background: var(--green); box-shadow: 0 0 6px var(--green); }
        .ws-dot.reconnecting { background: var(--amber); animation: pulse 1s infinite; }
        .ws-dot.disconnected { background: var(--red); }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }

        /* Skeleton Shimmer Loading */
        .skeleton {
            background: linear-gradient(90deg, #181f2c 25%, #232d3f 50%, #181f2c 75%);
            background-size: 200% 100%;
            animation: shimmer 1.5s infinite;
            border-radius: 4px;
        }
        @keyframes shimmer { 0% { background-position: 200% 0; } 100% { background-position: -200% 0; } }

        /* Workspace Grid Layout - Desktop (>= 1024px) */
        .main-workspace {
            display: grid;
            grid-template-columns: 310px 1fr 340px;
            flex: 1;
            height: calc(100vh - 56px);
            overflow: hidden;
        }
        .panel {
            background: var(--surface);
            border-right: 1px solid var(--border);
            display: flex;
            flex-direction: column;
            overflow: hidden;
            position: relative;
        }
        .panel-header {
            padding: 10px 14px;
            border-bottom: 1px solid var(--border);
            font-size: 11px;
            font-weight: 700;
            color: var(--text-dim);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: var(--surface);
            flex-shrink: 0;
        }

        /* Order Book Table */
        .orderbook-table {
            flex: 1;
            overflow-y: auto;
            font-size: 12px;
        }
        .ob-header-grid {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr;
            padding: 6px 14px;
            font-size: 11px;
            color: var(--text-dim);
            border-bottom: 1px solid var(--border);
            background: var(--surface-card);
        }
        .ob-row {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr;
            padding: 0 14px;
            min-height: 28px;
            align-items: center;
            position: relative;
            cursor: pointer;
            user-select: none;
        }
        .ob-row:hover { background: rgba(255,255,255,0.06); }
        .ob-bar {
            position: absolute;
            top: 0; bottom: 0; right: 0;
            pointer-events: none;
            opacity: 0.22;
        }
        .ob-bar.ask { background: var(--red); }
        .ob-bar.bid { background: var(--green); }
        .mid-price-bar {
            padding: 8px 14px;
            background: var(--surface-card);
            font-size: 14px;
            font-weight: 800;
            color: var(--text);
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-top: 1px solid var(--border);
            border-bottom: 1px solid var(--border);
            flex-shrink: 0;
        }

        /* Center Column Layout */
        .center-column {
            display: flex;
            flex-direction: column;
            overflow-y: auto;
            background: var(--bg);
        }
        .chart-container {
            height: 280px;
            width: 100%;
            background: var(--surface);
            border-bottom: 1px solid var(--border);
            position: relative;
            flex-shrink: 0;
        }
        .chart-header {
            position: absolute;
            top: 10px;
            left: 14px;
            z-index: 5;
            font-size: 12px;
            font-weight: 700;
            color: var(--text-dim);
            pointer-events: none;
        }

        /* Order Placement Form */
        .order-form-container {
            padding: 18px;
            max-width: 520px;
            margin: 0 auto;
            width: 100%;
            display: flex;
            flex-direction: column;
            gap: 12px;
        }
        .tab-group {
            display: flex;
            background: var(--surface-card);
            border-radius: 8px;
            padding: 3px;
            gap: 4px;
        }
        .tab-btn {
            flex: 1;
            min-height: var(--touch-target);
            border: none;
            background: transparent;
            color: var(--text-dim);
            font-weight: 700;
            font-size: 13px;
            border-radius: 6px;
            cursor: pointer;
            transition: 0.2s;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 6px;
        }
        .tab-btn.active.buy { background: var(--green); color: #fff; }
        .tab-btn.active.sell { background: var(--red); color: #fff; }
        .tab-btn.active.type { background: var(--border); color: #fff; }

        .input-group {
            display: flex;
            flex-direction: column;
            gap: 6px;
        }
        .input-label { font-size: 12px; color: var(--text-dim); font-weight: 600; }
        .input-box {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 0 14px;
            height: var(--touch-target);
            color: #fff;
            font-size: 15px;
            outline: none;
            font-weight: 600;
        }
        .input-box:focus { border-color: var(--amber); }

        .pct-buttons {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 6px;
        }
        .pct-btn {
            height: 32px;
            background: var(--surface-card);
            border: 1px solid var(--border);
            border-radius: 6px;
            color: var(--text-dim);
            font-size: 11px;
            font-weight: 700;
            cursor: pointer;
        }
        .pct-btn:hover { color: #fff; border-color: var(--text-dim); }

        .btn-submit {
            height: var(--touch-target);
            border: none;
            border-radius: 8px;
            font-size: 15px;
            font-weight: 800;
            cursor: pointer;
            transition: 0.2s;
            color: #fff;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
            margin-top: 4px;
        }
        .btn-submit.buy { background: var(--green); }
        .btn-submit.buy:hover { background: #0ca86b; }
        .btn-submit.sell { background: var(--red); }
        .btn-submit.sell:hover { background: #d9384e; }

        /* User Orders Tabs & Table */
        .user-orders-section {
            background: var(--surface);
            border-top: 1px solid var(--border);
            flex: 1;
            display: flex;
            flex-direction: column;
            min-height: 180px;
        }
        .subtabs {
            display: flex;
            border-bottom: 1px solid var(--border);
            padding: 0 14px;
            gap: 16px;
            background: var(--surface-card);
        }
        .subtab-item {
            padding: 12px 4px;
            font-size: 12px;
            font-weight: 700;
            color: var(--text-dim);
            border-bottom: 2px solid transparent;
            cursor: pointer;
        }
        .subtab-item.active {
            color: #fff;
            border-bottom-color: var(--amber);
        }
        .orders-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 12px;
        }
        .orders-table th {
            padding: 8px 14px;
            color: var(--text-dim);
            text-align: left;
            font-weight: 600;
            background: var(--surface);
        }
        .orders-table td {
            padding: 8px 14px;
            border-bottom: 1px solid var(--border);
        }
        .cancel-btn {
            background: var(--red-dim);
            color: var(--red);
            border: 1px solid var(--red);
            padding: 4px 10px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: 700;
            cursor: pointer;
            min-height: 28px;
        }
        .cancel-btn:hover { background: var(--red); color: #fff; }

        /* Right Panel: Portfolio & Trades */
        .balance-card {
            background: var(--surface-card);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 14px;
            margin: 12px;
            font-size: 13px;
        }
        .faucet-btn {
            background: var(--amber);
            color: #000;
            border: none;
            height: var(--touch-target);
            border-radius: 6px;
            font-weight: 800;
            font-size: 13px;
            cursor: pointer;
            margin-top: 10px;
            width: 100%;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 6px;
        }
        .faucet-btn:hover { background: #e5bf2a; }

        .tape-row {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr;
            padding: 5px 14px;
            font-size: 12px;
            align-items: center;
                /* Mobile Viewport Breakpoint Switcher (< 768px) */
        .mobile-tab-bar {
            display: none;
            background: var(--surface);
            border-bottom: 1px solid var(--border);
            overflow-x: auto;
            -webkit-overflow-scrolling: touch;
            white-space: nowrap;
            padding: 6px 10px;
            gap: 6px;
            flex-shrink: 0;
            scrollbar-width: none;
        }
        .mobile-tab-bar::-webkit-scrollbar {
            display: none;
        }
        .mobile-tab-btn {
            flex-shrink: 0;
            white-space: nowrap;
            padding: 8px 14px;
            min-height: var(--touch-target);
            border-radius: 6px;
            border: 1px solid transparent;
            background: var(--surface-card);
            color: var(--text-dim);
            font-size: 13px;
            font-weight: 700;
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            transition: 0.15s ease;
        }
        .mobile-tab-btn.active {
            background: var(--border);
            color: var(--amber);
            border-color: var(--amber);
        }

        /* RESPONSIVE BREAKPOINTS */
        @media (max-width: 1023px) {
            .main-workspace {
                grid-template-columns: 280px 1fr;
            }
            .panel.right-panel {
                display: none;
            }
        }

        @media (max-width: 768px) {
            body { overflow-y: auto; }
            .navbar { padding: 0 12px; }
            .nav-stats { display: none; }
            .mobile-tab-bar { display: flex; }
            .main-workspace {
                display: flex;
                flex-direction: column;
                height: auto;
                overflow: visible;
            }
            .chart-container { height: 260px; }
            .panel {
                border-right: none;
                border-bottom: 1px solid var(--border);
            }
            .ob-row { min-height: var(--touch-target); }
            .mobile-hide { display: none !important; }
            .mobile-show { display: flex !important; }
        }
    </style>
</head>
<body>
    <!-- Top Bar -->
    <div class="navbar">
        <div class="nav-brand">
            ⚡ <span>ApexTrade</span> DEX
        </div>
        <div class="nav-stats">
            <span class="pair-badge">
                <span id="price-arrow">▲</span> ETH/USDT: <strong id="top-price">$3,000.00</strong>
            </span>
            <span style="color: var(--text-dim); font-size: 12px;">24h Vol: 1,420.50 ETH</span>
        </div>
        <div class="ws-badge connected" id="ws-status" onclick="connectWS()">
            <div class="ws-dot connected" id="ws-dot"></div>
            <span id="ws-text">● WS LIVE (<50µs)</span>
        </div>
    </div>

    <!-- Mobile Tab Switcher (Appears below 768px) -->
    <div class="mobile-tab-bar">
        <button class="mobile-tab-btn active" onclick="switchMobileTab('trade')">⚡ Trade</button>
        <button class="mobile-tab-btn" onclick="switchMobileTab('chart')">📈 Chart</button>
        <button class="mobile-tab-btn" onclick="switchMobileTab('orderbook')">📖 Book</button>
        <button class="mobile-tab-btn" onclick="switchMobileTab('orders')">📋 Orders</button>
        <button class="mobile-tab-btn" onclick="switchMobileTab('portfolio')">💼 Portfolio</button>
    </div>

    <!-- Main Workspace -->
    <div class="main-workspace">
        <!-- Left: Order Book Panel -->
        <div class="panel" id="panel-orderbook">
            <div class="panel-header">
                <span>📖 Live Order Book (L2)</span>
                <span style="font-size:10px; color:var(--green);">FIFO Priority</span>
            </div>
            <div class="ob-header-grid">
                <span>Price (USDT)</span>
                <span style="text-align:center;">Size (ETH)</span>
                <span style="text-align:right;">Total</span>
            </div>
            <div class="orderbook-table" id="asks-container" style="display:flex; flex-direction:column-reverse; justify-content:flex-end;">
                <!-- Skeleton Loader -->
                <div class="ob-row skeleton" style="height:24px; margin:2px 0;"></div>
                <div class="ob-row skeleton" style="height:24px; margin:2px 0;"></div>
                <div class="ob-row skeleton" style="height:24px; margin:2px 0;"></div>
            </div>
            <div class="mid-price-bar">
                <span style="font-size:11px; color:var(--text-dim);">SPREAD / MID:</span>
                <span id="mid-price" style="color:var(--amber);">▲ $3,000.00</span>
            </div>
            <div class="orderbook-table" id="bids-container">
                <!-- Skeleton Loader -->
                <div class="ob-row skeleton" style="height:24px; margin:2px 0;"></div>
                <div class="ob-row skeleton" style="height:24px; margin:2px 0;"></div>
                <div class="ob-row skeleton" style="height:24px; margin:2px 0;"></div>
            </div>
        </div>

        <!-- Center: Interactive Candlestick Chart & Order Execution Panel -->
        <div class="center-column" id="panel-center">
            <!-- Candlestick Chart Section -->
            <div class="chart-container" id="panel-chart">
                <div class="chart-header">ETH/USDT • 1m Candlestick (Real-Time)</div>
                <div id="tv-chart" style="width:100%; height:100%;"></div>
            </div>

            <!-- Order Placement Form -->
            <div class="order-form-container" id="panel-trade">
                <div class="tab-group">
                    <button class="tab-btn active buy" id="tab-buy" onclick="setSide('BUY')">▲ BUY ETH (+)</button>
                    <button class="tab-btn" id="tab-sell" onclick="setSide('SELL')">▼ SELL ETH (-)</button>
                </div>
                <div class="tab-group">
                    <button class="tab-btn active type" id="tab-limit" onclick="setType('LIMIT')">Limit Order</button>
                    <button class="tab-btn" id="tab-market" onclick="setType('MARKET')">Market Order</button>
                </div>

                <div class="input-group" id="price-group">
                    <label class="input-label">Limit Price (USDT)</label>
                    <input class="input-box" type="number" id="input-price" value="3000.00" step="0.5">
                </div>

                <div class="input-group">
                    <label class="input-label">Order Amount (ETH)</label>
                    <input class="input-box" type="number" id="input-amount" value="1.0" step="0.1">
                </div>

                <div class="pct-buttons">
                    <button class="pct-btn" onclick="setAmountPct(0.25)">25%</button>
                    <button class="pct-btn" onclick="setAmountPct(0.50)">50%</button>
                    <button class="pct-btn" onclick="setAmountPct(0.75)">75%</button>
                    <button class="pct-btn" onclick="setAmountPct(1.00)">100%</button>
                </div>

                <button class="btn-submit buy" id="btn-submit-order" onclick="submitOrder()">▲ Place Buy Order (+)</button>
            </div>

            <!-- User Open Orders & Order History Subtabs -->
            <div class="user-orders-section" id="panel-orders">
                <div class="subtabs">
                    <div class="subtab-item active" id="subtab-open" onclick="switchOrdersTab('open')">Open Orders (<span id="open-count">0</span>)</div>
                    <div class="subtab-item" id="subtab-history" onclick="switchOrdersTab('history')">Order History (<span id="history-count">0</span>)</div>
                </div>
                <div style="flex:1; overflow-x:auto;">
                    <div id="orders-content" style="padding: 10px 14px;">
                        <span style="color:var(--text-dim); font-size:12px;">No resting limit orders.</span>
                    </div>
                </div>
            </div>
        </div>

        <!-- Right: Portfolio Balance & Recent Trades Tape -->
        <div class="panel right-panel" id="panel-portfolio">
            <div class="balance-card">
                <div style="color:var(--text-dim); font-size:11px; margin-bottom:6px; font-weight:700;">💼 DEMO TRADER PORTFOLIO</div>
                <div style="display:flex; justify-content:space-between; margin-bottom:6px;">
                    <span style="color:var(--text-dim);">USDT Available:</span>
                    <strong style="color:var(--green);" id="bal-usdt">$10,000.00</strong>
                </div>
                <div style="display:flex; justify-content:space-between; margin-bottom:6px;">
                    <span style="color:var(--text-dim);">USDT In Escrow:</span>
                    <strong style="color:var(--amber);" id="bal-usdt-locked">$0.00</strong>
                </div>
                <div style="display:flex; justify-content:space-between;">
                    <span style="color:var(--text-dim);">ETH Available:</span>
                    <strong style="color:#fff;" id="bal-eth">5.00 ETH</strong>
                </div>
                <button class="faucet-btn" onclick="claimFaucet()">+ Claim Free $5,000 Faucet</button>
            </div>

            <div class="panel-header">
                <span>⚡ Live Trade Tape</span>
                <span style="font-size:10px;">Time</span>
            </div>
            <div class="ob-header-grid">
                <span>Price (USDT)</span>
                <span style="text-align:center;">Size (ETH)</span>
                <span style="text-align:right;">Time</span>
            </div>
            <div class="orderbook-table" id="trade-tape"></div>
        </div>
    </div>

    <script>
        let currentSide = "BUY";
        let currentType = "LIMIT";
        let currentOrdersTab = "open";
        let latestAccountData = { account: null, active_orders: [], order_history: [] };
        let candleData = [];
        let wsRetryCount = 0;
        let ws = null;

        // Interactive High-Performance HTML5 Canvas Candlestick Chart
        function drawCanvasChart() {
            const container = document.getElementById('tv-chart');
            if (!container) return;

            let canvas = document.getElementById('chart-canvas');
            if (!canvas) {
                container.innerHTML = '<canvas id="chart-canvas" style="display:block; width:100%; height:100%; cursor:crosshair;"></canvas>';
                canvas = document.getElementById('chart-canvas');
            }

            const rect = container.getBoundingClientRect();
            const width = rect.width || container.clientWidth || 375;
            const height = rect.height || container.clientHeight || 260;

            const dpr = window.devicePixelRatio || 1;
            canvas.width = width * dpr;
            canvas.height = height * dpr;
            canvas.style.width = width + 'px';
            canvas.style.height = height + 'px';

            const ctx = canvas.getContext('2d');
            ctx.scale(dpr, dpr);

            // Background
            ctx.fillStyle = '#121721';
            ctx.fillRect(0, 0, width, height);

            if (!candleData || candleData.length === 0) {
                ctx.fillStyle = '#7d8590';
                ctx.font = '12px monospace';
                ctx.textAlign = 'center';
                ctx.fillText('Loading candlestick data...', width / 2, height / 2);
                return;
            }

            // Price boundaries
            let minPrice = Infinity;
            let maxPrice = -Infinity;
            const visibleCandles = candleData.slice(-35); // Show last 35 candles

            visibleCandles.forEach(c => {
                if (c.low < minPrice) minPrice = c.low;
                if (c.high > maxPrice) maxPrice = c.high;
            });

            const padding = (maxPrice - minPrice) * 0.1 || 2.0;
            minPrice -= padding;
            maxPrice += padding;
            const priceRange = maxPrice - minPrice;

            const chartRightMargin = 55;
            const chartBottomMargin = 24;
            const plotWidth = width - chartRightMargin;
            const plotHeight = height - chartBottomMargin;

            // Horizontal Grid Lines & Price Labels
            ctx.strokeStyle = '#181f2c';
            ctx.lineWidth = 1;
            ctx.fillStyle = '#7d8590';
            ctx.font = '10px monospace';
            ctx.textAlign = 'left';

            const gridSteps = 4;
            for (let i = 0; i <= gridSteps; i++) {
                const y = plotHeight * (i / gridSteps);
                const price = maxPrice - (priceRange * (i / gridSteps));

                ctx.beginPath();
                ctx.moveTo(0, y);
                ctx.lineTo(plotWidth, y);
                ctx.stroke();

                ctx.fillText('$' + price.toFixed(1), plotWidth + 6, y + 3);
            }

            // Candlesticks Drawing
            const numCandles = visibleCandles.length;
            const candleWidth = Math.max(3, (plotWidth / numCandles) * 0.65);
            const slotWidth = plotWidth / numCandles;

            visibleCandles.forEach((c, i) => {
                const x = (i * slotWidth) + (slotWidth / 2);
                const isGreen = c.close >= c.open;
                const color = isGreen ? '#0ecb81' : '#f6465d';

                const yHigh = plotHeight - ((c.high - minPrice) / priceRange) * plotHeight;
                const yLow = plotHeight - ((c.low - minPrice) / priceRange) * plotHeight;
                const yOpen = plotHeight - ((c.open - minPrice) / priceRange) * plotHeight;
                const yClose = plotHeight - ((c.close - minPrice) / priceRange) * plotHeight;

                const bodyTop = Math.min(yOpen, yClose);
                const bodyHeight = Math.max(2, Math.abs(yClose - yOpen));

                // Wick Line
                ctx.strokeStyle = color;
                ctx.lineWidth = 1.2;
                ctx.beginPath();
                ctx.moveTo(x, yHigh);
                ctx.lineTo(x, yLow);
                ctx.stroke();

                // Candle Body
                ctx.fillStyle = color;
                ctx.fillRect(x - candleWidth / 2, bodyTop, candleWidth, bodyHeight);
            });

            // Current Price Highlight Line
            if (visibleCandles.length > 0) {
                const lastCandle = visibleCandles[visibleCandles.length - 1];
                const lastY = plotHeight - ((lastCandle.close - minPrice) / priceRange) * plotHeight;
                const isGreen = lastCandle.close >= lastCandle.open;
                const priceColor = isGreen ? '#0ecb81' : '#f6465d';

                ctx.strokeStyle = priceColor;
                ctx.setLineDash([3, 3]);
                ctx.beginPath();
                ctx.moveTo(0, lastY);
                ctx.lineTo(plotWidth, lastY);
                ctx.stroke();
                ctx.setLineDash([]);

                // Price Tag on Axis
                ctx.fillStyle = priceColor;
                ctx.fillRect(plotWidth + 2, lastY - 9, 50, 18);
                ctx.fillStyle = '#000';
                ctx.font = 'bold 10px monospace';
                ctx.fillText(lastCandle.close.toFixed(1), plotWidth + 6, lastY + 3);
            }
        }

        function loadCandles() {
            fetch('/api/v1/candles')
                .then(r => r.json())
                .then(data => {
                    if (data && data.length) {
                        candleData = data;
                        drawCanvasChart();
                    }
                })
                .catch(err => console.error("Candle fetch error:", err));
        }

        function setSide(side) {
            currentSide = side;
            document.getElementById("tab-buy").className = "tab-btn " + (side === "BUY" ? "active buy" : "");
            document.getElementById("tab-sell").className = "tab-btn " + (side === "SELL" ? "active sell" : "");
            const submitBtn = document.getElementById("btn-submit-order");
            submitBtn.className = "btn-submit " + (side === "BUY" ? "buy" : "sell");
            const prefix = side === "BUY" ? "▲ Place Buy Order (+)" : "▼ Place Sell Order (-)";
            submitBtn.innerText = prefix;
        }

        function setType(type) {
            currentType = type;
            document.getElementById("tab-limit").className = "tab-btn " + (type === "LIMIT" ? "active type" : "");
            document.getElementById("tab-market").className = "tab-btn " + (type === "MARKET" ? "active type" : "");
            document.getElementById("price-group").style.display = type === "MARKET" ? "none" : "flex";
        }

        function setAmountPct(pct) {
            if (!latestAccountData.account || !latestAccountData.account.balances) return;
            const price = parseFloat(document.getElementById("input-price").value) || 3000.0;
            if (currentSide === "BUY") {
                const usdt = latestAccountData.account.balances.USDT ? latestAccountData.account.balances.USDT.available : 0;
                const maxEth = (usdt * pct) / price;
                document.getElementById("input-amount").value = maxEth.toFixed(2);
            } else {
                const eth = latestAccountData.account.balances.ETH ? latestAccountData.account.balances.ETH.available : 0;
                document.getElementById("input-amount").value = (eth * pct).toFixed(2);
            }
        }

        function renderOrderBook(data) {
            const asks = data.asks || [];
            const bids = data.bids || [];

            let maxVol = 1.0;
            asks.forEach(a => maxVol = Math.max(maxVol, a.volume));
            bids.forEach(b => maxVol = Math.max(maxVol, b.volume));

            let asksHtml = asks.map(a => {
                const width = Math.min(100, (a.volume / maxVol) * 100);
                return '<div class="ob-row" onclick="fillPrice(' + a.price + ')" role="button" aria-label="Ask price ' + a.price + '">' +
                    '<div class="ob-bar ask" style="width:' + width + '%;"></div>' +
                    '<span style="color:var(--red); font-weight:700;">▼ ' + a.price.toFixed(2) + '</span>' +
                    '<span style="text-align:center;">' + a.volume.toFixed(2) + '</span>' +
                    '<span style="text-align:right;">' + (a.price * a.volume).toFixed(0) + '</span>' +
                '</div>';
            }).join('');
            document.getElementById("asks-container").innerHTML = asksHtml || '<div style="padding:10px; color:var(--text-dim); text-align:center;">No asks</div>';

            let bidsHtml = bids.map(b => {
                const width = Math.min(100, (b.volume / maxVol) * 100);
                return '<div class="ob-row" onclick="fillPrice(' + b.price + ')" role="button" aria-label="Bid price ' + b.price + '">' +
                    '<div class="ob-bar bid" style="width:' + width + '%;"></div>' +
                    '<span style="color:var(--green); font-weight:700;">▲ ' + b.price.toFixed(2) + '</span>' +
                    '<span style="text-align:center;">' + b.volume.toFixed(2) + '</span>' +
                    '<span style="text-align:right;">' + (b.price * b.volume).toFixed(0) + '</span>' +
                '</div>';
            }).join('');
            document.getElementById("bids-container").innerHTML = bidsHtml || '<div style="padding:10px; color:var(--text-dim); text-align:center;">No bids</div>';

            if (bids.length > 0 && asks.length > 0) {
                const mid = ((bids[0].price + asks[0].price) / 2).toFixed(2);
                document.getElementById("mid-price").innerHTML = '▲ $' + mid;
                document.getElementById("top-price").innerText = '$' + mid;
            }
        }

        function fillPrice(price) {
            document.getElementById("input-price").value = price.toFixed(2);
        }

        function renderTrade(trd) {
            const tape = document.getElementById("trade-tape");
            const isBuy = trd.buyer_id === "demo-user" || Math.random() > 0.5;
            const prefix = isBuy ? "▲ +" : "▼ -";
            const color = isBuy ? "var(--green)" : "var(--red)";
            const timeStr = new Date().toLocaleTimeString();

            const row = document.createElement("div");
            row.className = "tape-row";
            row.innerHTML = 
                '<span style="color:' + color + '; font-weight:700;">' + prefix + trd.price.toFixed(2) + '</span>' +
                '<span style="text-align:center;">' + trd.amount.toFixed(2) + '</span>' +
                '<span style="color:var(--text-dim); text-align:right;">' + timeStr + '</span>';
            tape.insertBefore(row, tape.firstChild);
            if (tape.children.length > 30) tape.removeChild(tape.lastChild);

            // Update live candle tick
            if (candleData && candleData.length > 0) {
                const nowSec = Math.floor(Date.now() / 1000);
                const currentMinute = nowSec - (nowSec % 60);
                const last = candleData[candleData.length - 1];

                if (last && last.time === currentMinute) {
                    last.high = Math.max(last.high, trd.price);
                    last.low = Math.min(last.low, trd.price);
                    last.close = trd.price;
                } else {
                    candleData.push({
                        time: currentMinute,
                        open: trd.price,
                        high: trd.price,
                        low: trd.price,
                        close: trd.price
                    });
                }
                drawCanvasChart();
            }
        }

        function renderAccount(data) {
            latestAccountData = data;
            if (data.account && data.account.balances) {
                const usdtAvail = data.account.balances.USDT ? data.account.balances.USDT.available : 0;
                const usdtLocked = data.account.balances.USDT ? data.account.balances.USDT.locked : 0;
                const ethAvail = data.account.balances.ETH ? data.account.balances.ETH.available : 0;
                document.getElementById("bal-usdt").innerText = "$" + usdtAvail.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
                document.getElementById("bal-usdt-locked").innerText = "$" + usdtLocked.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
                document.getElementById("bal-eth").innerText = ethAvail.toFixed(2) + " ETH";
            }

            const activeOrders = data.active_orders || [];
            const history = data.order_history || [];
            document.getElementById("open-count").innerText = activeOrders.length;
            document.getElementById("history-count").innerText = history.length;

            renderOrdersTable();
        }

        function switchOrdersTab(tab) {
            currentOrdersTab = tab;
            document.getElementById("subtab-open").className = "subtab-item " + (tab === "open" ? "active" : "");
            document.getElementById("subtab-history").className = "subtab-item " + (tab === "history" ? "active" : "");
            renderOrdersTable();
        }

        function renderOrdersTable() {
            const container = document.getElementById("orders-content");
            if (currentOrdersTab === "open") {
                const orders = latestAccountData.active_orders || [];
                if (orders.length === 0) {
                    container.innerHTML = '<span style="color:var(--text-dim); font-size:12px;">No active resting limit orders.</span>';
                    return;
                }
                let html = '<table class="orders-table">' +
                    '<tr><th>Side</th><th>Price</th><th>Remaining</th><th>Time</th><th>Action</th></tr>';
                orders.forEach(o => {
                    const isBuy = o.side === "BUY";
                    const color = isBuy ? "var(--green)" : "var(--red)";
                    const prefix = isBuy ? "▲ BUY" : "▼ SELL";
                    html += '<tr>' +
                        '<td style="color:' + color + '; font-weight:700;">' + prefix + '</td>' +
                        '<td>$' + o.price.toFixed(2) + '</td>' +
                        '<td>' + (o.amount - o.filled).toFixed(2) + ' ETH</td>' +
                        '<td style="color:var(--text-dim);">' + new Date().toLocaleTimeString() + '</td>' +
                        '<td><button class="cancel-btn" onclick="cancelOrder(\'' + o.id + '\')">✕ Cancel</button></td>' +
                    '</tr>';
                });
                html += '</table>';
                container.innerHTML = html;
            } else {
                const history = latestAccountData.order_history || [];
                if (history.length === 0) {
                    container.innerHTML = '<span style="color:var(--text-dim); font-size:12px;">No past order history.</span>';
                    return;
                }
                let html = '<table class="orders-table">' +
                    '<tr><th>Side</th><th>Price</th><th>Filled Amount</th><th>Status</th><th>Completed</th></tr>';
                history.forEach(h => {
                    const isBuy = h.side === "BUY";
                    const color = isBuy ? "var(--green)" : "var(--red)";
                    const prefix = isBuy ? "▲ BUY" : "▼ SELL";
                    const statusColor = h.status === "FILLED" ? "var(--green)" : "var(--red)";
                    html += '<tr>' +
                        '<td style="color:' + color + '; font-weight:700;">' + prefix + '</td>' +
                        '<td>$' + h.price.toFixed(2) + '</td>' +
                        '<td>' + h.filled.toFixed(2) + ' / ' + h.amount.toFixed(2) + ' ETH</td>' +
                        '<td style="color:' + statusColor + '; font-weight:700;">' + h.status + '</td>' +
                        '<td style="color:var(--text-dim);">' + new Date(h.completed_at).toLocaleTimeString() + '</td>' +
                    '</tr>';
                });
                html += '</table>';
                container.innerHTML = html;
            }
        }

        // Mobile Viewport Switcher (< 768px)
        function switchMobileTab(tab) {
            const btns = document.querySelectorAll('.mobile-tab-btn');
            btns.forEach(b => b.classList.remove('active'));
            if (event && event.target) {
                event.target.classList.add('active');
            }

            const pBook = document.getElementById('panel-orderbook');
            const pChart = document.getElementById('panel-chart');
            const pTrade = document.getElementById('panel-trade');
            const pOrders = document.getElementById('panel-orders');
            const pPort = document.getElementById('panel-portfolio');

            // Hide all by default on mobile
            [pBook, pChart, pTrade, pOrders, pPort].forEach(p => {
                if (p) p.style.display = 'none';
            });

            if (tab === 'trade') {
                pTrade.style.display = 'flex';
                pOrders.style.display = 'flex';
            } else if (tab === 'chart') {
                pChart.style.display = 'block';
                setTimeout(drawCanvasChart, 50);
            } else if (tab === 'orderbook') {
                pBook.style.display = 'flex';
            } else if (tab === 'orders') {
                pOrders.style.display = 'flex';
            } else if (tab === 'portfolio') {
                pPort.style.display = 'flex';
                pPort.classList.remove('right-panel');
            }
        }

        // WebSocket State Handling & Auto-Reconnect
        function updateWSStatus(state, text) {
            const badge = document.getElementById("ws-status");
            const dot = document.getElementById("ws-dot");
            const label = document.getElementById("ws-text");

            badge.className = "ws-badge " + state;
            dot.className = "ws-dot " + state;
            label.innerText = text;
        }

        function connectWS() {
            updateWSStatus("reconnecting", "◌ CONNECTING...");
            const protocol = location.protocol === "https:" ? "wss:" : "ws:";
            ws = new WebSocket(protocol + "//" + location.host + "/ws");

            ws.onopen = () => {
                wsRetryCount = 0;
                updateWSStatus("connected", "● WS LIVE (<50µs)");
                fetch("/api/v1/orderbook").then(r => r.json()).then(renderOrderBook);
                fetch("/api/v1/account").then(r => r.json()).then(renderAccount);
            };

            ws.onmessage = (event) => {
                const msg = JSON.parse(event.data);
                if (msg.type === "ORDERBOOK_L2") renderOrderBook(msg.payload);
                if (msg.type === "TRADE_TICKER") renderTrade(msg.payload);
                if (msg.type === "ACCOUNT_UPDATE") renderAccount(msg.payload);
            };

            ws.onerror = () => {
                updateWSStatus("disconnected", "✕ WS DISCONNECTED");
            };

            ws.onclose = () => {
                wsRetryCount++;
                const delay = Math.min(8000, 1000 * Math.pow(2, wsRetryCount));
                updateWSStatus("reconnecting", "◌ RETRYING IN " + (delay / 1000) + "s...");
                setTimeout(connectWS, delay);
            };
        }

        async function submitOrder() {
            const price = parseFloat(document.getElementById("input-price").value);
            const amount = parseFloat(document.getElementById("input-amount").value);

            try {
                const res = await fetch("/api/v1/orders", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({
                        side: currentSide,
                        type: currentType,
                        price: price,
                        amount: amount
                    })
                });
                if (!res.ok) {
                    const err = await res.text();
                    alert("Order Error: " + err);
                }
            } catch (err) {
                alert("Failed to send order: " + err);
            }
        }

        async function cancelOrder(orderID) {
            await fetch("/api/v1/orders/" + orderID, { method: "DELETE" });
        }

        async function claimFaucet() {
            await fetch("/api/v1/faucet", { method: "POST" });
        }

        // Initialize App
        window.addEventListener('DOMContentLoaded', () => {
            loadCandles();
            window.addEventListener('resize', drawCanvasChart);
            fetch("/api/v1/orderbook").then(r => r.json()).then(renderOrderBook);
            fetch("/api/v1/account").then(r => r.json()).then(renderAccount);
            fetch("/api/v1/trades").then(r => r.json()).then(trades => trades.forEach(renderTrade));
            connectWS();

            // Set initial mobile view if viewport is small
            if (window.innerWidth < 768) {
                switchMobileTab('trade');
            }
        });
    </script>
</body>
</html>`

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(html))
}
