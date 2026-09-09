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

	log.Printf("🚀 ApexTrade 3D Pro Matching Engine & Terminal running on http://localhost:%s", port)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "UP",
		"engine": "ApexTrade 3D High-Speed Matching Engine",
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
    <title>ApexTrade PRO | 3D High-Speed Crypto DEX & Order Matching Engine</title>
    <!-- Google Fonts for sleek crypto typography -->
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;600;700;800&family=JetBrains+Mono:wght@400;500;700;800&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg: #06080e;
            --bg-mesh: radial-gradient(circle at 50% 0%, rgba(0, 229, 255, 0.08) 0%, rgba(6, 8, 14, 0.95) 75%);
            --surface: rgba(14, 19, 30, 0.75);
            --surface-card: rgba(20, 28, 44, 0.65);
            --surface-hover: rgba(30, 42, 66, 0.7);
            --border: rgba(255, 255, 255, 0.08);
            --border-glow: rgba(0, 229, 255, 0.3);
            --text: #f0f6fc;
            --text-dim: #8b9bb4;
            --green: #00F29D;
            --green-glow: rgba(0, 242, 157, 0.4);
            --green-dim: rgba(0, 242, 157, 0.12);
            --red: #FF3B69;
            --red-glow: rgba(255, 59, 105, 0.4);
            --red-dim: rgba(255, 59, 105, 0.12);
            --gold: #FFD000;
            --gold-glow: rgba(255, 208, 0, 0.4);
            --cyan: #00E5FF;
            --cyan-glow: rgba(0, 229, 255, 0.4);
            --purple: #9D4EDD;
            --touch-target: 44px;
        }

        * { box-sizing: border-box; margin: 0; padding: 0; -webkit-tap-highlight-color: transparent; }
        
        body {
            font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, sans-serif;
            background: var(--bg);
            background-image: var(--bg-mesh);
            background-attachment: fixed;
            color: var(--text);
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            overflow-x: hidden;
            font-feature-settings: "cv02", "cv03", "cv04", "cv11";
        }

        /* Top Navigation Header */
        .navbar {
            height: 62px;
            background: rgba(10, 14, 23, 0.85);
            backdrop-filter: blur(20px);
            -webkit-backdrop-filter: blur(20px);
            border-bottom: 1px solid var(--border);
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 0 20px;
            flex-shrink: 0;
            z-index: 50;
            position: sticky;
            top: 0;
        }

        /* 3D Dynamic Logo Brand */
        .nav-brand {
            display: flex;
            align-items: center;
            gap: 12px;
            cursor: pointer;
            text-decoration: none;
        }
        
        .logo-icon {
            width: 36px;
            height: 36px;
            position: relative;
            transform-style: preserve-3d;
            animation: floatLogo 4s ease-in-out infinite;
        }
        @keyframes floatLogo {
            0%, 100% { transform: translateY(0px) rotateY(0deg); }
            50% { transform: translateY(-3px) rotateY(15deg); }
        }

        .logo-text {
            display: flex;
            flex-direction: column;
            line-height: 1.1;
        }
        .logo-title {
            font-size: 19px;
            font-weight: 800;
            letter-spacing: -0.5px;
            background: linear-gradient(135deg, #ffffff 30%, var(--cyan) 70%, var(--gold) 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        .logo-badge {
            font-size: 9px;
            font-weight: 800;
            letter-spacing: 1.5px;
            color: var(--gold);
            text-transform: uppercase;
        }

        .nav-stats-bar {
            display: flex;
            align-items: center;
            gap: 24px;
            font-size: 13px;
        }
        .pair-selector {
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 6px 14px;
            background: var(--surface-card);
            border: 1px solid var(--border);
            border-radius: 10px;
            cursor: pointer;
            transition: all 0.2s ease;
        }
        .pair-selector:hover {
            border-color: var(--cyan);
            box-shadow: 0 0 15px rgba(0, 229, 255, 0.15);
        }
        .pair-name { font-weight: 800; color: #fff; font-size: 14px; }
        .pair-price {
            font-family: 'JetBrains Mono', monospace;
            font-size: 16px;
            font-weight: 800;
            color: var(--green);
            text-shadow: 0 0 12px var(--green-glow);
        }

        /* High-Tech WebSocket Pill */
        .ws-badge {
            padding: 6px 14px;
            border-radius: 30px;
            font-size: 11px;
            font-weight: 700;
            display: inline-flex;
            align-items: center;
            gap: 8px;
            background: rgba(0, 242, 157, 0.08);
            border: 1px solid rgba(0, 242, 157, 0.25);
            color: var(--green);
            box-shadow: 0 0 15px rgba(0, 242, 157, 0.15);
            cursor: pointer;
            transition: 0.3s;
        }
        .ws-badge.reconnecting {
            background: rgba(255, 208, 0, 0.08);
            border-color: rgba(255, 208, 0, 0.3);
            color: var(--gold);
            box-shadow: 0 0 15px rgba(255, 208, 0, 0.15);
        }
        .ws-badge.disconnected {
            background: rgba(255, 59, 105, 0.08);
            border-color: rgba(255, 59, 105, 0.3);
            color: var(--red);
            box-shadow: 0 0 15px rgba(255, 59, 105, 0.15);
        }
        .ws-pulse {
            width: 7px;
            height: 7px;
            border-radius: 50%;
            background: currentColor;
            box-shadow: 0 0 8px currentColor;
            animation: pulseGlow 1.5s infinite;
        }
        @keyframes pulseGlow { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.4; transform: scale(1.3); } }

        /* Main 3-Column Glassmorphism Workspace */
        .main-workspace {
            display: grid;
            grid-template-columns: 320px 1fr 340px;
            flex: 1;
            height: calc(100vh - 62px);
            overflow: hidden;
            gap: 1px;
            background: var(--border);
        }

        .panel {
            background: var(--surface);
            backdrop-filter: blur(16px);
            -webkit-backdrop-filter: blur(16px);
            display: flex;
            flex-direction: column;
            overflow: hidden;
            position: relative;
        }

        .panel-header {
            padding: 12px 16px;
            border-bottom: 1px solid var(--border);
            font-size: 11px;
            font-weight: 800;
            color: var(--text-dim);
            text-transform: uppercase;
            letter-spacing: 1px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: rgba(14, 19, 30, 0.9);
            flex-shrink: 0;
        }

        /* Order Book Table */
        .orderbook-table {
            flex: 1;
            overflow-y: auto;
            font-family: 'JetBrains Mono', monospace;
            font-size: 12px;
        }
        .ob-header-grid {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr;
            padding: 8px 16px;
            font-size: 10px;
            font-weight: 700;
            color: var(--text-dim);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            border-bottom: 1px solid var(--border);
            background: var(--surface-card);
        }
        .ob-row {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr;
            padding: 0 16px;
            min-height: 27px;
            align-items: center;
            position: relative;
            cursor: pointer;
            transition: background 0.15s;
        }
        .ob-row:hover { background: rgba(0, 229, 255, 0.08); }
        .ob-bar {
            position: absolute;
            top: 0; bottom: 0; right: 0;
            pointer-events: none;
            opacity: 0.18;
            transition: width 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        }
        .ob-bar.ask { background: linear-gradient(90deg, rgba(255,59,105,0) 0%, var(--red) 100%); }
        .ob-bar.bid { background: linear-gradient(90deg, rgba(0,242,157,0) 0%, var(--green) 100%); }

        .mid-price-bar {
            padding: 10px 16px;
            background: linear-gradient(90deg, rgba(20,28,44,0.95), rgba(30,42,66,0.95));
            font-family: 'JetBrains Mono', monospace;
            font-size: 15px;
            font-weight: 800;
            color: var(--text);
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-top: 1px solid var(--border);
            border-bottom: 1px solid var(--border);
            box-shadow: inset 0 0 20px rgba(0,0,0,0.4);
            flex-shrink: 0;
        }

        /* Center Column Layout: 3D Chart & Order Engine */
        .center-column {
            display: flex;
            flex-direction: column;
            overflow-y: auto;
            background: var(--bg);
        }

        /* 3D Futuristic Chart Stage */
        .chart-stage {
            height: 310px;
            width: 100%;
            background: radial-gradient(circle at 50% 30%, rgba(0, 229, 255, 0.06) 0%, rgba(10, 14, 23, 0.95) 80%);
            border-bottom: 1px solid var(--border);
            position: relative;
            flex-shrink: 0;
            overflow: hidden;
        }
        .chart-hud {
            position: absolute;
            top: 12px;
            left: 16px;
            right: 16px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            z-index: 10;
            pointer-events: none;
        }
        .chart-title-tag {
            font-size: 12px;
            font-weight: 800;
            color: #fff;
            display: flex;
            align-items: center;
            gap: 8px;
            background: rgba(14, 19, 30, 0.8);
            padding: 5px 12px;
            border-radius: 8px;
            border: 1px solid var(--border);
            backdrop-filter: blur(10px);
        }
        .chart-view-toggle {
            pointer-events: auto;
            display: flex;
            gap: 4px;
            background: rgba(14, 19, 30, 0.8);
            padding: 3px;
            border-radius: 8px;
            border: 1px solid var(--border);
        }
        .view-btn {
            background: transparent;
            border: none;
            color: var(--text-dim);
            padding: 4px 10px;
            border-radius: 6px;
            font-size: 11px;
            font-weight: 700;
            cursor: pointer;
            transition: all 0.2s;
        }
        .view-btn.active {
            background: var(--cyan-glow);
            color: var(--cyan);
            box-shadow: 0 0 10px rgba(0, 229, 255, 0.3);
        }

        /* Order Placement Panel */
        .order-form-container {
            padding: 20px;
            max-width: 560px;
            margin: 0 auto;
            width: 100%;
            display: flex;
            flex-direction: column;
            gap: 14px;
        }
        .trade-type-switcher {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 8px;
            background: var(--surface-card);
            border-radius: 12px;
            padding: 4px;
            border: 1px solid var(--border);
        }
        .trade-btn {
            min-height: var(--touch-target);
            border: none;
            background: transparent;
            color: var(--text-dim);
            font-weight: 800;
            font-size: 14px;
            border-radius: 10px;
            cursor: pointer;
            transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
        }
        .trade-btn.active.buy {
            background: linear-gradient(135deg, #00F29D 0%, #00B373 100%);
            color: #000;
            box-shadow: 0 0 20px var(--green-glow);
        }
        .trade-btn.active.sell {
            background: linear-gradient(135deg, #FF3B69 0%, #C91D45 100%);
            color: #fff;
            box-shadow: 0 0 20px var(--red-glow);
        }

        .order-mode-tabs {
            display: flex;
            gap: 8px;
        }
        .mode-tab {
            flex: 1;
            padding: 8px;
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 8px;
            color: var(--text-dim);
            font-size: 12px;
            font-weight: 700;
            cursor: pointer;
            transition: all 0.2s;
        }
        .mode-tab.active {
            border-color: var(--cyan);
            color: #fff;
            background: rgba(0, 229, 255, 0.08);
        }

        .input-group {
            display: flex;
            flex-direction: column;
            gap: 6px;
        }
        .input-label {
            font-size: 12px;
            font-weight: 700;
            color: var(--text-dim);
            display: flex;
            justify-content: space-between;
        }
        .input-box-wrapper {
            position: relative;
            display: flex;
            align-items: center;
        }
        .input-box {
            width: 100%;
            background: var(--surface-card);
            border: 1px solid var(--border);
            border-radius: 10px;
            padding: 0 50px 0 16px;
            height: 48px;
            color: #fff;
            font-family: 'JetBrains Mono', monospace;
            font-size: 16px;
            font-weight: 700;
            outline: none;
            transition: all 0.2s;
        }
        .input-box:focus {
            border-color: var(--cyan);
            box-shadow: 0 0 15px rgba(0, 229, 255, 0.2);
        }
        .input-suffix {
            position: absolute;
            right: 16px;
            font-size: 12px;
            font-weight: 800;
            color: var(--text-dim);
            pointer-events: none;
        }

        .pct-pills {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 8px;
        }
        .pct-pill {
            height: 32px;
            background: var(--surface-card);
            border: 1px solid var(--border);
            border-radius: 8px;
            color: var(--text-dim);
            font-size: 12px;
            font-weight: 700;
            cursor: pointer;
            transition: all 0.2s;
        }
        .pct-pill:hover {
            border-color: var(--cyan);
            color: #fff;
            background: rgba(0, 229, 255, 0.1);
        }

        .btn-submit-order {
            height: 52px;
            border: none;
            border-radius: 12px;
            font-size: 16px;
            font-weight: 800;
            cursor: pointer;
            transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
            color: #fff;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 10px;
            margin-top: 4px;
            letter-spacing: 0.5px;
        }
        .btn-submit-order.buy {
            background: linear-gradient(135deg, #00F29D 0%, #00B373 100%);
            color: #000;
            box-shadow: 0 4px 25px var(--green-glow);
        }
        .btn-submit-order.buy:hover { transform: translateY(-1px); box-shadow: 0 6px 30px rgba(0,242,157,0.6); }
        .btn-submit-order.sell {
            background: linear-gradient(135deg, #FF3B69 0%, #C91D45 100%);
            color: #fff;
            box-shadow: 0 4px 25px var(--red-glow);
        }
        .btn-submit-order.sell:hover { transform: translateY(-1px); box-shadow: 0 6px 30px rgba(255,59,105,0.6); }

        /* User Orders Section */
        .user-orders-section {
            background: var(--surface);
            border-top: 1px solid var(--border);
            flex: 1;
            display: flex;
            flex-direction: column;
            min-height: 200px;
        }
        .orders-tab-header {
            display: flex;
            border-bottom: 1px solid var(--border);
            padding: 0 16px;
            gap: 20px;
            background: rgba(14, 19, 30, 0.9);
        }
        .orders-tab-link {
            padding: 14px 4px;
            font-size: 12px;
            font-weight: 800;
            color: var(--text-dim);
            border-bottom: 2px solid transparent;
            cursor: pointer;
            transition: all 0.2s;
            display: flex;
            align-items: center;
            gap: 6px;
        }
        .orders-tab-link.active {
            color: #fff;
            border-bottom-color: var(--cyan);
        }
        .orders-count-badge {
            background: var(--surface-card);
            border: 1px solid var(--border);
            padding: 2px 7px;
            border-radius: 10px;
            font-size: 10px;
            font-weight: 800;
        }

        .orders-table {
            width: 100%;
            border-collapse: collapse;
            font-family: 'JetBrains Mono', monospace;
            font-size: 12px;
        }
        .orders-table th {
            padding: 10px 16px;
            color: var(--text-dim);
            text-align: left;
            font-weight: 600;
            background: rgba(10, 14, 23, 0.5);
            border-bottom: 1px solid var(--border);
        }
        .orders-table td {
            padding: 10px 16px;
            border-bottom: 1px solid var(--border);
        }
        .btn-cancel-order {
            background: var(--red-dim);
            color: var(--red);
            border: 1px solid rgba(255, 59, 105, 0.4);
            padding: 4px 10px;
            border-radius: 6px;
            font-size: 11px;
            font-weight: 800;
            cursor: pointer;
            transition: all 0.2s;
        }
        .btn-cancel-order:hover {
            background: var(--red);
            color: #fff;
            box-shadow: 0 0 10px var(--red-glow);
        }

        /* Right Panel: Portfolio & Live Tape */
        .balance-card-pro {
            background: linear-gradient(135deg, rgba(20, 28, 44, 0.8) 0%, rgba(30, 42, 66, 0.8) 100%);
            border: 1px solid rgba(0, 229, 255, 0.2);
            border-radius: 14px;
            padding: 18px;
            margin: 16px;
            box-shadow: 0 8px 30px rgba(0, 0, 0, 0.4);
            position: relative;
            overflow: hidden;
        }
        .balance-card-pro::before {
            content: '';
            position: absolute;
            top: -50%; left: -50%;
            width: 200%; height: 200%;
            background: radial-gradient(circle at 80% 20%, rgba(0, 229, 255, 0.1) 0%, transparent 60%);
            pointer-events: none;
        }
        .balance-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
            font-size: 13px;
        }
        .balance-val {
            font-family: 'JetBrains Mono', monospace;
            font-weight: 800;
        }
        .faucet-btn-glow {
            background: linear-gradient(135deg, #FFD000 0%, #E5A800 100%);
            color: #000;
            border: none;
            height: 42px;
            border-radius: 10px;
            font-weight: 800;
            font-size: 13px;
            cursor: pointer;
            margin-top: 12px;
            width: 100%;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
            box-shadow: 0 4px 15px var(--gold-glow);
            transition: all 0.2s;
        }
        .faucet-btn-glow:hover {
            transform: translateY(-1px);
            box-shadow: 0 6px 20px rgba(255, 208, 0, 0.6);
        }

        .tape-row {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr;
            padding: 6px 16px;
            font-family: 'JetBrains Mono', monospace;
            font-size: 12px;
            align-items: center;
            border-bottom: 1px solid rgba(255,255,255,0.02);
            animation: fadeInRow 0.3s ease;
        }
        @keyframes fadeInRow { from { opacity: 0; transform: translateY(-4px); } to { opacity: 1; transform: translateY(0); } }

        /* Mobile Segmented Navigation Bar (< 768px) */
        .mobile-nav-bar {
            display: none;
            background: rgba(10, 14, 23, 0.95);
            backdrop-filter: blur(20px);
            border-bottom: 1px solid var(--border);
            overflow-x: auto;
            -webkit-overflow-scrolling: touch;
            white-space: nowrap;
            padding: 6px 10px;
            gap: 8px;
            flex-shrink: 0;
            scrollbar-width: none;
            position: sticky;
            top: 62px;
            z-index: 40;
        }
        .mobile-nav-bar::-webkit-scrollbar { display: none; }
        .mobile-pill-btn {
            flex-shrink: 0;
            white-space: nowrap;
            padding: 8px 16px;
            min-height: var(--touch-target);
            border-radius: 10px;
            border: 1px solid var(--border);
            background: var(--surface-card);
            color: var(--text-dim);
            font-size: 13px;
            font-weight: 800;
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 6px;
            transition: all 0.2s;
        }
        .mobile-pill-btn.active {
            background: linear-gradient(135deg, rgba(0,229,255,0.2), rgba(0,229,255,0.05));
            color: var(--cyan);
            border-color: var(--cyan);
            box-shadow: 0 0 15px rgba(0, 229, 255, 0.25);
        }

        /* RESPONSIVE BREAKPOINTS */
        @media (max-width: 1023px) {
            .main-workspace { grid-template-columns: 290px 1fr; }
            .panel.right-panel { display: none; }
        }

        @media (max-width: 768px) {
            body { overflow-y: auto; }
            .navbar { padding: 0 14px; }
            .nav-stats-bar { display: none; }
            .mobile-nav-bar { display: flex; }
            .main-workspace {
                display: flex;
                flex-direction: column;
                height: auto;
                overflow: visible;
                gap: 0;
            }
            .chart-stage { height: 260px; }
            .panel { border-right: none; border-bottom: 1px solid var(--border); }
            .ob-row { min-height: var(--touch-target); }
        }
    </style>
</head>
<body>
    <!-- Top Futuristic Bar -->
    <header class="navbar">
        <div class="nav-brand">
            <!-- Custom High-Tech SVG 3D Logo -->
            <svg class="logo-icon" viewBox="0 0 100 100" fill="none" xmlns="http://www.w3.org/2000/svg">
                <defs>
                    <linearGradient id="apexGrad1" x1="0%" y1="0%" x2="100%" y2="100%">
                        <stop offset="0%" stop-color="#00E5FF" />
                        <stop offset="100%" stop-color="#9D4EDD" />
                    </linearGradient>
                    <linearGradient id="apexGrad2" x1="0%" y1="0%" x2="100%" y2="100%">
                        <stop offset="0%" stop-color="#FFD000" />
                        <stop offset="100%" stop-color="#00F29D" />
                    </linearGradient>
                    <filter id="glowFilter" x="-20%" y="-20%" width="140%" height="140%">
                        <feGaussianBlur stdDeviation="6" result="blur" />
                        <feComposite in="SourceGraphic" in2="blur" operator="over" />
                    </filter>
                </defs>
                <polygon points="50,12 88,78 50,65" fill="url(#apexGrad1)" opacity="0.9" />
                <polygon points="50,12 12,78 50,65" fill="url(#apexGrad2)" opacity="0.85" />
                <polygon points="50,65 88,78 50,92 12,78" fill="url(#apexGrad1)" opacity="0.7" />
                <circle cx="50" cy="12" r="5" fill="#FFFFFF" filter="url(#glowFilter)" />
            </svg>
            <div class="logo-text">
                <div class="logo-title">APEX<span>TRADE</span></div>
                <div class="logo-badge">ULTRA-FAST DEX</div>
            </div>
        </div>

        <div class="nav-stats-bar">
            <div class="pair-selector">
                <span style="color:var(--gold);">⚡</span>
                <span class="pair-name">ETH / USDT</span>
                <span class="pair-price" id="top-price">$3,000.00</span>
            </div>
            <div style="font-size:12px; color:var(--text-dim);">
                24h Vol: <strong style="color:#fff;">1,420.50 ETH</strong>
            </div>
        </div>

        <div class="ws-badge" id="ws-status" onclick="connectWS()">
            <div class="ws-pulse" id="ws-dot"></div>
            <span id="ws-text">WS LIVE ( < 50µs )</span>
        </div>
    </header>

    <!-- Mobile Segmented Navigation Bar (< 768px) -->
    <nav class="mobile-nav-bar">
        <button class="mobile-pill-btn active" onclick="switchMobileTab('trade')">⚡ Trade</button>
        <button class="mobile-pill-btn" onclick="switchMobileTab('chart')">📈 3D Chart</button>
        <button class="mobile-pill-btn" onclick="switchMobileTab('orderbook')">📖 Order Book</button>
        <button class="mobile-pill-btn" onclick="switchMobileTab('orders')">📋 Orders (<span id="mob-orders-count">0</span>)</button>
        <button class="mobile-pill-btn" onclick="switchMobileTab('portfolio')">💼 Portfolio & Tape</button>
    </nav>

    <!-- Main Workspace -->
    <main class="main-workspace">
        <!-- Left: Live Order Book Panel -->
        <section class="panel" id="panel-orderbook">
            <div class="panel-header">
                <span>📖 Live Order Book (L2)</span>
                <span style="color:var(--green); font-size:10px;">FIFO PRIORITY</span>
            </div>
            <div class="ob-header-grid">
                <span>Price (USDT)</span>
                <span style="text-align:center;">Size (ETH)</span>
                <span style="text-align:right;">Total (USDT)</span>
            </div>
            <div class="orderbook-table" id="asks-container" style="display:flex; flex-direction:column-reverse; justify-content:flex-end;"></div>
            <div class="mid-price-bar">
                <span style="font-size:11px; color:var(--text-dim); letter-spacing:0.5px;">SPREAD / MID</span>
                <span id="mid-price" style="color:var(--gold);">▲ $3,000.00</span>
            </div>
            <div class="orderbook-table" id="bids-container"></div>
        </section>

        <!-- Center: 3D Candlestick Chart & Order Execution -->
        <section class="center-column" id="panel-center">
            <!-- 3D Candlestick Chart Stage -->
            <div class="chart-stage" id="panel-chart">
                <div class="chart-hud">
                    <div class="chart-title-tag">
                        <span style="color:var(--cyan);">●</span> ETH/USDT • 1m Candlestick (Real-Time)
                    </div>
                    <div class="chart-view-toggle">
                        <button class="view-btn active" id="btn-view-3d" onclick="setChartView('3D')">3D Perspective</button>
                        <button class="view-btn" id="btn-view-pro" onclick="setChartView('PRO')">Pro View</button>
                    </div>
                </div>
                <div id="tv-chart" style="width:100%; height:100%; position:relative;"></div>
            </div>

            <!-- Trade Execution Panel -->
            <div class="order-form-container" id="panel-trade">
                <div class="trade-type-switcher">
                    <button class="trade-btn active buy" id="tab-buy" onclick="setSide('BUY')">▲ BUY ETH (+)</button>
                    <button class="trade-btn" id="tab-sell" onclick="setSide('SELL')">▼ SELL ETH (-)</button>
                </div>

                <div class="order-mode-tabs">
                    <button class="mode-tab active" id="tab-limit" onclick="setType('LIMIT')">Limit Order</button>
                    <button class="mode-tab" id="tab-market" onclick="setType('MARKET')">Market Order</button>
                </div>

                <div class="input-group" id="price-group">
                    <label class="input-label">
                        <span>Limit Price</span>
                        <span style="color:var(--cyan); cursor:pointer;" onclick="fillPrice(3000)">Best Ask</span>
                    </label>
                    <div class="input-box-wrapper">
                        <input class="input-box" type="number" id="input-price" value="3000.00" step="0.5">
                        <span class="input-suffix">USDT</span>
                    </div>
                </div>

                <div class="input-group">
                    <label class="input-label">
                        <span>Order Amount</span>
                        <span>Avail: <strong style="color:#fff;" id="avail-quote-display">10,000.00 USDT</strong></span>
                    </label>
                    <div class="input-box-wrapper">
                        <input class="input-box" type="number" id="input-amount" value="1.0" step="0.1">
                        <span class="input-suffix">ETH</span>
                    </div>
                </div>

                <div class="pct-pills">
                    <button class="pct-pill" onclick="setAmountPct(0.25)">25%</button>
                    <button class="pct-pill" onclick="setAmountPct(0.50)">50%</button>
                    <button class="pct-pill" onclick="setAmountPct(0.75)">75%</button>
                    <button class="pct-pill" onclick="setAmountPct(1.00)">100%</button>
                </div>

                <button class="btn-submit-order buy" id="btn-submit-order" onclick="submitOrder()">
                    ▲ Place Buy Order (+)
                </button>
            </div>

            <!-- Orders & History Subtabs -->
            <div class="user-orders-section" id="panel-orders">
                <div class="orders-tab-header">
                    <div class="orders-tab-link active" id="subtab-open" onclick="switchOrdersTab('open')">
                        Open Orders <span class="orders-count-badge" id="open-count">0</span>
                    </div>
                    <div class="orders-tab-link" id="subtab-history" onclick="switchOrdersTab('history')">
                        Order History <span class="orders-count-badge" id="history-count">0</span>
                    </div>
                </div>
                <div style="flex:1; overflow-x:auto;">
                    <div id="orders-content" style="padding: 12px 16px;">
                        <span style="color:var(--text-dim); font-size:12px;">No active resting limit orders.</span>
                    </div>
                </div>
            </div>
        </section>

        <!-- Right: Portfolio & Trade Tape -->
        <section class="panel right-panel" id="panel-portfolio">
            <div class="balance-card-pro">
                <div style="font-size:11px; font-weight:800; color:var(--cyan); letter-spacing:1px; margin-bottom:12px;">
                    💼 DEMO TRADER PORTFOLIO
                </div>
                <div class="balance-row">
                    <span style="color:var(--text-dim);">USDT Available:</span>
                    <span class="balance-val" style="color:var(--green); text-shadow:0 0 8px var(--green-glow);" id="bal-usdt">$10,000.00</span>
                </div>
                <div class="balance-row">
                    <span style="color:var(--text-dim);">USDT In Escrow:</span>
                    <span class="balance-val" style="color:var(--gold);" id="bal-usdt-locked">$0.00</span>
                </div>
                <div class="balance-row">
                    <span style="color:var(--text-dim);">ETH Balance:</span>
                    <span class="balance-val" style="color:#fff;" id="bal-eth">5.00 ETH</span>
                </div>
                <button class="faucet-btn-glow" onclick="claimFaucet()">
                    <span>⚡</span> + Claim Free $5,000 Faucet
                </button>
            </div>

            <div class="panel-header">
                <span>⚡ Live Trade Tape</span>
                <span style="font-size:10px; color:var(--cyan);">REAL-TIME</span>
            </div>
            <div class="ob-header-grid">
                <span>Price (USDT)</span>
                <span style="text-align:center;">Size (ETH)</span>
                <span style="text-align:right;">Time</span>
            </div>
            <div class="orderbook-table" id="trade-tape"></div>
        </section>
    </main>

    <script>
        let currentSide = "BUY";
        let currentType = "LIMIT";
        let currentOrdersTab = "open";
        let chartViewMode = "3D";
        let latestAccountData = { account: null, active_orders: [], order_history: [] };
        let candleData = [];
        let wsRetryCount = 0;
        let ws = null;
        let mouseX = 0, mouseY = 0;

        // 3D Moving Perspective Candlestick Canvas Engine
        function draw3DChart() {
            const container = document.getElementById('tv-chart');
            if (!container) return;

            let canvas = document.getElementById('chart-canvas-3d');
            if (!canvas) {
                container.innerHTML = '<canvas id="chart-canvas-3d" style="display:block; width:100%; height:100%; cursor:crosshair;"></canvas>';
                canvas = document.getElementById('chart-canvas-3d');

                // Interactive 3D Camera Tilt on mouse/touch move
                canvas.addEventListener('mousemove', (e) => {
                    const rect = canvas.getBoundingClientRect();
                    mouseX = ((e.clientX - rect.left) / rect.width) * 2 - 1;
                    mouseY = ((e.clientY - rect.top) / rect.height) * 2 - 1;
                    draw3DChart();
                });
                canvas.addEventListener('mouseleave', () => {
                    mouseX = 0; mouseY = 0;
                    draw3DChart();
                });
            }

            const rect = container.getBoundingClientRect();
            const width = rect.width || container.clientWidth || 375;
            const height = rect.height || container.clientHeight || 280;

            const dpr = window.devicePixelRatio || 1;
            canvas.width = width * dpr;
            canvas.height = height * dpr;
            canvas.style.width = width + 'px';
            canvas.style.height = height + 'px';

            const ctx = canvas.getContext('2d');
            ctx.scale(dpr, dpr);

            // 3D Cyber Ambient Background
            ctx.fillStyle = '#0a0e17';
            ctx.fillRect(0, 0, width, height);

            // 3D Cyber Perspective Grid
            const gridOffset = (chartViewMode === "3D") ? mouseX * 25 : 0;
            ctx.strokeStyle = 'rgba(0, 229, 255, 0.04)';
            ctx.lineWidth = 1;

            for (let x = 0; x < width; x += 40) {
                ctx.beginPath();
                ctx.moveTo(x + gridOffset, 0);
                ctx.lineTo(x - gridOffset * 0.5, height);
                ctx.stroke();
            }
            for (let y = 0; y < height; y += 40) {
                ctx.beginPath();
                ctx.moveTo(0, y);
                ctx.lineTo(width, y);
                ctx.stroke();
            }

            if (!candleData || candleData.length === 0) {
                ctx.fillStyle = 'var(--cyan)';
                ctx.font = '600 13px "Plus Jakarta Sans", sans-serif';
                ctx.textAlign = 'center';
                ctx.fillText('⚡ Initializing 3D High-Speed Chart...', width / 2, height / 2);
                return;
            }

            // Price boundaries
            let minPrice = Infinity;
            let maxPrice = -Infinity;
            const visibleCandles = candleData.slice(-32);

            visibleCandles.forEach(c => {
                if (c.low < minPrice) minPrice = c.low;
                if (c.high > maxPrice) maxPrice = c.high;
            });

            const padding = (maxPrice - minPrice) * 0.12 || 2.5;
            minPrice -= padding;
            maxPrice += padding;
            const priceRange = maxPrice - minPrice;

            const chartRightMargin = 60;
            const chartBottomMargin = 26;
            const plotWidth = width - chartRightMargin;
            const plotHeight = height - chartBottomMargin;

            // Price Labels on Right Axis
            ctx.fillStyle = '#8b9bb4';
            ctx.font = '500 10px "JetBrains Mono", monospace';
            ctx.textAlign = 'left';

            const gridSteps = 4;
            for (let i = 0; i <= gridSteps; i++) {
                const y = plotHeight * (i / gridSteps);
                const price = maxPrice - (priceRange * (i / gridSteps));

                ctx.strokeStyle = 'rgba(255, 255, 255, 0.05)';
                ctx.beginPath();
                ctx.moveTo(0, y);
                ctx.lineTo(plotWidth, y);
                ctx.stroke();

                ctx.fillText('$' + price.toFixed(1), plotWidth + 8, y + 3);
            }

            // Draw 3D Glowing Candlesticks
            const numCandles = visibleCandles.length;
            const candleWidth = Math.max(4, (plotWidth / numCandles) * 0.62);
            const slotWidth = plotWidth / numCandles;
            const depth3D = (chartViewMode === "3D") ? 4 : 0;

            visibleCandles.forEach((c, i) => {
                const x = (i * slotWidth) + (slotWidth / 2);
                const isGreen = c.close >= c.open;
                const mainColor = isGreen ? '#00F29D' : '#FF3B69';
                const shadowColor = isGreen ? 'rgba(0, 242, 157, 0.3)' : 'rgba(255, 59, 105, 0.3)';

                const yHigh = plotHeight - ((c.high - minPrice) / priceRange) * plotHeight;
                const yLow = plotHeight - ((c.low - minPrice) / priceRange) * plotHeight;
                const yOpen = plotHeight - ((c.open - minPrice) / priceRange) * plotHeight;
                const yClose = plotHeight - ((c.close - minPrice) / priceRange) * plotHeight;

                const bodyTop = Math.min(yOpen, yClose);
                const bodyHeight = Math.max(3, Math.abs(yClose - yOpen));

                // 3D Isometric Extrusion (Back / Shadow layer)
                if (chartViewMode === "3D") {
                    ctx.fillStyle = shadowColor;
                    ctx.fillRect(x - candleWidth / 2 + depth3D, bodyTop - depth3D, candleWidth, bodyHeight);
                }

                // Wick Line with Neon Glow
                ctx.strokeStyle = mainColor;
                ctx.lineWidth = 1.4;
                ctx.shadowColor = mainColor;
                ctx.shadowBlur = (chartViewMode === "3D") ? 8 : 0;
                ctx.beginPath();
                ctx.moveTo(x, yHigh);
                ctx.lineTo(x, yLow);
                ctx.stroke();
                ctx.shadowBlur = 0;

                // Candle Body (Gradient Glow)
                const grad = ctx.createLinearGradient(0, bodyTop, 0, bodyTop + bodyHeight);
                if (isGreen) {
                    grad.addColorStop(0, '#00F29D');
                    grad.addColorStop(1, '#00B373');
                } else {
                    grad.addColorStop(0, '#FF3B69');
                    grad.addColorStop(1, '#C91D45');
                }

                ctx.fillStyle = grad;
                ctx.fillRect(x - candleWidth / 2, bodyTop, candleWidth, bodyHeight);

                // Top highlight border
                ctx.strokeStyle = '#ffffff';
                ctx.lineWidth = 0.5;
                ctx.strokeRect(x - candleWidth / 2, bodyTop, candleWidth, bodyHeight);
            });

            // Live Animated Price Tracker Line & Glowing Badge
            if (visibleCandles.length > 0) {
                const lastCandle = visibleCandles[visibleCandles.length - 1];
                const lastY = plotHeight - ((lastCandle.close - minPrice) / priceRange) * plotHeight;
                const isGreen = lastCandle.close >= lastCandle.open;
                const priceColor = isGreen ? '#00F29D' : '#FF3B69';

                ctx.strokeStyle = priceColor;
                ctx.setLineDash([4, 4]);
                ctx.lineWidth = 1.2;
                ctx.beginPath();
                ctx.moveTo(0, lastY);
                ctx.lineTo(plotWidth, lastY);
                ctx.stroke();
                ctx.setLineDash([]);

                // 3D Glowing Price Tag on Axis
                ctx.fillStyle = priceColor;
                ctx.shadowColor = priceColor;
                ctx.shadowBlur = 10;
                ctx.beginPath();
                ctx.roundRect(plotWidth + 4, lastY - 10, 52, 20, 5);
                ctx.fill();
                ctx.shadowBlur = 0;

                ctx.fillStyle = '#000';
                ctx.font = 'bold 10px "JetBrains Mono", monospace';
                ctx.fillText(lastCandle.close.toFixed(1), plotWidth + 8, lastY + 4);
            }
        }

        function setChartView(mode) {
            chartViewMode = mode;
            document.getElementById('btn-view-3d').className = 'view-btn ' + (mode === '3D' ? 'active' : '');
            document.getElementById('btn-view-pro').className = 'view-btn ' + (mode === 'PRO' ? 'active' : '');
            draw3DChart();
        }

        function loadCandles() {
            fetch('/api/v1/candles')
                .then(r => r.json())
                .then(data => {
                    if (data && data.length) {
                        candleData = data;
                        draw3DChart();
                    }
                })
                .catch(err => console.error("Candle fetch error:", err));
        }

        function setSide(side) {
            currentSide = side;
            document.getElementById("tab-buy").className = "trade-btn " + (side === "BUY" ? "active buy" : "");
            document.getElementById("tab-sell").className = "trade-btn " + (side === "SELL" ? "active sell" : "");
            const submitBtn = document.getElementById("btn-submit-order");
            submitBtn.className = "btn-submit-order " + (side === "BUY" ? "buy" : "sell");
            submitBtn.innerText = side === "BUY" ? "▲ Place Buy Order (+)" : "▼ Place Sell Order (-)";
            updateAvailDisplay();
        }

        function setType(type) {
            currentType = type;
            document.getElementById("tab-limit").className = "mode-tab " + (type === "LIMIT" ? "active" : "");
            document.getElementById("tab-market").className = "mode-tab " + (type === "MARKET" ? "active" : "");
            document.getElementById("price-group").style.display = type === "MARKET" ? "none" : "flex";
        }

        function updateAvailDisplay() {
            if (!latestAccountData.account || !latestAccountData.account.balances) return;
            const usdt = latestAccountData.account.balances.USDT ? latestAccountData.account.balances.USDT.available : 0;
            const eth = latestAccountData.account.balances.ETH ? latestAccountData.account.balances.ETH.available : 0;
            const text = currentSide === "BUY" ? usdt.toLocaleString(undefined, {minimumFractionDigits: 2}) + " USDT" : eth.toFixed(2) + " ETH";
            document.getElementById("avail-quote-display").innerText = text;
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
                return '<div class="ob-row" onclick="fillPrice(' + a.price + ')" role="button">' +
                    '<div class="ob-bar ask" style="width:' + width + '%;"></div>' +
                    '<span style="color:var(--red); font-weight:700;">▼ ' + a.price.toFixed(2) + '</span>' +
                    '<span style="text-align:center;">' + a.volume.toFixed(2) + '</span>' +
                    '<span style="text-align:right;">' + (a.price * a.volume).toFixed(0) + '</span>' +
                '</div>';
            }).join('');
            document.getElementById("asks-container").innerHTML = asksHtml || '<div style="padding:10px; color:var(--text-dim); text-align:center;">No asks</div>';

            let bidsHtml = bids.map(b => {
                const width = Math.min(100, (b.volume / maxVol) * 100);
                return '<div class="ob-row" onclick="fillPrice(' + b.price + ')" role="button">' +
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

            // Live Candle Tick
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
                draw3DChart();
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
                updateAvailDisplay();
            }

            const activeOrders = data.active_orders || [];
            const history = data.order_history || [];
            document.getElementById("open-count").innerText = activeOrders.length;
            document.getElementById("history-count").innerText = history.length;
            const mobBadge = document.getElementById("mob-orders-count");
            if (mobBadge) mobBadge.innerText = activeOrders.length;

            renderOrdersTable();
        }

        function switchOrdersTab(tab) {
            currentOrdersTab = tab;
            document.getElementById("subtab-open").className = "orders-tab-link " + (tab === "open" ? "active" : "");
            document.getElementById("subtab-history").className = "orders-tab-link " + (tab === "history" ? "active" : "");
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
                        '<td style="color:' + color + '; font-weight:800;">' + prefix + '</td>' +
                        '<td>$' + o.price.toFixed(2) + '</td>' +
                        '<td>' + (o.amount - o.filled).toFixed(2) + ' ETH</td>' +
                        '<td style="color:var(--text-dim);">' + new Date().toLocaleTimeString() + '</td>' +
                        '<td><button class="btn-cancel-order" onclick="cancelOrder(\'' + o.id + '\')">✕ Cancel</button></td>' +
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
                        '<td style="color:' + color + '; font-weight:800;">' + prefix + '</td>' +
                        '<td>$' + h.price.toFixed(2) + '</td>' +
                        '<td>' + h.filled.toFixed(2) + ' / ' + h.amount.toFixed(2) + ' ETH</td>' +
                        '<td style="color:' + statusColor + '; font-weight:800;">' + h.status + '</td>' +
                        '<td style="color:var(--text-dim);">' + new Date(h.completed_at).toLocaleTimeString() + '</td>' +
                    '</tr>';
                });
                html += '</table>';
                container.innerHTML = html;
            }
        }

        // Mobile Segmented Tab Switcher (< 768px)
        function switchMobileTab(tab) {
            const btns = document.querySelectorAll('.mobile-pill-btn');
            btns.forEach(b => b.classList.remove('active'));
            if (event && event.target) {
                event.target.closest('.mobile-pill-btn').classList.add('active');
            }

            const pBook = document.getElementById('panel-orderbook');
            const pChart = document.getElementById('panel-chart');
            const pTrade = document.getElementById('panel-trade');
            const pOrders = document.getElementById('panel-orders');
            const pPort = document.getElementById('panel-portfolio');

            [pBook, pChart, pTrade, pOrders, pPort].forEach(p => {
                if (p) p.style.display = 'none';
            });

            if (tab === 'trade') {
                pTrade.style.display = 'flex';
                pOrders.style.display = 'flex';
            } else if (tab === 'chart') {
                pChart.style.display = 'block';
                setTimeout(draw3DChart, 50);
            } else if (tab === 'orderbook') {
                pBook.style.display = 'flex';
            } else if (tab === 'orders') {
                pOrders.style.display = 'flex';
            } else if (tab === 'portfolio') {
                pPort.style.display = 'flex';
                pPort.classList.remove('right-panel');
            }
        }

        // WebSocket Resilient Connection Handler
        function updateWSStatus(state, text) {
            const badge = document.getElementById("ws-status");
            const label = document.getElementById("ws-text");
            badge.className = "ws-badge " + state;
            label.innerText = text;
        }

        function connectWS() {
            updateWSStatus("reconnecting", "RECONNECTING...");
            const protocol = location.protocol === "https:" ? "wss:" : "ws:";
            ws = new WebSocket(protocol + "//" + location.host + "/ws");

            ws.onopen = () => {
                wsRetryCount = 0;
                updateWSStatus("connected", "WS LIVE ( < 50µs )");
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
                updateWSStatus("disconnected", "WS DISCONNECTED");
            };

            ws.onclose = () => {
                wsRetryCount++;
                const delay = Math.min(8000, 1000 * Math.pow(2, wsRetryCount));
                updateWSStatus("reconnecting", "RETRYING IN " + (delay / 1000) + "s...");
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

        // Initialize Everything
        window.addEventListener('DOMContentLoaded', () => {
            loadCandles();
            window.addEventListener('resize', draw3DChart);
            fetch("/api/v1/orderbook").then(r => r.json()).then(renderOrderBook);
            fetch("/api/v1/account").then(r => r.json()).then(renderAccount);
            fetch("/api/v1/trades").then(r => r.json()).then(trades => trades.forEach(renderTrade));
            connectWS();

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
