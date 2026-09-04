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

type Server struct {
	engine   *engine.MatchingEngine
	ledger   *ledger.Ledger
	hub      *websocket.Hub
	demoUser *ledger.UserAccount
	userOrders map[string]*orderbook.Order
	mu       sync.Mutex
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081" // Default port 8081 to avoid conflict with gopherflow on 8080
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
		engine:     matchingEngine,
		ledger:     led,
		hub:        hub,
		demoUser:   demoUser,
		userOrders: make(map[string]*orderbook.Order),
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

	log.Printf("🚀 ApexTrade Matching Engine & Terminal running on http://localhost:%s", port)
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
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(acc)
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
	trades := s.engine.GetRecentTrades(25)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(trades)
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
		isBuyerMaker := trd.MakerOrderID == trd.TakerOrderID // or check maker side
		_ = s.ledger.SettleTrade(trd.BuyerID, trd.SellerID, "ETH", "USDT", trd.Amount, trd.Price, isBuyerMaker)
		s.hub.BroadcastJSON("TRADE_TICKER", trd)
	}

	// 4. Save to User Active Orders if resting
	s.mu.Lock()
	if order.Remaining() > 0 && order.Type == orderbook.OrderTypeLimit {
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
	s.mu.Unlock()

	s.hub.BroadcastJSON("ACCOUNT_UPDATE", map[string]any{
		"account": acc,
		"orders":  activeOrders,
	})
}

// startMarketMakerBot creates realistic depth and occasional fills around the market price
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
		// Random small price fluctuation
		delta := (rand.Float64() - 0.5) * 4.0
		newMid := basePrice + delta

		// Insert/refresh top quotes
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
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ApexTrade | Ultra-Fast Crypto Matching Engine</title>
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
            --accent: #fcd535;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace;
            background-color: var(--bg);
            color: var(--text);
            height: 100vh;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }
        /* Top Navigation */
        .navbar {
            height: 52px;
            background: var(--surface);
            border-bottom: 1px solid var(--border);
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 0 20px;
        }
        .nav-brand {
            display: flex;
            align-items: center;
            gap: 10px;
            font-size: 18px;
            font-weight: bold;
            color: #fff;
        }
        .nav-brand span { color: var(--accent); }
        .nav-pair {
            display: flex;
            align-items: center;
            gap: 15px;
            font-size: 14px;
        }
        .pair-price { font-size: 18px; font-weight: bold; color: var(--green); }
        .ws-badge {
            background: var(--green-dim);
            color: var(--green);
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 11px;
            font-weight: bold;
            display: flex;
            align-items: center;
            gap: 6px;
        }
        .ws-dot { width: 6px; height: 6px; background: var(--green); border-radius: 50%; }

        /* Main Workspace Layout */
        .main-layout {
            display: grid;
            grid-template-columns: 320px 1fr 340px;
            flex: 1;
            overflow: hidden;
        }
        .panel {
            background: var(--surface);
            border-right: 1px solid var(--border);
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }
        .panel-header {
            padding: 10px 15px;
            border-bottom: 1px solid var(--border);
            font-size: 12px;
            font-weight: bold;
            color: var(--text-dim);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            display: flex;
            justify-content: space-between;
        }

        /* Order Book Table */
        .orderbook-table {
            flex: 1;
            overflow-y: auto;
            font-size: 12px;
        }
        .ob-row {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr;
            padding: 4px 15px;
            position: relative;
            cursor: pointer;
        }
        .ob-row:hover { background: rgba(255,255,255,0.05); }
        .ob-bar {
            position: absolute;
            top: 0; bottom: 0; right: 0;
            pointer-events: none;
            opacity: 0.25;
        }
        .ob-bar.ask { background: var(--red); }
        .ob-bar.bid { background: var(--green); }
        .mid-price-bar {
            padding: 8px 15px;
            background: var(--surface-card);
            font-size: 15px;
            font-weight: bold;
            color: var(--text);
            text-align: center;
            border-top: 1px solid var(--border);
            border-bottom: 1px solid var(--border);
        }

        /* Order Placement Center Form */
        .order-form-panel {
            padding: 20px;
            display: flex;
            flex-direction: column;
            gap: 15px;
            max-width: 480px;
            margin: 0 auto;
            width: 100%;
        }
        .tab-group {
            display: flex;
            background: var(--surface-card);
            border-radius: 6px;
            padding: 3px;
        }
        .tab-btn {
            flex: 1;
            padding: 8px;
            border: none;
            background: transparent;
            color: var(--text-dim);
            font-weight: bold;
            font-size: 13px;
            border-radius: 4px;
            cursor: pointer;
            transition: 0.2s;
        }
        .tab-btn.active.buy { background: var(--green); color: #fff; }
        .tab-btn.active.sell { background: var(--red); color: #fff; }
        .tab-btn.active.type { background: var(--border); color: #fff; }

        .input-group {
            display: flex;
            flex-direction: column;
            gap: 6px;
        }
        .input-label { font-size: 12px; color: var(--text-dim); }
        .input-box {
            background: var(--bg);
            border: 1px solid var(--border);
            border-radius: 6px;
            padding: 10px 12px;
            color: #fff;
            font-size: 14px;
            outline: none;
        }
        .input-box:focus { border-color: var(--accent); }

        .btn-submit {
            padding: 14px;
            border: none;
            border-radius: 6px;
            font-size: 15px;
            font-weight: bold;
            cursor: pointer;
            transition: 0.2s;
            color: #fff;
        }
        .btn-submit.buy { background: var(--green); }
        .btn-submit.buy:hover { background: #0ca86b; }
        .btn-submit.sell { background: var(--red); }
        .btn-submit.sell:hover { background: #d9384e; }

        /* Trade Tape & Portfolio */
        .tape-row {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr;
            padding: 4px 15px;
            font-size: 12px;
        }
        .balance-card {
            background: var(--surface-card);
            border: 1px solid var(--border);
            border-radius: 6px;
            padding: 12px 15px;
            margin: 10px 15px;
            font-size: 13px;
        }
        .faucet-btn {
            background: var(--accent);
            color: #000;
            border: none;
            padding: 6px 12px;
            border-radius: 4px;
            font-weight: bold;
            font-size: 11px;
            cursor: pointer;
            margin-top: 8px;
            width: 100%;
        }

        /* Active Orders Table */
        .bottom-panel {
            height: 140px;
            background: var(--surface);
            border-top: 1px solid var(--border);
            overflow-y: auto;
            font-size: 12px;
            padding: 10px 20px;
        }
        .cancel-btn {
            background: var(--red-dim);
            color: var(--red);
            border: none;
            padding: 2px 8px;
            border-radius: 3px;
            font-size: 11px;
            font-weight: bold;
            cursor: pointer;
        }
    </style>
</head>
<body>
    <div class="navbar">
        <div class="nav-brand">
            ⚡ <span>ApexTrade</span> DEX
        </div>
        <div class="nav-pair">
            <strong>ETH / USDT</strong>
            <span class="pair-price" id="top-price">$3,000.00</span>
            <span style="color: var(--text-dim); font-size: 12px;">24h Vol: 1,420.50 ETH</span>
        </div>
        <div class="ws-badge">
            <div class="ws-dot"></div> WS LIVE ( < 50µs )
        </div>
    </div>

    <div class="main-layout">
        <!-- Left: Live Order Book -->
        <div class="panel">
            <div class="panel-header">
                <span>Price (USDT)</span>
                <span>Size (ETH)</span>
                <span>Total</span>
            </div>
            <div class="orderbook-table" id="asks-container" style="display:flex; flex-direction:column-reverse; justify-content:flex-end;"></div>
            <div class="mid-price-bar" id="mid-price">$3,000.00</div>
            <div class="orderbook-table" id="bids-container"></div>
        </div>

        <!-- Center: Order Placement Form & Active Orders -->
        <div class="panel" style="background: var(--bg); display:flex; flex-direction:column; justify-content:space-between;">
            <div class="order-form-panel">
                <div class="tab-group">
                    <button class="tab-btn active buy" id="tab-buy" onclick="setSide('BUY')">BUY ETH</button>
                    <button class="tab-btn" id="tab-sell" onclick="setSide('SELL')">SELL ETH</button>
                </div>
                <div class="tab-group">
                    <button class="tab-btn active type" id="tab-limit" onclick="setType('LIMIT')">Limit Order</button>
                    <button class="tab-btn" id="tab-market" onclick="setType('MARKET')">Market Order</button>
                </div>

                <div class="input-group" id="price-group">
                    <label class="input-label">Price (USDT)</label>
                    <input class="input-box" type="number" id="input-price" value="3000.00" step="0.5">
                </div>

                <div class="input-group">
                    <label class="input-label">Amount (ETH)</label>
                    <input class="input-box" type="number" id="input-amount" value="1.0" step="0.1">
                </div>

                <button class="btn-submit buy" id="btn-submit-order" onclick="submitOrder()">Place Buy Order</button>
            </div>

            <!-- Bottom Active Orders -->
            <div class="bottom-panel">
                <div style="font-weight:bold; color:var(--text-dim); margin-bottom:8px;">MY ACTIVE RESTING ORDERS</div>
                <div id="active-orders-list">No resting limit orders.</div>
            </div>
        </div>

        <!-- Right: Real-Time Trade Tape & Virtual Portfolio -->
        <div class="panel">
            <div class="balance-card">
                <div style="color:var(--text-dim); font-size:11px; margin-bottom:4px;">DEMO TRADER PORTFOLIO</div>
                <div style="display:flex; justify-content:space-between; margin-bottom:4px;">
                    <span>USDT Available:</span>
                    <strong style="color:var(--green);" id="bal-usdt">$10,000.00</strong>
                </div>
                <div style="display:flex; justify-content:space-between;">
                    <span>ETH Available:</span>
                    <strong style="color:#fff;" id="bal-eth">5.00 ETH</strong>
                </div>
                <button class="faucet-btn" onclick="claimFaucet()">+ Claim Free $5,000 Faucet</button>
            </div>

            <div class="panel-header">
                <span>Recent Trades</span>
                <span>Size</span>
                <span>Time</span>
            </div>
            <div class="orderbook-table" id="trade-tape"></div>
        </div>
    </div>

    <script>
        let currentSide = "BUY";
        let currentType = "LIMIT";

        function setSide(side) {
            currentSide = side;
            document.getElementById("tab-buy").className = "tab-btn " + (side === "BUY" ? "active buy" : "");
            document.getElementById("tab-sell").className = "tab-btn " + (side === "SELL" ? "active sell" : "");
            const submitBtn = document.getElementById("btn-submit-order");
            submitBtn.className = "btn-submit " + (side === "BUY" ? "buy" : "sell");
            submitBtn.innerText = "Place " + (side === "BUY" ? "Buy" : "Sell") + " Order";
        }

        function setType(type) {
            currentType = type;
            document.getElementById("tab-limit").className = "tab-btn " + (type === "LIMIT" ? "active type" : "");
            document.getElementById("tab-market").className = "tab-btn " + (type === "MARKET" ? "active type" : "");
            document.getElementById("price-group").style.display = type === "MARKET" ? "none" : "flex";
        }

        function renderOrderBook(data) {
            const asks = data.asks || [];
            const bids = data.bids || [];

            let maxVol = 1.0;
            asks.forEach(a => maxVol = Math.max(maxVol, a.volume));
            bids.forEach(b => maxVol = Math.max(maxVol, b.volume));

            let asksHtml = asks.map(a => {
                const width = Math.min(100, (a.volume / maxVol) * 100);
                return '<div class="ob-row" onclick="fillPrice(' + a.price + ')">' +
                    '<div class="ob-bar ask" style="width:' + width + '%;"></div>' +
                    '<span style="color:var(--red);">' + a.price.toFixed(2) + '</span>' +
                    '<span>' + a.volume.toFixed(2) + '</span>' +
                    '<span>' + (a.price * a.volume).toFixed(0) + '</span>' +
                '</div>';
            }).join('');
            document.getElementById("asks-container").innerHTML = asksHtml;

            let bidsHtml = bids.map(b => {
                const width = Math.min(100, (b.volume / maxVol) * 100);
                return '<div class="ob-row" onclick="fillPrice(' + b.price + ')">' +
                    '<div class="ob-bar bid" style="width:' + width + '%;"></div>' +
                    '<span style="color:var(--green);">' + b.price.toFixed(2) + '</span>' +
                    '<span>' + b.volume.toFixed(2) + '</span>' +
                    '<span>' + (b.price * b.volume).toFixed(0) + '</span>' +
                '</div>';
            }).join('');
            document.getElementById("bids-container").innerHTML = bidsHtml;

            if (bids.length > 0 && asks.length > 0) {
                const mid = ((bids[0].price + asks[0].price) / 2).toFixed(2);
                document.getElementById("mid-price").innerText = "$" + mid;
                document.getElementById("top-price").innerText = "$" + mid;
            }
        }

        function fillPrice(price) {
            document.getElementById("input-price").value = price.toFixed(2);
        }

        function renderTrade(trd) {
            const tape = document.getElementById("trade-tape");
            const isBuy = trd.buyer_id === "demo-user" || Math.random() > 0.5;
            const color = isBuy ? "var(--green)" : "var(--red)";
            const timeStr = new Date().toLocaleTimeString();

            const row = document.createElement("div");
            row.className = "tape-row";
            row.innerHTML = 
                '<span style="color:' + color + ';">' + trd.price.toFixed(2) + '</span>' +
                '<span>' + trd.amount.toFixed(2) + '</span>' +
                '<span style="color:var(--text-dim);">' + timeStr + '</span>';
            tape.insertBefore(row, tape.firstChild);
            if (tape.children.length > 30) tape.removeChild(tape.lastChild);
        }

        function renderAccount(data) {
            if (data.account && data.account.balances) {
                const usdt = data.account.balances.USDT ? data.account.balances.USDT.available : 0;
                const eth = data.account.balances.ETH ? data.account.balances.ETH.available : 0;
                document.getElementById("bal-usdt").innerText = "$" + usdt.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
                document.getElementById("bal-eth").innerText = eth.toFixed(2) + " ETH";
            }

            const orders = data.orders || [];
            if (orders.length === 0) {
                document.getElementById("active-orders-list").innerHTML = '<span style="color:var(--text-dim);">No resting limit orders.</span>';
            } else {
                let html = '<table style="width:100%; text-align:left;">' +
                    '<tr style="color:var(--text-dim);"><th>Side</th><th>Price</th><th>Remaining</th><th>Action</th></tr>';
                orders.forEach(o => {
                    const sideColor = o.side === "BUY" ? "var(--green)" : "var(--red)";
                    html += '<tr>' +
                        '<td style="color:' + sideColor + ';">' + o.side + '</td>' +
                        '<td>$' + o.price.toFixed(2) + '</td>' +
                        '<td>' + (o.amount - o.filled).toFixed(2) + ' ETH</td>' +
                        '<td><button class="cancel-btn" onclick="cancelOrder(\'' + o.id + '\')">Cancel</button></td>' +
                    '</tr>';
                });
                html += '</table>';
                document.getElementById("active-orders-list").innerHTML = html;
            }
        }

        // WebSocket Connection
        function connectWS() {
            const protocol = location.protocol === "https:" ? "wss:" : "ws:";
            const ws = new WebSocket(protocol + "//" + location.host + "/ws");

            ws.onmessage = (event) => {
                const msg = JSON.parse(event.data);
                if (msg.type === "ORDERBOOK_L2") renderOrderBook(msg.payload);
                if (msg.type === "TRADE_TICKER") renderTrade(msg.payload);
                if (msg.type === "ACCOUNT_UPDATE") renderAccount(msg.payload);
            };

            ws.onclose = () => {
                setTimeout(connectWS, 2000); // Reconnect
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

        // Initial Load
        fetch("/api/v1/orderbook").then(r => r.json()).then(renderOrderBook);
        fetch("/api/v1/account").then(r => r.json()).then(acc => renderAccount({account: acc}));
        fetch("/api/v1/trades").then(r => r.json()).then(trades => trades.forEach(renderTrade));
        connectWS();
    </script>
</body>
</html>`

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(html))
}
