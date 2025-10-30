.PHONY: help test test-unit test-integration test-sma test-ema test-rsi test-vma test-macd test-atr test-bb test-stochastic test-obv test-adx coverage test-quick clean install-dev

# Default target
help:
	@echo "TradingChart - Test Commands"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  test              - Run all tests with verbose output"
	@echo "  test-unit         - Run only unit tests"
	@echo "  test-integration  - Run only integration tests (when available)"
	@echo "  test-sma          - Run only SMA loader tests (35 tests)"
	@echo "  test-ema          - Run only EMA loader tests (39 tests)"
	@echo "  test-rsi          - Run only RSI loader tests (37 tests)"
	@echo "  test-vma          - Run only VMA loader tests (35 tests)"
	@echo "  test-macd         - Run only MACD loader tests (45 tests)"
	@echo "  test-atr          - Run only ATR loader tests (48 tests)"
	@echo "  test-bb           - Run only Bollinger Bands loader tests (55 tests)"
	@echo "  test-stochastic   - Run only Stochastic & Williams %R tests (68 tests)"
	@echo "  test-obv          - Run only OBV (On-Balance Volume) tests (42 tests)"
	@echo "  test-adx          - Run only ADX (Average Directional Index) tests (55 tests)"
	@echo "  test-mfi          - Run only MFI (Money Flow Index) tests (58 tests)"
	@echo "  test-vwap         - Run only VWAP (Volume Weighted Average Price) tests (64 tests)"
	@echo "  test-long-short-ratio - Run only Long/Short Ratio tests (50 tests)"
	@echo "  test-fear-greed-alternative - Run only Fear & Greed Alternative.me tests (50 tests)"
	@echo "  test-fear-greed-coinmarketcap - Run only Fear & Greed CoinMarketCap tests (55 tests)"
	@echo "  test-quick        - Quick test run (no verbose)"
	@echo "  coverage          - Run tests with coverage report"
	@echo "  clean             - Clean pytest cache and coverage files"
	@echo "  install-dev       - Install development dependencies"
	@echo ""

# Run all tests
test:
	@echo "Running all tests..."
	pytest indicators/tests/ -v

# Run only unit tests
test-unit:
	@echo "Running unit tests..."
	pytest indicators/tests/unit/ -v

# Run only integration tests (for future)
test-integration:
	@echo "Running integration tests..."
	@if [ -d "indicators/tests/integration" ]; then \
		pytest indicators/tests/integration/ -v; \
	else \
		echo "Integration tests not yet created"; \
	fi

# Run only SMA tests
test-sma:
	@echo "Running SMA loader tests..."
	pytest indicators/tests/unit/test_sma_loader.py -v

# Run only EMA tests
test-ema:
	@echo "Running EMA loader tests..."
	pytest indicators/tests/unit/test_ema_loader.py -v

# Run only RSI tests
test-rsi:
	@echo "Running RSI loader tests..."
	pytest indicators/tests/unit/test_rsi_loader.py -v

# Run only VMA tests
test-vma:
	@echo "Running VMA loader tests..."
	pytest indicators/tests/unit/test_vma_loader.py -v

# Run only MACD tests
test-macd:
	@echo "Running MACD loader tests..."
	pytest indicators/tests/unit/test_macd_loader.py -v

# Run only ATR tests
test-atr:
	@echo "Running ATR loader tests..."
	pytest indicators/tests/unit/test_atr_loader.py -v

# Run only Bollinger Bands tests
test-bb:
	@echo "Running Bollinger Bands loader tests..."
	pytest indicators/tests/unit/test_bollinger_bands_loader.py -v

# Run only Stochastic & Williams %R tests
test-stochastic:
	@echo "Running Stochastic & Williams %R loader tests..."
	pytest indicators/tests/unit/test_stochastic_williams_loader.py -v

# Run only OBV tests
test-obv:
	@echo "Running OBV loader tests..."
	pytest indicators/tests/unit/test_obv_loader.py -v

# Run only ADX tests
test-adx:
	@echo "Running ADX loader tests..."
	pytest indicators/tests/unit/test_adx_loader.py -v

# Run only MFI tests
test-mfi:
	@echo "Running MFI loader tests..."
	pytest indicators/tests/unit/test_mfi_loader.py -v

# Run only VWAP tests
test-vwap:
	@echo "Running VWAP loader tests..."
	pytest indicators/tests/unit/test_vwap_loader.py -v

# Run only Long/Short Ratio tests
test-long-short-ratio:
	@echo "Running Long/Short Ratio loader tests..."
	pytest indicators/tests/unit/test_long_short_ratio_loader.py -v

# Run only Fear & Greed Alternative.me tests
test-fear-greed-alternative:
	@echo "Running Fear & Greed Alternative.me loader tests..."
	pytest indicators/tests/unit/test_fear_and_greed_alternative_loader.py -v

# Run only Fear & Greed CoinMarketCap tests
test-fear-greed-coinmarketcap:
	@echo "Running Fear & Greed CoinMarketCap loader tests..."
	pytest indicators/tests/unit/test_fear_and_greed_coinmarketcap_loader.py -v

# Quick test run (no verbose)
test-quick:
	@echo "Quick test run..."
	pytest indicators/tests/unit/ -q

# Run tests with coverage
coverage:
	@echo "Running tests with coverage..."
	pytest indicators/tests/unit/ \
		--cov=indicators \
		--cov-report=html \
		--cov-report=term-missing
	@echo ""
	@echo "Coverage HTML report: htmlcov/index.html"
	@echo "Open with: open htmlcov/index.html (macOS) or xdg-open htmlcov/index.html (Linux)"

# Run tests with specific marker
test-marker:
	@echo "Usage: make test-marker MARKER=unit"
	@echo "Available markers: unit, integration, slow, sma, ema, rsi"
	@if [ -n "$(MARKER)" ]; then \
		pytest indicators/tests/ -v -m $(MARKER); \
	else \
		echo "Please specify MARKER variable"; \
	fi

# Clean pytest cache and coverage files
clean:
	@echo "Cleaning pytest cache and coverage files..."
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf htmlcov .coverage 2>/dev/null || true
	@echo "Clean complete!"

# Install development dependencies
install-dev:
	@echo "Installing development dependencies..."
	pip install -r requirements.txt
	pip install pytest pytest-cov pytest-mock
	@echo "Development dependencies installed!"

# Run specific test by name
test-name:
	@echo "Usage: make test-name NAME=test_sma_calculation_real_periods"
	@if [ -n "$(NAME)" ]; then \
		pytest indicators/tests/ -v -k $(NAME); \
	else \
		echo "Please specify NAME variable"; \
	fi

# Watch mode (requires pytest-watch)
watch:
	@echo "Watch mode (requires pytest-watch: pip install pytest-watch)"
	@command -v ptw >/dev/null 2>&1 || { echo "pytest-watch not installed. Run: pip install pytest-watch"; exit 1; }
	ptw indicators/tests/unit/ -- -v
