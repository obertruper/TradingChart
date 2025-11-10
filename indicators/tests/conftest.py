"""
Pytest configuration and fixtures for indicators tests
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch


@pytest.fixture
def mock_config():
    """
    Mock configuration matching production indicators_config.yaml

    Returns real values used in production:
    - timeframes: ['1m', '15m', '1h']
    - sma.periods: [10, 30, 50, 100, 200]
    - ema.periods: [9, 12, 21, 26, 50, 100, 200]
    - rsi.periods: [7, 9, 14, 21, 25]
    - symbols: BTCUSDT, ETHUSDT, etc.
    """
    return {
        'timeframes': ['1m', '15m', '1h'],
        'symbols': [
            'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'XRPUSDT', 'SOLUSDT',
            'ADAUSDT', 'LINKUSDT', 'XLMUSDT', 'LTCUSDT', 'DOTUSDT'
        ],
        'indicators': {
            'sma': {
                'enabled': True,
                'periods': [10, 30, 50, 100, 200]
            },
            'ema': {
                'enabled': True,
                'periods': [9, 12, 21, 26, 50, 100, 200],
                'batch_days': 1
            },
            'rsi': {
                'enabled': True,
                'periods': [7, 9, 14, 21, 25],
                'batch_days': 1
            },
            'vma': {
                'enabled': True,
                'periods': [10, 20, 50, 100, 200],
                'batch_days': 1
            },
            'macd': {
                'enabled': True,
                'configurations': [
                    {
                        'name': 'classic',
                        'fast': 12,
                        'slow': 26,
                        'signal': 9
                    },
                    {
                        'name': 'crypto',
                        'fast': 6,
                        'slow': 13,
                        'signal': 5
                    }
                ],
                'batch_days': 1,
                'lookback_multiplier': 3
            },
            'atr': {
                'enabled': True,
                'periods': [7, 14, 21, 30, 50, 100],
                'batch_days': 1
            },
            'obv': {
                'enabled': True,
                'batch_days': 1
            },
            'adx': {
                'enabled': True,
                'periods': [7, 10, 14, 20, 21, 25, 30, 50],
                'batch_days': 1,
                'lookback_multiplier': 4
            },
            'mfi': {
                'enabled': True,
                'periods': [7, 10, 14, 20, 25],
                'batch_days': 1,
                'lookback_multiplier': 2
            },
            'vwap': {
                'enabled': True,
                'daily_enabled': True,
                'rolling_periods': [10, 14, 20, 30, 50, 55, 89, 100, 144, 200, 233, 300, 500, 1000, 1440],
                'batch_days': 1,
                'lookback_multiplier': 2
            },
            'long_short_ratio': {
                'enabled': True,
                'batch_size': 1000,
                'api_retry_attempts': 3,
                'api_retry_delay': 2
            },
            'fear_and_greed': {
                'enabled': True,
                'api_url': 'https://api.alternative.me/fng/?limit=0',
                'batch_days': 1,
                'retry_on_error': 3,
                'timeframes': ['1m', '15m', '1h']
            },
            'coinmarketcap_fear_and_greed': {
                'enabled': True,
                'api_key': 'test_api_key_12345',
                'base_url': 'https://pro-api.coinmarketcap.com',
                'batch_size': 500,
                'batch_days': 1,
                'retry_on_error': 3,
                'timeframes': ['1m', '15m', '1h']
            }
        }
    }


@pytest.fixture
def sample_prices_small():
    """
    Small sample of prices for quick tests

    Returns:
        pd.Series: 10 prices [10, 20, 30, ..., 100]
    """
    return pd.Series(range(10, 110, 10), dtype=float)


@pytest.fixture
def sample_prices_medium():
    """
    Medium sample of prices for SMA calculations

    Returns:
        pd.Series: 100 prices in range [100, 200]
    """
    np.random.seed(42)  # Reproducible random prices
    return pd.Series(np.random.uniform(100, 200, 100))


@pytest.fixture
def sample_prices_large():
    """
    Large sample of prices for realistic tests

    Returns:
        pd.Series: 250 prices (enough for SMA_200 + margin)
    """
    np.random.seed(42)
    # Simulate BTC-like price movement
    base_price = 43000
    volatility = 500
    prices = [base_price]

    for _ in range(249):
        change = np.random.normal(0, volatility)
        new_price = max(prices[-1] + change, 1000)  # Keep positive
        prices.append(new_price)

    return pd.Series(prices, dtype=float)


@pytest.fixture
def sample_candles_df():
    """
    DataFrame with sample candle data

    Returns:
        pd.DataFrame: Columns [timestamp, symbol, close]
    """
    timestamps = pd.date_range('2024-01-01 00:00', periods=100, freq='1min')
    np.random.seed(42)
    closes = np.random.uniform(43000, 44000, 100)

    return pd.DataFrame({
        'timestamp': timestamps,
        'symbol': 'BTCUSDT',
        'close': closes
    })


@pytest.fixture
def mock_database():
    """
    Mock database connection for isolated tests

    Returns:
        MagicMock: Mocked DatabaseConnection instance
    """
    mock_db = MagicMock()
    mock_db.get_connection.return_value.__enter__.return_value = MagicMock()
    return mock_db


@pytest.fixture
def real_btc_prices():
    """
    Real BTC price sequence for formula validation

    These are actual prices to verify SMA calculation matches expected values.
    """
    return pd.Series([
        43500.0, 43520.5, 43490.2, 43510.8, 43530.1,
        43515.4, 43525.9, 43540.3, 43535.7, 43550.2,
        43545.0, 43560.1, 43555.5, 43570.0, 43565.3
    ])


@pytest.fixture
def sample_timeframes():
    """
    Sample timeframes with expected minute conversion

    Returns:
        tuple: (input_list, expected_dict)
    """
    inputs = ['1m', '5m', '15m', '30m', '1h', '4h', '1d', '1w']
    expected = {
        '1m': 1,
        '5m': 5,
        '15m': 15,
        '30m': 30,
        '1h': 60,
        '4h': 240,
        '1d': 1440,
        '1w': 10080
    }
    return inputs, expected


# ============================================
# EMA-specific fixtures
# ============================================

@pytest.fixture
def sample_ema_initial_state():
    """
    Sample initial EMA state for checkpoint testing

    Returns:
        dict: {period: ema_value}
    """
    return {
        12: 43500.5,
        26: 43480.2,
        50: 43450.0
    }


@pytest.fixture
def sample_prices_for_ema():
    """
    Specific price sequence for EMA calculation testing

    Returns:
        pd.Series: Prices [100, 110, 105, 115, 120, ...]
    """
    prices = [100, 110, 105, 115, 120, 125, 118, 130, 135, 128, 140, 145]
    return pd.Series(prices, dtype=float)


# ============================================
# RSI-specific fixtures
# ============================================

@pytest.fixture
def sample_rsi_checkpoint():
    """
    Sample RSI checkpoint state (avg_gain, avg_loss)

    Used for continuing RSI calculation from saved state

    Returns:
        dict: {period: {'avg_gain': float, 'avg_loss': float}}
    """
    return {
        14: {
            'avg_gain': 10.5,
            'avg_loss': 8.2
        },
        21: {
            'avg_gain': 12.3,
            'avg_loss': 9.7
        }
    }


@pytest.fixture
def sample_prices_trending_up():
    """
    Price sequence with clear uptrend for RSI testing

    Returns:
        np.ndarray: Prices that trend upward (RSI should be > 50)
    """
    # Start at 100, increase with some noise
    np.random.seed(42)
    prices = [100]
    for _ in range(49):
        # Mostly positive changes with small negative variance
        change = np.random.uniform(0.5, 2.0) + np.random.normal(0, 0.3)
        prices.append(prices[-1] + change)
    return np.array(prices)


@pytest.fixture
def sample_prices_trending_down():
    """
    Price sequence with clear downtrend for RSI testing

    Returns:
        np.ndarray: Prices that trend downward (RSI should be < 50)
    """
    # Start at 100, decrease with some noise
    np.random.seed(42)
    prices = [100]
    for _ in range(49):
        # Mostly negative changes with small positive variance
        change = -(np.random.uniform(0.5, 2.0) + np.random.normal(0, 0.3))
        prices.append(prices[-1] + change)
    return np.array(prices)


@pytest.fixture
def sample_prices_sideways():
    """
    Price sequence oscillating around a mean (RSI should be near 50)

    Returns:
        np.ndarray: Prices oscillating around 100
    """
    np.random.seed(42)
    prices = []
    for i in range(50):
        # Sine wave around 100 with noise
        price = 100 + 5 * np.sin(i * 0.3) + np.random.normal(0, 1)
        prices.append(price)
    return np.array(prices)


# ============================================
# VMA-specific fixtures
# ============================================

@pytest.fixture
def sample_volumes_small():
    """
    Small sample of volumes for quick tests

    Returns:
        pd.Series: 10 volume values [1000, 1500, 2000, ...]
    """
    return pd.Series([1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500], dtype=float)


@pytest.fixture
def sample_volumes_large():
    """
    Large sample of volumes for VMA_200 testing

    Returns:
        pd.Series: 250 volume values with realistic variation
    """
    np.random.seed(42)
    # Simulate realistic volume: base + random variation
    base_volume = 50000
    volumes = []
    for _ in range(250):
        # Volume varies ±30% from base
        volume = base_volume * (1 + np.random.uniform(-0.3, 0.3))
        volumes.append(max(volume, 1000))  # Ensure positive
    return pd.Series(volumes, dtype=float)


@pytest.fixture
def sample_candles_with_volume():
    """
    DataFrame with sample candle data including volume

    Returns:
        pd.DataFrame: Columns [timestamp, symbol, volume]
    """
    timestamps = pd.date_range('2024-01-01 00:00', periods=100, freq='1min')
    np.random.seed(42)
    volumes = np.random.uniform(40000, 60000, 100)

    return pd.DataFrame({
        'timestamp': timestamps,
        'symbol': 'BTCUSDT',
        'volume': volumes
    })


# ============================================
# MACD-specific fixtures
# ============================================

@pytest.fixture
def sample_macd_configs():
    """
    Sample MACD configurations for testing

    Returns:
        list: List of MACD config dicts
    """
    return [
        {
            'name': 'classic',
            'fast': 12,
            'slow': 26,
            'signal': 9
        },
        {
            'name': 'crypto',
            'fast': 6,
            'slow': 13,
            'signal': 5
        }
    ]


@pytest.fixture
def sample_prices_for_macd():
    """
    Price sequence optimized for MACD testing

    Creates a clear trend that will produce visible MACD signals:
    - Uptrend: prices increase gradually
    - MACD line should show positive values
    - Signal line should lag behind MACD line

    Returns:
        pd.Series: 100 prices with upward trend
    """
    np.random.seed(42)
    # Start at 100, add upward trend with noise
    prices = [100]
    for i in range(99):
        # Trend up by ~0.5 per candle with some noise
        change = 0.5 + np.random.normal(0, 0.2)
        prices.append(prices[-1] + change)
    return pd.Series(prices, dtype=float)


@pytest.fixture
def sample_ema_values():
    """
    Pre-calculated EMA values for MACD component testing

    Returns:
        dict: {period: pd.Series of EMA values}
    """
    # Simple price sequence for predictable EMA
    prices = pd.Series([100, 102, 104, 103, 105, 107, 106, 108, 110, 109], dtype=float)

    # Calculate EMAs using pandas
    ema_12 = prices.ewm(span=12, adjust=False).mean()
    ema_26 = prices.ewm(span=26, adjust=False).mean()

    return {
        'prices': prices,
        'ema_12': ema_12,
        'ema_26': ema_26
    }


# ============================================
# ATR-specific fixtures
# ============================================

@pytest.fixture
def sample_atr_candles():
    """
    Sample candles for ATR calculation testing

    Returns:
        pd.DataFrame: Columns [timestamp, symbol, high, low, close]
    """
    timestamps = pd.date_range('2024-01-01 00:00', periods=30, freq='1min')

    # Create realistic candle data
    # Simulating BTC price movement with normal volatility
    np.random.seed(42)
    base_price = 43000

    data = []
    current_price = base_price

    for i in range(30):
        # Random price movement
        change = np.random.uniform(-100, 100)
        current_price += change

        # High/Low around close
        volatility = np.random.uniform(50, 150)
        high = current_price + np.random.uniform(0, volatility)
        low = current_price - np.random.uniform(0, volatility)
        close = np.random.uniform(low, high)

        data.append({
            'timestamp': timestamps[i],
            'symbol': 'BTCUSDT',
            'high': high,
            'low': low,
            'close': close
        })

    return pd.DataFrame(data)


@pytest.fixture
def sample_atr_high_volatility():
    """
    Candles with high volatility (large gaps) for ATR testing

    Simulates market with large price jumps between candles

    Returns:
        pd.DataFrame: Columns [high, low, close]
    """
    # Create candles with significant gaps (high ATR)
    candles = [
        {'high': 100, 'low': 95, 'close': 98},    # Normal candle
        {'high': 110, 'low': 105, 'close': 108},  # Gap up (close jumped from 98 to 105+)
        {'high': 115, 'low': 107, 'close': 110},  # Continuation
        {'high': 105, 'low': 95, 'close': 97},    # Gap down (large move)
        {'high': 108, 'low': 96, 'close': 105},   # Recovery
        {'high': 120, 'low': 115, 'close': 118},  # Another gap up
        {'high': 125, 'low': 117, 'close': 120},  # High volatility continues
    ]

    return pd.DataFrame(candles)


@pytest.fixture
def sample_atr_low_volatility():
    """
    Candles with low volatility (small ranges) for ATR testing

    Simulates calm market with tight price action

    Returns:
        pd.DataFrame: Columns [high, low, close]
    """
    # Create candles with very small ranges (low ATR)
    base_price = 100
    candles = []

    for i in range(20):
        # Very small price movements
        close = base_price + np.random.uniform(-0.5, 0.5)
        high = close + np.random.uniform(0, 0.3)
        low = close - np.random.uniform(0, 0.3)

        candles.append({
            'high': high,
            'low': low,
            'close': close
        })

    return pd.DataFrame(candles)


# ============================================
# Bollinger Bands-specific fixtures
# ============================================

@pytest.fixture
def sample_bb_configs():
    """
    Sample Bollinger Bands configurations for testing

    Returns:
        list: List of BB config dicts
    """
    return [
        {
            'name': 'classic',
            'period': 20,
            'std_dev': 2.0,
            'base': 'sma',
            'description': 'Classic Bollinger (20, 2)'
        },
        {
            'name': 'golden',
            'period': 20,
            'std_dev': 1.618,
            'base': 'sma',
            'description': 'Golden ratio deviation'
        },
        {
            'name': 'classic_ema',
            'period': 20,
            'std_dev': 2.0,
            'base': 'ema',
            'description': 'Classic BB on EMA'
        }
    ]


@pytest.fixture
def sample_prices_for_bb():
    """
    Price sequence optimized for Bollinger Bands testing

    Creates prices with known volatility characteristics:
    - Start at 100
    - Add trend with controlled volatility
    - Periods with high/low volatility for squeeze testing

    Returns:
        pd.Series: 50 prices with varying volatility
    """
    np.random.seed(42)
    prices = [100]

    # First 20: low volatility (tight range)
    for _ in range(19):
        change = np.random.uniform(-0.5, 0.5)
        prices.append(prices[-1] + change)

    # Next 15: high volatility (wide range)
    for _ in range(15):
        change = np.random.uniform(-3, 3)
        prices.append(prices[-1] + change)

    # Last 15: back to low volatility
    for _ in range(15):
        change = np.random.uniform(-0.5, 0.5)
        prices.append(prices[-1] + change)

    return pd.Series(prices, dtype=float)


@pytest.fixture
def sample_bb_constant_prices():
    """
    Constant prices for testing edge case

    When prices are constant, standard deviation = 0,
    so upper = middle = lower.

    Returns:
        pd.Series: 30 constant prices (all 100.0)
    """
    return pd.Series([100.0] * 30, dtype=float)


# ============================================
# Stochastic & Williams %R-specific fixtures
# ============================================

@pytest.fixture
def sample_stochastic_configs():
    """
    Sample Stochastic Oscillator configurations for testing

    Returns:
        list: List of Stochastic config dicts
    """
    return [
        {
            'name': 'scalping',
            'k_period': 5,
            'k_smooth': 1,
            'd_period': 3,
            'description': 'Ultra-fast scalping (5,1,3)'
        },
        {
            'name': 'fast',
            'k_period': 14,
            'k_smooth': 1,
            'd_period': 3,
            'description': 'Fast Stochastic (14,1,3)'
        },
        {
            'name': 'classic',
            'k_period': 14,
            'k_smooth': 3,
            'd_period': 3,
            'description': 'Classic Slow Stochastic (14,3,3)'
        }
    ]


@pytest.fixture
def sample_williams_periods():
    """
    Sample Williams %R periods for testing

    Returns:
        list: List of Williams %R periods [6, 10, 14, 20, 30]
    """
    return [6, 10, 14, 20, 30]


@pytest.fixture
def sample_ohlc_for_stochastic():
    """
    Sample OHLC data for Stochastic and Williams %R testing

    Creates realistic price action with high, low, close for testing
    oscillator calculations.

    Returns:
        pd.DataFrame: Columns [high, low, close]
    """
    np.random.seed(42)

    # Create 50 candles with realistic OHLC relationships
    candles = []
    base_price = 100.0

    for i in range(50):
        # Price movement with trend
        trend = 0.2 * (i % 10 - 5)  # Wave pattern
        noise = np.random.uniform(-2, 2)
        close = base_price + trend + noise

        # High and Low around close
        volatility = np.random.uniform(1, 3)
        high = close + np.random.uniform(0, volatility)
        low = close - np.random.uniform(0, volatility)

        # Ensure relationships: high >= close >= low
        high = max(high, close)
        low = min(low, close)

        candles.append({
            'high': high,
            'low': low,
            'close': close
        })

    return pd.DataFrame(candles)


@pytest.fixture
def sample_obv_data():
    """
    Sample close and volume data for OBV testing

    Creates simple price and volume data for testing OBV calculation logic.
    Includes scenarios: price up, price down, price unchanged.

    Returns:
        pd.DataFrame: Columns [close, volume]
    """
    data = pd.DataFrame({
        'close': [100.0, 105.0, 103.0, 103.0, 108.0, 102.0, 106.0],
        'volume': [1000, 1500, 800, 1200, 2000, 900, 1100]
    })
    return data


@pytest.fixture
def sample_obv_realistic():
    """
    Realistic OBV data with larger dataset

    Creates 30 candles with realistic price movements and volume
    for comprehensive OBV testing.

    Returns:
        pd.DataFrame: Columns [close, volume]
    """
    np.random.seed(42)

    candles = []
    base_price = 100.0
    base_volume = 1000

    for i in range(30):
        # Price movement with trend
        trend = 0.5 * (i % 10 - 5)
        noise = np.random.uniform(-2, 2)
        close = base_price + trend + noise

        # Volume with random variation
        volume = base_volume * np.random.uniform(0.5, 2.0)

        candles.append({
            'close': close,
            'volume': volume
        })

    return pd.DataFrame(candles)


@pytest.fixture
def sample_adx_periods():
    """
    Sample ADX periods for testing

    Returns:
        list: ADX periods [7, 10, 14, 20, 21, 25, 30, 50]
    """
    return [7, 10, 14, 20, 21, 25, 30, 50]


@pytest.fixture
def sample_adx_data():
    """
    Sample OHLC data for ADX testing

    Creates simple uptrend/downtrend data for testing ADX components.

    Returns:
        pd.DataFrame: Columns [high, low, close]
    """
    data = pd.DataFrame({
        'high': [102, 105, 108, 107, 110, 112, 111, 115, 117, 116],
        'low': [98, 101, 104, 103, 106, 108, 107, 111, 113, 112],
        'close': [100, 103, 106, 105, 108, 110, 109, 113, 115, 114]
    })
    return data


@pytest.fixture
def sample_adx_realistic():
    """
    Realistic ADX data with trend changes

    Creates 50 candles with uptrend, downtrend, and sideways movement
    for comprehensive ADX testing.

    Returns:
        pd.DataFrame: Columns [high, low, close]
    """
    np.random.seed(42)

    candles = []
    base_price = 100.0

    for i in range(50):
        # Different market phases
        if i < 15:  # Uptrend
            trend = i * 0.5
        elif i < 30:  # Downtrend
            trend = (15 - (i - 15)) * 0.5
        else:  # Sideways
            trend = 0

        noise = np.random.uniform(-0.5, 0.5)
        close = base_price + trend + noise

        # High and low around close
        volatility = np.random.uniform(0.5, 1.5)
        high = close + volatility
        low = close - volatility

        candles.append({
            'high': high,
            'low': low,
            'close': close
        })

    return pd.DataFrame(candles)


# ============================================
# MFI (Money Flow Index)-specific fixtures
# ============================================

@pytest.fixture
def sample_mfi_periods():
    """
    Sample MFI periods for testing

    Returns:
        list: MFI periods [7, 10, 14, 20, 25]
    """
    return [7, 10, 14, 20, 25]


@pytest.fixture
def sample_mfi_data():
    """
    Sample OHLCV data for MFI testing

    Creates simple price data with volume for testing MFI calculation.
    Includes price movements (up, down, flat) to test Positive/Negative MF split.

    Returns:
        pd.DataFrame: Columns [high, low, close, volume]
    """
    data = pd.DataFrame({
        'high': [102, 105, 104, 104, 108, 103, 107, 106, 110, 109],
        'low': [98, 101, 100, 100, 104, 99, 103, 102, 106, 105],
        'close': [100, 103, 102, 102, 106, 101, 105, 104, 108, 107],
        'volume': [1000, 1500, 800, 1200, 2000, 900, 1600, 1100, 1800, 1400]
    })
    return data


@pytest.fixture
def sample_mfi_realistic():
    """
    Realistic MFI data with trend changes and varying volume

    Creates 50 candles with uptrend, downtrend, and sideways movement
    along with realistic volume patterns for comprehensive MFI testing.

    Returns:
        pd.DataFrame: Columns [high, low, close, volume]
    """
    np.random.seed(42)

    candles = []
    base_price = 100.0
    base_volume = 10000

    for i in range(50):
        # Different market phases
        if i < 15:  # Uptrend with increasing volume
            trend = i * 0.5
            volume_multiplier = 1.0 + (i / 15) * 0.5
        elif i < 30:  # Downtrend with high volume
            trend = (15 - (i - 15)) * 0.5
            volume_multiplier = 1.5
        else:  # Sideways with low volume
            trend = 0
            volume_multiplier = 0.7

        noise = np.random.uniform(-0.5, 0.5)
        close = base_price + trend + noise

        # High and low around close
        volatility = np.random.uniform(0.5, 1.5)
        high = close + volatility
        low = close - volatility

        # Volume varies with market phase
        volume = base_volume * volume_multiplier * np.random.uniform(0.8, 1.2)

        candles.append({
            'high': high,
            'low': low,
            'close': close,
            'volume': volume
        })

    return pd.DataFrame(candles)


# ============================================
# VWAP (Volume Weighted Average Price)-specific fixtures
# ============================================

@pytest.fixture
def sample_vwap_rolling_periods():
    """
    Sample VWAP rolling periods for testing

    Returns:
        list: VWAP rolling periods [10, 14, 20, 30, 50, 100, 200]
    """
    return [10, 14, 20, 30, 50, 100, 200]


@pytest.fixture
def sample_vwap_data_single_day():
    """
    Sample OHLCV data within a single day for VWAP testing

    Creates 24 hours of hourly data (24 candles) within one day
    to test Daily VWAP without reset.

    Returns:
        pd.DataFrame: Columns [high, low, close, volume] with timestamp index
    """
    # Start at 2024-01-01 00:00:00 UTC
    start_time = pd.Timestamp('2024-01-01 00:00:00', tz='UTC')
    timestamps = pd.date_range(start=start_time, periods=24, freq='1h')

    data = pd.DataFrame({
        'high': [102 + i for i in range(24)],
        'low': [98 + i for i in range(24)],
        'close': [100 + i for i in range(24)],
        'volume': [1000 + i*100 for i in range(24)]
    }, index=timestamps)

    return data


@pytest.fixture
def sample_vwap_data_multi_day():
    """
    Sample OHLCV data spanning multiple days for VWAP reset testing

    Creates 3 days of data (72 hours, hourly candles) to test
    Daily VWAP reset at midnight (00:00 UTC).

    Returns:
        pd.DataFrame: Columns [high, low, close, volume] with timestamp index
    """
    # Start at 2024-01-01 00:00:00 UTC
    start_time = pd.Timestamp('2024-01-01 00:00:00', tz='UTC')
    timestamps = pd.date_range(start=start_time, periods=72, freq='1h')

    data = pd.DataFrame({
        'high': [102 + (i % 24) for i in range(72)],   # Cycles every 24 hours
        'low': [98 + (i % 24) for i in range(72)],
        'close': [100 + (i % 24) for i in range(72)],
        'volume': [1000 + (i % 24)*50 for i in range(72)]
    }, index=timestamps)

    return data


@pytest.fixture
def sample_vwap_realistic():
    """
    Realistic VWAP data with varying prices and volumes

    Creates 100 candles with realistic price movements and volume patterns
    for comprehensive VWAP testing.

    Returns:
        pd.DataFrame: Columns [high, low, close, volume] with timestamp index
    """
    np.random.seed(42)

    start_time = pd.Timestamp('2024-01-01 00:00:00', tz='UTC')
    timestamps = pd.date_range(start=start_time, periods=100, freq='1h')

    candles = []
    base_price = 100.0
    base_volume = 10000

    for i in range(100):
        # Price trend with noise
        trend = np.sin(i / 10) * 5  # Wave pattern
        noise = np.random.uniform(-1, 1)
        close = base_price + trend + noise

        # High and low around close
        volatility = np.random.uniform(0.5, 2.0)
        high = close + volatility
        low = close - volatility

        # Volume varies
        volume = base_volume * np.random.uniform(0.5, 2.0)

        candles.append({
            'high': high,
            'low': low,
            'close': close,
            'volume': volume
        })

    data = pd.DataFrame(candles, index=timestamps)
    return data


# ============================================
# Long/Short Ratio-specific fixtures
# ============================================

@pytest.fixture
def sample_long_short_api_response():
    """
    Sample successful API response from Bybit Long/Short Ratio endpoint

    Simulates the response structure from /v5/market/account-ratio:
    {
        "retCode": 0,
        "result": {
            "list": [
                {"timestamp": "1234567890000", "buyRatio": "0.55", "sellRatio": "0.45"},
                ...
            ]
        }
    }

    Returns:
        dict: API response with 5 sample records
    """
    return {
        'retCode': 0,
        'retMsg': 'OK',
        'result': {
            'list': [
                {
                    'timestamp': '1609459200000',  # 2021-01-01 00:00:00 UTC
                    'buyRatio': '0.5500',
                    'sellRatio': '0.4500'
                },
                {
                    'timestamp': '1609462800000',  # 2021-01-01 01:00:00 UTC
                    'buyRatio': '0.6200',
                    'sellRatio': '0.3800'
                },
                {
                    'timestamp': '1609466400000',  # 2021-01-01 02:00:00 UTC
                    'buyRatio': '0.4800',
                    'sellRatio': '0.5200'
                },
                {
                    'timestamp': '1609470000000',  # 2021-01-01 03:00:00 UTC
                    'buyRatio': '0.7000',
                    'sellRatio': '0.3000'
                },
                {
                    'timestamp': '1609473600000',  # 2021-01-01 04:00:00 UTC
                    'buyRatio': '0.5000',
                    'sellRatio': '0.5000'
                }
            ]
        }
    }


@pytest.fixture
def sample_long_short_api_response_empty():
    """
    Sample API response with empty result list

    Used to test handling of periods with no data.

    Returns:
        dict: API response with empty list
    """
    return {
        'retCode': 0,
        'retMsg': 'OK',
        'result': {
            'list': []
        }
    }


@pytest.fixture
def sample_long_short_api_response_error():
    """
    Sample API error response from Bybit

    Used to test error handling when API returns an error code.
    Common error codes:
    - 10001: Parameter error
    - 10002: Request too frequent
    - 10003: Server error

    Returns:
        dict: API error response
    """
    return {
        'retCode': 10001,
        'retMsg': 'error sign! origin_string',
        'result': {}
    }


# ============================================
# Fear & Greed Alternative.me-specific fixtures
# ============================================

@pytest.fixture
def sample_fear_greed_alternative_api_response():
    """
    Sample successful API response from Alternative.me Fear & Greed Index

    Simulates the response structure from https://api.alternative.me/fng/?limit=0:
    {
        "data": [
            {"timestamp": "1609459200", "value": "75", "value_classification": "Greed"},
            ...
        ]
    }

    Returns:
        dict: API response with 5 sample records
    """
    return {
        'name': 'Fear and Greed Index',
        'data': [
            {
                'value': '75',
                'value_classification': 'Greed',
                'timestamp': '1609459200',  # 2021-01-01 00:00:00 UTC
                'time_until_update': '0'
            },
            {
                'value': '62',
                'value_classification': 'Greed',
                'timestamp': '1609545600',  # 2021-01-02 00:00:00 UTC
                'time_until_update': '0'
            },
            {
                'value': '45',
                'value_classification': 'Fear',
                'timestamp': '1609632000',  # 2021-01-03 00:00:00 UTC
                'time_until_update': '0'
            },
            {
                'value': '28',
                'value_classification': 'Fear',
                'timestamp': '1609718400',  # 2021-01-04 00:00:00 UTC
                'time_until_update': '0'
            },
            {
                'value': '50',
                'value_classification': 'Neutral',
                'timestamp': '1609804800',  # 2021-01-05 00:00:00 UTC
                'time_until_update': '0'
            }
        ],
        'metadata': {
            'error': None
        }
    }


@pytest.fixture
def sample_fear_greed_alternative_api_response_empty():
    """
    Sample API response with empty data list from Alternative.me

    Used to test handling of periods with no data.

    Returns:
        dict: API response with empty list
    """
    return {
        'name': 'Fear and Greed Index',
        'data': [],
        'metadata': {
            'error': None
        }
    }


@pytest.fixture
def sample_fear_greed_alternative_api_response_error():
    """
    Sample API error response from Alternative.me

    Used to test error handling when API returns an error.

    Returns:
        dict: API error response
    """
    return {
        'name': 'Fear and Greed Index',
        'data': [],
        'metadata': {
            'error': 'API error occurred'
        }
    }


# ============================================
# Fear & Greed CoinMarketCap-specific fixtures
# ============================================

@pytest.fixture
def sample_fear_greed_coinmarketcap_api_response():
    """
    Sample successful API response from CoinMarketCap Fear & Greed Index

    Simulates the response structure from CoinMarketCap /v3/fear-and-greed/historical:
    {
        "status": {
            "timestamp": "...",
            "error_code": "0",
            "credit_count": 1
        },
        "data": [
            {"timestamp": 1609459200, "value": 75, "value_classification": "Greed"},
            ...
        ]
    }

    Returns:
        dict: API response with 5 sample records
    """
    return {
        'status': {
            'timestamp': '2024-01-15T10:00:00.000Z',
            'error_code': '0',
            'error_message': '',
            'elapsed': 10,
            'credit_count': 1
        },
        'data': [
            {
                'timestamp': 1609459200,  # 2021-01-01 00:00:00 UTC
                'value': 75,
                'value_classification': 'Greed'
            },
            {
                'timestamp': 1609545600,  # 2021-01-02 00:00:00 UTC
                'value': 62,
                'value_classification': 'Greed'
            },
            {
                'timestamp': 1609632000,  # 2021-01-03 00:00:00 UTC
                'value': 45,
                'value_classification': 'Fear'
            },
            {
                'timestamp': 1609718400,  # 2021-01-04 00:00:00 UTC
                'value': 28,
                'value_classification': 'Fear'
            },
            {
                'timestamp': 1609804800,  # 2021-01-05 00:00:00 UTC
                'value': 50,
                'value_classification': 'Neutral'
            }
        ]
    }


@pytest.fixture
def sample_fear_greed_coinmarketcap_api_response_empty():
    """
    Sample API response with empty data list from CoinMarketCap

    Used to test handling of periods with no data.

    Returns:
        dict: API response with empty list
    """
    return {
        'status': {
            'timestamp': '2024-01-15T10:00:00.000Z',
            'error_code': '0',
            'error_message': '',
            'elapsed': 10,
            'credit_count': 1
        },
        'data': []
    }


@pytest.fixture
def sample_fear_greed_coinmarketcap_api_response_error():
    """
    Sample API error response from CoinMarketCap

    Used to test error handling when API returns an error.
    Common error codes:
    - 1001: API key invalid
    - 1002: API key missing
    - 1003: API key plan limit reached

    Returns:
        dict: API error response
    """
    return {
        'status': {
            'timestamp': '2024-01-15T10:00:00.000Z',
            'error_code': '1001',
            'error_message': 'Invalid API Key',
            'elapsed': 10,
            'credit_count': 0
        },
        'data': None
    }
