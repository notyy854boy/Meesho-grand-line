import ccxt
import pandas as pd
import pandas_ta as ta
import numpy as np

class BadaBhaiRealQuant:
    def __init__(self):
        # Using CCXT for robust API connection, rate-limiting, and real data
        self.exchange = ccxt.binance({'enableRateLimit': True})
        
    def fetch_real_ohlcv(self, symbol: str, timeframe: str, limit: int = 200):
        """1. REAL OHLCV DATA"""
        try:
            bars = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            df = pd.DataFrame(bars, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            return df
        except Exception as e:
            print(f"[ERROR] Connection loss / Rate limit: {e}")
            return None

    def calculate_indicators(self, df: pd.DataFrame):
        """2. REAL INDICATORS (RSI, MACD, EMA, ATR)"""
        # Trend Indicators
        df.ta.ema(length=20, append=True)
        df.ta.ema(length=50, append=True)
        df.ta.ema(length=200, append=True)
        
        # Momentum & Volatility
        df.ta.rsi(length=14, append=True)
        df.ta.macd(fast=12, slow=26, signal=9, append=True)
        df.ta.atr(length=14, append=True) # Crucial for Dynamic SL/TP
        
        # Volume
        df.ta.vwap(append=True)
        return df

    def detect_market_structure(self, df: pd.DataFrame):
        """4. REAL MARKET STRUCTURE (Trending, Ranging, Breakouts)"""
        current_close = df['close'].iloc[-1]
        ema20 = df['EMA_20'].iloc[-1]
        ema50 = df['EMA_50'].iloc[-1]
        ema200 = df['EMA_200'].iloc[-1]
        
        # Regime Detection
        if current_close > ema20 > ema50 > ema200:
            regime = "STRONG_UPTREND"
        elif current_close < ema20 < ema50 < ema200:
            regime = "STRONG_DOWNTREND"
        else:
            regime = "CHOPPY_RANGING"
            
        return regime

    def generate_signal_with_risk_management(self, df: pd.DataFrame, regime: str):
        """5, 6 & 7. REAL SIGNAL ENGINE, DYNAMIC SL/TP, CONFIDENCE"""
        last_row = df.iloc[-1]
        prev_row = df.iloc[-2]
        
        signal = "WAIT"
        confidence = 0.0
        entry = last_row['close']
        atr = last_row['ATRr_14']
        
        # Example Logic: Pullback to 20 EMA in an Uptrend + RSI oversold
        if regime == "STRONG_UPTREND" and last_row['low'] <= last_row['EMA_20'] and last_row['RSI_14'] < 40:
            signal = "LONG"
            confidence = 75.0
            # Dynamic SL based on Volatility (1.5x ATR below entry)
            sl = entry - (atr * 1.5)
            # Dynamic TP based on 1:2 Risk/Reward
            risk = entry - sl
            tp = entry + (risk * 2.0)
            
        # Example Logic: Pullback to 20 EMA in a Downtrend + RSI overbought
        elif regime == "STRONG_DOWNTREND" and last_row['high'] >= last_row['EMA_20'] and last_row['RSI_14'] > 60:
            signal = "SHORT"
            confidence = 72.5
            sl = entry + (atr * 1.5)
            risk = sl - entry
            tp = entry - (risk * 2.0)
            
        else:
            # The most important signal: NO-TRADE
            sl = tp = 0.0
            
        return {
            "signal": signal,
            "confidence": f"{confidence}%",
            "regime": regime,
            "entry": round(entry, 4),
            "dynamic_sl": round(sl, 4),
            "dynamic_tp": round(tp, 4),
            "atr_volatility": round(atr, 4)
        }

# --- Execution ---
engine = BadaBhaiRealQuant()
print("Booting Real Quant Engine...")
df = engine.fetch_ohlcv("BTC/USDT", "15m")
if df is not None:
    df = engine.calculate_indicators(df)
    regime = engine.detect_market_structure(df)
    trade_plan = engine.generate_signal_with_risk_management(df, regime)
    print(f"Market Regime: {trade_plan['regime']}")
    print(f"Action: {trade_plan['signal']} | Conf: {trade_plan['confidence']}")
    if trade_plan['signal'] != "WAIT":
        print(f"Entry: {trade_plan['entry']} | TP: {trade_plan['dynamic_tp']} | SL: {trade_plan['dynamic_sl']}")
      
