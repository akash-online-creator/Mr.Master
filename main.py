import os
import time
import json
import threading
import requests
import datetime
import pandas as pd
import numpy as np
from binance.client import Client
from flask import Flask, request
import pytz  

# --- 1. CONFIGURATIONS ---
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
PORT = int(os.environ.get("PORT", 8080))

BOT_TIMEZONE = "Asia/Colombo" 

client = Client(API_KEY, API_SECRET, requests_params={"timeout": 20})
client.API_URL = 'https://fapi.binance.com' 

app = Flask(__name__)
DB_FILE = "trade_state.json"

# --- 2. STATE MANAGEMENT & DATABASE ---
def load_data():
    default_state = {
        'active_positions': {},        
        'symbol_recovery_step': {},     
        'symbol_accumulated_loss': {},  
        'block_list': [],  
        'signal_count': 0, 
        'is_paused': False,
        'is_scanning': True,
        'max_signals': 10,
        'stats': {'wins': 0, 'loss': 0, 'total_pnl': 0.0, 'blacklist_coins': []},
        'daily_stats': {'wins': 0, 'loss': 0, 'win_amount': 0.0, 'loss_amount': 0.0, 'blacklist_coins': [], 'last_reset_date': str(datetime.date.today())},
        
        'direct_mode': False,
        'recovery_only_mode': False,     
        
        'first_win_list': [],         
        'first_win_coins': [], 
        'shared_loss_buffer': 0.0,       
        'shared_loss_splits': 0,         
        'total_loss_cost': 0.0,

        'base_margin': 0.80,            
        'margin_sl_pct': 27.0,          
        'fast_tp_pct': 30.0,            
        'leverage': 10,                 
        
        'start_hour': 12,
        'start_minute': 30,
        'end_hour': 23,
        'end_minute': 59,
        
        'fw_start_hour': 0,
        'fw_start_minute': 0,
        'fw_end_hour': 8,
        'fw_end_minute': 0,

        # Reminder alerts feature variables
        'reminder_enabled': True,
        'active_reminders': {}  # {signal_num: True/False}
    }
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, 'r') as f: 
                loaded_state = json.load(f)
                for k, v in default_state.items():
                    if k not in loaded_state: loaded_state[k] = v
                return loaded_state
        except: pass
    return default_state

state = load_data()
state_lock = threading.Lock()

def sync_save():
    try:
        with state_lock:
            with open(DB_FILE, 'w') as f: json.dump(state, f)
    except Exception as e: print(f"Save Error: {e}")

def execute_telegram_send(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": str(TELEGRAM_CHAT_ID).strip(), "text": msg, "parse_mode": "HTML"}
    for attempt in range(3):
        try:
            res = requests.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=15)
            if res.status_code == 200: return True
            elif res.status_code == 429:
                retry_after = res.json().get('parameters', {}).get('retry_after', 5)
                time.sleep(retry_after)
        except:
            time.sleep(2)
    return False

def is_ict_trading_window():
    try:
        tz = pytz.timezone(BOT_TIMEZONE)
        tz_now = datetime.datetime.now(tz)
        total_minutes = (tz_now.hour * 60) + tz_now.minute
        with state_lock:
            start_time = (state.get('start_hour', 12) * 60) + state.get('start_minute', 30)
            end_time = (state.get('end_hour', 23) * 60) + state.get('end_minute', 59)
        return start_time <= total_minutes <= end_time
    except: return True

# --- ⏰ REMINDER THREAD WORKER ---
def signal_reminder_thread(signal_num, symbol, side, price):
    """විනාඩියෙන් විනාඩියට මතක් කිරීමේ පණිවිඩය යවන Thread එක"""
    time.sleep(60) # පළමු මතක් කිරීම විනාඩියකට පසුව
    
    while True:
        with state_lock:
            is_enabled = state.get('reminder_enabled', True)
            is_active = state.get('active_reminders', {}).get(str(signal_num), False)
            
        if not is_enabled or not is_active:
            break  # Reminder Off කර ඇත්නම් හෝ /ok ලබා දී ඇත්නම් Loop එක නතර වේ.

        msg = (f"⏰ <b>SIGNAL REMINDER (#{signal_num:02d})</b> 🔔\n\n"
               f"📍 Coin: <code>{symbol}</code> | Direction: <b>{side}</b>\n"
               f"💵 Price: <code>{price}</code>\n\n"
               f"👉 මෙම Alert එක නවතාලීමට <code>/ok</code> ලෙස Type කරන්න.")
        execute_telegram_send(msg)
        time.sleep(60) # විනාඩියක් ඉන්නවා

# --- 📊 PIVOT HIGH / LOW INDICATOR ---
def calculate_pivots(df, left=14, right=14):
    highs = df['high'].astype(float).values
    lows = df['low'].astype(float).values
    pivothigh = [np.nan] * len(df)
    pivotlow = [np.nan] * len(df)
    
    for i in range(left, len(df) - right):
        if highs[i] == max(highs[i - left:i + right + 1]):
            pivothigh[i] = highs[i]
        if lows[i] == min(lows[i - left:i + right + 1]):
            pivotlow[i] = lows[i]
            
    df['pivot_high'] = pivothigh
    df['pivot_low'] = pivotlow
    df['pivot_high'] = df['pivot_high'].ffill()
    df['pivot_low'] = df['pivot_low'].ffill()
    return df

# --- 🔍 STEP 1: SYMBOL SCANNER (FWL GENERATION) ---
def run_symbol_scanner_process():
    execute_telegram_send("🔍 <b>Binance Futures Scanner එක ආරම්භ කළා...</b>\nපැය 24 පරිමාව $40M වැඩි කාසි පරීක්ෂා කරමින් පවතී.")
    try:
        res = requests.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=15)
        all_coins = res.json()
        
        filtered_by_vol = []
        for ticker in all_coins:
            symbol = ticker['symbol']
            volume = float(ticker.get('quoteVolume', 0))
            if symbol.endswith("USDT") and volume >= 40000000:
                filtered_by_vol.append(symbol)
                
        new_fwl = []
        for s in filtered_by_vol:
            with state_lock:
                if s in state.get('block_list', []): continue
            
            try:
                time.sleep(0.5) 
                k_res = requests.get(f"https://fapi.binance.com/fapi/v1/klines?symbol={s}&interval=5m&limit=5000", timeout=15)
                df = pd.DataFrame(k_res.json(), columns=['t','open','high','low','close','v','ct','qv','nt','tb','tq','i'])
                
                closes = df['close'].astype(float)
                df['ema50'] = closes.ewm(span=50, adjust=False).mean()
                df['ema100'] = closes.ewm(span=100, adjust=False).mean()
                df['ema200'] = closes.ewm(span=200, adjust=False).mean()
                df = calculate_pivots(df, 14, 14)
                
                losses = 0
                for i in range(200, len(df)-1):
                    row = df.iloc[i]
                    next_row = df.iloc[i+1]
                    if row['ema50'] > row['ema100'] > row['ema200']:
                        if float(next_row['close']) < row['pivot_low'] * 0.997:
                            losses += 1
                    elif row['ema50'] < row['ema100'] < row['ema200']:
                        if float(next_row['close']) > row['pivot_high'] * 1.003:
                            losses += 1
                            
                if losses < 3:
                    new_fwl.append(s)
                else:
                    with state_lock:
                        if s not in state['block_list']: state['block_list'].append(s)
            except: pass
                
        with state_lock:
            state['first_win_list'] = new_fwl
        sync_save()
        
        fwl_str = " ".join(new_fwl)
        execute_telegram_send(f"✅ <b>Scanner Complete!</b>\n\n/fwl {fwl_str}")
    except Exception as e:
        execute_telegram_send(f"❌ Scanner Error: {str(e)}")

# --- 📈 DATA ANALYSIS & INDICATOR LOGIC ---
def analyze_and_check_signal(s):
    try:
        k_res = requests.get(f"https://fapi.binance.com/fapi/v1/klines?symbol={s}&interval=5m&limit=600", timeout=10)
        df = pd.DataFrame(k_res.json(), columns=['t','open','high','low','close','v','ct','qv','nt','tb','tq','i'])
        if len(df) < 250: return "NONE", 0.0
        
        closes = df['close'].astype(float)
        df['ema50'] = closes.ewm(span=50, adjust=False).mean()
        df['ema100'] = closes.ewm(span=100, adjust=False).mean()
        df['ema200'] = closes.ewm(span=200, adjust=False).mean()
        df = calculate_pivots(df, 14, 14)
        
        last_idx = len(df) - 1
        curr_price = float(closes.iloc[last_idx])
        prev_price = float(closes.iloc[last_idx - 1])
        
        ema50 = df['ema50'].iloc[last_idx]
        ema100 = df['ema100'].iloc[last_idx]
        ema200 = df['ema200'].iloc[last_idx]
        p_high = df['pivot_high'].iloc[last_idx - 1]
        p_low = df['pivot_low'].iloc[last_idx - 1]
        
        if ema50 > ema100 > ema200:
            if prev_price <= p_low and curr_price > p_low:
                return "BUY", curr_price
        elif ema50 < ema100 < ema200:
            if prev_price >= p_high and curr_price < p_high:
                return "SELL", curr_price
        return "NONE", curr_price
    except: return "NONE", 0.0

# --- 🔄 MARKET SCANNING LOOP ---
def scan_markets():
    while True:
        try:
            with state_lock:
                bot_paused = state.get('is_paused', False)
                direct_mode = state.get('direct_mode', False)
                recovery_only = state.get('recovery_only_mode', False)
                fwl = list(state.get('first_win_list', []))
                active_positions = dict(state['active_positions'])
                max_signals = state.get('max_signals', 10)

            print(f"⏰ [{datetime.datetime.now().strftime('%H:%M:%S')}] Market Scanning Active... (Positions: {len(active_positions)}/{max_signals})")

            # ------ 🔄 PENDING RECOVERY RESUME CHECK ------
            if not bot_paused and is_ict_trading_window():
                with state_lock:
                    recovery_steps_dict = dict(state.get('symbol_recovery_step', {}))
                
                for s, step in recovery_steps_dict.items():
                    if step > 0:  
                        with state_lock:
                            is_already_active = s in state['active_positions']
                            is_blacklisted = s in state.get('block_list', [])
                            current_active_count = len(state['active_positions'])
                        
                        if not is_already_active and not is_blacklisted and current_active_count < max_signals:
                            try:
                                k_res = requests.get(f"https://fapi.binance.com/fapi/v1/klines?symbol={s}&interval=5m&limit=1", timeout=10)
                                curr_p = float(k_res.json()[-1][4])
                                print(f"🔄 Resuming recovery for {s} at Step {step}")
                                execute_new_trade(s, "BUY", curr_p)
                                time.sleep(2)
                            except:
                                pass
            
            if not bot_paused and is_ict_trading_window() and not recovery_only:
                res = requests.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=15)
                symbols = [t['symbol'] for t in res.json() if t['symbol'].endswith("USDT")]
                
                for s in symbols:
                    if s in state.get('block_list', []): continue
                    if s in active_positions: continue
                    if not direct_mode and s not in fwl: continue
                    if len(active_positions) >= max_signals: break
                    
                    signal, price = analyze_and_check_signal(s)
                    if signal != "NONE":
                        print(f"🚨 SIGNAL FOUND! Coin: {s} | Signal: {signal} | Price: {price}")
                        execute_new_trade(s, signal, price)
                        time.sleep(1)
            time.sleep(5)
        except Exception as e: 
            print(f"❌ Scanner Error Loop: {e}")
            time.sleep(15)
            
# --- 🚀 ENTRY & EXECUTE TRADE ---
def execute_new_trade(s, side, current_p):
    with state_lock:
        state['signal_count'] += 1  
        signal_num = state['signal_count']
        
        step = state['symbol_recovery_step'].get(s, 0)
        accumulated_loss = state['symbol_accumulated_loss'].get(s, 0.0)
        current_margin = state.get('base_margin', 0.80)
        sl_margin_pct = state.get('margin_sl_pct', 27.0)
        leverage = state.get('leverage', 10)
        bh_balance_set = state.get('blacklist_balance_set', 0.10)
        
    if step == 0 and state.get('total_loss_cost', 0.0) >= 0.15:
        accumulated_loss += 0.15
        state['total_loss_cost'] -= 0.15

    position_size = current_margin * leverage 
    coin_sl_move_pct = (sl_margin_pct / leverage) / 100.0 
    
    if side == "BUY":
        initial_sl = current_p * (1.0 - coin_sl_move_pct)
        if step == 0:
            required_move_pct = (current_margin * (state.get('fast_tp_pct', 30.0) / 100.0) + bh_balance_set) / position_size
            initial_tp = current_p * (1.0 + required_move_pct)
        else:
            required_move_pct = (accumulated_loss + bh_balance_set + (position_size * 0.0008)) / position_size
            initial_tp = current_p * (1.0 + required_move_pct)
    else:
        initial_sl = current_p * (1.0 + coin_sl_move_pct)
        if step == 0:
            required_move_pct = (current_margin * (state.get('fast_tp_pct', 30.0) / 100.0) + bh_balance_set) / position_size
            initial_tp = current_p * (1.0 - required_move_pct)
        else:
            required_move_pct = (accumulated_loss + bh_balance_set + (position_size * 0.0008)) / position_size
            initial_tp = current_p * (1.0 - required_move_pct)
            
    with state_lock:
        state['active_positions'][s] = {
            "symbol": s, "side": side, "entry_price": current_p, "margin": current_margin,
            "step": step, "tp": initial_tp, "sl": initial_sl, "timestamp": time.time(),
            "signal_num": signal_num
        }
        if 'active_reminders' not in state: state['active_reminders'] = {}
        state['active_reminders'][str(signal_num)] = True
        
    msg = (f"🔔 <b>NEW TRADING SIGNAL #{signal_num:02d}</b> 🚨\n\n"
           f"📍 Coin: <code>{s}</code> | Direction: <b>{side}</b>\n"
           f"💵 Margin: <b>${current_margin}</b> | Leverage: <b>{leverage}x</b>\n"
           f"🎯 Target TP: <code>{round(initial_tp, 5)}</code>\n"
           f"🛑 Target SL: <code>{round(initial_sl, 5)}</code>\n"
           f"🔄 Step: <b>{step}/3</b>\n"
           f"📊 Accum. Loss: <b>${round(accumulated_loss, 2)}</b>\n\n"
           f"💡 <i>Alerts නතර කිරීමට <b>/ok</b> ලෙස Type කරන්න.</i>")
    execute_telegram_send(msg)
    sync_save()

    if state.get('reminder_enabled', True):
        threading.Thread(target=signal_reminder_thread, args=(signal_num, s, side, current_p), daemon=True).start()

# --- 🔄 LIVE MONITOR & AUTO-REVERSE ---
def live_monitor_loop():
    while True:
        try:
            with state_lock: 
                active_keys = list(state['active_positions'].keys())
            
            if active_keys:
                print(f"📊 Monitoring Active Coins: {active_keys}")

            for s in active_keys:
                with state_lock: 
                    pos = state['active_positions'].get(s)
                if not pos: continue
                
                side = pos['side']
                tp_price = pos['tp']
                sl_price = pos['sl']
                step = pos.get('step', 0)
                margin = pos.get('margin', 0.80)
                signal_num = pos.get('signal_num', None)
                leverage = state.get('leverage', 10)
                
                try:
                    k_res2 = requests.get(f"https://fapi.binance.com/fapi/v1/klines?symbol={s}&interval=5m&limit=1", timeout=10)
                    current_p = float(k_res2.json()[-1][4])
                    
                    is_tp = (side == "BUY" and current_p >= tp_price) or (side == "SELL" and current_p <= tp_price)
                    is_sl = (side == "BUY" and current_p <= sl_price) or (side == "SELL" and current_p >= sl_price)
                    
                    # 🎯 TAKE PROFIT (WIN)
                    if is_tp:
                        print(f"🎯 TP HIT for {s}! Price: {current_p}")
                        profit_amount = margin * (state.get('fast_tp_pct', 30.0) / 100.0)
                        
                        with state_lock:
                            if s in state['active_positions']:
                                del state['active_positions'][s]
                            state['symbol_recovery_step'][s] = 0
                            state['symbol_accumulated_loss'][s] = 0.0
                            
                            state['stats']['wins'] += 1
                            state['daily_stats']['wins'] += 1
                            state['daily_stats']['win_amount'] += profit_amount
                            
                            if step == 0 and s not in state.get('first_win_coins', []):
                                state['first_win_coins'].append(s)
                            
                            if signal_num and str(signal_num) in state.get('active_reminders', {}):
                                state['active_reminders'][str(signal_num)] = False
                        
                        sync_save()
                        
                        tp_msg = (f"🎯 <b>TAKE PROFIT (WIN)!</b> 🟢\n\n"
                                  f"📍 Coin: <code>{s}</code>\n"
                                  f"💰 Profit: <b>+${round(profit_amount, 2)}</b>\n"
                                  f"🏁 Exit Price: <code>{current_p}</code>\n"
                                  f"🔄 Step: <b>{step}</b> (Reset to 0)")
                        execute_telegram_send(tp_msg)

                    # 🛑 STOP LOSS (LOSS)
                    elif is_sl:
                        print(f"🛑 SL HIT for {s}! Price: {current_p}")
                        loss_amount = margin * (state.get('margin_sl_pct', 27.0) / 100.0)
                        
                        with state_lock:
                            if s in state['active_positions']:
                                del state['active_positions'][s]
                            new_step = step + 1
                            
                            state['stats']['loss'] += 1
                            state['daily_stats']['loss'] += 1
                            state['daily_stats']['loss_amount'] += loss_amount
                            
                            if signal_num and str(signal_num) in state.get('active_reminders', {}):
                                state['active_reminders'][str(signal_num)] = False
                                
                            if new_step > 3:
                                est_fee = margin * 0.0008 * leverage 
                                total_blacklist_loss = loss_amount + est_fee
                    
                                state['shared_loss_buffer'] += total_blacklist_loss
                                state['total_loss_cost'] += total_blacklist_loss
                
                                state['symbol_recovery_step'][s] = 0
                                state['symbol_accumulated_loss'][s] = 0.0
                                if s not in state['block_list']:
                                    state['block_list'].append(s)
                                if s not in state['daily_stats']['blacklist_coins']:
                                    state['daily_stats']['blacklist_coins'].append(s)
                                    
                                if s in state.get('first_win_list', []):
                                    state['first_win_list'].remove(s)
                                if s in state.get('first_win_coins', []):
                                    state['first_win_coins'].remove(s)
                                    
                                sl_msg = (f"🛑 <b>MAX RECOVERY FAILED (LOSS)!</b> 🔴\n\n"
                                          f"📍 Coin: <code>{s}</code>\n"
                                          f"💸 Loss: <b>-${round(loss_amount, 2)}</b>\n"
                                          f"🏁 Exit Price: <code>{current_p}</code>\n"
                                          f"🚫 Step 3 ඉක්මවා ඇති බැවින් මෙම කාසිය Blacklist කර FWL ලැයිස්තු වලින් ඉවත් කරන ලදී.")
                                execute_telegram_send(sl_msg)
                            else:
                                state['symbol_recovery_step'][s] = new_step
                                state['symbol_accumulated_loss'][s] = state['symbol_accumulated_loss'].get(s, 0.0) + loss_amount
                        
                        sync_save()

                except Exception as e:
                    print(f"Error checking price for {s}: {e}")
                    
            time.sleep(3) 
            
        except Exception as global_e:
            print(f"Global Error in live monitor: {global_e}")
            time.sleep(10)

# --- 📅 DAILY PERFORMANCE REPORT ---
def cron_daily_report_worker():
    while True:
        try:
            tz = pytz.timezone(BOT_TIMEZONE)
            tz_now = datetime.datetime.now(tz)
            if tz_now.hour == 23 and tz_now.minute == 59:
                with state_lock:
                    ds = state['daily_stats']
                    bl_coins = ", ".join(ds.get('blacklist_coins', [])) if ds.get('blacklist_coins') else "None"
                    
                    report = (f"📊 ✨ <b>FINAL PERFORMANCE REPORT</b>\n\n"
                              f"🟢 Wins: {ds.get('wins', 0)} ($ {round(ds.get('win_amount', 0.0), 2)})\n"
                              f"🔴 Loss: {ds.get('loss', 0)} ($ {round(ds.get('loss_amount', 0.0), 2)})\n"
                              f"Blacklist Coins: {bl_coins}")
                    
                    execute_telegram_send(report)
                    state['daily_stats'] = {'wins': 0, 'loss': 0, 'win_amount': 0.0, 'loss_amount': 0.0, 'blacklist_coins': [], 'last_reset_date': str(datetime.date.today())}
                sync_save()
                time.sleep(60)
            time.sleep(30)
        except: time.sleep(10)

# --- 💬 TELEGRAM WEBHOOK MANAGER ---
@app.route('/webhook', methods=['POST'])
def telegram_webhook():
    try:
        update = request.get_json()
        if not update or "message" not in update: return "OK", 200
        msg_obj = update["message"]; chat_id = msg_obj.get("chat", {}).get("id"); raw_text = msg_obj.get("text", "")
        
        if str(chat_id).strip() == str(TELEGRAM_CHAT_ID).strip() and raw_text:
            tokens = str(raw_text).strip().split()
            cmd = tokens[0].lower().replace("/", "")
            text = str(raw_text).strip()
            
            if cmd == "ok":
                with state_lock:
                    if 'active_reminders' in state:
                        for k in state['active_reminders']:
                            state['active_reminders'][k] = False
                sync_save()
                execute_telegram_send("✅ <b>Signal Reminder නවතාලන ලදී!</b>")

            elif cmd == "reminder_on":
                with state_lock: state['reminder_enabled'] = True
                sync_save()
                execute_telegram_send("🟢 <b>Minute Reminder Alert System එක සක්‍රීය කරන ලදී.</b>")

            elif cmd == "reminder_off":
                with state_lock: state['reminder_enabled'] = False
                sync_save()
                execute_telegram_send("🔴 <b>Minute Reminder Alert System එක අක්‍රීය කරන ලදී.</b>")

            elif cmd == "bot_on":
                with state_lock: state['is_paused'] = False
                sync_save(); execute_telegram_send("🟢 බොට් සාර්ථකව සක්‍රීය කරන ලදී. සිග්නල් සෙවීම ආරම්භ කලා!")
                
            elif cmd == "bot_off":
                with state_lock: state['is_paused'] = True
                sync_save(); execute_telegram_send("⏸️ බොට් තාවකාලිකව අක්‍රීය කරන ලදී. නව ට්‍රේඩ් ගැනීම නවතා ඇත.")

            elif cmd == "set_max_signals" and len(tokens) > 1:
                try:
                    new_limit = int(tokens[1])
                    with state_lock:
                        state['max_signals'] = new_limit
                    sync_save()
                    execute_telegram_send(f"⚙️ Active Trade Limit එක {new_limit} ලෙස සාර්ථකව වෙනස් කරන ලදී.")
                except:
                    execute_telegram_send("❌ දෝෂයකි! කරුණාකර නිවැරදි අංකයක් ලබා දෙන්න. (උදා: /set_max_signals 15)")
            
            elif cmd == "status":
                with state_lock:
                    if state.get('direct_mode'):
                        mode_str = "DIRECT MODE 🔥"
                    elif state.get('recovery_only_mode'):
                        mode_str = "RECOVERY ONLY ⚠️"
                    else:
                        mode_str = "NORMAL MODE 🔄"
                    
                    recovery_count = 0
                    for pos in state['active_positions'].values():
                        if pos.get('step', 0) > 0:
                            recovery_count += 1
                    
                    start_t = f"{state.get('start_hour', 12):02d}:{state.get('start_minute', 30):02d}"
                    end_t = f"{state.get('end_hour', 23):02d}:{state.get('end_minute', 59):02d}"
                    
                    fwl_list = state.get('first_win_list', [])
                    fwl_str = " ".join(fwl_list) if fwl_list else "None"
                    
                    fw_coins = state.get('first_win_coins', [])
                    fw_coins_str = " ".join(fw_coins) if fw_coins else "None"
                    rem_status = "සක්‍රීයයි 🟢" if state.get('reminder_enabled', True) else "අක්‍රීයයි 🔴"
                    
                    msg = (f"ℹ️ <b>[RED BULL MASTER STATUS REPORT]</b>\n"
                           f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                           f"▶️ ස්කෑනර් එන්ජිම: <b>{'අක්‍රීයයි (OFF)' if state.get('is_paused') else 'සක්‍රීයයි (ON)'}</b>\n"
                           f"🔥 Active ට්‍රේඩ් ගණන: <b>{len(state['active_positions'])} / {state.get('max_signals', 10)}</b>\n"
                           f"📢 මතක් කිරීමේ පද්ධතිය: <b>{rem_status}</b>\n"
                           f"⚙️ Mode: <b>{mode_str}</b>\n"
                           f"⏱️ BOT WINDOW STATUS : <b>{'OFFLINE 🔴' if state.get('is_paused') else 'ONLINE 🟢'}</b>\n"
                           f"⏰ සිග්නල් දෙන කාලය: <b>{start_t} - {end_t} දක්වා.</b>\n"
                           f"💵 මූලික ට්‍රේඩ් මාජින්: <b>${state.get('base_margin', 0.80)}</b>\n"
                           f"⚙️ Leverage: <b>{state.get('leverage', 10)}x</b>\n"
                           f"🛡️ SL: <b>{state.get('margin_sl_pct', 27.0)}%</b> | TP: <b>{state.get('fast_tp_pct', 30.0)}%</b>\n"
                           f"🔄 Recovery Trade ගණන: <b>{recovery_count}</b>\n"
                           f"🥇 First Win List ගණන: <b>{len(fwl_list)}</b>\n"
                           f"🏆 First Win Coins ගණන: <b>{len(fw_coins)}</b>\n"
                           f"🚫 Blacklist Coins ගණන: <b>{len(state.get('block_list', []))}</b>\n\n"
                           f"💰 <b>Total Loss Cost:</b> <b>${round(state.get('total_loss_cost', 0.0), 2)}</b>\n"
                           f"🏆 First Win Coin: <code>{fw_coins_str}</code>")
                execute_telegram_send(msg)

            elif cmd == "menu":
                menu_msg = ("📜 <b>RED BULL MASTER COMMANDS MENU</b>\n━━━━━━━━━━━━━━━━━\n"
                            "<b>1. මූලික පාලන විධානයන් (Basic Controls)</b>\n"
                            "• /bot_on : බොට් සක්‍රීය කරයි.\n"
                            "• /bot_off : බොට් තාවකාලිකව අක්‍රීය කරයි.\n"
                            "• /ok : Alert Reminder පද්ධතිය නවතාලයි.\n"
                            "• /reminder_on : Reminder Alert පද්ධතිය ON කරයි.\n"
                            "• /reminder_off : Reminder Alert පද්ධතිය OFF කරයි.\n"
                            "• /status : බොට්ගේ වත්මන් තත්ත්වය පෙන්වයි.\n"
                            "• /set_max_signals [NUMBER] : එකවර ගත හැකි උපරිම ට්‍රේඩ් ගණන වෙනස් කරයි.\n"
                            "• /menu : ප්‍රධාන විධානයන් ලැයිස්තුව ගෙන්වා ගනී.\n\n"
                            "<b>2. මාදිලි මාරු කිරීම (Mode Switching)</b>\n"
                            "• /direct_mode_on : Direct Mode සක්‍රීය කරයි.\n"
                            "• /direct_mode_off : Direct Mode අක්‍රීය කරයි (Safe Mode).\n"
                            "• /recovery_only_on : Recovery Only මාදිලිය සක්‍රීය කරයි.\n"
                            "• /recovery_only_off : Recovery Only මාදිලිය අක්‍රීය කරයි.\n\n"
                            "<b>3. කාසි ලැයිස්තු පාලනය (FWL Commands)</b>\n"
                            "• /fwl_scanner : First Win Scanner එක අතින් ක්‍රියාත්මක කරයි.\n"
                            "• /fwl_view : FWL ලැයිස්තුවේ ඇති සියලුම කාසි පෙන්වයි.\n"
                            "• /fwl_add [COIN] : කාසි අතින්ම FWL ලැයිස්තුවට එකතු කරයි.\n"
                            "• /fwl_remove [COIN] : FWL ලැයිස්තුවෙන් කාසි ඉවත් කරයි.\n"
                            "• /clear_lists : FWL ලැයිස්තුව සහ FW Coins ලැයිස්තුව සම්පූර්ණයෙන්ම හිස් කරයි.\n\n"
                            "<b>4. තහනම් කාසි ලැයිස්තුව (Blacklist Commands)</b>\n"
                            "• /blacklist_view : Blacklist කර ඇති කාසි ලැයිස්තුව පෙන්වයි.\n"
                            "• /backlist_add [COIN] : කාසි Blacklist එකට එකතු කරයි.\n"
                            "• /backlist_remo [COIN] : කාසි Blacklist එකෙන් ඉවත් කරයි.\n\n"
                            "<b>5. කාල සීමාවන් සහ ට්‍රේඩ් රීසෙට් (Time & Reset)</b>\n"
                            "• /symbol_scanner : මුළු Binance වෙළඳපොළම ස්කෑන් කිරීම ආරම්භ කරයි.\n"
                            "• /set_signal_time [START] [END] : සිග්නල් සෙවිය යුතු කාලය සකසයි.\n"
                            "• /set_fw_time [START] [END] : FW Scanner ක්‍රියාත්මක විය යුතු කාලය සකසයි.\n"
                            "• /reset_trades : පවතින Active Trades දත්ත පද්ධතියෙන් මකා දමයි.\n\n"
                            "<b>6. Buffer & Blacklist Settings</b>\n"
                            "• /blacklist_balance_set [VALUE] : Blacklist balance set අගය වෙනස් කරයි.\n"
                            "• /buffer_status : බෆර් සහ බ්ලැක්ලිස්ට් තත්ත්වය පෙන්වයි.")
                execute_telegram_send(menu_msg)

            elif cmd == "direct_mode_on":
                with state_lock: state['direct_mode'] = True
                sync_save(); execute_telegram_send("🔥 Direct Mode සක්‍රීයයි! (FWL නොමැතිව කෙලින්ම ට්‍රේඩ් විවෘත වේ)")

            elif cmd == "direct_mode_off":
                with state_lock: state['direct_mode'] = False
                sync_save(); execute_telegram_send("🛡️ Direct Mode අක්‍රීයයි! (Safe Mode - ට්‍රේඩ් ගන්නේ FWL වලින් පමණි)")

            elif cmd == "recovery_only_on":
                with state_lock: state['recovery_only_mode'] = True
                sync_save(); execute_telegram_send("⚠️ Recovery Only මාදිලිය සක්‍රීයයි! අලුත් කාසි වලට ට්‍රේඩ් ගන්නේ නැත.")

            elif cmd == "recovery_only_off":
                with state_lock: state['recovery_only_mode'] = False
                sync_save(); execute_telegram_send("🔄 Recovery Only මාදිලිය අක්‍රීයයි! සාමාන්‍ය පරිදි සියලුම ක්‍රියාවලීන් සිදුවේ.")

            elif cmd == "fwl_scanner":
                threading.Thread(target=run_symbol_scanner_process, daemon=True).start()

            elif cmd == "fwl_view":
                with state_lock: 
                    fwl = ", ".join(state.get('first_win_list', [])) if state.get('first_win_list') else "ලැයිස්තුව හිස් ය"
                    fw_coins = ", ".join(state.get('first_win_coins', [])) if state.get('first_win_coins') else "ලැයිස්තුව හිස් ය"
                execute_telegram_send(f"🥇 <b>[FIRST WIN LIST]</b>\n<code>{fwl}</code>\n\n🏆 <b>[FIRST WIN COINS]</b>\n<code>{fw_coins}</code>")

            elif cmd == "fwl_add" and len(tokens) > 1:
                with state_lock:
                    for coin in tokens[1:]:
                        c = coin.upper()
                        if c not in state['first_win_list']: state['first_win_list'].append(c)
                sync_save(); execute_telegram_send("✅ කාසි FWL ලැයිස්තුවට ඇතුලත් කලා.")

            elif cmd == "fwl_remove" and len(tokens) > 1:
                with state_lock:
                    for coin in tokens[1:]:
                        c = coin.upper()
                        if c in state['first_win_list']: state['first_win_list'].remove(c)
                sync_save(); execute_telegram_send("❌ කාසි FWL ලැයිස්තුවෙන් ඉවත් කලා.")

            elif cmd == "clear_lists":
                with state_lock: 
                    state['first_win_list'] = []
                    state['first_win_coins'] = []
                sync_save(); execute_telegram_send("🗑️ First Win ලැයිස්තු දෙකම සම්පූර්ණයෙන්ම හිස් කරන ලදී.")

            elif cmd == "blacklist_view":
                with state_lock: bl = ", ".join(state.get('block_list', [])) if state.get('block_list') else "ලැයිස්තුව හිස් ය"
                execute_telegram_send(f"🚫 <b>[BLACKLIST COINS]</b>\n<code>{bl}</code>")

            elif cmd == "backlist_add" and len(tokens) > 1:
                with state_lock:
                    for coin in tokens[1:]:
                        c = coin.upper()
                        if c not in state['block_list']: 
                            state['block_list'].append(c)
                        
                        if c in state.get('first_win_list', []):
                            state['first_win_list'].remove(c)
                        if c in state.get('first_win_coins', []):
                            state['first_win_coins'].remove(c)
                        
                sync_save(); execute_telegram_send("🚫 කාසි සාර්ථකව Blacklist එකට ඇතුලත් කර FWL ලැයිස්තු වලින් ඉවත් කලා.")

            elif cmd == "backlist_remo" and len(tokens) > 1:
                with state_lock:
                    for coin in tokens[1:]:
                        c = coin.upper()
                        if c in state['block_list']: state['block_list'].remove(c)
                sync_save(); execute_telegram_send("🟢 කාසි Blacklist එකෙන් ඉවත් කලා.")

            elif cmd == "symbol_scanner":
                threading.Thread(target=run_symbol_scanner_process, daemon=True).start()

            elif cmd == "set_signal_time" and len(tokens) > 2:
                try:
                    start = tokens[1].split(":")
                    end = tokens[2].split(":")
                    with state_lock:
                        state['start_hour'], state['start_minute'] = int(start[0]), int(start[1])
                        state['end_hour'], state['end_minute'] = int(end[0]), int(end[1])
                    sync_save(); execute_telegram_send(f"⏰ සිග්නල් සෙවීමේ කාලය {tokens[1]} සිට {tokens[2]} දක්වා සකස් කලා.")
                except: pass

            elif cmd == "set_fw_time" and len(tokens) > 2:
                try:
                    start = tokens[1].split(":")
                    end = tokens[2].split(":")
                    with state_lock:
                        state['fw_start_hour'], state['fw_start_minute'] = int(start[0]), int(start[1])
                        state['fw_end_hour'], state['fw_end_minute'] = int(end[0]), int(end[1])
                    sync_save(); execute_telegram_send(f"⏰ First Win Scanner කාලය {tokens[1]} සිට {tokens[2]} දක්වා සකස් කලා.")
                except: pass

            elif cmd == "reset_trades":
                with state_lock: state['active_positions'] = {}
                sync_save(); execute_telegram_send("🗑️ සියලුම ක්‍රියාකාරී ට්‍රේඩ් දත්ත පද්ධතියෙන් මකා දමන ලදී.")
                
            elif text.startswith('/blacklist_balance_set'):
                try:
                    parts = text.split()
                    if len(parts) > 1:
                        new_val = float(parts[1])
                        with state_lock:
                            state['blacklist_balance_set'] = new_val
                        sync_save()
                        execute_telegram_send(f"✅ Blacklist balance set updated to: {new_val}")
                    else:
                        current_val = state.get('blacklist_balance_set', 0.10)
                        execute_telegram_send(f"ℹ️ Current blacklist balance set: {current_val}\nUsage: /blacklist_balance_set 0.10")
                except Exception as e:
                    execute_telegram_send(f"❌ Error: {e}")

            elif cmd == "buffer_status" or text.startswith('/buffer_status') or text.startswith('/blacklist_amount'):
                with state_lock:
                    buf = state.get('shared_loss_buffer', 0.0)
                    tot_cost = state.get('total_loss_cost', 0.0)
                    bh_set = state.get('blacklist_balance_set', 0.10)
                
                msg = (
                    f"📊 <b>Buffer & Blacklist Status</b>\n\n"
                    f"🔹 Shared Loss Buffer: <code>{round(buf, 4)}</code>\n"
                    f"🔹 Total Loss Cost: <code>{round(tot_cost, 4)}</code>\n"
                    f"🔹 Blacklist Balance Set (TP Extra): <code>{round(bh_set, 4)}</code>"
                )
                execute_telegram_send(msg)

    except Exception as e:
        print(f"Webhook Error: {e}")
    return "OK", 200

@app.route('/', methods=['GET'])
def health(): return "RedBull Loss Recovery Master Bot Live!", 200

if __name__ == '__main__':
    threading.Thread(target=scan_markets, daemon=True).start()
    threading.Thread(target=live_monitor_loop, daemon=True).start()
    threading.Thread(target=cron_daily_report_worker, daemon=True).start()
    app.run(port=PORT, host='0.0.0.0', debug=False, use_reloader=False)​
