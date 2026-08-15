import os
import time
import json
import threading
import requests
import datetime
import traceback
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

# --- 🚨 ERROR NOTIFICATION HELPER ---
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

def notify_error(context_msg, error_obj):
    try:
        tb_str = traceback.format_exc()
        error_msg = (
            f"❌ <b>CRITICAL BOT ERROR!</b> 🚨\n\n"
            f"📍 <b>Context:</b> {context_msg}\n"
            f"💬 <b>Error:</b> <code>{str(error_obj)}</code>\n\n"
            f"📜 <b>Traceback:</b>\n<pre>{tb_str[-1000:]}</pre>"
        )
        execute_telegram_send(error_msg)
    except Exception as e:
        print(f"Failed to send error notification: {e}")

# --- 2. STATE MANAGEMENT & DATABASE ---
def load_data():
    default_state = {
        'active_positions': {},        
        'symbol_recovery_step': {},     
        'symbol_loss_details': {},      # {symbol: {'count': count, 'amounts': [amounts...]}}
        'block_list': [],  
        'signal_count': 0, 
        'is_paused': True,              
        'is_scanning': True,
        'max_signals': 10,
        'stats': {'wins': 0, 'loss': 0, 'total_pnl': 0.0, 'blacklist_coins': []},
        'daily_stats': {'wins': 0, 'loss': 0, 'win_amount': 0.0, 'loss_amount': 0.0, 'blacklist_coins': [], 'last_reset_date': str(datetime.date.today())},
        
        'first_win_list': [],         
        'first_win_coins': [], 
        'shared_loss_buffer': 0.0,       
        'total_loss_cost': 0.0,
        'total_recovered_amount': 0.0,  # නව එකතු කිරීම: මෙතෙක් cover කරන ලද අගය

        'base_margin': 0.80,            
        'margin_sl_pct': 27.0,          
        'fast_tp_pct': 30.0,            
        'rtp_pct': 45.0,                # නව RTP අගය (Default = 45%)
        'leverage': 10,                 
        'blacklist_balance_set': 0.10,
        
        'start_hour': 0,
        'start_minute': 0,
        'end_hour': 23,
        'end_minute': 59,

        'reminder_enabled': True,
        'active_reminders': {}
    }
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, 'r') as f: 
                loaded_state = json.load(f)
                for k, v in default_state.items():
                    if k not in loaded_state: loaded_state[k] = v
                return loaded_state
        except Exception as e: 
            notify_error("Load Data Error", e)
    return default_state

state = load_data()
state_lock = threading.Lock()

def sync_save():
    try:
        with state_lock:
            with open(DB_FILE, 'w') as f: 
                json.dump(state, f, indent=4)
    except Exception as e: 
        print(f"Save Error: {e}")
        notify_error("Sync Save Error", e)

def is_ict_trading_window():
    try:
        tz = pytz.timezone(BOT_TIMEZONE)
        tz_now = datetime.datetime.now(tz)
        total_minutes = (tz_now.hour * 60) + tz_now.minute
        with state_lock:
            start_time = (state.get('start_hour', 0) * 60) + state.get('start_minute', 0)
            end_time = (state.get('end_hour', 23) * 60) + state.get('end_minute', 59)
        return start_time <= total_minutes <= end_time
    except Exception as e: 
        notify_error("ICT Trading Window Error", e)
        return True

# --- ⏰ REMINDER THREAD WORKER ---
def signal_reminder_thread(signal_num, symbol, side, price):
    try:
        time.sleep(60)
        while True:
            with state_lock:
                is_enabled = state.get('reminder_enabled', True)
                is_active = state.get('active_reminders', {}).get(str(signal_num), False)
                
            if not is_enabled or not is_active:
                break

            msg = (f"⏰ <b>SIGNAL REMINDER (#{signal_num:02d})</b> 🔔\n\n"
                   f"📍 Coin: <code>{symbol}</code> | Direction: <b>{side}</b>\n"
                   f"💵 Price: <code>{price}</code>\n\n"
                   f"👉 මෙම Alert එක නවතාලීමට <code>/ok</code> ලෙස Type කරන්න.")
            execute_telegram_send(msg)
            time.sleep(60)
    except Exception as e:
        notify_error(f"Signal Reminder Thread Error ({symbol})", e)

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

# --- 📈 DATA ANALYSIS & INDICATOR LOGIC (REVERSED: BUY <-> SELL) ---
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
                return "SELL", curr_price  
        elif ema50 < ema100 < ema200:
            if prev_price >= p_high and curr_price < p_high:
                return "BUY", curr_price   
        return "NONE", curr_price
    except Exception as e: 
        return "NONE", 0.0

# --- 🔄 MANUAL FWL MARKET SCANNING LOOP ---
def scan_markets():
    while True:
        try:
            with state_lock:
                bot_paused = state.get('is_paused', False)
                fwl = list(state.get('first_win_list', []))
                active_positions = dict(state['active_positions'])
                max_signals = state.get('max_signals', 10)

            if not bot_paused and is_ict_trading_window():
                for s in fwl:
                    with state_lock:
                        if s in state.get('block_list', []): continue
                        if s in active_positions: continue
                        if len(state['active_positions']) >= max_signals: break
                    
                    signal, price = analyze_and_check_signal(s)
                    if signal != "NONE":
                        print(f"🚨 SIGNAL FOUND! Coin: {s} | Signal: {signal} | Price: {price}")
                        execute_new_trade(s, signal, price)
                        time.sleep(1)
            time.sleep(5)
        except Exception as e: 
            print(f"❌ Scanner Error Loop: {e}")
            notify_error("Market Scanner Loop Error", e)
            time.sleep(15)
            
# --- 🚀 ENTRY & EXECUTE TRADE ---
def execute_new_trade(s, side, current_p):
    try:
        with state_lock:
            state['signal_count'] += 1  
            signal_num = state['signal_count']
            
            step = state['symbol_recovery_step'].get(s, 0)
            current_margin = state.get('base_margin', 0.80)
            sl_margin_pct = state.get('margin_sl_pct', 27.0)
            leverage = state.get('leverage', 10)
            bh_balance_set = state.get('blacklist_balance_set', 0.10)
            rtp_pct = state.get('rtp_pct', 45.0)
            fast_tp_pct = state.get('fast_tp_pct', 30.0)
            
            # පරීක්ෂා කිරීම: මෙම කොයින් එකට හෝ general loss balance එකක් ඇත්දැයි බැලීම
            has_loss_balance = False
            pending_loss_to_add = 0.0
            
            if s in state.get('symbol_loss_details', {}):
                loss_info = state['symbol_loss_details'][s]
                if loss_info['count'] > 0 and loss_info['amounts']:
                    pending_loss_to_add = loss_info['amounts'].pop(0)
                    loss_info['count'] = len(loss_info['amounts'])
                    if loss_info['count'] == 0:
                        del state['symbol_loss_details'][s]
                    has_loss_balance = True

        position_size = current_margin * leverage 
        coin_sl_move_pct = (sl_margin_pct / leverage) / 100.0 
        est_fee = position_size * 0.0016  
        
        # 2 සහ 3 රීති වලට අනුව TP ගණනය කිරීම:
        # පාඩුවක් පියවා ගැනීමට ඇති විට (Recovery Mode / Loss balance තිබේ නම්) RTP ප්‍රතිශතය භාවිත කරයි.
        if has_loss_balance or pending_loss_to_add > 0.0:
            target_profit_dollar = (current_margin * (rtp_pct / 100.0)) + pending_loss_to_add + est_fee + bh_balance_set
        else:
            target_profit_dollar = (current_margin * (fast_tp_pct / 100.0)) + bh_balance_set

        required_move_pct = target_profit_dollar / position_size

        if side == "BUY":
            initial_sl = current_p * (1.0 - coin_sl_move_pct)
            initial_tp = current_p * (1.0 + required_move_pct)
        else:
            initial_sl = current_p * (1.0 + coin_sl_move_pct)
            initial_tp = current_p * (1.0 - required_move_pct)
                
        with state_lock:
            state['active_positions'][s] = {
                "symbol": s, "side": side, "entry_price": current_p, "margin": current_margin,
                "step": step, "tp": initial_tp, "sl": initial_sl, "timestamp": time.time(),
                "signal_num": signal_num, "attached_loss": pending_loss_to_add
            }
            if 'active_reminders' not in state: state['active_reminders'] = {}
            state['active_reminders'][str(signal_num)] = True
            
        msg = (f"🔔 <b>NEW TRADING SIGNAL #{signal_num:02d}</b> 🚨\n\n"
               f"📍 Coin: <code>{s}</code> | Direction: <b>{side}</b>\n"
               f"💵 Margin: <b>${current_margin}</b> | Leverage: <b>{leverage}x</b>\n"
               f"🎯 Target TP: <code>{round(initial_tp, 5)}</code>\n"
               f"🛑 Target SL: <code>{round(initial_sl, 5)}</code>\n"
               f"📈 Step: <b>{step}</b> | Attached Loss: <b>${round(pending_loss_to_add, 2)}</b>\n\n"
               f"💡 <i>Alerts නතර කිරීමට <b>/ok</b> ලෙස Type කරන්න.</i>")
        execute_telegram_send(msg)
        sync_save()

        if state.get('reminder_enabled', True):
            threading.Thread(target=signal_reminder_thread, args=(signal_num, s, side, current_p), daemon=True).start()
    except Exception as e:
        notify_error(f"Execute New Trade Error ({s})", e)

# --- 🔄 LIVE MONITOR & WIN-STEP RECOVERY LOGIC ---
def live_monitor_loop():
    while True:
        try:
            with state_lock: 
                active_keys = list(state['active_positions'].keys())
            
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
                attached_loss = pos.get('attached_loss', 0.0)
                
                try:
                    k_res2 = requests.get(f"https://fapi.binance.com/fapi/v1/klines?symbol={s}&interval=5m&limit=1", timeout=10)
                    current_p = float(k_res2.json()[-1][4])
                    
                    is_tp = (side == "BUY" and current_p >= tp_price) or (side == "SELL" and current_p <= tp_price)
                    is_sl = (side == "BUY" and current_p <= sl_price) or (side == "SELL" and current_p >= sl_price)
                    
                    if is_tp:
                        print(f"🎯 TP HIT for {s}! Price: {current_p}")
                        fast_tp_pct = state.get('fast_tp_pct', 30.0)
                        rtp_pct = state.get('rtp_pct', 45.0)
                        
                        # රීති 3 අනුව ලාභය හෝ recovery ගණනය කිරීම
                        if attached_loss > 0.0:
                            profit_amount = margin * (rtp_pct / 100.0)
                            recovered_val = attached_loss
                        else:
                            profit_amount = margin * (fast_tp_pct / 100.0)
                            recovered_val = 0.0
                        
                        with state_lock:
                            if s in state['active_positions']:
                                del state['active_positions'][s]
                            
                            new_step = step + 1
                            state['symbol_recovery_step'][s] = new_step
                            
                            state['stats']['wins'] += 1
                            state['daily_stats']['wins'] += 1
                            state['daily_stats']['win_amount'] += profit_amount
                            
                            if recovered_val > 0.0:
                                state['total_recovered_amount'] = state.get('total_recovered_amount', 0.0) + recovered_val
                            
                            if signal_num and str(signal_num) in state.get('active_reminders', {}):
                                state['active_reminders'][str(signal_num)] = False
                        
                        sync_save()
                        
                        tp_msg = (f"🎯 <b>TAKE PROFIT (WIN)!</b> 🟢\n\n"
                                  f"📍 Coin: <code>{s}</code>\n"
                                  f"💰 Profit: <b>+${round(profit_amount, 2)}</b>\n"
                                  f"🏁 Exit Price: <code>{current_p}</code>\n"
                                  f"📈 New Step: <b>{new_step}</b>")
                        execute_telegram_send(tp_msg)

                    elif is_sl:
                        print(f"🛑 SL HIT for {s}! Price: {current_p}")
                        loss_amount = margin * (state.get('margin_sl_pct', 27.0) / 100.0)
                        
                        with state_lock:
                            if s in state['active_positions']:
                                del state['active_positions'][s]
                            
                            state['symbol_recovery_step'][s] = 0
                            
                            # 1 සහ 4 රීති අනුව: SL වූ හෝ Blacklist වූ සියලුම පාඩු Loss Balance එකට එකතු කරන්න
                            if s not in state['symbol_loss_details']:
                                state['symbol_loss_details'][s] = {'count': 0, 'amounts': []}
                            state['symbol_loss_details'][s]['count'] += 1
                            state['symbol_loss_details'][s]['amounts'].append(loss_amount)
                            
                            # total_loss_cost යාවත්කාලීන කිරීම
                            state['total_loss_cost'] = state.get('total_loss_cost', 0.0) + loss_amount
                            
                            loss_count_total = state['symbol_loss_details'][s]['count']
                            if loss_count_total >= 3:
                                if s not in state['block_list']:
                                    state['block_list'].append(s)
                                if s not in state['daily_stats']['blacklist_coins']:
                                    state['daily_stats']['blacklist_coins'].append(s)
                                if s in state.get('first_win_list', []):
                                    state['first_win_list'].remove(s)
                                
                                bl_msg = (f"🚫 <b>MAX LOSS REACHED (BLACKLISTED)!</b> 🔴\n\n"
                                          f"📍 Coin: <code>{s}</code>\n"
                                          f"⚠️ මෙම කාසිය 3 වරක් Loss වී ඇති බැවින් ස්වයංක්‍රීයව Blacklist කරන ලදී.")
                                execute_telegram_send(bl_msg)
                            
                            state['stats']['loss'] += 1
                            state['daily_stats']['loss'] += 1
                            state['daily_stats']['loss_amount'] += loss_amount
                            
                            if signal_num and str(signal_num) in state.get('active_reminders', {}):
                                state['active_reminders'][str(signal_num)] = False
                                
                        sl_msg = (f"🛑 <b>STOP LOSS (LOSS)!</b> 🔴\n\n"
                                  f"📍 Coin: <code>{s}</code>\n"
                                  f"💸 Loss Amount: <b>-${round(loss_amount, 2)}</b>\n"
                                  f"🏁 Exit Price: <code>{current_p}</code>\n"
                                  f"📊 Total Losses Recorded: <b>{state['symbol_loss_details'].get(s, {}).get('count', 0)}</b>")
                        execute_telegram_send(sl_msg)
                        sync_save()

                except Exception as inner_e:
                    print(f"Error checking price for {s}: {inner_e}")
                    
            time.sleep(3) 
            
        except Exception as global_e:
            print(f"Global Error in live monitor: {global_e}")
            notify_error("Live Monitor Loop Error", global_e)
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
        except Exception as e: 
            notify_error("Cron Daily Report Worker Error", e)
            time.sleep(10)

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

            elif cmd == "botrun":
                with state_lock: state['is_paused'] = False
                sync_save(); execute_telegram_send("🚀 <b>/botrun විධානය ක්‍රියාත්මකයි!</b> බොට් දැන් සිග්නල් සෙවීම ආරම්භ කළා.")

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
                sync_save(); execute_telegram_send("🟢 බොට් සාර්ථකව සක්‍රීය කරන ලදී!")
                
            elif cmd == "bot_off":
                with state_lock: state['is_paused'] = True
                sync_save(); execute_telegram_send("⏸️ බොට් තාවකාලිකව අක්‍රීය කරන ලදී. නව ට්‍රේඩ් ගැනීම නවතා ඇත.")

            elif cmd == "loss_balance":
                with state_lock:
                    loss_details = dict(state.get('symbol_loss_details', {}))
                    tot_loss_cost = state.get('total_loss_cost', 0.0)
                    tot_recovered = state.get('total_recovered_amount', 0.0)
                
                report_lines = [
                    "📊 <b>Loss Balance වාර්තාව:</b>\n",
                    f"🔹 <b>Loss Cover කිරීමට ඇති අගය (Total Loss Cost):</b> <code>${round(tot_loss_cost, 2)}</code>",
                    f"🔹 <b>Loss Cover කරන ලද අගය (Total Recovered):</b> <code>${round(tot_recovered, 2)}</code>\n"
                ]
                
                if not loss_details:
                    report_lines.append("කිසිදු අස්ථානගත වූ Loss එකක් වාර්තා වී නැත.")
                else:
                    report_lines.append("<code>Coin Name       Loss Count    Loss Amount</code>")
                    report_lines.append("<code>-----------------------------------------</code>")
                    for coin, info in loss_details.items():
                        count = info['count']
                        total_amt = round(sum(info['amounts']), 2)
                        report_lines.append(f"<code>{coin:<15} {count:<13} {total_amt}</code>")
                
                execute_telegram_send("\n".join(report_lines))

            elif cmd == "rtp" and len(tokens) > 1:
                try:
                    new_rtp = float(tokens[1])
                    with state_lock:
                        state['rtp_pct'] = new_rtp
                    sync_save()
                    execute_telegram_send(f"⚙️ RTP (Recovery Take Profit) ප්‍රතිශතය <b>{new_rtp}%</b> ලෙස සාර්ථකව වෙනස් කරන ලදී.")
                except Exception as e:
                    execute_telegram_send("❌ දෝෂයකි! කරුණාකර නිවැරදි අංකයක් ලබා දෙන්න (උදා: /rtp 45).")

            elif cmd == "set_max_signals" and len(tokens) > 1:
                try:
                    new_limit = int(tokens[1])
                    with state_lock:
                        state['max_signals'] = new_limit
                    sync_save()
                    execute_telegram_send(f"⚙️ Active Trade Limit එක {new_limit} ලෙස සාර්ථකව වෙනස් කරන ලදී.")
                except Exception as e:
                    execute_telegram_send("❌ දෝෂයකි! කරුණාකර නිවැරදි අංකයක් ලබා දෙන්න.")
            
            elif cmd == "status":
                with state_lock:
                    start_t = f"{state.get('start_hour', 0):02d}:{state.get('start_minute', 0):02d}"
                    end_t = f"{state.get('end_hour', 23):02d}:{state.get('end_minute', 59):02d}"
                    fwl_list = state.get('first_win_list', [])
                    rem_status = "සක්‍රීයයි 🟢" if state.get('reminder_enabled', True) else "අක්‍රීයයි 🔴"
                    mode_status = "අක්‍රීයයි (OFF)" if state.get('is_paused') else "NORMAL MODE 🔄"
                    bot_window_status = "ONLINE 🟢" if is_ict_trading_window() else "OFFLINE 🔴"
                    
                    total_loss_cost_val = state.get('total_loss_cost', 0.0)
                    
                    msg = (
                        f"ℹ️ <b>[Mr. MASTER STATUS REPORT]</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"🔥 Active ට්‍රේඩ් ගණන: <b>{len(state['active_positions'])} / {state.get('max_signals', 10)}</b>\n"
                        f"📢 මතක් කිරීමේ පද්ධතිය: <b>{rem_status}</b>\n"
                        f"⚙️ Mode: <b>{mode_status}</b>\n"
                        f"⏱️ BOT WINDOW STATUS : <b>{bot_window_status}</b>\n"
                        f"⏰ සිග්නල් දෙන කාලය: <b>{start_t} - {end_t} දක්වා.</b>\n"
                        f"💵 මූලික ට්‍රේඩ් මාජින්: <b>${state.get('base_margin', 0.80)}</b>\n"
                        f"⚙️ Leverage: <b>{state.get('leverage', 10)}x</b>\n"
                        f"🛡️ SL: <b>{state.get('margin_sl_pct', 27.0)}%</b> | TP: <b>{state.get('fast_tp_pct', 30.0)}%</b> | RTP: <b>{state.get('rtp_pct', 45.0)}%</b>\n"
                        f"🔄 Recovery Trade ගණන: <b>{len(state.get('symbol_loss_details', {}))}</b>\n"
                        f"🥇 First Win List ගණන: <b>{len(fwl_list)}</b>\n"
                        f"🚫 Blacklist Coins ගණන: <b>{len(state.get('block_list', []))}</b>\n\n"
                        f"💰 Total Loss Cost: <b>${round(total_loss_cost_val, 1)}</b>\n"
                        f"🏆 First Win Coin:\n<code>{' '.join(fwl_list) if fwl_list else 'None'}</code>"
                    )
                execute_telegram_send(msg)

            elif cmd == "menu":
                menu_msg = ("📜 <b>RED BULL MASTER COMMANDS MENU</b>\n━━━━━━━━━━━━━━━━━\n"
                            "<b>1. මූලික පාලන විධානයන් (Basic Controls)</b>\n"
                            "• /botrun : බොට් ක්‍රියාත්මක කිරීම ආරම්භ කිරීම.\n"
                            "• /bot_on : බොට් සක්‍රීය කරයි.\n"
                            "• /bot_off : බොට් තාවකාලිකව අක්‍රීය කරයි.\n"
                            "• /ok : Alert Reminder පද්ධතිය නවතාලයි.\n"
                            "• /reminder_on : Reminder Alert පද්ධතිය ON කරයි.\n"
                            "• /reminder_off : Reminder Alert පද්ධතිය OFF කරයි.\n"
                            "• /status : බොට්ගේ වත්මන් තත්ත්වය පෙන්වයි.\n"
                            "• /rtp [VALUE] : Recovery Take Profit ප්‍රතිශතය සකසයි.\n"
                            "• /set_max_signals [NUMBER] : එකවර ගත හැකි උපරිම ට්‍රේඩ් ගණන වෙනස් කරයි.\n"
                            "• /menu : ප්‍රධාන විධානයන් ලැයිස්තුව ගෙන්වා ගනී.\n"
                            "• /loss_balance : එකතු වූ loss සහ recovered අගයන් පෙන්වයි.\n\n"
                            "<b>2. කාසි ලැයිස්තු පාලනය (FWL Commands)</b>\n"
                            "• /fwl_add [COIN] : කාසි අතින්ම FWL ලැයිස්තුවට එකතු කරයි.\n"
                            "• /fwl_remove [COIN] : කාසි ලැයිස්තුවෙන් ඉවත් කරයි.\n"
                            "• /clear_lists : FWL ලැයිස්තුව සම්පූර්ණයෙන්ම හිස් කරයි.\n\n"
                            "<b>3. තහනම් කාසි ලැයිස්තුව (Blacklist Commands)</b>\n"
                            "• /blacklist_view : Blacklist කර ඇති කාසි ලැයිස්තුව පෙන්වයි.\n"
                            "• /backlist_add [COIN] : කාසි Blacklist එකට එකතු කරයි.\n"
                            "• /backlist_remo [COIN] : කාසි Blacklist එකෙන් ඉවත් කරයි.\n\n"
                            "<b>4. කාල සීමාවන් සහ ට්‍රේඩ් රීසෙට් (Time & Reset)</b>\n"
                            "• /set_signal_time [START] [END] : සිග්නල් සෙවිය යුතු කාලය සකසයි.\n"
                            "• /reset_trades : පවතින Active Trades දත්ත පද්ධතියෙන් මකා දමයි.\n\n"
                            "<b>5. Buffer & Blacklist Settings</b>\n"
                            "• /blacklist_balance_set [VALUE] : Blacklist balance set අගය වෙනස් කරයි.\n"
                            "• /buffer_status : බෆර් සහ බ්ලැක්ලිස්ට් තත්ත්වය පෙන්වයි.")
                execute_telegram_send(menu_msg)

            elif cmd == "fwl_add" and len(tokens) > 1:
                with state_lock:
                    for coin in tokens[1:]:
                        c = coin.upper()
                        if not c.endswith("USDT"): c += "USDT"
                        if c not in state['first_win_list']: state['first_win_list'].append(c)
                sync_save(); execute_telegram_send("✅ කාසි FWL ලැයිස්තුවට සාර්ථකව ඇතුළත් කළා.")

            elif cmd == "fwl_remove" and len(tokens) > 1:
                with state_lock:
                    for coin in tokens[1:]:
                        c = coin.upper()
                        if c in state['first_win_list']: state['first_win_list'].remove(c)
                sync_save(); execute_telegram_send("❌ කාසි FWL ලැයිස්තුවෙන් ඉවත් කළා.")

            elif cmd == "clear_lists":
                with state_lock: state['first_win_list'] = []
                sync_save(); execute_telegram_send("🗑️ First Win ලැයිස්තුව සම්පූර්ණයෙන්ම හිස් කරන ලදී.")

            elif cmd == "blacklist_view":
                with state_lock: bl = ", ".join(state.get('block_list', [])) if state.get('block_list') else "ලැයිස්තුව හිස් ය"
                execute_telegram_send(f"🚫 <b>[BLACKLIST COINS]</b>\n<code>{bl}</code>")

            elif cmd == "backlist_add" and len(tokens) > 1:
                with state_lock:
                    for coin in tokens[1:]:
                        c = coin.upper()
                        if c not in state['block_list']: state['block_list'].append(c)
                        if c in state.get('first_win_list', []): state['first_win_list'].remove(c)
                sync_save(); execute_telegram_send("🚫 කාසි සාර්ථකව Blacklist එකට ඇතුළත් කළා.")

            elif cmd == "backlist_remo" and len(tokens) > 1:
                with state_lock:
                    for coin in tokens[1:]:
                        c = coin.upper()
                        if c in state['block_list']: state['block_list'].remove(c)
                sync_save(); execute_telegram_send("🟢 කාසි Blacklist එකෙන් ඉවත් කළා.")

            elif cmd == "set_signal_time" and len(tokens) > 2:
                try:
                    start = tokens[1].split(":")
                    end = tokens[2].split(":")
                    with state_lock:
                        state['start_hour'], state['start_minute'] = int(start[0]), int(start[1])
                        state['end_hour'], state['end_minute'] = int(end[0]), int(end[1])
                    sync_save(); execute_telegram_send(f"⏰ සිග්නල් සෙවීමේ කාලය {tokens[1]} සිට {tokens[2]} දක්වා සකස් කළා.")
                except Exception as e: pass

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
                        execute_telegram_send(f"ℹ️ Current blacklist balance set: {current_val}")
                except Exception as e:
                    execute_telegram_send(f"❌ Error: {e}")

            elif cmd == "buffer_status" or text.startswith('/buffer_status'):
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
        notify_error("Telegram Webhook Error", e)
    return "OK", 200

@app.route('/', methods=['GET'])
def health(): return "RedBull Loss Recovery Master Bot Live!", 200

if __name__ == '__main__':
    threading.Thread(target=scan_markets, daemon=True).start()
    threading.Thread(target=live_monitor_loop, daemon=True).start()
    threading.Thread(target=cron_daily_report_worker, daemon=True).start()
    app.run(port=PORT, host='0.0.0.0', debug=False, use_reloader=False)
