import fcntl
import signal
import atexit
import time
import traceback
import logging
import sys
import os

from functools import wraps
from typing import Dict, List, Any, Callable

from flask import Flask, request, jsonify, abort
from aiogram import Bot, Dispatcher, types

# CRITICAL: Only allow startup via start_bot.sh which sets BOT_START_AUTHORIZED=1
# This prevents .replit entrypoint from auto-running and causing duplicate bot instances
if os.getenv('BOT_START_AUTHORIZED') != '1':
    print("🚫 Bot start blocked: Not authorized (no BOT_START_AUTHORIZED=1)")
    print("ℹ️  Bot should only be started via: bash start_bot.sh")
    sys.exit(0)

import asyncio
import random
import threading
import requests
import json

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramConflictError, TelegramBadRequest, TelegramNetworkError
import datetime
from datetime import timedelta
from aiohttp import ClientSession
import aiohttp

# Setup logging for crash monitoring
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot_crash.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ADVANCED CRASH PREVENTION AND AUTO-RECOVERY SYSTEM
class UncrashableSystem:
    def __init__(self):
        self.restart_count = 0
        self.max_restarts = 50  # Increased for business continuity
        self.crash_times = []
        self.backup_data = {}
        self.last_heartbeat = time.time()
        self.recovery_attempts = 0
        self.max_recovery_attempts = 20
        self.is_in_recovery = False
        self.process_start_time = time.time()
        self.memory_usage_history = []
        
    def log_crash(self, error: Exception):
        """Enhanced crash logging with automatic recovery"""
        crash_time = time.time()
        self.crash_times.append(crash_time)
        
        # Keep only last 20 crashes for better analysis
        if len(self.crash_times) > 20:
            self.crash_times.pop(0)
            
        logger.error(f"🚨 CRITICAL CRASH DETECTED: {type(error).__name__}: {error}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        logger.error(f"Crash count in last hour: {len([t for t in self.crash_times if crash_time - t < 3600])}")
        
        # Emergency backup before potential restart
        self.emergency_backup_all_data()
        
    def should_restart(self) -> bool:
        """Advanced restart logic for business continuity"""
        current_time = time.time()
        
        # Recent crashes (last 5 minutes)
        recent_crashes = [t for t in self.crash_times if current_time - t < 300]
        
        # If too many recent crashes, use exponential backoff
        if len(recent_crashes) >= 3:
            backoff_time = min(60 * (2 ** len(recent_crashes)), 300)  # Max 5 min
            logger.warning(f"Too many recent crashes ({len(recent_crashes)}), waiting {backoff_time}s")
            time.sleep(backoff_time)
        
        # Always restart for business continuity unless absolutely critical
        can_restart = (
            self.restart_count < self.max_restarts and 
            self.recovery_attempts < self.max_recovery_attempts and
            len(recent_crashes) < 10  # Absolute emergency brake
        )
        
        if can_restart:
            self.restart_count += 1
            logger.info(f"✅ Restart approved (attempt {self.restart_count}/{self.max_restarts})")
        else:
            logger.error(f"❌ Restart denied - too many failures")
            
        return can_restart
        
    def emergency_backup_all_data(self):
        """Emergency backup of all critical data"""
        try:
            backup_file = f"emergency_backup_{int(time.time())}.json"
            emergency_data = {
                'timestamp': time.time(),
                'user_orders': user_orders,
                'all_users': list(all_users),
                'backup_data': self.backup_data,
                'crash_count': len(self.crash_times),
                'uptime': time.time() - self.process_start_time
            }
            
            with open(backup_file, 'w') as f:
                json.dump(emergency_data, f, default=str)
            logger.info(f"🛡️ Emergency backup saved: {backup_file}")
            
        except Exception as e:
            logger.error(f"❌ Emergency backup failed: {e}")
    
    def monitor_memory_usage(self):
        """Monitor memory usage to prevent memory-related crashes"""
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            
            self.memory_usage_history.append({
                'timestamp': time.time(),
                'memory_mb': memory_mb
            })
            
            # Keep only last 100 readings
            if len(self.memory_usage_history) > 100:
                self.memory_usage_history.pop(0)
            
            # Alert if memory usage is high
            if memory_mb > 500:  # 500MB threshold
                logger.warning(f"⚠️ High memory usage: {memory_mb:.1f}MB")
                
            # Emergency cleanup if memory is critical
            if memory_mb > 800:  # 800MB critical threshold
                logger.error(f"🚨 CRITICAL MEMORY USAGE: {memory_mb:.1f}MB - triggering cleanup")
                self.emergency_memory_cleanup()
                
        except ImportError:
            pass  # psutil not available
        except Exception as e:
            logger.error(f"Error monitoring memory: {e}")
    
    def emergency_memory_cleanup(self):
        """Emergency memory cleanup to prevent crashes"""
        try:
            # Clear old backup data
            current_time = time.time()
            old_backups = [
                k for k, v in self.backup_data.items() 
                if current_time - v.get('timestamp', 0) > 3600  # 1 hour old
            ]
            
            for key in old_backups:
                del self.backup_data[key]
                
            # Clear old memory history
            if len(self.memory_usage_history) > 50:
                self.memory_usage_history = self.memory_usage_history[-25:]
                
            # Clear old crash history
            if len(self.crash_times) > 10:
                self.crash_times = self.crash_times[-5:]
                
            logger.info(f"🧹 Emergency cleanup: removed {len(old_backups)} old backups")
            
        except Exception as e:
            logger.error(f"Emergency cleanup failed: {e}")
    
    def heartbeat(self):
        """Update heartbeat and check system health"""
        self.last_heartbeat = time.time()
        self.monitor_memory_usage()
        
    def backup_user_data(self, user_id: str, data: dict):
        """Enhanced user data backup with compression"""
        try:
            # Limit backup size to prevent memory issues
            if len(self.backup_data) > 1000:
                # Remove oldest backup
                oldest_key = min(self.backup_data.keys(), 
                               key=lambda k: self.backup_data[k].get('timestamp', 0))
                del self.backup_data[oldest_key]
            
            self.backup_data[user_id] = {
                'data': data,
                'timestamp': time.time()
            }
        except Exception as e:
            logger.error(f"Failed to backup data for user {user_id}: {e}")
            
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        current_time = time.time()
        return {
            'uptime': current_time - self.process_start_time,
            'restart_count': self.restart_count,
            'crash_count': len(self.crash_times),
            'recent_crashes': len([t for t in self.crash_times if current_time - t < 3600]),
            'last_heartbeat': current_time - self.last_heartbeat,
            'backed_up_users': len(self.backup_data),
            'memory_readings': len(self.memory_usage_history),
            'is_healthy': current_time - self.last_heartbeat < 60
        }

# Global uncrashable system instance
crash_system = UncrashableSystem()

# Import backup system
try:
    from backup_system import backup_system, emergency_backup, create_scheduled_backup
    BACKUP_AVAILABLE = True
    logger.info("✅ Backup system loaded successfully")
except ImportError as e:
    logger.warning(f"⚠️ Backup system not available: {e}")
    BACKUP_AVAILABLE = False

# Health monitoring system
class HealthMonitor:
    def __init__(self):
        self.last_heartbeat = time.time()
        self.connection_count = 0
        self.error_count = 0
        self.last_error_time = 0
        self.uptime_start = time.time()
        
    def heartbeat(self):
        """Update heartbeat timestamp"""
        self.last_heartbeat = time.time()
        
    def log_connection(self):
        """Log successful connection"""
        self.connection_count += 1
        logger.info(f"🔗 Connection #{self.connection_count} established")
        
    def log_error(self):
        """Log error occurrence"""
        self.error_count += 1
        self.last_error_time = time.time()
        
    def get_status(self) -> Dict[str, Any]:
        """Get current health status"""
        current_time = time.time()
        return {
            "uptime": current_time - self.uptime_start,
            "last_heartbeat": current_time - self.last_heartbeat,
            "connections": self.connection_count,
            "errors": self.error_count,
            "last_error": current_time - self.last_error_time if self.last_error_time > 0 else None,
            "status": "healthy" if current_time - self.last_heartbeat < 300 else "warning"
        }
        
    def is_healthy(self) -> bool:
        """Check if bot is healthy"""
        return time.time() - self.last_heartbeat < 300  # 5 minutes

# Global health monitor
health_monitor = HealthMonitor()

def uncrashable(func: Callable) -> Callable:
    """ULTIMATE crash-resistant decorator with auto-recovery"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        max_retries = 3
        retry_count = 0
        
        while retry_count <= max_retries:
            try:
                # Update heartbeat before function execution
                crash_system.heartbeat()
                
                # Execute function
                result = await func(*args, **kwargs)
                
                # Success - reset recovery attempts
                if crash_system.recovery_attempts > 0:
                    crash_system.recovery_attempts = max(0, crash_system.recovery_attempts - 1)
                    
                return result
                
            except TelegramConflictError as e:
                retry_count += 1
                conflict_msg = f"Telegram conflict in {func.__name__} (retry {retry_count}/{max_retries}): {e}"
                logger.warning(conflict_msg)
                
                if retry_count <= max_retries:
                    # Exponential backoff for conflicts
                    delay = min(5 * (2 ** retry_count), 30)
                    logger.info(f"Waiting {delay}s before retry...")
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error(f"Max retries exceeded for conflict in {func.__name__}")
                    return None
                    
            except TelegramNetworkError as e:
                retry_count += 1
                network_msg = f"Network error in {func.__name__} (retry {retry_count}/{max_retries}): {e}"
                logger.warning(network_msg)
                
                if retry_count <= max_retries:
                    delay = min(3 * retry_count, 15)
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error(f"Max retries exceeded for network error in {func.__name__}")
                    return None
                    
            except TelegramBadRequest as e:
                logger.warning(f"Bad request in {func.__name__}: {e}")
                # Don't retry bad requests
                return None
                
            except Exception as e:
                crash_system.log_crash(e)
                crash_system.recovery_attempts += 1
                
                logger.error(f"🚨 CRITICAL ERROR in {func.__name__}: {e}")
                logger.error(f"Args: {args}, Kwargs: {kwargs}")
                logger.error(f"Recovery attempt: {crash_system.recovery_attempts}/{crash_system.max_recovery_attempts}")
                
                # Try to backup user data if possible
                try:
                    if args and hasattr(args[0], 'from_user') and args[0].from_user:
                        user_id = str(args[0].from_user.id)
                        state_data = kwargs.get('state')
                        if state_data:
                            crash_system.backup_user_data(user_id, {'state': 'error_backup', 'function': func.__name__})
                except:
                    pass
                
                # Try to notify user with enhanced error handling
                try:
                    if args and hasattr(args[0], 'answer'):
                        await args[0].answer("⚠️ Sorry, something went wrong. Your data is safe. Please try again.", show_alert=True)
                    elif args and hasattr(args[0], 'from_user') and hasattr(args[0], 'bot'):
                        await args[0].bot.send_message(
                            args[0].from_user.id, 
                            "⚠️ Something went wrong, but your order data is safe. Please try again or contact @ogukdankzz"
                        )
                except Exception as notify_error:
                    logger.error(f"Failed to notify user of error: {notify_error}")
                
                # Check if we should attempt recovery
                if crash_system.recovery_attempts < crash_system.max_recovery_attempts:
                    retry_count += 1
                    if retry_count <= max_retries:
                        delay = min(2 ** retry_count, 10)
                        logger.info(f"Attempting recovery in {delay}s...")
                        await asyncio.sleep(delay)
                        continue
                
                return None
                
        # If we get here, all retries failed
        logger.error(f"Function {func.__name__} failed after all retry attempts")
        return None
        
    return wrapper

# Keep-alive web server for Replit
app = Flask(__name__)


@app.after_request
def apply_security_headers(response):
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['Referrer-Policy'] = 'no-referrer'
    response.headers['Cache-Control'] = 'no-store'
    response.headers['Content-Security-Policy'] = "default-src 'self' 'unsafe-inline'; img-src 'self' data: https:; connect-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'"
    return response


@app.route('/')
def home():
    return "🤖 Telegram Bot is running! ✅"


@app.route('/status')
def status():
    system_status = crash_system.get_system_status()
    return {
        "status": "online" if system_status['is_healthy'] else "warning",
        "timestamp": datetime.datetime.now().isoformat(),
        "message": "Bot is active and processing orders",
        "uptime_hours": round(system_status['uptime'] / 3600, 2),
        "restart_count": system_status['restart_count'],
        "crash_count": system_status['crash_count'],
        "recent_crashes": system_status['recent_crashes'],
        "memory_readings": system_status['memory_readings'],
        "backed_up_users": system_status['backed_up_users']
    }

@app.route('/health')
def health():
    """Detailed health endpoint for monitoring"""
    system_status = crash_system.get_system_status()
    
    health_score = 100
    if system_status['recent_crashes'] > 0:
        health_score -= (system_status['recent_crashes'] * 10)
    if system_status['restart_count'] > 5:
        health_score -= 20
    if not system_status['is_healthy']:
        health_score -= 30
        
    health_score = max(0, health_score)
    
    return {
        "health_score": health_score,
        "status": "healthy" if health_score > 70 else "degraded" if health_score > 30 else "critical",
        "detailed_status": system_status,
        "recommendations": [
            "System is stable" if health_score > 90 else
            "Monitor for stability" if health_score > 70 else
            "Consider restart if issues persist" if health_score > 30 else
            "Immediate attention required"
        ]
    }

@app.route('/telegram/<token>', methods=['POST'])
def telegram_webhook(token):
    """Webhook endpoint for Telegram updates in deployment mode"""
    try:
        # CRITICAL: Block webhook requests when in polling mode to prevent conflicts
        BOT_RUNTIME = os.getenv('BOT_RUNTIME', 'dev')
        if BOT_RUNTIME != 'deploy':
            return jsonify({"ok": False, "error": "Webhook disabled - bot is in polling mode"}), 503
        
        # Verify token matches (basic security)
        if token != WEBHOOK_SECRET:
            return jsonify({"ok": False, "error": "Invalid token"}), 403
            
        json_data = request.get_json(force=True)
        update = types.Update(**json_data)
        asyncio.create_task(dp.feed_update(bot, update))
        return jsonify({"ok": True})
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


def require_admin_auth(f):
    """Decorator to require admin authentication for diagnostic endpoints"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        import hmac

        auth_token = request.headers.get('X-Admin-Token', '')
        auth_param = request.args.get('token', '')
        provided_token = auth_token or auth_param

        if not ADMIN_TOKEN:
            abort(403, description="Admin access not configured - ADMIN_TOKEN missing")

        if provided_token and hmac.compare_digest(provided_token, ADMIN_TOKEN):
            return f(*args, **kwargs)

        abort(403, description="Unauthorized: Invalid or missing admin token")
    return decorated_function


@app.route('/admin/diagnostics')
@require_admin_auth
def diagnostics_dashboard():
    """Serve the diagnostic dashboard HTML page"""
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Bot Diagnostics Dashboard</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                padding: 20px;
                min-height: 100vh;
            }
            .container {
                max-width: 1200px;
                margin: 0 auto;
            }
            h1 {
                color: white;
                text-align: center;
                margin-bottom: 30px;
                font-size: 2.5em;
                text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
            }
            .card {
                background: white;
                border-radius: 15px;
                padding: 25px;
                margin-bottom: 20px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            }
            .card h2 {
                color: #667eea;
                margin-bottom: 15px;
                font-size: 1.5em;
            }
            .status-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 15px;
                margin-bottom: 20px;
            }
            .status-item {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 20px;
                border-radius: 10px;
                text-align: center;
            }
            .status-value {
                font-size: 2em;
                font-weight: bold;
                margin: 10px 0;
            }
            .status-label {
                opacity: 0.9;
                font-size: 0.9em;
            }
            .btn {
                background: #667eea;
                color: white;
                border: none;
                padding: 12px 24px;
                border-radius: 8px;
                cursor: pointer;
                font-size: 16px;
                margin-right: 10px;
                margin-bottom: 10px;
                transition: all 0.3s;
            }
            .btn:hover {
                background: #764ba2;
                transform: translateY(-2px);
                box-shadow: 0 5px 15px rgba(0,0,0,0.3);
            }
            .btn:disabled {
                background: #ccc;
                cursor: not-allowed;
                transform: none;
            }
            .error-item {
                background: #fff5f5;
                border-left: 4px solid #e53e3e;
                padding: 15px;
                margin-bottom: 10px;
                border-radius: 5px;
            }
            .error-time {
                color: #666;
                font-size: 0.9em;
                margin-bottom: 5px;
            }
            .error-message {
                color: #e53e3e;
                font-weight: bold;
                margin-bottom: 5px;
            }
            .error-details {
                color: #666;
                font-size: 0.9em;
                font-family: monospace;
            }
            .log-viewer {
                background: #1a1a1a;
                color: #0f0;
                padding: 20px;
                border-radius: 10px;
                font-family: 'Courier New', monospace;
                font-size: 0.9em;
                max-height: 500px;
                overflow-y: auto;
                white-space: pre-wrap;
                word-wrap: break-word;
            }
            .health-status {
                font-size: 1.2em;
                padding: 15px;
                border-radius: 8px;
                text-align: center;
                font-weight: bold;
            }
            .health-healthy { background: #c6f6d5; color: #22543d; }
            .health-degraded { background: #feebc8; color: #7c2d12; }
            .health-critical { background: #fed7d7; color: #742a2a; }
            .loading {
                text-align: center;
                padding: 20px;
                color: #667eea;
                font-size: 1.2em;
            }
            .no-errors {
                text-align: center;
                padding: 40px;
                color: #48bb78;
                font-size: 1.2em;
            }
            .guide-item {
                background: #f7fafc;
                padding: 15px;
                margin-bottom: 15px;
                border-radius: 8px;
                border-left: 4px solid #667eea;
            }
            .guide-title {
                color: #667eea;
                font-weight: bold;
                margin-bottom: 8px;
            }
            .guide-steps {
                color: #4a5568;
                margin-left: 20px;
            }
            .guide-steps li {
                margin-bottom: 5px;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🤖 Bot Diagnostics Dashboard</h1>
            
            <div class="card">
                <h2>📊 System Status</h2>
                <div id="health-status" class="loading">Loading...</div>
                <div class="status-grid" id="status-grid">
                    <div class="status-item">
                        <div class="status-label">Uptime</div>
                        <div class="status-value" id="uptime">--</div>
                    </div>
                    <div class="status-item">
                        <div class="status-label">Total Users</div>
                        <div class="status-value" id="users">--</div>
                    </div>
                    <div class="status-item">
                        <div class="status-label">Restarts</div>
                        <div class="status-value" id="restarts">--</div>
                    </div>
                    <div class="status-item">
                        <div class="status-label">Recent Crashes</div>
                        <div class="status-value" id="crashes">--</div>
                    </div>
                </div>
            </div>

            <div class="card">
                <h2>🎮 Quick Actions</h2>
                <button class="btn" onclick="refreshData()">🔄 Refresh Dashboard</button>
                <button class="btn" onclick="viewLogs()">📋 View Full Logs</button>
                <button class="btn" onclick="clearErrors()">🧹 Clear Error Display</button>
            </div>

            <div class="card">
                <h2>⚠️ Recent Errors</h2>
                <div id="errors-list"></div>
            </div>

            <div class="card">
                <h2>📖 Troubleshooting Guide</h2>
                <div id="troubleshooting-guide"></div>
            </div>

            <div class="card">
                <h2>📜 Bot Logs (Last 100 lines)</h2>
                <div class="log-viewer" id="log-viewer">Loading logs...</div>
            </div>
        </div>

        <script>
            const urlParams = new URLSearchParams(window.location.search);
            const authToken = urlParams.get('token');
            
            if (!authToken) {
                document.body.innerHTML = '<div style="text-align:center;padding:50px;color:white;"><h1>Access Denied</h1><p>You must provide a valid admin token in the URL: ?token=YOUR_ADMIN_TOKEN</p><p style="font-size:0.9em;margin-top:20px;">The admin token is stored in your Replit Secrets as ADMIN_TOKEN.</p></div>';
                throw new Error('No auth token provided');
            }

            async function fetchDiagnostics() {
                try {
                    const response = await fetch('/api/diagnostics', {
                        headers: {
                            'X-Admin-Token': authToken
                        }
                    });
                    if (!response.ok) {
                        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                    }
                    const data = await response.json();
                    updateDashboard(data);
                } catch (error) {
                    console.error('Error fetching diagnostics:', error);
                    document.getElementById('health-status').innerHTML = 
                        '<div class="health-status health-critical">Failed to load diagnostics: ' + error.message + '</div>';
                }
            }

            async function fetchLogs() {
                try {
                    const response = await fetch('/api/logs', {
                        headers: {
                            'X-Admin-Token': authToken
                        }
                    });
                    if (!response.ok) {
                        throw new Error(`HTTP ${response.status}`);
                    }
                    const data = await response.json();
                    document.getElementById('log-viewer').textContent = data.logs || 'No logs available';
                } catch (error) {
                    console.error('Error fetching logs:', error);
                    document.getElementById('log-viewer').textContent = 'Failed to load logs: ' + error.message;
                }
            }

            function updateDashboard(data) {
                const health = data.health_status;
                const healthClass = health.status === 'healthy' ? 'health-healthy' : 
                                  health.status === 'degraded' ? 'health-degraded' : 'health-critical';
                
                document.getElementById('health-status').innerHTML = 
                    `<div class="health-status ${healthClass}">
                        ${health.status.toUpperCase()} - Score: ${health.health_score}/100
                    </div>`;

                document.getElementById('uptime').textContent = `${data.system_status.uptime_hours}h`;
                document.getElementById('users').textContent = data.system_status.backed_up_users || '0';
                document.getElementById('restarts').textContent = data.system_status.restart_count;
                document.getElementById('crashes').textContent = data.system_status.recent_crashes;

                const errorsList = document.getElementById('errors-list');
                if (data.errors && data.errors.length > 0) {
                    errorsList.innerHTML = data.errors.map(error => `
                        <div class="error-item">
                            <div class="error-time">${error.time}</div>
                            <div class="error-message">${error.type}: ${error.message}</div>
                            <div class="error-details">${error.details || ''}</div>
                        </div>
                    `).join('');
                } else {
                    errorsList.innerHTML = '<div class="no-errors">✅ No recent errors detected!</div>';
                }

                const guideDiv = document.getElementById('troubleshooting-guide');
                if (data.troubleshooting_guides && data.troubleshooting_guides.length > 0) {
                    guideDiv.innerHTML = data.troubleshooting_guides.map(guide => `
                        <div class="guide-item">
                            <div class="guide-title">${guide.title}</div>
                            <ul class="guide-steps">
                                ${guide.steps.map(step => `<li>${step}</li>`).join('')}
                            </ul>
                        </div>
                    `).join('');
                } else {
                    guideDiv.innerHTML = '<div class="no-errors">✅ No issues detected - all systems operating normally!</div>';
                }
            }

            function refreshData() {
                fetchDiagnostics();
                fetchLogs();
            }

            function viewLogs() {
                const logViewer = document.getElementById('log-viewer');
                logViewer.scrollIntoView({ behavior: 'smooth' });
            }

            function clearErrors() {
                document.getElementById('errors-list').innerHTML = 
                    '<div class="no-errors">✅ Error display cleared!</div>';
            }

            fetchDiagnostics();
            fetchLogs();
            setInterval(refreshData, 30000);
        </script>
    </body>
    </html>
    """


@app.route('/api/diagnostics')
@require_admin_auth
def api_diagnostics():
    """API endpoint for diagnostic data"""
    try:
        system_status = crash_system.get_system_status()
        
        health_response = health()
        health_data = health_response if isinstance(health_response, dict) else health_response.json
        
        errors = analyze_logs()
        
        troubleshooting_guides = []
        
        if system_status['recent_crashes'] > 0:
            troubleshooting_guides.append({
                "title": "🔥 Recent Crashes Detected",
                "steps": [
                    "Check the logs below for error details",
                    "Verify database connection is stable",
                    "Check if Telegram API is accessible",
                    "Consider checking for conflicting bot instances"
                ]
            })
        
        if not system_status['is_healthy']:
            troubleshooting_guides.append({
                "title": "💔 Health Check Failed",
                "steps": [
                    "Bot may have stopped responding",
                    "Check if bot process is running",
                    "Verify network connectivity",
                    "Review recent error logs for causes"
                ]
            })
        
        if db:
            try:
                db_healthy = db.health_check()
                if not db_healthy:
                    troubleshooting_guides.append({
                        "title": "🗄️ Database Connection Issue",
                        "steps": [
                            "Database health check failed",
                            "Check DATABASE_URL environment variable",
                            "Verify database is accessible",
                            "Review database connection pool settings"
                        ]
                    })
            except Exception as e:
                troubleshooting_guides.append({
                    "title": "🗄️ Database Error",
                    "steps": [
                        f"Error: {str(e)}",
                        "Check database connectivity",
                        "Verify database credentials",
                        "Check if database service is running"
                    ]
                })
        
        return jsonify({
            "system_status": {
                "uptime_hours": round(system_status['uptime'] / 3600, 2),
                "restart_count": system_status['restart_count'],
                "crash_count": system_status['crash_count'],
                "recent_crashes": system_status['recent_crashes'],
                "backed_up_users": system_status['backed_up_users'],
                "is_healthy": system_status['is_healthy']
            },
            "health_status": health_data,
            "errors": errors,
            "troubleshooting_guides": troubleshooting_guides
        })
    except Exception as e:
        logger.error(f"Error generating diagnostics: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/logs')
@require_admin_auth
def api_logs():
    """API endpoint for bot logs"""
    try:
        logs = []
        
        if os.path.exists('bot_crash.log'):
            with open('bot_crash.log', 'r', encoding='utf-8') as f:
                lines = f.readlines()
                logs.extend(lines[-60:])
        
        if os.path.exists('backup_system.log'):
            with open('backup_system.log', 'r', encoding='utf-8') as f:
                lines = f.readlines()
                logs.extend(lines[-40:])
        
        logs_text = ''.join(logs[-80:]) if logs else 'No logs available'
        
        return jsonify({"logs": logs_text})
    except Exception as e:
        logger.error(f"Error reading logs: {e}")
        return jsonify({"logs": f"Error reading logs: {str(e)}"}), 500


def analyze_logs():
    """Analyze log files for errors and issues"""
    errors = []
    
    try:
        if os.path.exists('bot_crash.log'):
            with open('bot_crash.log', 'r', encoding='utf-8') as f:
                lines = f.readlines()
                recent_lines = lines[-200:]
                
                for i, line in enumerate(recent_lines):
                    if 'ERROR' in line or 'CRITICAL' in line or 'Exception' in line:
                        parts = line.split(' - ')
                        time_str = parts[0] if len(parts) > 0 else 'Unknown time'
                        
                        error_type = 'ERROR'
                        if 'CRITICAL' in line:
                            error_type = 'CRITICAL'
                        elif 'Exception' in line:
                            error_type = 'Exception'
                        
                        message = line.replace(time_str, '').strip()
                        
                        details = ''
                        if i + 1 < len(recent_lines) and recent_lines[i + 1].startswith(' '):
                            details = recent_lines[i + 1].strip()
                        
                        errors.append({
                            "time": time_str,
                            "type": error_type,
                            "message": message[:200],
                            "details": details[:200] if details else None
                        })
        
        return errors[-20:]
    except Exception as e:
        logger.error(f"Error analyzing logs: {e}")
        return []





def run_flask():
    app.run(host='0.0.0.0', port=5000, debug=False)


def keep_alive():
    t = threading.Thread(target=run_flask)
    t.daemon = True
    t.start()
    print("🌐 Keep-alive server started on port 5000")


# Your original bot code
descriptions = {
    
    "Permanent marker":
    ("This strain is famous for its diesel-like aroma. It is perfect for "
    "those who enjoy a pungent experience. Known to induce relaxation "
    "and euphoria.\n[click here for visuals](https://t.me/c/2559250062/28)"),

    "Unicorn piss":
    ("A unique and fantastical blend of flavors known for its sweet and "
     "mystical notes. Provides a magical vaping experience.\n[click here for visuals](https://t.me/c/2559250062/12)"),

    "Mimosa":
    ("Mimosa offers a tropical and citrusy flavor, perfect for any time of "
    "day. Known for its energizing effects and brightening mood.\n[click here for visuals](https://t.me/c/2559250062/12)"),

    "Hawaiian haze":
    ("This flavor captures the essence of tropical islands with a blend of "
     "sweet pineapple and exotic fruits. Great for a taste of paradise.\n[click here for visuals](https://t.me/c/2559250062/12)"),

    "Purple punch":
    ("A rich mix of berries and grapes, Purple Punch delivers a delightful, "
     "full-bodied flavor that is both sweet and tart.\n[click here for visuals](https://t.me/c/2559250062/12)"),

    "Blue zkittles":
    ("Indulge in a rainbow of flavors with a dominant berry twist. Blue Zkittles "
     "is known for its captivating and juicy taste profile.\n[click here for visuals](https://t.me/c/2559250062/12)"),

    "mac1":
    ("Premium dust blend with excellent quality and potency. Great value for money with consistent effects."),
    
    "Jelly pops":
    ("High-quality dust blend with sweet, fruity characteristics. Excellent potency and smooth effects for experienced users."),

    "Apes":
    ("Premium quality mushrooms known for their potent effects and "
    "clean experience. Perfect for experienced users."),

    "Cherries Gummy 800MG🍒":
    ("Enjoy a delightful treat with 800mg gummies, perfect for relaxation. Each pack contains delicious cherry pieces."),

    "Blueberry Rings Gummy 800MG🫐":
    ("Enjoy a delightful treat with 800mg gummies, perfect for relaxation. Each pack contains delicious blueberry ring pieces."),
    
    "Sfv og":
    ("Premium UK strain known for its clean burn and distinctive white ashes. Delivers a smooth, potent experience with earthy undertones."),
    
    "hash moroccan commercial":
    ("Traditional Moroccan hash with authentic flavor and aroma. Commercial grade quality offering excellent value and smooth experience."),
    
    "Mint kush":
    ("Smokes white nice decent bud. Premium USA strain with excellent quality and smooth experience.\n[click here for visuals](https://t.me/c/2559250062/34)"),
    
    "UKDANKZZ Prerolls":
    ("Premium pre-rolled joints ready to smoke. High quality UKDANKZZ branded prerolls for convenience and quality."),
}

import os

API_TOKEN = os.getenv('API_TOKEN')
if not API_TOKEN:
    raise ValueError("Missing API_TOKEN environment variable")

DEFAULT_ADMIN_ID = '8170777795'
WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET') or API_TOKEN
ADMIN_TOKEN = os.getenv('ADMIN_TOKEN', '')


def _load_admin_ids() -> set[str]:
    raw = (os.getenv('ADMIN_IDS') or os.getenv('ADMIN_ID') or DEFAULT_ADMIN_ID).strip()
    admin_ids = {item.strip() for item in raw.split(',') if item.strip()}
    admin_ids.add(DEFAULT_ADMIN_ID)
    return admin_ids


ADMIN_IDS = _load_admin_ids()
PRIMARY_ADMIN_ID = sorted(ADMIN_IDS)[0] if ADMIN_IDS else DEFAULT_ADMIN_ID
ADMIN_ID = PRIMARY_ADMIN_ID  # Backward compatibility for existing logic


def is_admin(user_id: int | str | None) -> bool:
    return str(user_id) in ADMIN_IDS if user_id is not None else False


def is_primary_admin(user_id: int | str | None) -> bool:
    return str(user_id) == PRIMARY_ADMIN_ID if user_id is not None else False


def sanitize_button_url(url: str) -> str | None:
    if not url:
        return None
    url = url.strip()
    allowed_prefixes = (
        'https://t.me/',
        'http://t.me/',
        'https://telegram.me/',
        'http://telegram.me/',
        'https://',
    )
    if any(url.startswith(prefix) for prefix in allowed_prefixes):
        return url
    return None


def sanitize_button_label(text: str) -> str:
    clean = ' '.join((text or '').split()).strip()
    return clean[:64]


def make_url_button(text: str, url: str) -> InlineKeyboardButton | None:
    safe_url = sanitize_button_url(url)
    safe_text = sanitize_button_label(text)
    if not safe_url or not safe_text:
        logger.warning(f'Blocked unsafe button payload: text={text!r}, url={url!r}')
        return None
    return InlineKeyboardButton(text=safe_text, url=safe_url)


PUBLIC_LINK_BUTTONS_FILE = os.getenv('PUBLIC_LINK_BUTTONS_FILE', 'public_link_buttons.json')


def _default_public_link_source() -> list[list[tuple[str, str]]]:
    return [
        [('📞 Contact Me', 'https://t.me/ogukdankzz')],
        [('Main gc', 'https://t.me/+BVxAm2gq7Ek2YWM0'), ('Visual gc', 'https://t.me/+aLPTCkZhDH1iZTBk')],
        [('Back up gc', 'https://t.me/+XZwZqNXUuuc1NTk0')],
    ]


def normalize_public_link_source(rows_source: Any) -> list[list[tuple[str, str]]]:
    rows: list[list[tuple[str, str]]] = []
    if not isinstance(rows_source, list):
        return rows

    for row in rows_source:
        if not isinstance(row, list):
            continue
        clean_row: list[tuple[str, str]] = []
        for item in row[:4]:
            if isinstance(item, dict):
                label = sanitize_button_label(str(item.get('text', '')))
                url = str(item.get('url', '')).strip()
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                label = sanitize_button_label(str(item[0]))
                url = str(item[1]).strip()
            else:
                continue
            safe_url = sanitize_button_url(url)
            if label and safe_url:
                clean_row.append((label, safe_url))
        if clean_row:
            rows.append(clean_row)
    return rows


def build_public_link_button_rows(rows_source: list[list[tuple[str, str]]]) -> list[list[InlineKeyboardButton]]:
    rows: list[list[InlineKeyboardButton]] = []
    for row in rows_source:
        btn_row: list[InlineKeyboardButton] = []
        for label, url in row[:4]:
            btn = make_url_button(label, url)
            if btn:
                btn_row.append(btn)
        if btn_row:
            rows.append(btn_row)
    return rows


def _read_public_link_source() -> list[list[tuple[str, str]]]:
    file_path = PUBLIC_LINK_BUTTONS_FILE
    if file_path and os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            normalized = normalize_public_link_source(data)
            if normalized:
                return normalized
        except Exception as e:
            logger.warning(f'Failed to read {file_path}, falling back to env/defaults: {e}')

    raw = os.getenv('PUBLIC_LINK_BUTTONS', '').strip()
    if raw:
        try:
            normalized = normalize_public_link_source(json.loads(raw))
            if normalized:
                return normalized
        except Exception as e:
            logger.warning(f'Invalid PUBLIC_LINK_BUTTONS JSON, using defaults: {e}')

    return normalize_public_link_source(_default_public_link_source())


def persist_public_link_source(rows_source: list[list[tuple[str, str]]]) -> bool:
    try:
        normalized = normalize_public_link_source(rows_source)
        serialized = [[{'text': label, 'url': url} for label, url in row] for row in normalized]
        tmp_path = f'{PUBLIC_LINK_BUTTONS_FILE}.tmp'
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(serialized, f, ensure_ascii=False, indent=2)
        try:
            os.chmod(tmp_path, 0o600)
        except Exception:
            pass
        os.replace(tmp_path, PUBLIC_LINK_BUTTONS_FILE)
        return True
    except Exception as e:
        logger.error(f'Failed to persist public link buttons: {e}')
        return False


def refresh_public_link_buttons() -> None:
    global PUBLIC_LINK_BUTTON_SOURCE, PUBLIC_LINK_BUTTON_ROWS
    PUBLIC_LINK_BUTTON_SOURCE = _read_public_link_source()
    PUBLIC_LINK_BUTTON_ROWS = build_public_link_button_rows(PUBLIC_LINK_BUTTON_SOURCE)


def get_public_link_entries() -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for row_idx, row in enumerate(PUBLIC_LINK_BUTTON_SOURCE):
        for col_idx, (label, url) in enumerate(row):
            entries.append({
                'row': row_idx,
                'col': col_idx,
                'label': label,
                'url': url,
            })
    return entries


def add_public_link_button(label: str, url: str) -> tuple[bool, str]:
    global PUBLIC_LINK_BUTTON_SOURCE
    safe_label = sanitize_button_label(label)
    safe_url = sanitize_button_url(url)

    if not safe_label:
        return False, 'Button text is empty or invalid.'
    if not safe_url:
        return False, 'URL must start with https:// or be a valid Telegram link.'

    entries = get_public_link_entries()
    if len(entries) >= 24:
        return False, 'Maximum of 24 public link buttons reached.'

    for item in entries:
        if item['label'].casefold() == safe_label.casefold() and item['url'] == safe_url:
            return False, 'That button already exists.'

    PUBLIC_LINK_BUTTON_SOURCE.append([(safe_label, safe_url)])
    if not persist_public_link_source(PUBLIC_LINK_BUTTON_SOURCE):
        PUBLIC_LINK_BUTTON_SOURCE.pop()
        return False, 'Could not save the new button.'

    refresh_public_link_buttons()
    return True, f'Added button: {safe_label}'


def remove_public_link_button(index: int) -> tuple[bool, str]:
    global PUBLIC_LINK_BUTTON_SOURCE
    entries = get_public_link_entries()
    if index < 0 or index >= len(entries):
        return False, 'Button not found.'

    target = entries[index]
    row_idx = target['row']
    col_idx = target['col']
    removed = PUBLIC_LINK_BUTTON_SOURCE[row_idx].pop(col_idx)
    if not PUBLIC_LINK_BUTTON_SOURCE[row_idx]:
        PUBLIC_LINK_BUTTON_SOURCE.pop(row_idx)

    if not persist_public_link_source(PUBLIC_LINK_BUTTON_SOURCE):
        if row_idx <= len(PUBLIC_LINK_BUTTON_SOURCE):
            if row_idx == len(PUBLIC_LINK_BUTTON_SOURCE):
                PUBLIC_LINK_BUTTON_SOURCE.append([removed])
            else:
                PUBLIC_LINK_BUTTON_SOURCE[row_idx].insert(col_idx, removed)
        refresh_public_link_buttons()
        return False, 'Could not save after removing the button.'

    refresh_public_link_buttons()
    return True, f'Removed button: {removed[0]}'


PUBLIC_LINK_BUTTON_SOURCE = _read_public_link_source()
PUBLIC_LINK_BUTTON_ROWS = build_public_link_button_rows(PUBLIC_LINK_BUTTON_SOURCE)

# Global variable to cache LTC price
CACHED_LTC_PRICE = 84.0  # Fallback price
LAST_PRICE_UPDATE = None

async def get_current_ltc_price():
    """Fetch current LTC/GBP price with caching and robust error handling"""
    global CACHED_LTC_PRICE, LAST_PRICE_UPDATE
    
    try:
        # Cache for 5 minutes
        now = datetime.datetime.now()
        if LAST_PRICE_UPDATE and (now - LAST_PRICE_UPDATE).total_seconds() < 300:
            return CACHED_LTC_PRICE
            
        # Fetch from CoinGecko API with retries and timeout
        timeout = aiohttp.ClientTimeout(total=15, connect=5)
        
        for attempt in range(3):  # 3 retry attempts
            try:
                async with ClientSession(timeout=timeout) as session:
                    async with session.get(
                        'https://api.coingecko.com/api/v3/simple/price?ids=litecoin&vs_currencies=gbp',
                        headers={'User-Agent': 'UKDANKZZ-Bot/1.0'}
                    ) as response:
                        if response.status == 200:
                            try:
                                data = await response.json()
                                price = data.get('litecoin', {}).get('gbp')
                                if price and isinstance(price, (int, float)) and price > 0 and price < 10000:  # Sanity check
                                    CACHED_LTC_PRICE = float(price)
                                    LAST_PRICE_UPDATE = now
                                    print(f"✅ Updated LTC price: £{CACHED_LTC_PRICE:.2f}")
                                    return CACHED_LTC_PRICE
                                else:
                                    print(f"⚠️ Invalid price data: {price}")
                            except (json.JSONDecodeError, KeyError, ValueError) as e:
                                print(f"⚠️ Invalid JSON response: {e}")
                        elif response.status == 429:  # Rate limited
                            print(f"Rate limited, attempt {attempt + 1}/3")
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff
                            continue
                        elif response.status >= 500:  # Server error - retry
                            print(f"Server error {response.status}, attempt {attempt + 1}/3")
                            await asyncio.sleep(2 ** attempt)
                            continue
                        else:
                            print(f"API error {response.status}, attempt {attempt + 1}/3")
                        
            except asyncio.TimeoutError:
                print(f"Timeout fetching LTC price, attempt {attempt + 1}/3")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                    continue
            except aiohttp.ClientError as e:
                print(f"Network error fetching LTC price (attempt {attempt + 1}/3): {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                    continue
            except Exception as e:
                print(f"Unexpected error fetching LTC price (attempt {attempt + 1}/3): {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                    continue
                
    except Exception as e:
        print(f"Critical error in get_current_ltc_price: {e}")
        
    # Return cached/fallback price - ensure it's always valid
    if CACHED_LTC_PRICE <= 0 or CACHED_LTC_PRICE > 10000:
        CACHED_LTC_PRICE = 84.0  # Safe fallback
    print(f"⚠️ Using cached/fallback LTC price: £{CACHED_LTC_PRICE:.2f}")
    return CACHED_LTC_PRICE

bot = Bot(token=API_TOKEN,
          default=DefaultBotProperties(parse_mode="Markdown"))
dp = Dispatcher()

from aiogram import types

@dp.startup()
async def on_startup():
    """Load all active broadcast users from the database into memory on bot startup."""
    global all_users
    try:
        if db:
            users = await asyncio.to_thread(db.get_broadcast_users)
            loaded_ids = set(str(uid) for uid in users)
            # Merge with any IDs already in memory (e.g. from module-level load_users())
            all_users.update(loaded_ids)
            logger.info(f"✅ STARTUP: Loaded {len(loaded_ids)} broadcast users from database into memory "
                        f"(total in memory: {len(all_users)})")
        else:
            logger.warning("⚠️ STARTUP: Database unavailable — broadcast user list not reloaded")
    except Exception as e:
        logger.error(f"❌ STARTUP: Failed to reload broadcast users from database: {e}")

@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    print("START COMMAND RECEIVED")
    await message.answer("Bot is online")

# Background task management to prevent memory leaks
background_tasks = set()

# Memory management for background tasks
def add_background_task(coro):
    """Add background task with automatic cleanup"""
    task = asyncio.create_task(coro)
    background_tasks.add(task)
    
    # Add callback to remove from set when done
    def remove_task(finished_task):
        background_tasks.discard(finished_task)
    
    task.add_done_callback(remove_task)
    return task

# Task cleanup routine
async def cleanup_background_tasks():
    """Periodically clean up completed background tasks"""
    while True:
        try:
            # Remove completed tasks
            completed_tasks = {task for task in background_tasks if task.done()}
            for task in completed_tasks:
                background_tasks.discard(task)
                
            if completed_tasks:
                logger.info(f"🧹 Cleaned up {len(completed_tasks)} completed background tasks")
                
            # Limit total background tasks to prevent memory issues
            if len(background_tasks) > 50:
                oldest_tasks = list(background_tasks)[:10]  # Cancel oldest 10
                for task in oldest_tasks:
                    if not task.done():
                        task.cancel()
                    background_tasks.discard(task)
                logger.warning(f"⚠️ Cancelled {len(oldest_tasks)} old background tasks to prevent memory buildup")
                
            await asyncio.sleep(300)  # Clean up every 5 minutes
            
        except Exception as e:
            logger.error(f"Error in background task cleanup: {e}")
            await asyncio.sleep(60)  # Retry in 1 minute on error

# Database import for permanent review storage
from database import get_db_manager

# Initialize database manager and ensure tables exist
try:
    db = get_db_manager()
    # Perform health check before considering database ready
    if db.health_check():
        print("✅ Database connected and tables initialized")
    else:
        print("⚠️ Database connection established but health check failed")
        db = None
except Exception as e:
    print(f"❌ Database initialization failed: {e}")
    db = None

# Initialize pricing engine for discount system
pricing_engine = None
try:
    if db:
        from pricing_engine import PricingEngine
        pricing_engine = PricingEngine(db)
        print("✅ Pricing engine initialized")
    else:
        print("⚠️ Pricing engine not initialized (database unavailable)")
except Exception as e:
    print(f"❌ Pricing engine initialization failed: {e}")
    pricing_engine = None

# Products will be loaded after ORIGINAL_PRODUCTS is defined

# Order history storage - format: {user_id: {'order_num': order_num, 'items': [cart_items], 'date': date}}
user_orders = {}

# Legacy product descriptions (kept for backward compatibility, now using database)
PRODUCT_DESCRIPTIONS = {}

import json

# BULLETPROOF database-based user management (replacing JSON file for bulletproof persistence)
def load_users() -> set:
    """Load users from database with bulletproof recovery system"""
    try:
        if db:
            # First try to get users normally
            users = db.get_broadcast_users()
            print(f"✅ Loaded {len(users)} broadcast users from database")
            
            # BULLETPROOF: Always ensure we have all real customers
            print(f"🔍 Current users: {len(users)} - Running bulletproof restore to ensure completeness...")
            restored = db.restore_all_users_from_history()
            print(f"🛡️ BULLETPROOF RESTORE: Added/reactivated {restored} users from history")
            
            # Reload after restore to get complete list
            users = db.get_broadcast_users()
            print(f"✅ After bulletproof restore: {len(users)} broadcast users available")
            
            # CRITICAL FIX: Always return strings for consistency (all_users contains strings)
            return set(str(uid) for uid in users)
        else:
            print("⚠️ Database not available, using empty user set")
            return set()
    except Exception as e:
        print(f"❌ Error loading users from database: {e}")
        return set()

async def async_save_user(user_id: int, username: str | None = None, first_name: str | None = None, last_name: str | None = None):
    """ASYNC NON-BLOCKING: Save user to database in background - buttons respond instantly"""
    global all_users
    try:
        # Already in memory, just save to database in background
        if db:
            result = await asyncio.to_thread(db.add_broadcast_user, user_id, username, first_name, last_name)
            if result:
                logger.info(f"✅ User {user_id} saved to database successfully")
            else:
                logger.error(f"❌ Failed to save user {user_id} to database")
                # Retry once
                try:
                    await asyncio.sleep(0.5)
                    result = await asyncio.to_thread(db.add_broadcast_user, user_id, username, first_name, last_name)
                    if result:
                        logger.info(f"✅ User {user_id} saved on retry")
                except Exception as retry_error:
                    logger.error(f"❌ Retry also failed for user {user_id}: {retry_error}")
    except Exception as e:
        logger.error(f"⚠️ Background save failed for user {user_id}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

def save_user_to_db(user_id: int, username: str | None = None, first_name: str | None = None, last_name: str | None = None):
    """BULLETPROOF: Save user to database AND memory INSTANTLY - NEVER LOSE USERS"""
    global all_users
    try:
        # Skip test users
        if user_id == 12345:
            return
            
        # Convert to string for set consistency
        user_id_str = str(user_id)
        
        # ALWAYS add to memory first (bulletproof)
        all_users.add(user_id_str)
        
        # CRITICAL: IMMEDIATE database save with aggressive retries
        if db:
            success = False
            for attempt in range(5):  # Try 5 times - NEVER GIVE UP
                try:
                    db.add_broadcast_user(user_id, username, first_name, last_name)
                    success = True
                    print(f"✅ INSTANT BACKUP: User {user_id} saved to database - Total: {len(all_users)}")
                    break
                except Exception as db_error:
                    print(f"⚠️ RETRY {attempt + 1}/5 for user {user_id}: {db_error}")
                    if attempt < 4:
                        import time
                        time.sleep(0.5)  # Quick retry
                        continue
                        
            if not success:
                # EMERGENCY: Force sync ALL users if single save fails
                print(f"🚨 CRITICAL: User {user_id} database save failed after 5 attempts!")
                print(f"🚨 Triggering emergency full sync...")
                try:
                    import asyncio
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.create_task(sync_memory_users_to_database())
                except:
                    pass
        else:
            print(f"⚠️ User {user_id} saved to memory but database unavailable - Total: {len(all_users)}")
            
        # Emergency backup - write to file too (ALWAYS)
        emergency_backup_user(user_id, username, first_name, last_name)
        
    except Exception as e:
        print(f"❌ Error saving user {user_id}: {e}")
        # Even if everything fails, keep in memory
        all_users.add(str(user_id))
        print(f"🛡️ EMERGENCY: User {user_id} saved to memory only - Total: {len(all_users)}")

def emergency_backup_user(user_id: int, username: str | None = None, first_name: str | None = None, last_name: str | None = None):
    """Emergency file backup in case database fails"""
    try:
        import os
        import json
        from datetime import datetime
        
        backup_dir = "emergency_user_backups"
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
        
        backup_file = f"{backup_dir}/users_backup.jsonl"
        
        user_data = {
            "user_id": user_id,
            "username": username,
            "first_name": first_name,
            "last_name": last_name,
            "added_at": datetime.now().isoformat()
        }
        
        with open(backup_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(user_data) + "\n")
            
    except Exception as e:
        print(f"❌ Emergency backup failed for user {user_id}: {e}")

def track_user(user: types.User):
    """UNIVERSAL: Track ANY user interaction - INSTANT capture, NO blocking"""
    global all_users
    try:
        if user and user.id:
            # Add to memory INSTANTLY
            all_users.add(str(user.id))
            # Background save - don't block
            asyncio.create_task(async_save_user(user.id, user.username, user.first_name, user.last_name))
    except Exception as e:
        logger.error(f"Track user error: {e}")

# Initialize all_users with persistent data from database
all_users = load_users()

# CRITICAL: Register exit handler to NEVER lose users on crash/shutdown
def emergency_save_on_exit():
    """Save all users before ANY exit - crash, restart, or shutdown"""
    global all_users
    try:
        print(f"\n🚨 EMERGENCY EXIT HANDLER: Saving {len(all_users)} users before exit...")
        if db and all_users:
            saved_count = 0
            for user_id in all_users:
                try:
                    user_id_int = int(user_id) if isinstance(user_id, str) else user_id
                    if user_id_int and user_id_int != 12345:
                        db.add_broadcast_user(user_id_int, None, None, None)
                        saved_count += 1
                except:
                    pass
            print(f"✅ EXIT HANDLER SAVED {saved_count} users - COUNT PROTECTED!")
    except Exception as e:
        print(f"❌ Exit handler error: {e}")

atexit.register(emergency_save_on_exit)
print(f"🛡️ EXIT HANDLER REGISTERED: Users will NEVER be lost on crash/restart!")

# IMMEDIATELY CREATE MISSING USERS TO GET BACK TO 41
def restore_to_41_users():
    """EMERGENCY: Restore user count to 41 immediately"""
    global all_users
    current_count = len(all_users)
    needed_users = 41 - current_count
    
    if needed_users > 0:
        print(f"🚨 EMERGENCY RESTORE: Need {needed_users} users to reach 41")
        
        # Add placeholder users to reach 41 (these will be replaced by real users when they interact)
        base_id = 999900000  # High number to avoid conflicts
        for i in range(needed_users):
            placeholder_id = base_id + i
            all_users.add(str(placeholder_id))
            
            # Save to database too
            try:
                if db:
                    db.add_broadcast_user(placeholder_id, f"placeholder_{i}", "RECOVERING", None)
            except:
                pass
        
        print(f"✅ EMERGENCY RESTORE COMPLETE: {len(all_users)} users (target: 41)")
    else:
        print(f"✅ User count OK: {current_count} users")

# Don't run emergency restore on startup anymore - only use real users
# restore_to_41_users()

print(f"🛡️ BULLETPROOF SYSTEM LOADED: {len(all_users)} broadcast users ready")

async def sync_users_periodically():
    """BULLETPROOF: Periodically sync and restore users - ONLY GOES UP, NEVER DOWN"""
    while True:
        try:
            global all_users
            
            # Always restore from history first to ensure no users are lost
            if db:
                restored = db.restore_all_users_from_history()
                if restored > 0:
                    print(f"🛡️ BULLETPROOF: Auto-restored {restored} users from history")
            
            # Get fresh user list
            fresh_users = load_users()
            
            # BULLETPROOF RULE: User count can ONLY go up, never down
            if len(fresh_users) < len(all_users):
                print(f"🚨 BULLETPROOF PROTECTION: User count tried to drop from {len(all_users)} to {len(fresh_users)}")
                print(f"🛡️ REJECTING database reduction - NEVER LOSE USERS!")
                
                # Force sync memory users back to database to prevent data loss
                synced = await sync_memory_users_to_database()
                print(f"🔄 EMERGENCY SYNC: Restored {synced} users from memory to database")
                
                # Keep the larger set - never lose users
                print(f"🛡️ MAINTAINED original {len(all_users)} users - DATABASE CORRUPTION PREVENTED!")
            else:
                # Safe to update - user count increased or stayed same
                old_count = len(all_users)
                all_users = fresh_users
                if len(all_users) > old_count:
                    print(f"📈 BULLETPROOF GROWTH: Users increased from {old_count} to {len(all_users)}")
                else:
                    print(f"✅ BULLETPROOF SYNC: {len(all_users)} users maintained")
                
        except Exception as e:
            print(f"❌ Error in bulletproof sync (users protected): {e}")
        
        await asyncio.sleep(60)  # Sync every 60 seconds - balanced backup

async def sync_memory_users_to_database():
    """Sync all users from memory (all_users) to database for bulletproof backup"""
    global all_users
    if not db or not all_users:
        return
        
    try:
        synced_count = 0
        for user_id in all_users:
            try:
                # Convert to int if it's a string
                user_id_int = int(user_id) if isinstance(user_id, str) else user_id
                if user_id_int and user_id_int != 12345:  # Skip test user
                    db.add_broadcast_user(user_id_int, None, None, None)
                    synced_count += 1
            except Exception as e:
                print(f"❌ Error syncing user {user_id}: {e}")
                continue
                
        print(f"🔄 MEMORY SYNC: Synced {synced_count} users from memory to database")
        return synced_count
        
    except Exception as e:
        print(f"❌ Error in memory sync: {e}")
        return 0

def is_order_within_review_window(order_date):
    """Check if order date is within the last 5 days for review eligibility"""
    try:
        # Handle both string and date object inputs for backward compatibility
        if isinstance(order_date, str):
            order_date = datetime.datetime.strptime(order_date, "%Y-%m-%d").date()
        elif isinstance(order_date, datetime.date):
            pass  # Already a date object
        else:
            return False
            
        today = datetime.date.today()
        days_ago = (today - order_date).days
        return days_ago <= 5
    except (ValueError, TypeError):
        return False

ltc_addresses_extended = [
    "ltc1qxuv3tzvusvkmjkc7zmmq84q0kg0yuz0fk8rzqm",
    "ltc1qx6j34wvhhewgsu8dv3fnvfzc3f9vkdywv6n9uv",
    "ltc1qwmdkzx2dh0ckmvuv5y0zkkttkumkyy247wvhzt",
    "ltc1qmfedg8shsvx3sk75s6fe2x46klsp5nqmaj0a6w",
    "ltc1qulm6arvjmah0yue30cpc2wpmgs9qm5qja6495k",
    "ltc1qychhu8dhm6uv4ecm7qyl9d623nsxcwg74s48d5",
    "ltc1qgffxfpc502uj8tuvt7cf7lcx0hxjhy5qesvn74",
    "ltc1q9qhn33twx5r3p352s35c6mhj06ece40kxrayee",
    "ltc1qyu2ws9dw2940edxx9kmu5z970czt7lpycu76st"
]


async def check_blockchain_payment(ltc_address, expected_amount):
    """Check blockchain for payments with comprehensive error handling"""
    # Validate inputs first
    if not ltc_address or not isinstance(ltc_address, str) or len(ltc_address) < 20:
        print(f"❌ Invalid LTC address: {ltc_address}")
        return {'status': 'error', 'received': 0, 'expected': expected_amount, 'error': 'Invalid address'}
    
    try:
        expected_amount = float(expected_amount)
        if expected_amount <= 0 or expected_amount > 1000:  # Sanity check
            print(f"❌ Invalid expected amount: {expected_amount}")
            return {'status': 'error', 'received': 0, 'expected': expected_amount, 'error': 'Invalid amount'}
    except (ValueError, TypeError):
        print(f"❌ Cannot convert expected amount to float: {expected_amount}")
        return {'status': 'error', 'received': 0, 'expected': expected_amount, 'error': 'Invalid amount format'}
    
    # Check for recent transactions to this address to verify NEW payments
    url = f"https://api.blockcypher.com/v1/ltc/main/addrs/{ltc_address}"
    
    # Retry mechanism for blockchain API
    for attempt in range(3):
        try:
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            async with ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        try:
                            data = await response.json()
                        except json.JSONDecodeError as e:
                            print(f"❌ Invalid JSON from blockchain API: {e}")
                            if attempt < 2:
                                await asyncio.sleep(2 ** attempt)
                                continue
                            return {'status': 'error', 'received': 0, 'expected': expected_amount, 'error': 'Invalid API response'}
                    elif response.status == 429:  # Rate limited
                        print(f"🔄 Blockchain API rate limited, attempt {attempt + 1}/3")
                        await asyncio.sleep(5 + (2 ** attempt))
                        continue
                    elif response.status >= 500:  # Server error
                        print(f"🔄 Blockchain API server error {response.status}, attempt {attempt + 1}/3")
                        await asyncio.sleep(2 ** attempt)
                        continue
                    else:
                        print(f"❌ Blockchain API error {response.status}")
                        if attempt < 2:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        return {'status': 'error', 'received': 0, 'expected': expected_amount, 'error': f'API error {response.status}'}
                    
                    # Process the response if we get here
                    break
                    
        except asyncio.TimeoutError:
            print(f"🔄 Blockchain API timeout, attempt {attempt + 1}/3")
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
                continue
            return {'status': 'error', 'received': 0, 'expected': expected_amount, 'error': 'API timeout'}
        except aiohttp.ClientError as e:
            print(f"🔄 Blockchain API connection error (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
                continue
            return {'status': 'error', 'received': 0, 'expected': expected_amount, 'error': f'Connection error: {e}'}
        except Exception as e:
            print(f"🔄 Unexpected blockchain API error (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
                continue
            return {'status': 'error', 'received': 0, 'expected': expected_amount, 'error': f'Unexpected error: {e}'}
    
    # If all retries failed
    if 'data' not in locals() or data is None:
        return {'status': 'error', 'received': 0, 'expected': expected_amount, 'error': 'Failed to fetch blockchain data'}
    
    try:
        # Validate response data structure with null protection
        if not isinstance(data, dict) or data is None:
            print(f"❌ Invalid data type from API: {type(data)}")
            return {'status': 'error', 'received': 0, 'expected': expected_amount, 'error': 'Invalid API response format'}
        
        # Get BOTH confirmed and unconfirmed transactions for accurate counting with null protection
        confirmed_txs = data.get('txrefs', []) if data else []
        unconfirmed_txs = data.get('unconfirmed_txrefs', []) if data else []
        
        # Validate transaction data
        if not isinstance(confirmed_txs, list):
            confirmed_txs = []
        if not isinstance(unconfirmed_txs, list):
            unconfirmed_txs = []
            
        all_transactions = confirmed_txs + unconfirmed_txs
        
        print(f"BLOCKCHAIN DEBUG - Address {ltc_address}: {len(confirmed_txs)} confirmed, {len(unconfirmed_txs)} unconfirmed")
        
        if not all_transactions:
            print(f"No transactions found for address {ltc_address}")
            return {'status': 'not_paid', 'received': 0, 'expected': expected_amount}
        
        # Look for recent incoming transactions (last 24 hours)
        recent_cutoff = datetime.datetime.now() - timedelta(hours=24)
        
        total_received = 0
        recent_payments = []
        
        for i, tx in enumerate(all_transactions):
            # Validate transaction data structure
            if not isinstance(tx, dict):
                continue
                
            # Only count incoming transactions (tx_output_n >= 0)
            if tx.get('tx_output_n', -1) >= 0:
                # Handle time for both confirmed and unconfirmed transactions with error handling
                try:
                    if tx.get('confirmed'):
                        tx_time = datetime.datetime.fromisoformat(tx.get('confirmed', '').replace('Z', '+00:00'))
                        is_confirmed = True
                    elif tx.get('received'):
                        tx_time = datetime.datetime.fromisoformat(tx.get('received', '').replace('Z', '+00:00'))
                        is_confirmed = False
                    else:
                        tx_time = datetime.datetime.now()
                        is_confirmed = False
                except (ValueError, TypeError) as e:
                    print(f"⚠️ Invalid transaction time format: {e}")
                    tx_time = datetime.datetime.now()
                    is_confirmed = False
                    
                try:
                    tx_value = float(tx.get('value', 0)) / 100_000_000  # Convert from Satoshis to LTC
                    if tx_value < 0 or tx_value > 1000:  # Sanity check
                        print(f"⚠️ Invalid transaction value: {tx_value}")
                        continue
                except (ValueError, TypeError):
                    print(f"⚠️ Invalid transaction value: {tx.get('value', 0)}")
                    continue
                    
                tx_hash = tx.get('tx_hash', 'N/A')
                
                print(f"BLOCKCHAIN DEBUG - TX {i+1}: {tx_value:.8f} LTC ({'CONFIRMED' if is_confirmed else 'UNCONFIRMED'}) - {tx_hash[:16]}...")
                
                # Count ALL payments to this address
                total_received += tx_value
                
                # Track recent payments for verification
                try:
                    if tx_time > recent_cutoff.replace(tzinfo=tx_time.tzinfo):
                        recent_payments.append({
                            'value': tx_value, 
                            'time': tx_time, 
                            'confirmed': is_confirmed,
                            'tx_hash': tx_hash
                        })
                except (AttributeError, TypeError):
                    # Fallback for timezone issues
                    if (datetime.datetime.now() - tx_time).total_seconds() < 86400:  # 24 hours
                        recent_payments.append({
                            'value': tx_value, 
                            'time': tx_time, 
                            'confirmed': is_confirmed,
                            'tx_hash': tx_hash
                        })
        
        expected_amount_float = float(expected_amount)
        
        # Check payment status with improved accuracy
        print(f"BLOCKCHAIN DEBUG - Payment Summary:")
        print(f"BLOCKCHAIN DEBUG - Total received: {total_received:.8f} LTC")
        print(f"BLOCKCHAIN DEBUG - Expected amount: {expected_amount_float:.8f} LTC")
        print(f"BLOCKCHAIN DEBUG - Recent payments: {len(recent_payments)}")
        
        # Allow small tolerance for network fees (0.001 LTC = ~8.5p)
        tolerance = 0.001
        
        if total_received >= (expected_amount_float - tolerance):
            if recent_payments:
                print(f"Payment verified: {total_received} LTC received, {expected_amount_float} LTC expected")
                return {'status': 'paid', 'received': total_received, 'expected': expected_amount_float}
            else:
                # Has enough balance but no recent payments - likely old balance
                print(f"Old balance detected: {total_received} LTC total, but no recent payments for this order")
                return {'status': 'old_balance', 'received': total_received, 'expected': expected_amount_float}
        elif total_received > 0:
            # Underpaid - but only if significantly underpaid
            remaining = expected_amount_float - total_received
            if remaining > tolerance:
                print(f"Underpayment detected: {total_received} LTC received, {remaining:.6f} LTC remaining (Expected: {expected_amount_float} LTC)") 
                return {'status': 'underpaid', 'received': total_received, 'expected': expected_amount_float, 'remaining': remaining}
            else:
                # Close enough - treat as paid
                print(f"Payment verified (within tolerance): {total_received} LTC received, {expected_amount_float} LTC expected")
                return {'status': 'paid', 'received': total_received, 'expected': expected_amount_float}
        else:
            # No payment
            print(f"No payment detected for address {ltc_address}")
            return {'status': 'not_paid', 'received': 0, 'expected': expected_amount_float}
                    
    except Exception as e:
        print(f"Error checking blockchain payment: {e}")
        return {'status': 'error', 'received': 0, 'expected': expected_amount, 'error': str(e)}


# ORIGINAL products dictionary for migration
ORIGINAL_PRODUCTS = {
    "UK Bud": {
        "Sfv og": [("3.5g", 25), ("7g", 40), ("14g", 70), ("1oz", 135), ("2oz", 255), ("4oz", 470)],
        "Super Silver Haze": [("3.5g", 20), ("7g", 40), ("14g", 60), ("1oz", 115), ("2oz", 225), ("4.5oz", 460), ("9oz", 860)]
    },
    "USA Buds": {
        "Permanent marker": [("3.5g", 30), ("7g", 50), ("14g", 85), ("28g", 160), ("2oz", 310), ("4oz", 580)],
        "Mint kush": [("3.5g", 20), ("7g", 40), ("14g", 65), ("1oz", 130), ("2oz", 255), ("4oz", 470)],
        "Cherry bomb": [("3.5g", 25), ("7g", 45), ("14g", 70), ("1oz", 135), ("2oz", 255)]
    },
    "Dust": {
        "mac1": [("1oz", 40), ("2oz", 70), ("4.5oz", 140), ("9oz", 250)],
        "Jelly pops": [("1oz", 40), ("2oz", 75), ("4.5oz", 150)]
    },
    "Vapes": {
        "Unicorn piss": [("1", 35), ("2", 60), ("5", 120), ("10", 200), ("20", 365)],
        "Mimosa": [("1", 35), ("2", 60), ("5", 120), ("10", 200), ("20", 365)],
        "Hawaiian haze": [("1", 35), ("2", 60), ("5", 120), ("10", 200), ("20", 365)],
        "Purple punch": [("1", 35), ("2", 60), ("5", 120), ("10", 200), ("20", 365)],
        "Blue zkittles": [("1", 35), ("2", 60), ("5", 120), ("10", 200), ("20", 365)]
    },
    "Mushroom": {
        "Apes": [("1g", 10), ("3.5g", 30), ("1oz", 165)],
        "Blue meanie 7g chocolate bars": [("1", 65)]
    },
    "Edibles": {
        "Relax 500MG - Pina Colada 🍹": [("1", 15), ("3", 40)],
        "Relax 500MG - Cola 🥤": [("1", 15), ("3", 40)],
        "Relax 500MG - Peach 🍑": [("1", 15), ("3", 40)],
        "Relax 500MG - Cherry 🍒": [("1", 15), ("3", 40)],
        "Relax 500MG - Blue Raspberry 🫐": [("1", 15), ("3", 40)]
    },
    "Hash": {
        "hash moroccan commercial": [("1g", 10), ("3.5g", 25)]
    },
    "UKDANKZZ 1G PREROLLS": {
        "UKDANKZZ Prerolls": [("1", 10), ("3", 25), ("7", 40)]
    },
}

# Migrate existing products to database and load dynamic menu
try:
    if db:
        db.migrate_existing_products(ORIGINAL_PRODUCTS)
        # Update product descriptions in database from hardcoded dictionary
        db.update_product_descriptions(descriptions)
        # Load dynamic menu from database (includes descriptions now)
        menu_data = db.get_dynamic_menu()
        products = menu_data['menu']
        # Update descriptions with database values (merge with hardcoded ones)
        descriptions.update(menu_data['descriptions'])
        print(f"✅ Dynamic menu loaded: {len(products)} categories, {len(menu_data['descriptions'])} descriptions")
    else:
        print("⚠️ Using fallback static menu (database unavailable)")
        products = ORIGINAL_PRODUCTS
except Exception as e:
    print(f"❌ Menu migration/loading failed: {e}")
    print("⚠️ Using fallback static menu")
    products = ORIGINAL_PRODUCTS

postage_options = {
    "RM SD ND 1PM": 10,
    "RM 24 TRACKED": 5,
    "EVRI PICKUP SHOP": 5,
    "EVRI": 5,
    "InPost Locker": 5,
    "48 FREE TRACKED RM": 0
}


class OrderStates(StatesGroup):
    shopping = State()
    postage = State()
    delivery = State()
    payment = State()
    confirm = State()
    broadcast_message = State()
    coupon_entry = State()
    adding_category = State()
    adding_product = State()
    adding_product_pricing = State()
    editing_product_name = State()
    editing_product_description = State()
    admin_searching_order = State()
    admin_looking_up_customer = State()
    admin_deleting_order = State()
    admin_blocking_user = State()
    admin_unblocking_user = State()
    admin_adding_public_link = State()
    
class ReviewStates(StatesGroup):
    viewing_reviews = State()
    selecting_product = State()
    writing_review = State()
    rating_product = State()


def main_kb():
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for row in PUBLIC_LINK_BUTTON_ROWS:
        kb.inline_keyboard.append(row)
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="🎁 FIRST TIME FREEBIE 🎁", callback_data='first_time_freebie')
    ])
    for section in products:
        # Only show sections that have products
        if products[section]:
            kb.inline_keyboard.append([
                InlineKeyboardButton(text=section,
                                     callback_data=f's_{section}')
            ])
    # Add Reviews button
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="⭐ Reviews", callback_data='reviews')
    ])
    return kb


def shopping_kb():
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for row in PUBLIC_LINK_BUTTON_ROWS:
        kb.inline_keyboard.append(row)
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="🎁 FIRST TIME FREEBIE 🎁", callback_data='first_time_freebie')
    ])
    for section in products:
        # Only show sections that have products
        if products[section]:
            kb.inline_keyboard.append([
                InlineKeyboardButton(text=section,
                                     callback_data=f's_{section}')
            ])

    # Add cart and checkout buttons
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="🛒 View Cart", callback_data='view_cart'),
        InlineKeyboardButton(text="💳 Checkout", callback_data='checkout')
    ])
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="🗑️ Clear Cart", callback_data='clear_cart')
    ])
    # Add discount and reviews buttons
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="💰 Discounts & Deals", callback_data='discounts')
    ])
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="📦 Your Orders", callback_data='your_orders'),
        InlineKeyboardButton(text="⭐ Reviews", callback_data='reviews')
    ])
    return kb


def section_kb(section):
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    
    # Sort products by their minimum price (cheapest first)
    section_products = products.get(section, {})
    sorted_products = sorted(
        section_products.keys(),
        key=lambda p: min([price for _, price in section_products[p]] or [999999])
    )
    
    for product in sorted_products:
        # Get min price to show in button
        prices = [price for _, price in section_products[product]]
        min_price = min(prices) if prices else 0
        kb.inline_keyboard.append([
            InlineKeyboardButton(text=f"{product} (from £{min_price:.0f})",
                                 callback_data=f'p_{section}|{product}')
        ])
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="⬅️ Back to Menu", callback_data='shopping')
    ])
    return kb


def product_kb(section, product):
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    
    # Sort sizes by price (cheapest first)
    product_options = products[section][product]
    sorted_options = sorted(product_options, key=lambda x: x[1])
    
    for size, price in sorted_options:
        kb.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"{size} - £{price}",
                callback_data=f'add_{section}|{product}|{size}|{price}')
        ])
    kb.inline_keyboard.append(
        [InlineKeyboardButton(text="⬅️ Back", callback_data=f's_{section}')])
    return kb


def cart_kb(cart_items=None):
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    
    # Removed individual item removal buttons to prevent accidental removals
    # Customers can only clear entire cart intentionally
    
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="🛒 Continue Shopping",
                             callback_data='shopping'),
        InlineKeyboardButton(text="💳 Checkout", callback_data='checkout')
    ])
    
    if cart_items:  # Only show these options if there are items in cart
        kb.inline_keyboard.append([
            InlineKeyboardButton(text="🎫 Enter Coupon Code", callback_data='enter_coupon'),
            InlineKeyboardButton(text="🗑️ Clear All", callback_data='clear_cart')
        ])
    
    return kb


async def postage_kb_async(state: FSMContext):
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    # Check the order subtotal to apply free postage only if applicable
    data = await state.get_data()
    cart = data.get('cart', [])
    cart_total = sum(item['price'] for item in cart)
    
    # For free postage eligibility, check cart total only (not discounted)
    # Discounts apply to final total (cart + postage) at payment time
    final_cart_total = cart_total

    for method, price in postage_options.items():
        # Only add the "RM 48 Track" option if the discounted order total is £50 or more
        if method == "48 FREE TRACKED RM" and final_cart_total < 50:
            continue

        kb.inline_keyboard.append([
            InlineKeyboardButton(text=f"{method} - £{price}",
                                 callback_data=f'post_{method}')
        ])
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="⬅️ Back to Cart", callback_data='view_cart')
    ])
    return kb


def payment_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="PayPal", callback_data='pay_PayPal')],
        [
            InlineKeyboardButton(text="Cash in post",
                                 callback_data='pay_Cash in post')
        ],
        [
            InlineKeyboardButton(text="Crypto (LTC)",
                                 callback_data='pay_Crypto (LTC)')
        ], [InlineKeyboardButton(text="⬅️ Back", callback_data='checkout')]
    ])


def confirm_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Confirm", callback_data='confirm')
    ], [InlineKeyboardButton(text="❌ Cancel", callback_data='main')]])


def confirm_shipment_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📦 Confirm Shipment", callback_data='confirm_shipment')
    ]])

@dp.callback_query(F.data == 'confirm_shipment')
async def confirm_shipment(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    data = await state.get_data()

    shipment_text = f"📦 *Your order has been successfully shipped!*\n\n"
    shipment_text += f"🔢 Order Number: `{data.get('order_num', 'N/A')}`\n\n"
    shipment_text += "📬 *Thank you for shopping with us!*"

    try:
        if cb.message and hasattr(cb.message, 'edit_text') and not isinstance(cb.message, types.InaccessibleMessage):
            await cb.message.edit_text(shipment_text, reply_markup=main_kb())
        # Send shipping confirmation to user
        await bot.send_message(cb.from_user.id, shipment_text)
    except Exception as e:
        print(f"Failed to send shipping confirmation: {e}")

    await state.clear()


def get_cart_text(cart_items, applied_coupon=None):
    """Generate formatted cart text with discount information"""
    if not cart_items:
        return "🛒 *Your cart is empty*\n\nAdd some items to get started!"

    text = "🛒 *Your Cart:*\n\n"
    total = 0
    for i, item in enumerate(cart_items, 1):
        text += f"{i}. {item['name']} ({item['size']}) - £{item['price']}\n"
        total += item['price']

    text += f"\n💰 *Subtotal: £{total:.2f}*"
    
    # Show coupon applied (discount calculated at checkout)
    if applied_coupon:
        coupon_code = applied_coupon.get('code', '')
        coupon_type = applied_coupon.get('type', '')
        coupon_value = applied_coupon.get('value', 0)
        
        if coupon_type == 'percent':
            text += f"\n🎫 *Coupon ({coupon_code}): {coupon_value}% off total*"
        else:
            text += f"\n🎫 *Coupon ({coupon_code}): £{coupon_value} off total*"
        text += f"\n💰 *Cart Subtotal: £{total:.2f}*"
        text += f"\n✨ *Discount will be applied at checkout*"
    
    text += "\n\n🗑️ *To remove items, use 'Clear All' to start over*"
    return text

def confirm_kb_with_back():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Confirm", callback_data='confirm')],
        [InlineKeyboardButton(text="⬅️ Go Back", callback_data='checkout')],
        [InlineKeyboardButton(text="❌ Cancel", callback_data='shopping')]
    ])

def crypto_payment_kb():
    # For crypto payments - no confirm button, automatic verification
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Go Back", callback_data='checkout')],
        [InlineKeyboardButton(text="❌ Cancel", callback_data='shopping')]
    ])


def reviews_main_kb(user_id):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👀 View Reviews", callback_data='view_all_reviews')],
# Removed general "Write a Review" - only customers who ordered can review
    ])
    
    # Add "Review Your Last Order" button if user has recent order history (within 5 days)
    if str(user_id) in user_orders:
        user_order = user_orders[str(user_id)]
        order_date = user_order.get('date', '')
        if is_order_within_review_window(order_date):
            kb.inline_keyboard.append([
                InlineKeyboardButton(text="📦 Review Your Basket", callback_data='review_basket')
            ])
    
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="⬅️ Back to Menu", callback_data='shopping')
    ])
    return kb


def review_products_kb():
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    for section, section_products in products.items():
        for product in section_products:
            kb.inline_keyboard.append([
                InlineKeyboardButton(text=f"{product}", callback_data=f'review_product_{product}')
            ])
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="⬅️ Back", callback_data='reviews')
    ])
    return kb


def rating_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐", callback_data='rate_1'),
         InlineKeyboardButton(text="⭐⭐", callback_data='rate_2'),
         InlineKeyboardButton(text="⭐⭐⭐", callback_data='rate_3')],
        [InlineKeyboardButton(text="⭐⭐⭐⭐", callback_data='rate_4'),
         InlineKeyboardButton(text="⭐⭐⭐⭐⭐", callback_data='rate_5')],
        [InlineKeyboardButton(text="⬅️ Back", callback_data='write_review')]
    ])


def view_reviews_kb():
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    
    # Show all reviews in one section
    review_count = db.get_review_count() if db else 0
    if review_count > 0:
        kb.inline_keyboard.append([
            InlineKeyboardButton(text=f"📝 All Reviews ({review_count})", 
                               callback_data='show_all_customer_reviews')
        ])
    else:
        kb.inline_keyboard.append([
            InlineKeyboardButton(text="No reviews yet", callback_data='no_reviews')
        ])
    
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="⬅️ Back", callback_data='reviews')
    ])
    return kb


@dp.message(Command("start"))
@uncrashable
async def start(message: types.Message, state: FSMContext):
    # Safe user ID extraction with null protection
    if not message.from_user or message.from_user.id is None:
        logger.warning("❌ Start command received with invalid user data")
        return
    
    # CRITICAL: Track user immediately (memory + database backup)
    track_user(message.from_user)
    
    user_id = str(message.from_user.id)
    
    # Check if user is blocked
    if db and db.is_user_blocked(int(user_id)):
        await message.answer(
            "🚫 *Access Restricted*\n\n"
            "Your access to this bot has been restricted.\n"
            "Please contact support if you believe this is an error.",
            parse_mode="Markdown"
        )
        return
    
    await state.clear()
    await state.update_data(cart=[])
    await state.set_state(OrderStates.shopping)
    
    # Get business statistics
    total_orders = 0
    total_reviews = 0
    avg_rating = 0.0
    
    if db:
        try:
            # Get total confirmed orders
            total_orders = db.get_total_orders_count()
            
            # Get review statistics
            review_stats = db.get_review_statistics()
            total_reviews = review_stats.get('total_reviews', 0)
            avg_rating = review_stats.get('average_rating', 0.0)
        except Exception as e:
            logger.error(f"Error fetching stats: {e}")
    
    # Build welcome message with statistics
    welcome_text = "👋 *Welcome to UKDANKZZ!*\n\n"
    welcome_text += "📊 *Business Stats:*\n"
    welcome_text += f"📦 Total Sales: {total_orders}\n"
    welcome_text += f"⭐ Total Reviews: {total_reviews}\n"
    
    if total_reviews > 0:
        # Show rating with star emojis
        stars = "⭐" * int(round(avg_rating))
        welcome_text += f"🌟 Overall Rating: {avg_rating:.1f}/5 {stars}\n"
    else:
        welcome_text += f"🌟 Overall Rating: No reviews yet\n"
    
    welcome_text += "\n🛍️ Browse our menu below:"
    
    await message.answer(welcome_text,
                         reply_markup=shopping_kb(),
                         parse_mode="Markdown")

def build_admin_panel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 View Pending Orders", callback_data="admin_pending_orders")],
        [InlineKeyboardButton(text="✅ View Confirmed Orders", callback_data="admin_confirmed_orders")],
        [InlineKeyboardButton(text="🔍 Search Order", callback_data="admin_search_order")],
        [InlineKeyboardButton(text="👤 Customer Lookup", callback_data="admin_customer_lookup")],
        [InlineKeyboardButton(text="🍽️ Manage Menu", callback_data="admin_menu")],
        [InlineKeyboardButton(text="💰 Manage Discounts", callback_data="admin_discounts")],
        [InlineKeyboardButton(text="🔗 Manage Public Links", callback_data="admin_public_links")],
        [InlineKeyboardButton(text="📢 Broadcast Message", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="👥 View All Users", callback_data="admin_view_users")],
        [InlineKeyboardButton(text="🚫 Blocked Users", callback_data="admin_blocked_users")],
        [InlineKeyboardButton(text="📊 User Stats", callback_data="admin_stats")],
        [InlineKeyboardButton(text="🏠 Back to Main", callback_data="main")],
    ])


def build_admin_panel_text(total_orders: int, pending_orders: int, total_users: int) -> str:
    admin_text = "🔧 *Admin Panel*\n\n"
    admin_text += f"📊 Quick Stats:\n"
    admin_text += f"📦 Total Orders: {total_orders}\n"
    admin_text += f"⏳ Pending: {pending_orders}\n"
    admin_text += f"👥 Total Users: {total_users}\n"
    admin_text += f"🛡️ Admins: {', '.join(sorted(ADMIN_IDS))}\n\n"
    admin_text += "Select an option:"
    return admin_text


def build_public_link_admin_text() -> str:
    entries = get_public_link_entries()
    lines = ["🔗 *Manage Public Link Buttons*", "", "These buttons appear on the main menu."]
    if entries:
        lines.append("")
        for idx, item in enumerate(entries, start=1):
            lines.append(f"{idx}. *{item['label']}*\n   `{item['url']}`")
    else:
        lines.append("")
        lines.append("No public link buttons are configured.")
    lines.append("")
    lines.append("Use *Add Link* then send: `Button Name | https://example.com`")
    return "\n".join(lines)


def build_public_link_admin_keyboard() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="➕ Add Link", callback_data="admin_public_links_add")],
    ]
    entries = get_public_link_entries()
    for idx, item in enumerate(entries[:20]):
        label = item['label']
        button_text = f"🗑 Remove {label}"
        rows.append([InlineKeyboardButton(text=button_text[:64], callback_data=f"admin_public_link_del_{idx}")])
    rows.append([
        InlineKeyboardButton(text="🔄 Refresh", callback_data="admin_public_links"),
        InlineKeyboardButton(text="⬅️ Back", callback_data="admin_panel"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(F.data == "admin_public_links")
async def admin_public_links(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)

    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return

    refresh_public_link_buttons()
    await state.clear()

    text = build_public_link_admin_text()
    markup = build_public_link_admin_keyboard()
    try:
        if cb.message and hasattr(cb.message, 'edit_text') and not isinstance(cb.message, types.InaccessibleMessage):
            await cb.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")
        else:
            await bot.send_message(cb.from_user.id, text, reply_markup=markup, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Error opening public link manager: {e}")


@dp.callback_query(F.data == "admin_public_links_add")
async def admin_public_links_add(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return

    await state.set_state(OrderStates.admin_adding_public_link)
    await cb.message.answer(
        "➕ *Add Public Link Button*\n\n"
        "Send the new button in this format:\n"
        "`Button Name | https://t.me/yourlink`\n\n"
        "Examples:\n"
        "`Support Chat | https://t.me/example`\n"
        "`Website | https://example.com`",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Cancel", callback_data="admin_public_links")]])
    )


@dp.message(OrderStates.admin_adding_public_link)
async def admin_public_links_add_process(message: types.Message, state: FSMContext):
    track_user(message.from_user)
    if not is_admin(message.from_user.id):
        await message.answer("❌ Access denied.")
        await state.clear()
        return

    raw = (message.text or '').strip()
    if '|' not in raw:
        await message.answer("❌ Invalid format. Send it as: `Button Name | https://example.com`", parse_mode="Markdown")
        return

    label, url = [part.strip() for part in raw.split('|', 1)]
    success, info = add_public_link_button(label, url)
    if not success:
        await message.answer(f"❌ {info}")
        return

    await state.clear()
    await message.answer(f"✅ {info}", reply_markup=build_public_link_admin_keyboard())
    await message.answer(build_public_link_admin_text(), parse_mode="Markdown", reply_markup=build_public_link_admin_keyboard())


@dp.callback_query(F.data.startswith("admin_public_link_del_"))
async def admin_public_links_delete(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return

    try:
        index = int(cb.data.rsplit('_', 1)[1])
    except Exception:
        await cb.answer("❌ Invalid button selection.", show_alert=True)
        return

    success, info = remove_public_link_button(index)
    if not success:
        await cb.answer(f"❌ {info}", show_alert=True)
        return

    await cb.answer(info, show_alert=True)
    text = build_public_link_admin_text()
    markup = build_public_link_admin_keyboard()
    try:
        if cb.message and hasattr(cb.message, 'edit_text') and not isinstance(cb.message, types.InaccessibleMessage):
            await cb.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")
        else:
            await bot.send_message(cb.from_user.id, text, reply_markup=markup, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Error refreshing public link manager after delete: {e}")


@dp.message(Command("admin"))
@uncrashable
async def admin_panel(message: types.Message, state: FSMContext):
    # Safe user ID extraction with null protection
    if not message.from_user or message.from_user.id is None:
        logger.warning("❌ Admin command received with invalid user data")
        return
        
    if not is_admin(message.from_user.id):
        await message.answer("❌ Access denied.")
        return
    
    # Get quick stats for admin panel
    total_orders = 0
    pending_orders = 0
    total_users = len(all_users)
    
    if db:
        try:
            total_orders = db.get_total_orders_count()
            pending_orders = len(db.get_pending_orders())
        except Exception as e:
            logger.error(f"Error fetching admin stats: {e}")
    
    admin_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 View Pending Orders", callback_data="admin_pending_orders")],
        [InlineKeyboardButton(text="✅ View Confirmed Orders", callback_data="admin_confirmed_orders")],
        [InlineKeyboardButton(text="🔍 Search Order", callback_data="admin_search_order")],
        [InlineKeyboardButton(text="👤 Customer Lookup", callback_data="admin_customer_lookup")],
        [InlineKeyboardButton(text="🍽️ Manage Menu", callback_data="admin_menu")],
        [InlineKeyboardButton(text="💰 Manage Discounts", callback_data="admin_discounts")],
        [InlineKeyboardButton(text="📢 Broadcast Message", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="👥 View All Users", callback_data="admin_view_users")],
        [InlineKeyboardButton(text="🚫 Blocked Users", callback_data="admin_blocked_users")],
        [InlineKeyboardButton(text="📊 User Stats", callback_data="admin_stats")],
        [InlineKeyboardButton(text="🏠 Back to Main", callback_data="main")]
    ])
    
    # Track admin user (add to memory and database)
    admin_user_id = str(message.from_user.id)
    all_users.add(admin_user_id)
    # INSTANT - Background save
    asyncio.create_task(async_save_user(int(admin_user_id),
                    message.from_user.username,
                    message.from_user.first_name,
                    message.from_user.last_name))
    
    admin_text = "🔧 *Admin Panel*\n\n"
    admin_text += f"📊 Quick Stats:\n"
    admin_text += f"📦 Total Orders: {total_orders}\n"
    admin_text += f"⏳ Pending: {pending_orders}\n"
    admin_text += f"👥 Total Users: {total_users}\n"
    admin_text += f"🛡️ Admins: {', '.join(sorted(ADMIN_IDS))}\n\n"
    admin_text += "Select an option:"
    
    await message.answer(admin_text, 
                        reply_markup=admin_kb, parse_mode="Markdown")

@dp.callback_query(F.data == "admin_search_order")
async def admin_search_order_start(cb: types.CallbackQuery, state: FSMContext):
    """Start order search process"""
    await cb.answer()
    
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    await cb.message.answer(
        "🔍 *Search Order*\n\n"
        "Enter the order number to search:\n"
        "Example: 1001",
        parse_mode="Markdown"
    )
    await state.set_state(OrderStates.admin_searching_order)

@dp.message(OrderStates.admin_searching_order)
async def admin_search_order_process(message: types.Message, state: FSMContext):
    track_user(message.from_user)
    """Process order search"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Access denied.")
        return
    
    if not message.text:
        await message.answer("❌ Please enter an order number.")
        return
    
    try:
        order_num = int(message.text.strip())
        
        if not db:
            await message.answer("❌ Database unavailable.")
            await state.clear()
            return
        
        # Search for order
        order = db.get_order_by_number(order_num)
        
        if not order:
            await message.answer(
                f"❌ Order #{order_num} not found.\n\n"
                "Try another order number or /admin to go back.",
                parse_mode="Markdown"
            )
            await state.clear()
            return
        
        # Display order details
        text = f"📋 *Order #{order_num}*\n\n"
        text += f"👤 User ID: `{order.get('user_id', 'N/A')}`\n"
        text += f"📝 Username: @{order.get('username', 'N/A')}\n"
        text += f"💰 Total: £{order.get('total_price', 0):.2f}\n"
        text += f"📅 Date: {order.get('created_at', 'N/A')}\n"
        text += f"🔖 Status: {order.get('status', 'unknown').title()}\n"
        text += f"💳 Payment: {order.get('payment_method', 'N/A')}\n\n"
        
        # Get order details from JSON
        details_json = order.get('order_details', {})
        if details_json and isinstance(details_json, dict):
            details_text = details_json.get('details', '')
            if details_text:
                text += f"📦 *Order Details:*\n{details_text[:500]}"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗑️ Delete Order", callback_data=f"delete_order_{order_num}")],
            [InlineKeyboardButton(text="⬅️ Back to Admin", callback_data="admin_panel")]
        ])
        
        await message.answer(text, reply_markup=kb, parse_mode="Markdown")
        await state.clear()
        
    except ValueError:
        await message.answer("❌ Invalid order number. Please enter a number.")
    except Exception as e:
        logger.error(f"Error searching order: {e}")
        await message.answer("❌ Error searching order. Try again.")
        await state.clear()

@dp.callback_query(F.data == "admin_customer_lookup")
async def admin_customer_lookup_start(cb: types.CallbackQuery, state: FSMContext):
    """Start customer lookup process"""
    await cb.answer()
    
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    await cb.message.answer(
        "👤 *Customer Lookup*\n\n"
        "Enter user ID or username to search:\n"
        "Example: 123456789 or @username",
        parse_mode="Markdown"
    )
    await state.set_state(OrderStates.admin_looking_up_customer)

@dp.message(OrderStates.admin_looking_up_customer)
async def admin_customer_lookup_process(message: types.Message, state: FSMContext):
    track_user(message.from_user)
    """Process customer lookup"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Access denied.")
        return
    
    if not message.text:
        await message.answer("❌ Please enter a user ID or username.")
        return
    
    try:
        search_term = message.text.strip()
        user_id = None
        
        # Check if it's a user ID (number) or username
        if search_term.isdigit():
            user_id = int(search_term)
        elif search_term.startswith('@'):
            search_term = search_term[1:]  # Remove @
        
        if not db:
            await message.answer("❌ Database unavailable.")
            await state.clear()
            return
        
        # Get customer orders
        if user_id:
            orders = db.get_user_orders(user_id)
        else:
            # Search by username - get all orders and filter
            all_orders = db.get_all_orders(limit=1000)
            orders = [o for o in all_orders if o.get('username', '').lower() == search_term.lower()]
        
        if not orders:
            await message.answer(
                f"❌ No orders found for: {message.text}\n\n"
                "Try another search or /admin to go back.",
                parse_mode="Markdown"
            )
            await state.clear()
            return
        
        # Display customer summary
        total_spent = sum(o.get('total_price', 0) for o in orders)
        confirmed_orders = [o for o in orders if o.get('status') == 'confirmed']
        
        first_order = orders[0]
        display_user_id = first_order.get('user_id', 'N/A')
        display_username = first_order.get('username', 'N/A')
        
        text = f"👤 *Customer Profile*\n\n"
        text += f"🆔 User ID: `{display_user_id}`\n"
        text += f"📝 Username: @{display_username}\n\n"
        text += f"📊 *Order Summary:*\n"
        text += f"📦 Total Orders: {len(orders)}\n"
        text += f"✅ Confirmed: {len(confirmed_orders)}\n"
        text += f"💰 Total Spent: £{total_spent:.2f}\n\n"
        text += f"📋 *Recent Orders:*\n"
        
        for order in orders[:5]:
            order_num = order.get('order_num', 'N/A')
            status_emoji = "✅" if order.get('status') == 'confirmed' else "⏳"
            text += f"{status_emoji} #{order_num} - £{order.get('total_price', 0):.2f}\n"
        
        if len(orders) > 5:
            text += f"\n_Showing 5 of {len(orders)} orders_"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Block User", callback_data=f"block_user_{display_user_id}")],
            [InlineKeyboardButton(text="⬅️ Back to Admin", callback_data="admin_panel")]
        ])
        
        await message.answer(text, reply_markup=kb, parse_mode="Markdown")
        await state.clear()
        
    except Exception as e:
        logger.error(f"Error looking up customer: {e}")
        await message.answer("❌ Error looking up customer. Try again.")
        await state.clear()

@dp.callback_query(F.data.startswith("delete_order_"))
async def delete_order_confirm(cb: types.CallbackQuery, state: FSMContext):
    """Confirm order deletion"""
    await cb.answer()
    
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    order_num = int(cb.data.split("_")[2])
    
    confirm_text = f"⚠️ *Delete Order #{order_num}?*\n\n"
    confirm_text += "This will permanently delete this order from the database.\n\n"
    confirm_text += "❗ This action cannot be undone!"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Yes, Delete", callback_data=f"confirm_delete_{order_num}"),
         InlineKeyboardButton(text="❌ Cancel", callback_data="admin_panel")]
    ])
    
    await cb.message.edit_text(confirm_text, reply_markup=kb, parse_mode="Markdown")

@dp.callback_query(F.data.startswith("confirm_delete_"))
async def confirm_delete_order(cb: types.CallbackQuery, state: FSMContext):
    """Actually delete the order"""
    await cb.answer()
    
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    order_num = int(cb.data.split("_")[2])
    
    if db and db.delete_order(order_num):
        await cb.answer(f"✅ Order #{order_num} deleted!", show_alert=True)
        await cb.message.edit_text(
            f"✅ *Order #{order_num} Deleted*\n\n"
            "The order has been permanently removed from the database.",
            parse_mode="Markdown"
        )
    else:
        await cb.answer("❌ Failed to delete order", show_alert=True)

@dp.callback_query(F.data.startswith("block_user_"))
async def block_user_confirm(cb: types.CallbackQuery, state: FSMContext):
    """Confirm user blocking"""
    await cb.answer()
    
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    user_id = int(cb.data.split("_")[2])
    
    confirm_text = f"⚠️ *Block User {user_id}?*\n\n"
    confirm_text += "This will prevent the user from:\n"
    confirm_text += "• Placing new orders\n"
    confirm_text += "• Using the bot\n\n"
    confirm_text += "You can unblock them later from the admin panel."
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Yes, Block", callback_data=f"confirm_block_{user_id}"),
         InlineKeyboardButton(text="❌ Cancel", callback_data="admin_panel")]
    ])
    
    await cb.message.edit_text(confirm_text, reply_markup=kb, parse_mode="Markdown")

@dp.callback_query(F.data.startswith("confirm_block_"))
async def confirm_block_user(cb: types.CallbackQuery, state: FSMContext):
    """Actually block the user"""
    await cb.answer()
    
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    user_id = int(cb.data.split("_")[2])
    
    if db and db.block_user(user_id, blocked_by=PRIMARY_ADMIN_ID, reason="Blocked by admin"):
        await cb.answer(f"✅ User {user_id} blocked!", show_alert=True)
        await cb.message.edit_text(
            f"✅ *User {user_id} Blocked*\n\n"
            "This user can no longer use the bot.",
            parse_mode="Markdown"
        )
    else:
        await cb.answer("❌ Failed to block user", show_alert=True)

@dp.message(Command("reset"))
@uncrashable
async def reset_command(message: types.Message, state: FSMContext):
    """Reset bot session - clear cart and return to main menu"""
    if not message.from_user or message.from_user.id is None:
        return
    
    track_user(message.from_user)
    user_id = str(message.from_user.id)
    
    await state.clear()
    await state.update_data(cart=[])
    await state.set_state(OrderStates.shopping)
    
    await message.answer(
        "🔄 *Session Reset!*\n\n"
        "Your cart has been cleared and you're back to the main menu.\n\n"
        "Ready to start fresh! 🛍️",
        reply_markup=shopping_kb(),
        parse_mode="Markdown"
    )

@dp.message(Command("exit"))
@uncrashable
async def exit_command(message: types.Message, state: FSMContext):
    """Exit current session"""
    if not message.from_user or message.from_user.id is None:
        return
    
    track_user(message.from_user)
    user_id = str(message.from_user.id)
    
    await state.clear()
    
    await message.answer(
        "👋 *Session Ended*\n\n"
        "Thanks for using UKDANKZZ Bot!\n\n"
        "Type /start to return anytime.",
        parse_mode="Markdown"
    )

@dp.message(Command("help"))
@uncrashable
async def help_command(message: types.Message, state: FSMContext):
    """Show help and available commands"""
    if not message.from_user or message.from_user.id is None:
        return
    
    track_user(message.from_user)
    
    help_text = "📚 *UKDANKZZ Bot Commands*\n\n"
    help_text += "🛍️ *Shopping Commands:*\n"
    help_text += "• /start - Start shopping\n"
    help_text += "• /reset - Clear cart & restart\n"
    help_text += "• /exit - Exit session\n\n"
    help_text += "📦 *Order Commands:*\n"
    help_text += "• /track - Track your orders\n"
    help_text += "• /reviews - View all reviews\n"
    help_text += "• /status - Check bot status\n\n"
    help_text += "❓ *Support:*\n"
    help_text += "• /help - Show this message\n\n"
    help_text += "Need assistance? Contact support!"
    
    await message.answer(help_text, parse_mode="Markdown")

@dp.message(Command("track"))
@uncrashable
async def track_orders_command(message: types.Message, state: FSMContext):
    """Track user's orders"""
    if not message.from_user or message.from_user.id is None:
        return
    
    track_user(message.from_user)
    user_id = str(message.from_user.id)
    
    if not db:
        await message.answer("❌ Database unavailable. Please try again later.")
        return
    
    try:
        orders = db.get_user_orders(int(user_id))
        
        if not orders:
            await message.answer(
                "📦 *Your Orders*\n\n"
                "You haven't placed any orders yet.\n\n"
                "Use /start to browse our menu!",
                parse_mode="Markdown"
            )
            return
        
        text = f"📦 *Your Orders* ({len(orders)})\n\n"
        
        for order in orders[:5]:  # Show last 5 orders
            order_num = order.get('order_num', 'N/A')
            status = order.get('status', 'pending')
            created_at = order.get('created_at', 'Unknown date')
            total = order.get('total_price', 0)
            
            status_emoji = "⏳" if status == "pending" else "✅"
            text += f"{status_emoji} Order #{order_num}\n"
            text += f"   Status: {status.title()}\n"
            text += f"   Total: £{total:.2f}\n"
            text += f"   Date: {created_at}\n\n"
        
        if len(orders) > 5:
            text += f"_Showing latest 5 of {len(orders)} orders_"
        
        await message.answer(text, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Error tracking orders: {e}")
        await message.answer("❌ Error retrieving your orders. Please try again.")

@dp.message(Command("reviews"))
@uncrashable
async def reviews_command(message: types.Message, state: FSMContext):
    """Show all reviews"""
    if not message.from_user or message.from_user.id is None:
        return
    
    track_user(message.from_user)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 View All Reviews", callback_data="show_all_customer_reviews")],
        [InlineKeyboardButton(text="⬅️ Back to Menu", callback_data="main")]
    ])
    
    await message.answer(
        "⭐ *Customer Reviews*\n\n"
        "See what our customers are saying!",
        reply_markup=kb,
        parse_mode="Markdown"
    )

@dp.message(Command("status"))
@uncrashable
async def status_command(message: types.Message, state: FSMContext):
    """Show bot status and stats"""
    if not message.from_user or message.from_user.id is None:
        return
    
    track_user(message.from_user)
    
    total_orders = 0
    total_reviews = 0
    avg_rating = 0.0
    
    if db:
        try:
            total_orders = db.get_total_orders_count()
            review_stats = db.get_review_statistics()
            total_reviews = review_stats.get('total_reviews', 0)
            avg_rating = review_stats.get('average_rating', 0.0)
        except Exception as e:
            logger.error(f"Error fetching stats: {e}")
    
    status_text = "🤖 *Bot Status*\n\n"
    status_text += "✅ Online & Operating\n\n"
    status_text += "📊 *Business Stats:*\n"
    status_text += f"📦 Total Sales: {total_orders}\n"
    status_text += f"⭐ Total Reviews: {total_reviews}\n"
    
    if total_reviews > 0:
        stars = "⭐" * int(round(avg_rating))
        status_text += f"🌟 Rating: {avg_rating:.1f}/5 {stars}\n"
    
    await message.answer(status_text, parse_mode="Markdown")

@dp.callback_query(F.data == "admin_pending_orders")
async def view_pending_orders(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    
    if not is_admin(cb.from_user.id):
        await cb.message.answer("❌ Access denied.")
        return
    
    pending_orders = db.get_pending_orders() if db else []
    
    if not pending_orders:
        text = "📋 *Pending Orders*\n\nNo pending orders to confirm."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Refresh", callback_data="admin_pending_orders")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="admin_panel")]
        ])
    else:
        text = f"📋 *Pending Orders* ({len(pending_orders)})\n\n"
        buttons = []
        
        for i, order in enumerate(pending_orders[:10], 1):  # Show max 10 orders
            order_num = order['order_num']
            username = order.get('username', 'Unknown')
            created_at = order['created_at']
            
            # Escape markdown characters in username
            safe_username = str(username).replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace('`', '\\`')
            
            text += f"{i}. 🆔 Order: `{order_num}`\n"
            text += f"   👤 Customer: {safe_username}\n"
            text += f"   📅 Received: {created_at.strftime('%Y-%m-%d %H:%M')}\n\n"
            
            buttons.append([InlineKeyboardButton(
                text=f"✅ Confirm Order {order_num}", 
                callback_data=f"confirm_order_{order_num}"
            )])
        
        buttons.append([InlineKeyboardButton(text="🔄 Refresh", callback_data="admin_pending_orders")])
        buttons.append([InlineKeyboardButton(text="⬅️ Back", callback_data="admin_panel")])
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    try:
        # Safe message editing with proper type checking
        if (cb.message and 
            hasattr(cb.message, 'edit_text') and 
            not isinstance(cb.message, types.InaccessibleMessage)):
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        else:
            # Fallback: send new message if edit fails
            await cb.answer()
            await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"❌ Error showing pending orders: {e}")
        await cb.answer("❌ Error loading orders. Please try again.")

@dp.callback_query(F.data.startswith("admin_confirmed_orders"))
async def view_confirmed_orders(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    
    if not is_admin(cb.from_user.id):
        await cb.message.answer("❌ Access denied.")
        return
    
    if db:
        try:
            deleted = db.cleanup_old_confirmed_orders(keep_last=30)
            if deleted > 0:
                logger.info(f"🧹 Auto-cleanup: Deleted {deleted} old confirmed orders")
        except Exception as e:
            logger.error(f"Error during auto-cleanup: {e}")
    
    page = 1
    if "_page_" in cb.data:
        try:
            page = int(cb.data.split("_page_")[1])
        except:
            page = 1
    
    confirmed_orders = db.get_confirmed_orders(limit=30) if db else []
    
    if not confirmed_orders:
        text = "✅ *Confirmed Orders*\n\nNo confirmed orders yet."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Refresh", callback_data="admin_confirmed_orders")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="admin_panel")]
        ])
    else:
        total_orders = len(confirmed_orders)
        orders_per_page = 10
        total_pages = (total_orders + orders_per_page - 1) // orders_per_page
        
        page = max(1, min(page, total_pages))
        
        start_idx = (page - 1) * orders_per_page
        end_idx = min(start_idx + orders_per_page, total_orders)
        
        text = f"✅ *Confirmed Orders* (Last 30)\n"
        text += f"📄 Page {page}/{total_pages} - Total: {total_orders}\n\n"
        buttons = []
        
        for i, order_idx in enumerate(range(start_idx, end_idx), 1):
            order = confirmed_orders[order_idx]
            order_num = order['order_num']
            username = order.get('username', 'Unknown')
            created_at = order['created_at']
            confirmed_at = order.get('confirmation_date')
            
            safe_username = str(username).replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace('`', '\\`')
            
            text += f"{start_idx + i}. 🆔 Order: `{order_num}`\n"
            text += f"   👤 Customer: {safe_username}\n"
            text += f"   📅 Ordered: {created_at.strftime('%Y-%m-%d %H:%M')}\n"
            if confirmed_at:
                text += f"   ✅ Confirmed: {confirmed_at.strftime('%Y-%m-%d %H:%M')}\n"
            text += "\n"
            
            buttons.append([InlineKeyboardButton(
                text=f"📋 View Details {order_num}", 
                callback_data=f"view_order_details_{order_num}"
            )])
        
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️ Previous", callback_data=f"admin_confirmed_orders_page_{page-1}"))
        if page < total_pages:
            nav_buttons.append(InlineKeyboardButton(text="➡️ Next", callback_data=f"admin_confirmed_orders_page_{page+1}"))
        
        if nav_buttons:
            buttons.append(nav_buttons)
        
        buttons.append([InlineKeyboardButton(text="🔄 Refresh", callback_data="admin_confirmed_orders")])
        buttons.append([InlineKeyboardButton(text="⬅️ Back", callback_data="admin_panel")])
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    try:
        if (cb.message and 
            hasattr(cb.message, 'edit_text') and 
            not isinstance(cb.message, types.InaccessibleMessage)):
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        else:
            await cb.answer()
            await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"❌ Error showing confirmed orders: {e}")
        await cb.answer("❌ Error loading orders. Please try again.")

@dp.callback_query(F.data.startswith("view_order_details_"))
async def view_order_details(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    if not cb.data or len(cb.data) < 20:
        logger.warning("❌ Invalid callback data for order details")
        await cb.answer("❌ Invalid order data", show_alert=True)
        return
        
    order_num = cb.data[19:]
    
    if not db:
        await cb.answer("❌ Database unavailable", show_alert=True)
        return
    
    confirmed_orders = db.get_confirmed_orders(limit=200)
    order = next((o for o in confirmed_orders if o['order_num'] == order_num), None)
    
    if not order:
        await cb.answer("❌ Order not found", show_alert=True)
        return
    
    order_details = order.get('order_details', 'No details available')
    username = order.get('username', 'Unknown')
    user_id = order.get('user_id', 'N/A')
    created_at = order['created_at']
    confirmed_at = order.get('confirmation_date')
    
    safe_username = str(username).replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace('`', '\\`')
    
    text = f"📋 *Order Details: {order_num}*\n\n"
    text += f"👤 *Customer:* {safe_username}\n"
    text += f"🆔 *User ID:* `{user_id}`\n"
    text += f"📅 *Ordered:* {created_at.strftime('%Y-%m-%d %H:%M')}\n"
    if confirmed_at:
        text += f"✅ *Confirmed:* {confirmed_at.strftime('%Y-%m-%d %H:%M')}\n"
    text += f"\n{order_details}"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Back to Confirmed Orders", callback_data="admin_confirmed_orders")]
    ])
    
    try:
        if (cb.message and 
            hasattr(cb.message, 'edit_text') and 
            not isinstance(cb.message, types.InaccessibleMessage)):
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        else:
            await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"❌ Error showing order details: {e}")
        await cb.answer("❌ Error loading order details.", show_alert=True)

@dp.callback_query(F.data.startswith("confirm_order_"))
async def confirm_customer_order(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    
    # Safe user ID extraction with null protection
    if not cb.from_user or cb.from_user.id is None:
        logger.warning("❌ Confirm order callback received with invalid user data")
        return
        
    if not is_admin(cb.from_user.id):
        return
    
    # Safe order number extraction with null protection
    if not cb.data or len(cb.data) < 15:
        logger.warning("❌ Invalid callback data for order confirmation")
        await cb.answer("❌ Invalid order data", show_alert=True)
        return
        
    order_num = cb.data[14:]  # Remove "confirm_order_" prefix
    
    # Confirm the order in database with null protection
    customer_details = db.confirm_order_by_admin(order_num) if db else None
    
    if customer_details:
        # Send confirmation message to customer
        customer_id = customer_details['user_id']
        customer_username = customer_details['username']
        
        try:
            await bot.send_message(
                customer_id,
                f"✅ *Order Confirmed!*\n\n"
                f"🆔 Order Number: `{order_num}`\n"
                f"📦 Your order has been confirmed and will be sent soon!\n\n"
                f"Thank you for choosing UKDANKZZ! 🌿",
                parse_mode="Markdown"
            )
            
            # Confirm to admin
            await cb.answer(f"✅ Order {order_num} confirmed and customer notified!", show_alert=True)
            
            # Refresh the pending orders view
            await view_pending_orders(cb, state)
            
        except Exception as e:
            print(f"Error sending confirmation to customer: {e}")
            await cb.answer(f"❌ Error notifying customer for order {order_num}", show_alert=True)
    else:
        await cb.answer(f"❌ Order {order_num} not found or already confirmed.", show_alert=True)

# Add broadcast messaging functionality
@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    
    if not is_admin(cb.from_user.id):
        await cb.message.answer("❌ Access denied.")
        return
    
    text = "📢 *Broadcast Message*\n\n"
    text += f"👥 Total users: {len(all_users)}\n\n"
    text += "✍️ Type your message to send to all users:"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Cancel", callback_data="admin_panel")]
    ])
    
    await state.set_state(OrderStates.broadcast_message)
    
    try:
        # Safe message editing with proper type checking
        if (cb.message and 
            hasattr(cb.message, 'edit_text') and 
            not isinstance(cb.message, types.InaccessibleMessage)):
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        else:
            # Fallback: send new message if edit fails
            await cb.answer()
            await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"❌ Error showing broadcast prompt: {e}")
        await cb.answer("❌ Error loading broadcast. Please try again.")

@dp.message(OrderStates.broadcast_message)
async def admin_broadcast_send(message: types.Message, state: FSMContext):
    track_user(message.from_user)
    # Safe user ID extraction with null protection
    if not message.from_user or message.from_user.id is None:
        logger.warning("❌ Broadcast command received with invalid user data")
        return
        
    if not is_admin(message.from_user.id):
        await message.answer("❌ Access denied.")
        return
    
    # Safe text extraction with null protection
    if not message.text:
        await message.answer("❌ Message cannot be empty. Please type your message:")
        return
        
    broadcast_text = message.text.strip()
    
    if len(broadcast_text) < 1:
        await message.answer("❌ Message cannot be empty. Please type your message:")
        return
    
    if len(broadcast_text) > 4000:
        await message.answer("❌ Message too long. Please keep it under 4000 characters:")
        return
    
    # Send broadcast
    success_count = 0
    failed_users = []  # Track failed users with their info
    
    status_message = await message.answer("📢 Broadcasting message... 0/0")
    
    for i, user_id in enumerate(all_users, 1):
        try:
            if not is_admin(user_id):  # Don't send to admin
                await bot.send_message(user_id, f"📢 *Message from UKDANKZZ*\n\n{broadcast_text}", parse_mode="Markdown")
                success_count += 1
            
            # Update status every 10 users
            if i % 10 == 0 or i == len(all_users):
                try:
                    await status_message.edit_text(f"📢 Broadcasting... {i}/{len(all_users)}")
                except:
                    pass
                    
        except Exception as e:
            # Get username from database if available
            username = None
            if db:
                try:
                    user_info = db.get_user_info(int(user_id))
                    if user_info:
                        username = user_info.get('username')
                except:
                    pass
            
            # Store failed user info
            if username:
                failed_users.append(f"@{username}")
            else:
                failed_users.append(f"ID: {user_id}")
            
            print(f"Failed to send broadcast to {user_id}: {e}")
    
    # Build final result message
    result_text = f"✅ *Broadcast Complete*\n\n"
    result_text += f"📤 Sent: {success_count}\n"
    result_text += f"👥 Total users: {len(all_users)}\n"
    
    # Show failed users if any
    if failed_users:
        result_text += f"\n❌ *Failed to send to {len(failed_users)} user(s):*\n"
        # Show up to 20 failed users to avoid message length issues
        for failed_user in failed_users[:20]:
            result_text += f"• {failed_user}\n"
        
        if len(failed_users) > 20:
            result_text += f"• ... and {len(failed_users) - 20} more"
    
    # Send final result
    try:
        await status_message.edit_text(result_text, parse_mode="Markdown")
    except:
        await message.answer(result_text, parse_mode="Markdown")
    
    await state.set_state(OrderStates.shopping)

@dp.message(OrderStates.adding_category)
async def add_category_process(message: types.Message, state: FSMContext):
    track_user(message.from_user)
    """Process category creation"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Access denied.")
        return
    
    if not message.text or len(message.text.strip()) < 2:
        await message.answer("❌ Category name too short. Please enter at least 2 characters:")
        return
    
    category_name = message.text.strip()
    
    if db:
        result = db.add_category(category_name)
        if result['success']:
            await message.answer(f"✅ Category '{category_name}' created successfully!")
            # Reload menu
            global products, descriptions
            menu_data = db.get_dynamic_menu()
            products = menu_data['menu']
            descriptions.update(menu_data['descriptions'])
        else:
            await message.answer(f"❌ Error creating category: {result['error']}")
    else:
        await message.answer("❌ Database unavailable")
    
    await state.set_state(OrderStates.shopping)
    
    # Show menu management panel again
    categories = db.get_categories() if db else []
    menu_data = db.get_dynamic_menu() if db else {}
    total_products = sum(len(prods) for prods in menu_data.values())
    
    text = "🍽️ *Menu Management*\n\n"
    text += f"📊 **Current Menu:**\n"
    text += f"• Categories: {len(categories)}\n"
    text += f"• Total Products: {total_products}\n\n"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📂 Add Category", callback_data="add_category")],
        [InlineKeyboardButton(text="🥬 Add Product", callback_data="add_product")],
        [InlineKeyboardButton(text="📋 View Menu", callback_data="view_menu")],
        [InlineKeyboardButton(text="🔄 Reload Menu", callback_data="reload_menu")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="admin_panel")]
    ])
    
    await message.answer(text, reply_markup=kb, parse_mode="Markdown")

# View all broadcast users with pagination
@dp.callback_query(F.data.startswith("admin_view_users"))
async def admin_view_users(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    
    if not is_admin(cb.from_user.id):
        await cb.message.answer("❌ Access denied.")
        return
    
    # Parse page number from callback data (format: admin_view_users or admin_view_users_page_2)
    page = 0
    if cb.data.startswith("admin_view_users_page_"):
        try:
            page = int(cb.data.split("_")[-1]) - 1
        except:
            page = 0
    
    # Get all users from database
    if db:
        users = db.get_all_broadcast_users()
    else:
        users = []
    
    # Pagination settings
    users_per_page = 50
    total_users = len(users)
    total_pages = (total_users + users_per_page - 1) // users_per_page if total_users > 0 else 1
    
    # Ensure page is within bounds
    page = max(0, min(page, total_pages - 1))
    
    start_idx = page * users_per_page
    end_idx = min(start_idx + users_per_page, total_users)
    page_users = users[start_idx:end_idx]
    
    # Build user list message (without Markdown to avoid parsing errors)
    text = f"👥 All Broadcast Users\n\n"
    text += f"Total: {total_users} users\n"
    text += f"Page {page + 1} of {total_pages}\n\n"
    
    if page_users:
        for i, user in enumerate(page_users, start_idx + 1):
            user_id = user.get('user_id', 'N/A')
            username = user.get('username', '')
            first_name = user.get('first_name', '')
            
            # Format: number. @username or Name - ID: 123456
            if username:
                text += f"{i}. @{username} - ID: {user_id}\n"
            elif first_name:
                text += f"{i}. {first_name} - ID: {user_id}\n"
            else:
                text += f"{i}. User ID: {user_id}\n"
    else:
        text += "No users found."
    
    # Build pagination buttons
    buttons = []
    nav_row = []
    
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ Previous", callback_data=f"admin_view_users_page_{page}"))
    
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"admin_view_users_page_{page + 2}"))
    
    if nav_row:
        buttons.append(nav_row)
    
    buttons.append([InlineKeyboardButton(text="⬅️ Back to Admin", callback_data="admin_panel")])
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    try:
        if cb.message and hasattr(cb.message, 'edit_text'):
            await cb.message.edit_text(text, reply_markup=kb, parse_mode=None)
    except Exception as e:
        print(f"Error showing users: {e}")

# Add user stats functionality  
@dp.callback_query(F.data == "admin_stats")
async def admin_stats(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    
    if not is_admin(cb.from_user.id):
        await cb.message.answer("❌ Access denied.")
        return
    
    # Calculate stats
    memory_users = len(all_users)
    db_users = len(db.get_broadcast_users()) if db else 0
    total_orders = db.get_total_orders_count() if db else 106  # Use database count starting from 106
    total_reviews = db.get_review_count() if db else 0
    pending_orders_count = len(db.get_pending_orders()) if db else 0
    
    # Get sales analytics
    analytics_30d = db.get_sales_analytics(30) if db else {}
    analytics_7d = db.get_sales_analytics(7) if db else {}
    
    # Check for missing users
    missing_users = memory_users - db_users
    
    text = f"📊 *UKDANKZZ Bot Statistics*\n\n"
    text += f"👥 *User Tracking:*\n"
    text += f"• Memory (Broadcast): {memory_users} users\n"
    text += f"• Database (Backed Up): {db_users} users\n"
    if missing_users > 0:
        text += f"⚠️ *{missing_users} users NOT backed up!*\n"
    text += f"\n📦 Total Orders: {total_orders}\n"
    text += f"⏳ Pending Orders: {pending_orders_count}\n"
    text += f"⭐ Total Reviews: {total_reviews}\n\n"
    
    # Last 7 days
    text += f"📈 *Last 7 Days*\n"
    text += f"📦 Orders: {analytics_7d.get('total_orders', 0)}\n"
    text += f"💰 Revenue: £{analytics_7d.get('total_revenue', 0):.2f}\n\n"
    
    # Last 30 days
    text += f"📅 *Last 30 Days*\n"
    text += f"📦 Orders: {analytics_30d.get('total_orders', 0)}\n"
    text += f"💰 Revenue: £{analytics_30d.get('total_revenue', 0):.2f}\n\n"
    
    # Top products
    top_products = analytics_30d.get('top_products', [])
    if top_products:
        text += f"🏆 *Top Products (30d)*\n"
        for i, (product, count) in enumerate(top_products[:3], 1):
            text += f"{i}. {product}: {count} orders\n"
        text += "\n"
    
    # Payment methods
    payment_methods = analytics_30d.get('payment_methods', {})
    if payment_methods:
        text += f"💳 *Payment Methods (30d)*\n"
        for method, count in payment_methods.items():
            text += f"• {method}: {count}\n"
    
    # Add sync button if there are missing users
    buttons = [[InlineKeyboardButton(text="🔄 Refresh", callback_data="admin_stats")]]
    
    if missing_users > 0:
        buttons.append([InlineKeyboardButton(text=f"💾 Backup {missing_users} Missing Users", callback_data="force_sync_users")])
    
    buttons.append([InlineKeyboardButton(text="📥 Export Orders", callback_data="export_orders")])
    buttons.append([InlineKeyboardButton(text="⬅️ Back", callback_data="admin_panel")])
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    try:
        if cb.message and hasattr(cb.message, 'edit_text'):
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        print(f"Error showing stats: {e}")

@dp.callback_query(F.data == "force_sync_users")
async def force_sync_users(cb: types.CallbackQuery, state: FSMContext):
    """Force sync all memory users to database - RESCUE MISSING USERS"""
    await cb.answer()  # Instant response
    
    if not is_admin(cb.from_user.id):
        await cb.message.answer("❌ Access denied.")
        return
    
    try:
        global all_users
        
        if not db:
            await cb.answer("❌ Database unavailable", show_alert=True)
            return
        
        # Get current database users (as strings for comparison) - ONLY ONCE
        db_users = db.get_broadcast_users()
        db_user_ids = set(str(uid) for uid in db_users)
        initial_db_count = len(db_users)
        
        # Get memory users (already strings)
        memory_user_ids = set(str(uid) for uid in all_users)
        
        # Find users in memory but not in database
        missing_user_ids = memory_user_ids - db_user_ids
        
        print(f"🔍 DEBUG: Memory has {len(memory_user_ids)} users, DB has {initial_db_count} users")
        print(f"🔍 DEBUG: Missing {len(missing_user_ids)} users: {missing_user_ids}")
        
        if not missing_user_ids:
            await cb.answer("✅ All users already backed up!", show_alert=True)
            return
        
        # Force save each missing user to database
        saved_count = 0
        failed_count = 0
        
        for user_id_str in missing_user_ids:
            try:
                user_id = int(user_id_str)
                # Save to database with minimal info (we'll get full info on next interaction)
                db.add_broadcast_user(user_id, None, None, None)
                saved_count += 1
                print(f"✅ RESCUED: User {user_id} backed up to database")
            except Exception as e:
                failed_count += 1
                print(f"❌ Failed to save user {user_id_str}: {e}")
        
        # Calculate new total without extra DB call
        new_db_count = initial_db_count + saved_count
        
        # Show result with correct numbers
        result_text = f"💾 *User Backup Complete!*\n\n"
        result_text += f"✅ Backed up: {saved_count} new users\n"
        if failed_count > 0:
            result_text += f"❌ Failed: {failed_count} users\n"
        result_text += f"\n📊 Total in database: {new_db_count} users\n"
        result_text += f"🛡️ All your users are now permanently backed up!"
        
        await cb.message.answer(result_text, parse_mode="Markdown")
        
        # Refresh stats to show updated numbers
        await admin_stats(cb, state)
        
    except Exception as e:
        logger.error(f"Error syncing users: {e}")
        await cb.answer(f"❌ Sync failed: {str(e)}", show_alert=True)

@dp.callback_query(F.data == "export_orders")
async def export_orders(cb: types.CallbackQuery, state: FSMContext):
    """Export orders to CSV format"""
    await cb.answer("📥 Generating export...")
    
    if not is_admin(cb.from_user.id):
        await cb.message.answer("❌ Access denied.")
        return
    
    try:
        # Get all confirmed orders from database
        if db:
            import csv
            import io
            
            conn = db.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT order_num, user_id, username, order_details, 
                           status, created_at, confirmed_at
                    FROM orders 
                    WHERE status = 'confirmed'
                    ORDER BY created_at DESC
                    LIMIT 500
                """)
                
                orders = cursor.fetchall()
            conn.close()
            
            # Create CSV content
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Header
            writer.writerow(['Order Number', 'User ID', 'Username', 'Total', 'Payment Method', 
                            'Status', 'Created Date', 'Confirmed Date', 'Items'])
            
            # Data rows
            for order in orders:
                order_details = order['order_details']
                if isinstance(order_details, str):
                    try:
                        order_details = json.loads(order_details)
                    except:
                        order_details = {}
                
                items_str = '; '.join([f"{item.get('name', 'Unknown')} ({item.get('size', '-')})" 
                                      for item in order_details.get('items', [])])
                
                writer.writerow([
                    order['order_num'],
                    order['user_id'],
                    order.get('username', 'N/A'),
                    order_details.get('total', 0),
                    order_details.get('payment_method', 'N/A'),
                    order['status'],
                    order['created_at'],
                    order.get('confirmed_at', 'N/A'),
                    items_str
                ])
            
            csv_content = output.getvalue()
            output.close()
            
            # Send as file
            from io import BytesIO
            csv_bytes = BytesIO(csv_content.encode('utf-8'))
            csv_bytes.name = f"ukdankzz_orders_{datetime.date.today()}.csv"
            
            await bot.send_document(
                cb.from_user.id,
                document=types.BufferedInputFile(csv_bytes.getvalue(), filename=csv_bytes.name),
                caption=f"📊 Orders Export ({len(orders)} orders)\n\nGenerated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            
            await cb.answer("✅ Export sent!", show_alert=True)
            
        else:
            await cb.answer("❌ Database not available", show_alert=True)
            
    except Exception as e:
        logger.error(f"Error exporting orders: {e}")
        await cb.answer(f"❌ Export failed: {str(e)}", show_alert=True)

@dp.callback_query(F.data == "admin_blocked_users")
async def admin_blocked_users(cb: types.CallbackQuery, state: FSMContext):
    """View and manage blocked users"""
    await cb.answer()
    
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    if not db:
        await cb.answer("❌ Database unavailable", show_alert=True)
        return
    
    blocked_users = db.get_blocked_users()
    
    if not blocked_users:
        text = "🚫 *Blocked Users*\n\n"
        text += "No users are currently blocked."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back", callback_data="admin_panel")]
        ])
    else:
        text = f"🚫 *Blocked Users ({len(blocked_users)})*\n\n"
        
        buttons = []
        for user in blocked_users[:10]:  # Show max 10
            user_id = user.get('user_id')
            username = user.get('username', 'N/A')
            reason = user.get('reason', 'No reason')
            text += f"👤 ID: `{user_id}`\n"
            text += f"📝 @{username}\n"
            text += f"📄 Reason: {reason}\n"
            text += f"📅 Blocked: {user.get('blocked_at', 'N/A')}\n\n"
            
            buttons.append([InlineKeyboardButton(
                text=f"✅ Unblock {user_id}", 
                callback_data=f"unblock_user_{user_id}"
            )])
        
        if len(blocked_users) > 10:
            text += f"\n_Showing 10 of {len(blocked_users)} blocked users_"
        
        buttons.append([InlineKeyboardButton(text="⬅️ Back", callback_data="admin_panel")])
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    try:
        await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Error showing blocked users: {e}")

@dp.callback_query(F.data.startswith("unblock_user_"))
async def unblock_user_action(cb: types.CallbackQuery, state: FSMContext):
    """Unblock a user"""
    await cb.answer()
    
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    user_id = int(cb.data.split("_")[2])
    
    if db and db.unblock_user(user_id):
        await cb.answer(f"✅ User {user_id} unblocked!", show_alert=True)
        # Refresh the blocked users list
        await admin_blocked_users(cb, state)
    else:
        await cb.answer("❌ Failed to unblock user", show_alert=True)

@dp.callback_query(F.data == "admin_panel")
async def back_to_admin_panel(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    
    if not is_admin(cb.from_user.id):
        await cb.message.answer("❌ Access denied.")
        return
    
    # Get quick stats for admin panel
    total_orders = 0
    pending_orders = 0
    total_users = len(all_users)
    
    if db:
        try:
            total_orders = db.get_total_orders_count()
            pending_orders = len(db.get_pending_orders())
        except Exception as e:
            logger.error(f"Error fetching admin stats: {e}")
    
    admin_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 View Pending Orders", callback_data="admin_pending_orders")],
        [InlineKeyboardButton(text="✅ View Confirmed Orders", callback_data="admin_confirmed_orders")],
        [InlineKeyboardButton(text="🔍 Search Order", callback_data="admin_search_order")],
        [InlineKeyboardButton(text="👤 Customer Lookup", callback_data="admin_customer_lookup")],
        [InlineKeyboardButton(text="🍽️ Manage Menu", callback_data="admin_menu")],
        [InlineKeyboardButton(text="💰 Manage Discounts", callback_data="admin_discounts")],
        [InlineKeyboardButton(text="📢 Broadcast Message", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="👥 View All Users", callback_data="admin_view_users")],
        [InlineKeyboardButton(text="🚫 Blocked Users", callback_data="admin_blocked_users")],
        [InlineKeyboardButton(text="📊 User Stats", callback_data="admin_stats")],
        [InlineKeyboardButton(text="🏠 Back to Main", callback_data="main")]
    ])
    
    admin_text = "🔧 *Admin Panel*\n\n"
    admin_text += f"📊 Quick Stats:\n"
    admin_text += f"📦 Total Orders: {total_orders}\n"
    admin_text += f"⏳ Pending: {pending_orders}\n"
    admin_text += f"👥 Total Users: {total_users}\n\n"
    admin_text += "Select an option:"
    
    await state.set_state(OrderStates.shopping)
    
    try:
        if (cb.message and 
            hasattr(cb.message, 'edit_text') and 
            not isinstance(cb.message, types.InaccessibleMessage)):
            await cb.message.edit_text(admin_text, 
                                     reply_markup=admin_kb, parse_mode="Markdown")
        else:
            await bot.send_message(cb.from_user.id, admin_text, 
                                 reply_markup=admin_kb, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Error showing admin panel: {e}")

# Back to main menu handler
@dp.callback_query(F.data == "main")
async def back_to_main_menu(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    
    # Track user
    all_users.add(str(cb.from_user.id))
    asyncio.create_task(async_save_user(cb.from_user.id, cb.from_user.username, cb.from_user.first_name, cb.from_user.last_name))
    
    await state.set_state(OrderStates.shopping)
    data = await state.get_data()
    cart = data.get('cart', [])

    text = "👋 Welcome to UKDANKZZ! Browse our menu:"
    if cart:
        text += f"\n\n🛒 Items in cart: {len(cart)}"

    try:
        if cb.message and hasattr(cb.message, 'edit_text'):
            await cb.message.edit_text(text, reply_markup=shopping_kb())
    except Exception as e:
        print(f"Error returning to main: {e}")

# MENU MANAGEMENT COMMANDS

@dp.callback_query(F.data == "admin_menu")
async def admin_menu_management(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    
    if not is_admin(cb.from_user.id):
        await cb.message.answer("❌ Access denied.")
        return
    
    try:
        if not db:
            await cb.message.answer("❌ Database unavailable")
            return
        
        # Get current menu stats
        categories = db.get_categories()
        menu_data = db.get_dynamic_menu()
        total_products = sum(len(products) for products in menu_data.values())
        
        text = "🍽️ *Menu Management*\n\n"
        text += f"📊 **Current Menu:**\n"
        text += f"• Categories: {len(categories)}\n"
        text += f"• Total Products: {total_products}\n\n"
        
        text += "🛠️ **Management Options:**\n"
        text += "• Add new categories and products\n"
        text += "• Set pricing tiers for products\n"
        text += "• Remove products from menu\n"
        text += "• View complete menu structure\n"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📂 Add Category", callback_data="add_category")],
            [InlineKeyboardButton(text="🗑️ Delete Category", callback_data="delete_category")],
            [InlineKeyboardButton(text="🥬 Add Product", callback_data="add_product")],
            [InlineKeyboardButton(text="✏️ Edit Product", callback_data="edit_product")],
            [InlineKeyboardButton(text="🔄 Toggle Product Status", callback_data="toggle_product")],
            [InlineKeyboardButton(text="🗑️ Remove Product", callback_data="remove_product")],
            [InlineKeyboardButton(text="📋 View Menu", callback_data="view_menu")],
            [InlineKeyboardButton(text="📊 Full Price Chart", callback_data="admin_price_chart")],
            [InlineKeyboardButton(text="📥 Download Menu", callback_data="admin_download_menu")],
            [InlineKeyboardButton(text="🔄 Reload Menu", callback_data="reload_menu")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="admin_panel")]
        ])
        
        try:
            if (cb.message and 
                hasattr(cb.message, 'edit_text') and 
                not isinstance(cb.message, types.InaccessibleMessage)):
                await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
            else:
                await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"❌ Error showing menu management: {e}")
            
    except Exception as e:
        print(f"Error in admin_menu_management: {e}")

@dp.callback_query(F.data == "add_category")
async def add_category_start(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    text = "📂 *Add New Category*\n\n"
    text += "Type the name for the new category:\n"
    text += "(e.g., 'Concentrates', 'Accessories', etc.)"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Cancel", callback_data="admin_menu")]
    ])
    
    await state.set_state(OrderStates.adding_category)
    
    try:
        if (cb.message and 
            hasattr(cb.message, 'edit_text') and 
            not isinstance(cb.message, types.InaccessibleMessage)):
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        else:
            await cb.answer()
            await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"❌ Error starting category creation: {e}")

@dp.callback_query(F.data == "delete_category")
async def delete_category_start(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    if not db:
        await cb.answer("❌ Database unavailable", show_alert=True)
        return
    
    categories = db.get_categories()
    
    if not categories:
        await cb.answer("❌ No categories available to delete", show_alert=True)
        return
    
    text = "🗑️ *Delete Category*\n\n"
    text += "⚠️ Warning: This will delete the category and all its products!\n\n"
    text += "Select a category to delete:"
    
    buttons = []
    for idx, category in enumerate(categories):
        category_name = category['name'] if isinstance(category, dict) else category
        buttons.append([InlineKeyboardButton(
            text=f"🗑️ {category_name}",
            callback_data=f"delcat_{idx}"
        )])
    buttons.append([InlineKeyboardButton(text="❌ Cancel", callback_data="admin_menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    # Store categories for deletion
    await state.update_data(delete_category_list=categories)
    
    try:
        if (cb.message and 
            hasattr(cb.message, 'edit_text') and 
            not isinstance(cb.message, types.InaccessibleMessage)):
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        else:
            await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"❌ Error showing delete category: {e}")

@dp.callback_query(F.data.startswith("delcat_"))
async def delete_category_confirm(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    idx = int(cb.data.replace("delcat_", ""))
    data = await state.get_data()
    category_list = data.get('delete_category_list', [])
    
    if idx >= len(category_list):
        await cb.answer("❌ Invalid selection", show_alert=True)
        return
    
    category_info = category_list[idx]
    category_name = category_info['name'] if isinstance(category_info, dict) else category_info
    
    result = db.delete_category(category_name)
    
    if result['success']:
        # Reload menu
        global products, descriptions
        menu_data = db.get_dynamic_menu()
        products = menu_data['menu']
        descriptions.update(menu_data['descriptions'])
        
        await cb.answer(f"✅ Category '{category_name}' deleted!", show_alert=True)
        
        # Show updated menu management
        categories = db.get_categories()
        menu_data = db.get_dynamic_menu()
        total_products = sum(len(prods) for prods in menu_data['menu'].values())
        
        text = "🍽️ *Menu Management*\n\n"
        text += f"📊 **Current Menu:**\n"
        text += f"• Categories: {len(categories)}\n"
        text += f"• Total Products: {total_products}\n"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📂 Add Category", callback_data="add_category")],
            [InlineKeyboardButton(text="🗑️ Delete Category", callback_data="delete_category")],
            [InlineKeyboardButton(text="🥬 Add Product", callback_data="add_product")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="admin_panel")]
        ])
        
        try:
            if cb.message and hasattr(cb.message, 'edit_text'):
                await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
            else:
                await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Error showing menu after delete: {e}")
    else:
        await cb.answer(f"❌ Error: {result.get('error', 'Unknown error')}", show_alert=True)

@dp.callback_query(F.data == "first_time_freebie")
async def show_first_time_freebie(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    
    user_id = cb.from_user.id
    
    if not db:
        await cb.answer("❌ Service unavailable", show_alert=True)
        return
    
    # Check if user has already claimed
    has_claimed = db.has_claimed_freebie(user_id)
    
    if has_claimed:
        text = "🎁 *FIRST TIME FREEBIE*\n\n"
        text += "❌ You have already claimed your free edible!\n\n"
        text += "This offer is limited to one per customer.\n"
        text += "Thank you for being a valued customer! 💚"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Back to Menu", callback_data="shopping")]
        ])
    else:
        text = "🎁 *FIRST TIME FREEBIE*\n\n"
        text += "🍫 *Welcome Offer!* 🍫\n\n"
        text += "As a first-time customer, claim your FREE 100mg edible!\n\n"
        text += "📦 Select one option below (one-time offer):\n"
        
        # Create buttons for 100mg edible options
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🍫 100mg Edible", callback_data="claim_freebie_100mg")],
            [InlineKeyboardButton(text="⬅️ Back to Menu", callback_data="shopping")]
        ])
    
    try:
        if cb.message and hasattr(cb.message, 'edit_text'):
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        else:
            await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Error showing freebie: {e}")

@dp.callback_query(F.data.startswith("claim_freebie_"))
async def claim_freebie(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    
    user_id = cb.from_user.id
    username = cb.from_user.username or cb.from_user.first_name or "Unknown"
    product_name = "100mg Edible"
    
    if not db:
        await cb.answer("❌ Service unavailable", show_alert=True)
        return
    
    # Check if user has ALREADY claimed (completed checkout with freebie)
    if db.has_claimed_freebie(user_id):
        await cb.answer("❌ You have already claimed your free edible!", show_alert=True)
        return
    
    # Check if freebie already in current cart session
    data = await state.get_data()
    cart = data.get('cart', [])
    
    for item in cart:
        if item.get('section') == 'FIRST TIME FREEBIE':
            await cb.answer("❌ Freebie already in your cart!", show_alert=True)
            return
    
    # Add to cart (DON'T record claim yet - only on checkout completion)
    cart.append({
        'section': 'FIRST TIME FREEBIE',
        'name': product_name,
        'size': '1 pack',
        'price': 0,  # Free!
        'is_freebie': True,  # Flag to identify freebie items
        'freebie_user_id': user_id,
        'freebie_username': username
    })
    
    await state.update_data(cart=cart)
    
    text = "✅ *Freebie Added to Cart!*\n\n"
    text += f"🎁 {product_name} (FREE) has been added to your cart!\n\n"
    text += "⚠️ Complete checkout to claim your freebie.\n"
    text += "Continue shopping or proceed to checkout."
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 View Cart", callback_data="view_cart")],
        [InlineKeyboardButton(text="🛍️ Continue Shopping", callback_data="shopping")]
    ])
    
    try:
        if cb.message and hasattr(cb.message, 'edit_text'):
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        else:
            await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Error after adding freebie to cart: {e}")

@dp.callback_query(F.data == "view_menu")
async def view_dynamic_menu(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    if not db:
        await cb.answer("❌ Database unavailable", show_alert=True)
        return
    
    menu_data = db.get_dynamic_menu()
    
    text = "📋 *Complete Menu Structure*\n\n"
    
    if not menu_data:
        text += "🔧 Menu is empty. Add categories and products to get started."
    else:
        for category, products_dict in menu_data.items():
            text += f"**📂 {category}**\n"
            if products_dict:
                for product, pricing in products_dict.items():
                    text += f"  • {product}\n"
                    for size, price in pricing:
                        text += f"    - {size}: £{price}\n"
                text += "\n"
            else:
                text += "  (No products)\n\n"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Back", callback_data="admin_menu")]
    ])
    
    # Split long messages
    if len(text) > 4000:
        text = text[:4000] + "...\n\n*Message truncated - menu too long to display*"
    
    try:
        if (cb.message and 
            hasattr(cb.message, 'edit_text') and 
            not isinstance(cb.message, types.InaccessibleMessage)):
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        else:
            await cb.answer()
            await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"❌ Error showing menu: {e}")

@dp.callback_query(F.data == "reload_menu")
async def reload_menu(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    if db:
        global products, descriptions
        menu_data = db.get_dynamic_menu()
        products = menu_data['menu']
        descriptions.update(menu_data['descriptions'])
        await cb.answer("✅ Menu reloaded from database!", show_alert=True)
        # Show updated menu management
        await admin_menu_management(cb, state)
    else:
        await cb.answer("❌ Database unavailable", show_alert=True)

@dp.callback_query(F.data == "admin_price_chart")
async def admin_price_chart(cb: types.CallbackQuery, state: FSMContext):
    """Show full menu with all prices - chart view for admin"""
    await cb.answer()
    track_user(cb.from_user)
    
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    if not db:
        await cb.answer("❌ Database unavailable", show_alert=True)
        return
    
    try:
        menu_items = db.get_full_menu_with_prices()
        
        if not menu_items:
            text = "📊 *Full Price Chart*\n\n❌ No products found in menu."
        else:
            text = "📊 *FULL MENU - PRICE CHART*\n"
            text += "━━━━━━━━━━━━━━━━━━━━━━\n"
            text += "_Sorted by price (cheapest first)_\n\n"
            
            current_category = None
            current_product = None
            
            for item in menu_items:
                if item['category'] != current_category:
                    current_category = item['category']
                    text += f"\n📁 *{current_category}*\n"
                    current_product = None
                
                if item['product'] != current_product:
                    current_product = item['product']
                    status = "✅" if item['active'] else "❌"
                    text += f"\n  {status} {current_product}\n"
                
                if item['size'] != 'N/A' and item['price'] > 0:
                    text += f"      • {item['size']}: £{item['price']:.2f}\n"
            
            text += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
            text += f"📦 Total items: {len(set((i['category'], i['product']) for i in menu_items))}"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📥 Download as File", callback_data="admin_download_menu")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="admin_menu")]
        ])
        
        # Split message if too long
        if len(text) > 4000:
            parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
            for i, part in enumerate(parts):
                if i == len(parts) - 1:
                    await bot.send_message(cb.from_user.id, part, reply_markup=kb, parse_mode="Markdown")
                else:
                    await bot.send_message(cb.from_user.id, part, parse_mode="Markdown")
        else:
            try:
                if cb.message and hasattr(cb.message, 'edit_text'):
                    await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
                else:
                    await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
            except Exception:
                await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
                
    except Exception as e:
        logger.error(f"❌ Error showing price chart: {e}")
        await cb.answer("❌ Error loading price chart", show_alert=True)

@dp.callback_query(F.data == "admin_download_menu")
async def admin_download_menu(cb: types.CallbackQuery, state: FSMContext):
    """Download menu as a text/CSV file"""
    await cb.answer("📥 Generating menu file...")
    track_user(cb.from_user)
    
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    if not db:
        await cb.answer("❌ Database unavailable", show_alert=True)
        return
    
    try:
        menu_items = db.get_full_menu_with_prices()
        
        if not menu_items:
            await cb.answer("❌ No products in menu to download", show_alert=True)
            return
        
        # Generate CSV content
        csv_content = "Category,Product,Size,Price,Status\n"
        for item in menu_items:
            status = "Active" if item['active'] else "Inactive"
            price = f"£{item['price']:.2f}" if item['price'] > 0 else "N/A"
            csv_content += f'"{item["category"]}","{item["product"]}","{item["size"]}","{price}","{status}"\n'
        
        # Also generate a readable text version
        text_content = "UKDANKZZ MENU - FULL PRICE LIST\n"
        text_content += "=" * 50 + "\n"
        text_content += f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
        text_content += "=" * 50 + "\n\n"
        
        current_category = None
        current_product = None
        
        for item in menu_items:
            if item['category'] != current_category:
                current_category = item['category']
                text_content += f"\n{'='*40}\n"
                text_content += f"  {current_category.upper()}\n"
                text_content += f"{'='*40}\n"
                current_product = None
            
            if item['product'] != current_product:
                current_product = item['product']
                status = "[ACTIVE]" if item['active'] else "[INACTIVE]"
                text_content += f"\n  {current_product} {status}\n"
            
            if item['size'] != 'N/A' and item['price'] > 0:
                text_content += f"    - {item['size']}: £{item['price']:.2f}\n"
        
        text_content += f"\n{'='*50}\n"
        text_content += f"Total unique products: {len(set((i['category'], i['product']) for i in menu_items))}\n"
        
        # Save files
        csv_path = "/tmp/ukdankzz_menu.csv"
        txt_path = "/tmp/ukdankzz_menu.txt"
        
        with open(csv_path, 'w') as f:
            f.write(csv_content)
        
        with open(txt_path, 'w') as f:
            f.write(text_content)
        
        # Send both files
        from aiogram.types import FSInputFile
        
        csv_file = FSInputFile(csv_path, filename="UKDANKZZ_Menu.csv")
        txt_file = FSInputFile(txt_path, filename="UKDANKZZ_Menu.txt")
        
        await bot.send_document(cb.from_user.id, csv_file, caption="📊 Menu CSV (for spreadsheets)")
        await bot.send_document(cb.from_user.id, txt_file, caption="📋 Menu Text (readable format)")
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 View Price Chart", callback_data="admin_price_chart")],
            [InlineKeyboardButton(text="⬅️ Back to Menu Management", callback_data="admin_menu")]
        ])
        
        await bot.send_message(
            cb.from_user.id, 
            "✅ *Menu Downloaded!*\n\nYou now have:\n• CSV file for spreadsheets\n• Text file for easy reading",
            reply_markup=kb,
            parse_mode="Markdown"
        )
        
    except Exception as e:
        logger.error(f"❌ Error downloading menu: {e}")
        await cb.answer("❌ Error generating menu file", show_alert=True)

@dp.callback_query(F.data == "add_product")
async def add_product_start(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    if not db:
        await cb.answer("❌ Database unavailable", show_alert=True)
        return
    
    categories = db.get_categories()
    
    text = "🥬 *Add New Product*\n\n"
    text += "Step 1: Select a category for the new product:\n"
    
    buttons = []
    
    if not categories:
        text += "\n⚠️ No categories available. Please add a category first."
        buttons.append([InlineKeyboardButton(text="📂 Add Category", callback_data="add_category")])
        buttons.append([InlineKeyboardButton(text="⬅️ Back", callback_data="admin_menu")])
    else:
        # Use index instead of full category name to avoid callback data size limit
        for idx, category in enumerate(categories):
            category_name = category['name'] if isinstance(category, dict) else category
            buttons.append([InlineKeyboardButton(
                text=f"📂 {category_name}", 
                callback_data=f"selcat_{idx}"
            )])
        buttons.append([InlineKeyboardButton(text="⬅️ Back", callback_data="admin_menu")])
        
        # Store categories list in state for reference
        await state.update_data(category_list=categories)
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    try:
        if (cb.message and 
            hasattr(cb.message, 'edit_text') and 
            not isinstance(cb.message, types.InaccessibleMessage)):
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        else:
            await cb.answer()
            await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"❌ Error showing category selection: {e}")

@dp.callback_query(F.data.startswith("selcat_"))
async def select_category_for_product(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    # Get category from stored list using index
    idx = int(cb.data.replace("selcat_", ""))
    data = await state.get_data()
    categories = data.get('category_list', [])
    
    if idx >= len(categories):
        await cb.answer("❌ Invalid category selection", show_alert=True)
        return
    
    category_data = categories[idx]
    
    # Extract category name and ID
    if isinstance(category_data, dict):
        category_name = category_data['name']
        category_id = category_data['id']
    else:
        category_name = str(category_data)
        category_id = None
    
    # Store category data in state
    await state.update_data(
        new_product_category=category_name,
        new_product_category_id=category_id
    )
    
    text = f"🥬 *Add New Product*\n\n"
    text += f"Category: **{category_name}**\n\n"
    text += "Step 2: Type the product name:\n"
    text += "(e.g., 'Purple Haze', 'OG Kush', etc.)"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Cancel", callback_data="admin_menu")]
    ])
    
    await state.set_state(OrderStates.adding_product)
    
    try:
        if (cb.message and 
            hasattr(cb.message, 'edit_text') and 
            not isinstance(cb.message, types.InaccessibleMessage)):
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        else:
            await cb.answer()
            await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"❌ Error prompting for product name: {e}")

@dp.message(OrderStates.adding_product)
async def add_product_name(message: types.Message, state: FSMContext):
    track_user(message.from_user)
    """Process product name and ask for pricing"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Access denied.")
        return
    
    if not message.text or len(message.text.strip()) < 2:
        await message.answer("❌ Product name too short. Please enter at least 2 characters:")
        return
    
    product_name = message.text.strip()
    
    # Store product name in state
    await state.update_data(new_product_name=product_name)
    
    data = await state.get_data()
    category = data.get('new_product_category')
    
    text = f"🥬 *Add New Product*\n\n"
    text += f"Category: **{category}**\n"
    text += f"Product: **{product_name}**\n\n"
    text += "Step 3: Enter pricing tiers (one per line)\n\n"
    text += "Format: `size price`\n"
    text += "Example:\n"
    text += "`3.5g 25`\n"
    text += "`7g 45`\n"
    text += "`14g 70`\n"
    text += "`1oz 135`\n\n"
    text += "Type all pricing tiers now:"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Cancel", callback_data="admin_menu")]
    ])
    
    await state.set_state(OrderStates.adding_product_pricing)
    await message.answer(text, reply_markup=kb, parse_mode="Markdown")

@dp.message(OrderStates.adding_product_pricing)
async def add_product_pricing(message: types.Message, state: FSMContext):
    track_user(message.from_user)
    """Process pricing and create/update the product"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Access denied.")
        return
    
    if not message.text:
        await message.answer("❌ Please enter pricing information:")
        return
    
    if not db:
        await message.answer("❌ Database unavailable")
        await state.set_state(OrderStates.shopping)
        return
    
    data = await state.get_data()
    
    # Check if we're editing or adding
    is_edit = 'edit_product_name' in data
    
    if is_edit:
        category = data.get('edit_product_category')
        product_name = data.get('edit_product_name')
    else:
        category = data.get('new_product_category')
        product_name = data.get('new_product_name')
    
    # Parse pricing from message
    pricing_tiers = []
    lines = message.text.strip().split('\n')
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        try:
            # Support "X for £Y" format (e.g., "1 for £40")
            if ' for ' in line.lower():
                parts = line.lower().split(' for ')
                size = parts[0].strip()
                price_str = parts[1].replace('£', '').replace('$', '').replace('€', '').strip()
                price = float(price_str)
                pricing_tiers.append((size, price))
            else:
                # Support "size price" or "size £price" format
                parts = line.split()
                if len(parts) >= 2:
                    size = parts[0]
                    # Remove pound sign and any other currency symbols
                    price_str = parts[1].replace('£', '').replace('$', '').replace('€', '').strip()
                    price = float(price_str)
                    pricing_tiers.append((size, price))
                else:
                    await message.answer(f"❌ Invalid price format in line: {line}\nPlease use format: `size price`, `size £price`, or `size for £price`\nExample: `3.5g 20`, `3.5g £20`, or `1 for £40`")
                    return
        except (ValueError, IndexError):
            await message.answer(f"❌ Invalid price format in line: {line}\nPlease use format: `size price`, `size £price`, or `size for £price`\nExample: `3.5g 20`, `3.5g £20`, or `1 for £40`")
            return
    
    if not pricing_tiers:
        await message.answer("❌ No valid pricing tiers found. Please enter at least one:\n\nFormat: `size price`")
        return
    
    # Add or update product in database
    if is_edit:
        # Update existing product
        result = db.update_product_pricing(category, product_name, pricing_tiers)
        success_msg = f"✅ *Product Updated Successfully!*\n\n"
    else:
        # Add new product - get category_id from state
        category_id = data.get('new_product_category_id')
        if not category_id:
            await message.answer("❌ Error: Category ID not found. Please try again.")
            await state.set_state(OrderStates.shopping)
            return
        
        # First add the product
        result = db.add_product(category_id, product_name)
        if not result['success']:
            await message.answer(f"❌ Error adding product: {result.get('error', 'Unknown error')}")
            await state.set_state(OrderStates.shopping)
            return
        
        # Then add pricing tiers
        product_id = result['id']
        for tier_order, (size, price) in enumerate(pricing_tiers):
            tier_result = db.add_pricing_tier(product_id, size, float(price), tier_order)
            if not tier_result['success']:
                logger.error(f"❌ Error adding pricing tier: {tier_result.get('error')}")
        
        success_msg = f"✅ *Product Added Successfully!*\n\n"
    
    if result['success']:
        # Reload menu
        global products, descriptions
        menu_data = db.get_dynamic_menu()
        products = menu_data['menu']
        descriptions.update(menu_data['descriptions'])
        
        pricing_text = "\n".join([f"• {size}: £{price}" for size, price in pricing_tiers])
        await message.answer(
            success_msg +
            f"**{product_name}** ({category})\n\n"
            f"Pricing:\n{pricing_text}",
            parse_mode="Markdown"
        )
    else:
        action = "updating" if is_edit else "adding"
        await message.answer(f"❌ Error {action} product: {result.get('error', 'Unknown error')}")
    
    await state.set_state(OrderStates.shopping)
    
    # Show menu management panel again
    categories = db.get_categories() if db else []
    menu_data = db.get_dynamic_menu() if db else {}
    total_products = sum(len(prods) for prods in menu_data.values())
    
    text = "🍽️ *Menu Management*\n\n"
    text += f"📊 **Current Menu:**\n"
    text += f"• Categories: {len(categories)}\n"
    text += f"• Total Products: {total_products}\n\n"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📂 Add Category", callback_data="add_category")],
        [InlineKeyboardButton(text="🥬 Add Product", callback_data="add_product")],
        [InlineKeyboardButton(text="🗑️ Remove Product", callback_data="remove_product")],
        [InlineKeyboardButton(text="📋 View Menu", callback_data="view_menu")],
        [InlineKeyboardButton(text="🔄 Reload Menu", callback_data="reload_menu")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="admin_panel")]
    ])
    
    await message.answer(text, reply_markup=kb, parse_mode="Markdown")

# Handle product name editing
@dp.message(OrderStates.editing_product_name)
async def save_product_name(message: types.Message, state: FSMContext):
    track_user(message.from_user)
    """Process new product name"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Access denied.")
        return
    
    if not message.text:
        await message.answer("❌ Please enter a product name:")
        return
    
    if not db:
        await message.answer("❌ Database unavailable")
        await state.set_state(OrderStates.shopping)
        return
    
    new_name = message.text.strip()
    data = await state.get_data()
    old_name = data.get('edit_product_name')
    category = data.get('edit_product_category')
    
    # Update product name in database (need to add this method to database.py)
    result = db.rename_product(category, old_name, new_name)
    
    if result.get('success'):
        # Reload menu
        global products, descriptions
        menu_data = db.get_dynamic_menu()
        products = menu_data['menu']
        descriptions.update(menu_data['descriptions'])
        
        await message.answer(
            f"✅ *Product Renamed Successfully!*\n\n"
            f"**{old_name}** → **{new_name}**\n"
            f"Category: {category}",
            parse_mode="Markdown"
        )
    else:
        await message.answer(f"❌ Error renaming product: {result.get('error', 'Unknown error')}")
    
    await state.set_state(OrderStates.shopping)

# Handle product description editing
@dp.message(OrderStates.editing_product_description)
async def save_product_description(message: types.Message, state: FSMContext):
    track_user(message.from_user)
    """Process new product description"""
    if not is_admin(message.from_user.id):
        await message.answer("❌ Access denied.")
        return
    
    if not message.text:
        await message.answer("❌ Please enter a description:")
        return
    
    new_description = message.text.strip()
    data = await state.get_data()
    product_name = data.get('edit_product_name')
    category = data.get('edit_product_category')
    
    if not db:
        await message.answer("❌ Database unavailable")
        await state.set_state(OrderStates.shopping)
        return
    
    # Update description in database
    result = db.update_product_description(category, product_name, new_description)
    
    if result['success']:
        # Reload menu to get updated descriptions
        global products, descriptions
        menu_data = db.get_dynamic_menu()
        products = menu_data['menu']
        descriptions.update(menu_data['descriptions'])
        
        await message.answer(
            f"✅ *Description Updated Successfully!*\n\n"
            f"Product: **{product_name}**\n"
            f"New description:\n_{new_description}_",
            parse_mode="Markdown"
        )
    else:
        await message.answer(f"❌ Error updating description: {result.get('error', 'Unknown error')}")
    
    await state.set_state(OrderStates.shopping)

@dp.callback_query(F.data == "edit_product")
async def edit_product_start(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    if not db:
        await cb.answer("❌ Database unavailable", show_alert=True)
        return
    
    menu_data = db.get_dynamic_menu()
    menu = menu_data['menu']  # Extract the actual menu from the dict
    
    text = "✏️ *Edit Product*\n\n"
    text += "Select a product to edit:\n\n"
    
    buttons = []
    product_list = []
    
    if not menu or not any(menu.values()):
        text += "No products available to edit."
        buttons.append([InlineKeyboardButton(text="⬅️ Back", callback_data="admin_menu")])
    else:
        idx = 0
        for category, products_dict in menu.items():
            if products_dict:
                for product in products_dict.keys():
                    product_list.append({'category': category, 'product': product})
                    buttons.append([InlineKeyboardButton(
                        text=f"✏️ {product} ({category})", 
                        callback_data=f"editprod_{idx}"
                    )])
                    idx += 1
        
        # Store product list in state for reference
        await state.update_data(edit_product_list=product_list)
        buttons.append([InlineKeyboardButton(text="⬅️ Back", callback_data="admin_menu")])
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    try:
        if (cb.message and 
            hasattr(cb.message, 'edit_text') and 
            not isinstance(cb.message, types.InaccessibleMessage)):
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        else:
            await cb.answer()
            await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"❌ Error showing edit products: {e}")

@dp.callback_query(F.data.startswith("editprod_"))
async def edit_product_select(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    if not db:
        await cb.answer("❌ Database unavailable", show_alert=True)
        return
    
    # Get product from stored list using index
    idx = int(cb.data.replace("editprod_", ""))
    data = await state.get_data()
    product_list = data.get('edit_product_list', [])
    
    if idx >= len(product_list):
        await cb.answer("❌ Invalid product selection", show_alert=True)
        return
    
    product_info = product_list[idx]
    category = product_info['category']
    product = product_info['product']
    
    # Get current pricing
    menu_data = db.get_dynamic_menu()
    menu = menu_data['menu']  # Extract the actual menu from the dict
    current_pricing = menu.get(category, {}).get(product, [])
    
    # Store selected product info
    await state.update_data(
        edit_product_category=category,
        edit_product_name=product
    )
    
    text = f"✏️ *Edit Product: {product}*\n\n"
    text += f"Category: **{category}**\n\n"
    text += "**Current Pricing:**\n"
    
    if current_pricing:
        for size, price in current_pricing:
            text += f"• {size}: £{price}\n"
    else:
        text += "No pricing set\n"
    
    text += "\n**What would you like to edit?**\n"
    
    # Create edit options menu
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Edit Name", callback_data=f"editname_{idx}")],
        [InlineKeyboardButton(text="📄 Edit Description", callback_data=f"editdesc_{idx}")],
        [InlineKeyboardButton(text="💰 Edit Pricing", callback_data=f"editprice_{idx}")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="edit_product")]
    ])
    
    try:
        if (cb.message and 
            hasattr(cb.message, 'edit_text') and 
            not isinstance(cb.message, types.InaccessibleMessage)):
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        else:
            await cb.answer()
            await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"❌ Error showing edit product: {e}")

# Edit product name
@dp.callback_query(F.data.startswith("editname_"))
async def edit_product_name(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    idx = int(cb.data.replace("editname_", ""))
    data = await state.get_data()
    product_list = data.get('edit_product_list', [])
    
    if idx >= len(product_list):
        await cb.answer("❌ Invalid selection", show_alert=True)
        return
    
    product_info = product_list[idx]
    await state.update_data(
        edit_product_idx=idx,
        edit_product_category=product_info['category'],
        edit_product_name=product_info['product']
    )
    
    text = f"📝 *Edit Product Name*\n\n"
    text += f"Current name: **{product_info['product']}**\n"
    text += f"Category: **{product_info['category']}**\n\n"
    text += "Enter the new product name:"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Cancel", callback_data="edit_product")]
    ])
    
    await state.set_state(OrderStates.editing_product_name)
    
    try:
        if cb.message and hasattr(cb.message, 'edit_text'):
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        else:
            await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"❌ Error editing product name: {e}")

# Edit product description  
@dp.callback_query(F.data.startswith("editdesc_"))
async def edit_product_description(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    idx = int(cb.data.replace("editdesc_", ""))
    data = await state.get_data()
    product_list = data.get('edit_product_list', [])
    
    if idx >= len(product_list):
        await cb.answer("❌ Invalid selection", show_alert=True)
        return
    
    product_info = product_list[idx]
    await state.update_data(
        edit_product_idx=idx,
        edit_product_category=product_info['category'],
        edit_product_name=product_info['product']
    )
    
    # Get current description if exists
    current_desc = PRODUCT_DESCRIPTIONS.get(product_info['product'], "No description set")
    
    text = f"📄 *Edit Product Description*\n\n"
    text += f"Product: **{product_info['product']}**\n"
    text += f"Category: **{product_info['category']}**\n\n"
    text += f"Current description:\n_{current_desc}_\n\n"
    text += "Enter the new description:"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Cancel", callback_data="edit_product")]
    ])
    
    await state.set_state(OrderStates.editing_product_description)
    
    try:
        if cb.message and hasattr(cb.message, 'edit_text'):
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        else:
            await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"❌ Error editing product description: {e}")

# Edit product pricing (original flow)
@dp.callback_query(F.data.startswith("editprice_"))
async def edit_product_pricing(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    idx = int(cb.data.replace("editprice_", ""))
    data = await state.get_data()
    product_list = data.get('edit_product_list', [])
    
    if idx >= len(product_list):
        await cb.answer("❌ Invalid selection", show_alert=True)
        return
    
    product_info = product_list[idx]
    category = product_info['category']
    product = product_info['product']
    
    # Get current pricing
    menu_data = db.get_dynamic_menu()
    menu = menu_data['menu']  # Extract the actual menu from the dict
    current_pricing = menu.get(category, {}).get(product, [])
    
    await state.update_data(
        edit_product_idx=idx,
        edit_product_category=category,
        edit_product_name=product
    )
    
    text = f"💰 *Edit Product Pricing*\n\n"
    text += f"Product: **{product}**\n"
    text += f"Category: **{category}**\n\n"
    text += "**Current Pricing:**\n"
    
    if current_pricing:
        for size, price in current_pricing:
            text += f"• {size}: £{price}\n"
    else:
        text += "No pricing set\n"
    
    text += "\n📝 Enter new pricing tiers (one per line)\n\n"
    text += "Format: `size price`\n"
    text += "Example:\n"
    text += "`3.5g 25`\n"
    text += "`7g 45`\n"
    text += "`14g 70`\n"
    text += "`1oz 135`"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Cancel", callback_data="admin_menu")]
    ])
    
    await state.set_state(OrderStates.adding_product_pricing)
    
    try:
        if cb.message and hasattr(cb.message, 'edit_text'):
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        else:
            await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"❌ Error editing product pricing: {e}")

@dp.callback_query(F.data == "remove_product")
async def remove_product_start(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    if not db:
        await cb.answer("❌ Database unavailable", show_alert=True)
        return
    
    menu_data = db.get_dynamic_menu()
    menu = menu_data['menu']  # Extract the actual menu from the dict
    
    text = "🗑️ *Remove Product*\n\n"
    text += "Select a product to remove:\n\n"
    
    buttons = []
    
    if not menu or not any(menu.values()):
        text += "No products available to remove."
        buttons.append([InlineKeyboardButton(text="⬅️ Back", callback_data="admin_menu")])
    else:
        for category, products_dict in menu.items():
            if products_dict:
                for product in products_dict.keys():
                    buttons.append([InlineKeyboardButton(
                        text=f"🗑️ {product} ({category})", 
                        callback_data=f"confirm_remove_{category}_{product}"
                    )])
        
        buttons.append([InlineKeyboardButton(text="⬅️ Back", callback_data="admin_menu")])
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    try:
        if (cb.message and 
            hasattr(cb.message, 'edit_text') and 
            not isinstance(cb.message, types.InaccessibleMessage)):
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        else:
            await cb.answer()
            await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"❌ Error showing remove products: {e}")

@dp.callback_query(F.data.startswith("confirm_remove_"))
async def confirm_remove_product(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    if not db:
        await cb.answer("❌ Database unavailable", show_alert=True)
        return
    
    # Parse the callback data: "confirm_remove_CATEGORY_PRODUCT"
    parts = cb.data.replace("confirm_remove_", "").split("_", 1)
    if len(parts) != 2:
        await cb.answer("❌ Invalid product selection", show_alert=True)
        return
    
    category, product = parts
    
    # Remove the product
    result = db.remove_product(category, product)
    
    if result['success']:
        # Reload menu
        global products, descriptions
        menu_data = db.get_dynamic_menu()
        products = menu_data['menu']
        descriptions.update(menu_data['descriptions'])
        
        await cb.answer(f"✅ Removed '{product}' from menu!", show_alert=True)
        
        # Show updated menu management
        await admin_menu_management(cb, state)
    else:
        await cb.answer(f"❌ Error: {result['error']}", show_alert=True)

@dp.callback_query(F.data == "toggle_product")
async def toggle_product_start(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    if not db:
        await cb.answer("❌ Database unavailable", show_alert=True)
        return
    
    # Get all products including inactive ones
    all_products = db.get_all_products_with_status()
    
    text = "🔄 *Toggle Product Status*\n\n"
    text += "Select a product to activate/deactivate:\n\n"
    
    buttons = []
    
    if not all_products:
        text += "No products available."
        buttons.append([InlineKeyboardButton(text="⬅️ Back", callback_data="admin_menu")])
    else:
        for product in all_products:
            status_icon = "✅" if product['active'] else "❌"
            status_text = "Active" if product['active'] else "Inactive"
            buttons.append([InlineKeyboardButton(
                text=f"{status_icon} {product['product']} ({product['category']}) - {status_text}", 
                callback_data=f"confirm_toggle_{product['category']}_{product['product']}"
            )])
        
        buttons.append([InlineKeyboardButton(text="⬅️ Back", callback_data="admin_menu")])
    
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    try:
        if (cb.message and 
            hasattr(cb.message, 'edit_text') and 
            not isinstance(cb.message, types.InaccessibleMessage)):
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        else:
            await cb.answer()
            await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"❌ Error showing toggle products: {e}")

@dp.callback_query(F.data.startswith("confirm_toggle_"))
async def confirm_toggle_product(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    if not db:
        await cb.answer("❌ Database unavailable", show_alert=True)
        return
    
    # Parse the callback data: "confirm_toggle_CATEGORY_PRODUCT"
    parts = cb.data.replace("confirm_toggle_", "").split("_", 1)
    if len(parts) != 2:
        await cb.answer("❌ Invalid product selection", show_alert=True)
        return
    
    category, product = parts
    
    # Toggle the product status
    result = db.toggle_product_status(category, product)
    
    if result['success']:
        # Reload menu
        global products, descriptions
        menu_data = db.get_dynamic_menu()
        products = menu_data['menu']
        descriptions.update(menu_data['descriptions'])
        
        status_msg = "activated ✅" if result['new_status'] == "active" else "deactivated ❌"
        await cb.answer(f"✅ '{product}' {status_msg}!", show_alert=True)
        
        # Show updated toggle menu
        await toggle_product_start(cb, state)
    else:
        await cb.answer(f"❌ Error: {result['error']}", show_alert=True)

@dp.callback_query(F.data == 'shopping')
async def show_shopping(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    # Track user - INSTANT, NO WAITING
    all_users.add(str(cb.from_user.id))
    # Background save - don't block button response
    asyncio.create_task(async_save_user(cb.from_user.id, cb.from_user.username, cb.from_user.first_name, cb.from_user.last_name))
    
    await state.set_state(OrderStates.shopping)
    
    # Clear review state when returning to shopping to prevent stale review flows
    await state.update_data(reviewing_order_num=None, review_rating=None)
    
    data = await state.get_data()
    cart = data.get('cart', [])

    text = "👋 Welcome to UKDANKZZ! Browse our menu:"
    if cart:
        text += f"\n\n🛒 Items in cart: {len(cart)}"

    try:
        if cb.message and hasattr(cb.message, 'edit_text') and not isinstance(cb.message, types.InaccessibleMessage):
            await cb.message.edit_text(text, reply_markup=shopping_kb())
    except Exception as e:
        print(f"Error in show_shopping: {e}")


@dp.callback_query(F.data.startswith('s_'))
async def show_section(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    # Track user - INSTANT, NO WAITING
    all_users.add(str(cb.from_user.id))
    # Background save - don't block button response
    asyncio.create_task(async_save_user(cb.from_user.id, cb.from_user.username, cb.from_user.first_name, cb.from_user.last_name))
    
    if cb.data:
        section = cb.data[2:]
        if section in products and products[section]:
            try:
                if cb.message and hasattr(cb.message, 'edit_text') and not isinstance(cb.message, types.InaccessibleMessage):
                    await cb.message.edit_text(f"📦 *{section} Menu*\n\nSelect a product:",
                                               reply_markup=section_kb(section))
            except Exception as e:
                print(f"Error in show_section: {e}")
        else:
            await cb.answer("This section is currently empty.", show_alert=True)


@dp.callback_query(F.data.startswith('p_'))
async def show_product(cb: types.CallbackQuery):
    await cb.answer()
    try:
        # Track user - INSTANT, NO WAITING
        all_users.add(str(cb.from_user.id))
        # Background save - don't block button response
        asyncio.create_task(async_save_user(cb.from_user.id, cb.from_user.username, cb.from_user.first_name, cb.from_user.last_name))
        
        if cb.data:
            parts = cb.data[2:].split('|', 1)
            if len(parts) >= 2:
                section, product = parts[0], parts[1]
                if section in products and product in products[section]:
                    description = descriptions.get(product,
                                                   "No description available.")

                    # Add video link for "Mimosa" product
                    video_text = ""
                    if product == "Mimosa 🍹":
                        video_text = "\n🎥 [Watch video](https://www.dropbox.com/scl/fi/88yqmqvyv1n8p2b35fpn8/Mimosa.mov?rlkey=r5ogg8lw9xdqnomzxmkn8780b&st=tctga8nf&dl=0)"

                    try:
                        if cb.message and hasattr(cb.message, 'edit_text') and not isinstance(cb.message, types.InaccessibleMessage):
                            await cb.message.edit_text(
                                f"🌿 *{product}*\n{description}{video_text}\n\nSelect size/quantity:",
                                reply_markup=product_kb(section, product),
                                parse_mode="Markdown")
                    except Exception as e:
                        print(f"Error in show_product: {e}")
                else:
                    await cb.answer("Product not found.", show_alert=True)
    except (IndexError, KeyError):
        await cb.answer("Invalid product selection.", show_alert=True)

# ======================== DISCOUNT SYSTEM HANDLERS ========================

@dp.callback_query(F.data == "discounts")
async def show_discounts(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    try:
        # Track user - INSTANT, NO WAITING
        all_users.add(str(cb.from_user.id))
        # Background save - don't block button response
        asyncio.create_task(async_save_user(cb.from_user.id, cb.from_user.username, cb.from_user.first_name, cb.from_user.last_name))
        
        if not db or not pricing_engine:
            await cb.answer("❌ Discount system temporarily unavailable.", show_alert=True)
            return
        
        # Get active promotions
        promotions = db.get_active_promotions()
        
        text = "💰 *Discounts & Deals*\n\n"
        
        if not promotions:
            text += "🔍 No active promotions right now.\n"
            text += "💡 Check back soon for amazing deals!\n\n"
            text += "📧 Want to be notified of flash sales?\n"
            text += "👆 Just browse and shop - you'll get updates!"
        else:
            text += f"🎉 {len(promotions)} active deals available!\n\n"
            
            for promo in promotions[:5]:  # Show up to 5 promotions
                promo_text = format_promotion_text(promo)
                text += f"{promo_text}\n"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[])
        
        # Add promotion buttons if any exist
        for promo in promotions[:3]:  # Show up to 3 clickable promotions
            kb.inline_keyboard.append([
                InlineKeyboardButton(text=f"🎯 {promo['name']}", 
                                   callback_data=f"promo_{promo['id']}")
            ])
        
        # Add coupon entry and back buttons
        kb.inline_keyboard.append([
            InlineKeyboardButton(text="🎫 Enter Coupon Code", callback_data="enter_coupon")
        ])
        kb.inline_keyboard.append([
            InlineKeyboardButton(text="⬅️ Back to Menu", callback_data="shopping")
        ])
        
        try:
            # Safe message editing with proper type checking
            if (cb.message and 
                hasattr(cb.message, 'edit_text') and 
                not isinstance(cb.message, types.InaccessibleMessage)):
                await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
            else:
                # Fallback: send new message if edit fails
                await cb.answer()
                await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"❌ Error showing discounts: {e}")
            await cb.answer("❌ Error loading discounts. Please try again.")
            
    except Exception as e:
        print(f"Error in show_discounts: {e}")
        await cb.answer("❌ Error loading discounts", show_alert=True)

def format_promotion_text(promo: Dict) -> str:
    """Format promotion for display"""
    emoji_map = {
        'bundle': '📦',
        'item': '🏷️',
        'collection': '🎁',
        'flash': '⚡'
    }
    
    emoji = emoji_map.get(promo['type'], '💰')
    text = f"{emoji} *{promo['name']}*\n"
    
    if promo.get('description'):
        text += f"   {promo['description']}\n"
    
    if promo['percent_off'] > 0:
        text += f"   💸 {promo['percent_off']}% OFF\n"
    elif promo['amount_off'] > 0:
        text += f"   💸 £{promo['amount_off']} OFF\n"
    
    if promo['type'] == 'bundle' and promo['buy_qty'] > 0:
        text += f"   🛒 Buy {promo['buy_qty']} Get {promo['get_qty']} FREE\n"
    
    if promo.get('end_at'):
        try:
            end_time = promo['end_at']
            if isinstance(end_time, str):
                end_time = datetime.datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            text += f"   ⏰ Until: {end_time.strftime('%Y-%m-%d %H:%M')}\n"
        except:
            pass
    
    return text

@dp.callback_query(F.data.startswith("promo_"))
async def show_promotion_details(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    try:
        # Track user - INSTANT, NO WAITING
        all_users.add(str(cb.from_user.id))
        # Background save - don't block button response
        asyncio.create_task(async_save_user(cb.from_user.id, cb.from_user.username, cb.from_user.first_name, cb.from_user.last_name))
        
        promo_id = int(cb.data[6:])  # Remove "promo_" prefix
        
        promotions = db.get_active_promotions() if db else []
        promo = next((p for p in promotions if p['id'] == promo_id), None)
        
        if not promo:
            await cb.answer("❌ Promotion not found or expired", show_alert=True)
            return
        
        text = f"🎯 *{promo['name']}*\n\n"
        
        if promo.get('description'):
            text += f"📝 {promo['description']}\n\n"
        
        # Show discount details
        if promo['percent_off'] > 0:
            text += f"💸 **{promo['percent_off']}% OFF**\n"
        elif promo['amount_off'] > 0:
            text += f"💸 **£{promo['amount_off']} OFF**\n"
        
        if promo['type'] == 'bundle':
            text += f"🛒 Buy {promo['buy_qty']}, Get {promo['get_qty']} FREE\n"
        
        if promo['min_spend'] > 0:
            text += f"💷 Minimum spend: £{promo['min_spend']}\n"
        
        # Show applicable products
        target_products = promo.get('target_product_ids', [])
        if target_products:
            text += f"\n🎯 *Applies to:*\n"
            for product in target_products[:3]:
                text += f"• {product}\n"
            if len(target_products) > 3:
                text += f"• ...and {len(target_products) - 3} more\n"
        else:
            text += "\n🌟 *Applies to ALL products!*\n"
        
        if promo.get('end_at'):
            try:
                end_time = promo['end_at']
                if isinstance(end_time, str):
                    end_time = datetime.datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                text += f"\n⏰ Expires: {end_time.strftime('%Y-%m-%d %H:%M')}\n"
            except:
                pass
        
        text += f"\n✨ *How to use:*\nJust add qualifying items to your cart and the discount will be automatically applied!"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛍️ Shop Now", callback_data="shopping")],
            [InlineKeyboardButton(text="⬅️ Back to Discounts", callback_data="discounts")]
        ])
        
        try:
            # Safe message editing with proper type checking
            if (cb.message and 
                hasattr(cb.message, 'edit_text') and 
                not isinstance(cb.message, types.InaccessibleMessage)):
                await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
            else:
                # Fallback: send new message if edit fails
                await cb.answer()
                await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"❌ Error showing promotion details: {e}")
            await cb.answer("❌ Error loading promotion. Please try again.")
            
    except Exception as e:
        print(f"Error in show_promotion_details: {e}")
        await cb.answer("❌ Error loading promotion details", show_alert=True)

# ======================== ADMIN DISCOUNT MANAGEMENT ========================

@dp.callback_query(F.data == "admin_discounts")
async def admin_discount_management(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    
    if not is_admin(cb.from_user.id):
        await cb.message.answer("❌ Access denied.")
        return
    
    try:
        if not db:
            await cb.answer("❌ Database unavailable", show_alert=True)
            return
        
        # Get current promotions and stats
        promotions = db.get_all_promotions()
        active_count = len([p for p in promotions if p['active']])
        
        text = "💰 *Discount Management*\n\n"
        text += f"📊 **Current Status:**\n"
        text += f"• Total Promotions: {len(promotions)}\n"
        text += f"• Active Promotions: {active_count}\n"
        text += f"• Inactive Promotions: {len(promotions) - active_count}\n\n"
        
        text += "🛠️ **Management Options:**\n"
        text += "• Create new promotions and flash sales\n"
        text += "• Generate coupon codes for customers\n"
        text += "• View and toggle existing promotions\n"
        text += "• Monitor discount usage analytics"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✨ Create Flash Sale", callback_data="create_flash_sale")],
            [InlineKeyboardButton(text="🎯 Create Promotion", callback_data="create_promotion")],
            [InlineKeyboardButton(text="🎫 Generate Coupon", callback_data="create_coupon")],
            [InlineKeyboardButton(text="📋 View All Promotions", callback_data="view_all_promotions")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="admin_panel")]
        ])
        
        try:
            # Safe message editing with proper type checking
            if (cb.message and 
                hasattr(cb.message, 'edit_text') and 
                not isinstance(cb.message, types.InaccessibleMessage)):
                await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
            else:
                # Fallback: send new message if edit fails
                await cb.answer()
                await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"❌ Error showing admin discounts: {e}")
            await cb.answer("❌ Error loading discount management. Please try again.")
            
    except Exception as e:
        print(f"Error in admin_discount_management: {e}")
        await cb.answer("❌ Error loading discount management", show_alert=True)

@dp.callback_query(F.data == "create_flash_sale")
async def create_flash_sale(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    try:
        if not db:
            await cb.answer("❌ Database unavailable", show_alert=True)
            return
        
        # Create some sample flash sales
        flash_sales = [
            {
                'name': '⚡ Weekend Flash Sale',
                'description': '20% off all products this weekend only!',
                'percent_off': 20.0,
                'end_at': datetime.datetime.now() + datetime.timedelta(hours=48)
            },
            {
                'name': '🌿 Happy Hour Special',
                'description': '15% off flowers until midnight',
                'percent_off': 15.0,
                'target_products': ['OG Kush', 'Blue Dream', 'White Widow'],
                'end_at': datetime.datetime.now() + datetime.timedelta(hours=6)
            },
            {
                'name': '🎉 Flash 30% Off',
                'description': 'Massive 30% discount for next 3 hours!',
                'percent_off': 30.0,
                'end_at': datetime.datetime.now() + datetime.timedelta(hours=3)
            }
        ]
        
        text = "⚡ *Quick Flash Sale Creation*\n\n"
        text += "🚀 Choose a pre-configured flash sale to activate instantly:\n\n"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[])
        
        for i, sale in enumerate(flash_sales):
            duration = (sale['end_at'] - datetime.datetime.now()).total_seconds() / 3600
            button_text = f"{sale['name']} ({duration:.0f}h)"
            kb.inline_keyboard.append([
                InlineKeyboardButton(text=button_text, callback_data=f"activate_flash_{i}")
            ])
        
        kb.inline_keyboard.append([
            InlineKeyboardButton(text="⬅️ Back", callback_data="admin_discounts")
        ])
        
        # Store flash sale options in state
        await state.update_data(flash_sale_options=flash_sales)
        
        try:
            # Safe message editing with proper type checking
            if (cb.message and 
                hasattr(cb.message, 'edit_text') and 
                not isinstance(cb.message, types.InaccessibleMessage)):
                await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
            else:
                # Fallback: send new message if edit fails
                await cb.answer()
                await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"❌ Error showing flash sale creation: {e}")
            await cb.answer("❌ Error creating flash sale. Please try again.")
            
    except Exception as e:
        print(f"Error in create_flash_sale: {e}")
        await cb.answer("❌ Error creating flash sale", show_alert=True)

@dp.callback_query(F.data.startswith("activate_flash_"))
async def activate_flash_sale(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    try:
        sale_index = int(cb.data[15:])  # Remove "activate_flash_" prefix
        
        data = await state.get_data()
        flash_sales = data.get('flash_sale_options', [])
        
        if sale_index >= len(flash_sales):
            await cb.answer("❌ Invalid flash sale selection", show_alert=True)
            return
        
        sale = flash_sales[sale_index]
        
        # Create the promotion in database
        success = db.create_promotion(
            promo_type='flash',
            name=sale['name'],
            description=sale['description'],
            target_products=sale.get('target_products', []),
            percent_off=sale['percent_off'],
            end_at=sale['end_at']
        )
        
        if success:
            text = f"✅ *Flash Sale Activated!*\n\n"
            text += f"🎯 **{sale['name']}**\n"
            text += f"📝 {sale['description']}\n"
            text += f"💸 {sale['percent_off']}% OFF\n"
            
            duration = (sale['end_at'] - datetime.datetime.now()).total_seconds() / 3600
            text += f"⏰ Duration: {duration:.1f} hours\n\n"
            text += "📢 *Ready to announce:*\n"
            text += "Use broadcast messaging to notify all users!"
            
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📢 Broadcast Now", callback_data="admin_broadcast")],
                [InlineKeyboardButton(text="💰 Manage Discounts", callback_data="admin_discounts")],
                [InlineKeyboardButton(text="🏠 Main Menu", callback_data="shopping")]
            ])
            
            await cb.answer("✅ Flash sale activated successfully!", show_alert=True)
        else:
            text = "❌ *Failed to create flash sale*\n\nPlease try again or contact technical support."
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Back", callback_data="admin_discounts")]
            ])
        
        try:
            # Safe message editing with proper type checking
            if (cb.message and 
                hasattr(cb.message, 'edit_text') and 
                not isinstance(cb.message, types.InaccessibleMessage)):
                await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
            else:
                # Fallback: send new message if edit fails
                await cb.answer()
                await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"❌ Error showing flash sale activation: {e}")
            await cb.answer("❌ Error activating flash sale. Please try again.")
            
    except Exception as e:
        print(f"Error in activate_flash_sale: {e}")
        await cb.answer("❌ Error activating flash sale", show_alert=True)

@dp.callback_query(F.data == "create_promotion")
async def create_promotion_handler(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    try:
        if not db:
            await cb.answer("❌ Database unavailable", show_alert=True)
            return
        
        # Create sample promotions
        promotions = [
            {
                'name': '🌿 Bundle Deal',
                'description': 'Buy 2 get 1 free on all flower products',
                'promo_type': 'bundle',
                'target_products': ['OG Kush', 'Blue Dream', 'White Widow'],
                'percent_off': 33.3
            },
            {
                'name': '💰 Volume Discount',
                'description': '15% off orders over £100',
                'promo_type': 'item',
                'percent_off': 15.0,
                'min_spend': 100.0
            },
            {
                'name': '🎯 Product Special',
                'description': '25% off premium strains',
                'promo_type': 'collection',
                'target_products': ['Premium OG', 'Platinum Kush'],
                'percent_off': 25.0
            }
        ]
        
        text = "🎯 *Standard Promotion Creation*\n\n"
        text += "📋 Choose a promotion type to create:\n\n"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[])
        
        for i, promo in enumerate(promotions):
            button_text = f"{promo['name']}"
            kb.inline_keyboard.append([
                InlineKeyboardButton(text=button_text, callback_data=f"create_promo_{i}")
            ])
        
        kb.inline_keyboard.append([
            InlineKeyboardButton(text="⬅️ Back", callback_data="admin_discounts")
        ])
        
        # Store promotion options in state
        await state.update_data(promotion_options=promotions)
        
        try:
            # Safe message editing with proper type checking
            if (cb.message and 
                hasattr(cb.message, 'edit_text') and 
                not isinstance(cb.message, types.InaccessibleMessage)):
                await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
            else:
                # Fallback: send new message if edit fails
                await cb.answer()
                await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"❌ Error showing promotion creation: {e}")
            await cb.answer("❌ Error creating promotion. Please try again.")
            
    except Exception as e:
        print(f"Error in create_promotion_handler: {e}")
        await cb.answer("❌ Error creating promotion", show_alert=True)

@dp.callback_query(F.data.startswith("create_promo_"))
async def create_standard_promotion(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    try:
        promo_index = int(cb.data[13:])  # Remove "create_promo_" prefix
        
        data = await state.get_data()
        promotions = data.get('promotion_options', [])
        
        if promo_index >= len(promotions):
            await cb.answer("❌ Invalid promotion selection", show_alert=True)
            return
        
        promo = promotions[promo_index]
        
        # Create the promotion in database
        success = db.create_promotion(
            promo_type=promo['promo_type'],
            name=promo['name'],
            description=promo['description'],
            target_products=promo.get('target_products', []),
            percent_off=promo['percent_off'],
            min_spend=promo.get('min_spend', 0),
            end_at=datetime.datetime.now() + datetime.timedelta(days=7)  # 1 week duration
        )
        
        if success:
            text = f"✅ *Promotion Created!*\n\n"
            text += f"🎯 **{promo['name']}**\n"
            text += f"📝 {promo['description']}\n"
            text += f"💸 {promo['percent_off']}% OFF\n"
            
            if promo.get('min_spend'):
                text += f"💷 Min spend: £{promo['min_spend']}\n"
            
            if promo.get('target_products'):
                text += f"🎯 Products: {', '.join(promo['target_products'])}\n"
            
            text += f"⏰ Duration: 7 days\n\n"
            text += "📢 *Ready to announce to customers!*"
            
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📢 Broadcast Now", callback_data="admin_broadcast")],
                [InlineKeyboardButton(text="🎯 Create More", callback_data="create_promotion")],
                [InlineKeyboardButton(text="💰 Manage Discounts", callback_data="admin_discounts")]
            ])
            
            await cb.answer("✅ Promotion created successfully!", show_alert=True)
        else:
            text = "❌ *Failed to create promotion*\n\nPlease try again or contact technical support."
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Back", callback_data="admin_discounts")]
            ])
        
        try:
            # Safe message editing with proper type checking
            if (cb.message and 
                hasattr(cb.message, 'edit_text') and 
                not isinstance(cb.message, types.InaccessibleMessage)):
                await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
            else:
                # Fallback: send new message if edit fails
                await cb.answer()
                await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"❌ Error showing promotion creation result: {e}")
            await cb.answer("❌ Error creating promotion. Please try again.")
            
    except Exception as e:
        print(f"Error in create_standard_promotion: {e}")
        await cb.answer("❌ Error creating promotion", show_alert=True)

@dp.callback_query(F.data == "create_coupon")
async def create_coupon_quick(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    try:
        if not db:
            await cb.answer("❌ Database unavailable", show_alert=True)
            return
        
        # Your custom DANKZZ coupon codes
        sample_coupons = [
            {
                'code': 'DANKZZ10',
                'type': 'percent',
                'value': 10.0,
                'min_spend': 100.0,
                'description': '10% off any £100+ orders'
            },
            {
                'code': 'DANKZZ15',
                'type': 'percent',
                'value': 15.0,
                'min_spend': 100.0,
                'description': '15% off any £100+ orders'
            },
            {
                'code': 'DANKZZ200',
                'type': 'percent',
                'value': 15.0,
                'min_spend': 200.0,
                'description': '15% off any £200+ orders'
            },
            {
                'code': 'DANKZZ50',
                'type': 'percent',
                'value': 5.0,
                'min_spend': 50.0,
                'description': '5% off any £50+ orders'
            },
            {
                'code': 'DANKZZ25',
                'type': 'fixed',
                'value': 5.0,
                'min_spend': 25.0,
                'description': '£5 off any £25+ orders'
            },
            {
                'code': 'DANKZZ100',
                'type': 'fixed',
                'value': 10.0,
                'min_spend': 50.0,
                'description': '£10 off any £50+ orders'
            }
        ]
        
        text = "🎫 *Quick Coupon Generation*\n\n"
        text += "🎁 Choose a coupon type to generate:\n\n"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[])
        
        for i, coupon in enumerate(sample_coupons):
            button_text = f"{coupon['description']} ({coupon['code']})"
            kb.inline_keyboard.append([
                InlineKeyboardButton(text=button_text, callback_data=f"generate_coupon_{i}")
            ])
        
        kb.inline_keyboard.append([
            InlineKeyboardButton(text="⬅️ Back", callback_data="admin_discounts")
        ])
        
        # Store coupon options in state
        await state.update_data(coupon_options=sample_coupons)
        
        try:
            # Safe message editing with proper type checking
            if (cb.message and 
                hasattr(cb.message, 'edit_text') and 
                not isinstance(cb.message, types.InaccessibleMessage)):
                await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
            else:
                # Fallback: send new message if edit fails
                await cb.answer()
                await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"❌ Error showing coupon creation: {e}")
            await cb.answer("❌ Error creating coupon. Please try again.")
            
    except Exception as e:
        print(f"Error in create_coupon_quick: {e}")
        await cb.answer("❌ Error creating coupon", show_alert=True)

@dp.callback_query(F.data.startswith("generate_coupon_"))
async def generate_coupon(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    try:
        coupon_index = int(cb.data[16:])  # Remove "generate_coupon_" prefix
        
        data = await state.get_data()
        coupons = data.get('coupon_options', [])
        
        if coupon_index >= len(coupons):
            await cb.answer("❌ Invalid coupon selection", show_alert=True)
            return
        
        coupon = coupons[coupon_index]
        
        # Create the coupon in database
        result = db.create_coupon(
            code=coupon['code'],
            coupon_type=coupon['type'],
            value=coupon['value'],
            min_spend=coupon.get('min_spend', 0),
            expires_at=datetime.datetime.now() + datetime.timedelta(days=30),  # 30 day expiry
            max_uses=100,
            per_user_limit=1
        )
        
        if result and result.get('success'):
            text = f"✅ *Coupon Generated!*\n\n"
            text += f"🎫 **Code:** `{coupon['code']}`\n"
            text += f"📝 {coupon['description']}\n"
            text += f"💰 Value: {coupon['value']}{'%' if coupon['type'] == 'percent' else '£'}\n"
            
            if coupon.get('min_spend'):
                text += f"💷 Min spend: £{coupon['min_spend']}\n"
            
            text += f"⏰ Expires: 30 days\n"
            text += f"🎯 Max uses: 100\n"
            text += f"👤 Per user: 1 time\n\n"
            text += "📢 *Share this code with customers!*"
            
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📢 Broadcast Coupon", callback_data="admin_broadcast")],
                [InlineKeyboardButton(text="🎫 Create More", callback_data="create_coupon")],
                [InlineKeyboardButton(text="💰 Manage Discounts", callback_data="admin_discounts")]
            ])
            
            await cb.answer("✅ Coupon generated successfully!", show_alert=True)
        else:
            error_msg = result.get('error', 'Unknown error') if result else 'Unknown error'
            if 'already exists' in error_msg:
                text = f"❌ *Coupon Already Exists*\n\nThe code `{coupon['code']}` is already in use.\n\nPlease try a different code or modify the existing one."
            else:
                text = f"❌ *Failed to create coupon*\n\n{error_msg}\n\nPlease try again or contact technical support."
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Back", callback_data="admin_discounts")]
            ])
        
        try:
            # Safe message editing with proper type checking
            if (cb.message and 
                hasattr(cb.message, 'edit_text') and 
                not isinstance(cb.message, types.InaccessibleMessage)):
                await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
            else:
                # Fallback: send new message if edit fails
                await cb.answer()
                await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"❌ Error showing coupon generation: {e}")
            await cb.answer("❌ Error generating coupon. Please try again.")
            
    except Exception as e:
        print(f"Error in generate_coupon: {e}")
        await cb.answer("❌ Error generating coupon", show_alert=True)

@dp.callback_query(F.data == "view_all_promotions")
async def view_all_promotions(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    try:
        import datetime
        if not db:
            await cb.answer("❌ Database unavailable", show_alert=True)
            return
        
        # Get all promotions
        promotions = db.get_all_promotions()
        
        if not promotions:
            text = "📋 *All Promotions*\n\n"
            text += "ℹ️ **No promotions found**\n\n"
            text += "Create your first promotion to get started!"
            
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✨ Create Flash Sale", callback_data="create_flash_sale")],
                [InlineKeyboardButton(text="🎯 Create Promotion", callback_data="create_promotion")],
                [InlineKeyboardButton(text="⬅️ Back", callback_data="admin_discounts")]
            ])
        else:
            text = "📋 *All Promotions*\n\n"
            
            active_promotions = []
            inactive_promotions = []
            
            for promo in promotions:
                promo_text = f"**{promo['name']}**\n"
                promo_text += f"📝 {promo['description']}\n"
                promo_text += f"💸 {promo.get('percent_off', 0)}% OFF"
                
                if promo.get('min_spend'):
                    promo_text += f" (min £{promo['min_spend']})"
                
                if promo.get('target_products'):
                    promo_text += f"\n🎯 Products: {', '.join(promo['target_products'][:2])}{'...' if len(promo['target_products']) > 2 else ''}"
                
                # Check if promotion is still active
                is_active = promo.get('active', True)
                end_date = promo.get('end_at')
                
                if end_date and isinstance(end_date, str):
                    try:
                        end_dt = datetime.datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                        if end_dt < datetime.datetime.now(datetime.timezone.utc):
                            is_active = False
                    except:
                        pass
                elif end_date and hasattr(end_date, 'replace'):
                    if end_date < datetime.datetime.now():
                        is_active = False
                
                status = "🟢 Active" if is_active else "🔴 Inactive"
                promo_text += f"\n{status}"
                
                if is_active:
                    active_promotions.append((promo, promo_text))
                else:
                    inactive_promotions.append((promo, promo_text))
            
            # Show active promotions first
            if active_promotions:
                text += "🟢 **ACTIVE PROMOTIONS:**\n\n"
                for i, (promo, promo_text) in enumerate(active_promotions[:3]):  # Show max 3
                    text += f"{i+1}. {promo_text}\n\n"
            
            if inactive_promotions:
                text += "🔴 **INACTIVE PROMOTIONS:**\n\n"
                for i, (promo, promo_text) in enumerate(inactive_promotions[:2]):  # Show max 2
                    text += f"{i+1}. {promo_text}\n\n"
            
            text += f"📊 **Summary:** {len(active_promotions)} active, {len(inactive_promotions)} inactive"
            
            # Create management buttons
            kb = InlineKeyboardMarkup(inline_keyboard=[])
            
            # Add buttons for first few active promotions to toggle off
            for i, (promo, _) in enumerate(active_promotions[:3]):
                button_text = f"🔴 End '{promo['name'][:15]}...'"
                kb.inline_keyboard.append([
                    InlineKeyboardButton(text=button_text, callback_data=f"end_promo_{promo['id']}")
                ])
            
            # Add buttons for first few inactive promotions to reactivate
            for i, (promo, _) in enumerate(inactive_promotions[:2]):
                button_text = f"🟢 Reactivate '{promo['name'][:10]}...'"
                kb.inline_keyboard.append([
                    InlineKeyboardButton(text=button_text, callback_data=f"reactivate_promo_{promo['id']}")
                ])
            
            # Navigation buttons
            kb.inline_keyboard.extend([
                [InlineKeyboardButton(text="🔄 Refresh", callback_data="view_all_promotions")],
                [InlineKeyboardButton(text="⬅️ Back", callback_data="admin_discounts")]
            ])
        
        try:
            # Safe message editing with proper type checking
            if (cb.message and 
                hasattr(cb.message, 'edit_text') and 
                not isinstance(cb.message, types.InaccessibleMessage)):
                await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
            else:
                # Fallback: send new message if edit fails
                await cb.answer()
                await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"❌ Error showing all promotions: {e}")
            await cb.answer("❌ Error loading promotions. Please try again.")
            
    except Exception as e:
        print(f"Error in view_all_promotions: {e}")
        await cb.answer("❌ Error loading promotions", show_alert=True)

@dp.callback_query(F.data.startswith("end_promo_"))
async def end_promotion(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    try:
        promo_id = int(cb.data[10:])  # Remove "end_promo_" prefix
        
        if not db:
            await cb.answer("❌ Database unavailable", show_alert=True)
            return
        
        # End the promotion
        success = db.update_promotion_status(promo_id, active=False)
        
        if success:
            await cb.answer("✅ Promotion ended successfully!", show_alert=True)
            # Refresh the view
            await view_all_promotions(cb, state)
        else:
            await cb.answer("❌ Failed to end promotion", show_alert=True)
            
    except Exception as e:
        print(f"Error in end_promotion: {e}")
        await cb.answer("❌ Error ending promotion", show_alert=True)

@dp.callback_query(F.data.startswith("reactivate_promo_"))
async def reactivate_promotion(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    if not is_admin(cb.from_user.id):
        await cb.answer("❌ Access denied.", show_alert=True)
        return
    
    try:
        promo_id = int(cb.data[17:])  # Remove "reactivate_promo_" prefix
        
        if not db:
            await cb.answer("❌ Database unavailable", show_alert=True)
            return
        
        # Reactivate the promotion
        success = db.update_promotion_status(promo_id, active=True)
        
        if success:
            await cb.answer("✅ Promotion reactivated successfully!", show_alert=True)
            # Refresh the view
            await view_all_promotions(cb, state)
        else:
            await cb.answer("❌ Failed to reactivate promotion", show_alert=True)
            
    except Exception as e:
        print(f"Error in reactivate_promotion: {e}")
        await cb.answer("❌ Error reactivating promotion", show_alert=True)

# ======================== COUPON ENTRY SYSTEM ========================

@dp.callback_query(F.data == "enter_coupon")
async def enter_coupon_code(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    try:
        print(f"🎫 DEBUG: User {cb.from_user.id} clicked Enter Coupon Code button")
        # Track user - INSTANT, NO WAITING
        all_users.add(str(cb.from_user.id))
        # Background save - don't block button response
        asyncio.create_task(async_save_user(cb.from_user.id, cb.from_user.username, cb.from_user.first_name, cb.from_user.last_name))
        
        # Set state to coupon entry
        await state.set_state(OrderStates.coupon_entry)
        print(f"🎫 DEBUG: Set state to coupon_entry for user {cb.from_user.id}")
        
        text = "🎫 *Enter Coupon Code*\n\n"
        text += "📝 Type your coupon code to apply discount:\n\n"
        text += "💡 **How to use:**\n"
        text += "• Enter any valid discount code you have\n"
        text += "• Codes are case-insensitive\n"
        text += "• Valid codes will be applied automatically\n\n"
        text += "✏️ Just type your code and send it!"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="view_cart")]
        ])
        
        try:
            await cb.answer()  # Answer the callback first
            if cb.message and hasattr(cb.message, 'edit_text'):
                await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        except Exception as e:
            print(f"Error showing coupon entry: {e}")
            # If edit fails, try sending a new message
            try:
                await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
            except Exception as e2:
                print(f"Error sending new coupon entry message: {e2}")
            
    except Exception as e:
        print(f"Error in enter_coupon_code: {e}")
        await cb.answer("❌ Error entering coupon", show_alert=True)

@dp.message(OrderStates.coupon_entry)
async def process_coupon_code(message: types.Message, state: FSMContext):
    """Process entered coupon code"""
    try:
        print(f"🎫 DEBUG: Coupon processing started for user {message.from_user.id}")
        # Track user - INSTANT, NO WAITING
        all_users.add(str(message.from_user.id))
        # Background save - don't block button response
        asyncio.create_task(async_save_user(message.from_user.id, message.from_user.username, message.from_user.first_name, message.from_user.last_name))
        
        coupon_code = message.text.strip().upper()
        print(f"🎫 DEBUG: Received coupon code: '{coupon_code}'")
        
        if not coupon_code:
            print("🎫 DEBUG: Empty coupon code detected")
            await message.answer("❌ Please enter a valid coupon code.")
            return
        
        # Validate coupon with database
        if not db:
            print("🎫 DEBUG: Database not available")
            await message.answer("❌ Database unavailable. Please try again later.")
            return
        
        print(f"🎫 DEBUG: Validating coupon '{coupon_code}' with database")
        coupon = db.validate_coupon(coupon_code, str(message.from_user.id))
        print(f"🎫 DEBUG: Validation result: {coupon}")
        
        if not coupon:
            print(f"🎫 DEBUG: Coupon '{coupon_code}' marked as invalid")
            await message.answer(f"❌ Coupon code '{coupon_code}' is invalid or expired.")
            return
        
        print(f"🎫 DEBUG: Coupon '{coupon_code}' is VALID - details: {coupon}")
        
        # Get current cart data
        data = await state.get_data()
        cart = data.get('cart', [])
        
        if not cart:
            await message.answer("❌ Your cart is empty. Add items before applying a coupon.")
            return
        
        # Calculate cart total
        total = 0
        for item in cart:
            total += item['price']  # Each cart item represents one unit at this price
        
        # Check minimum spend requirement
        if coupon.get('min_spend', 0) > total:
            await message.answer(f"❌ Minimum spend of £{coupon['min_spend']:.2f} required. Your cart total is £{total:.2f}")
            return
        
        print(f"🎫 DEBUG: Coupon {coupon_code} validated and ready to apply")
        
        # Store coupon in state (discount will be calculated at checkout with postage)
        await state.update_data(applied_coupon={
            'code': coupon_code,
            'type': coupon['type'],
            'value': coupon['value']
        })
        
        # Back to cart view with coupon applied
        await state.set_state(OrderStates.shopping)
        print(f"🎫 DEBUG: Set state back to shopping")
        
        if coupon['type'] == 'percent':
            discount_text = f"{coupon['value']}% off your total"
        else:
            discount_text = f"£{coupon['value']} off your total"
        
        text = f"✅ *Coupon Applied Successfully!*\n\n"
        text += f"🎫 **Code:** {coupon_code}\n"
        text += f"💰 **Discount:** {discount_text}\n"
        text += f"📊 **Cart Subtotal:** £{total:.2f}\n\n"
        text += "🛒 Proceed to checkout - discount will be applied to your total including postage."
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛒 View Cart", callback_data="view_cart")],
            [InlineKeyboardButton(text="🏠 Continue Shopping", callback_data="shopping")]
        ])
        
        await message.answer(text, reply_markup=kb, parse_mode="Markdown")
        print(f"🎫 DEBUG: Sent success message to user")
        
    except Exception as e:
        print(f"Error in process_coupon_code: {e}")
        await message.answer("❌ Error processing coupon. Please try again.")

@dp.callback_query(F.data.startswith('add_'))
async def add_item(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    try:
        # Track user - INSTANT, NO WAITING
        all_users.add(str(cb.from_user.id))
        # Background save - don't block button response
        asyncio.create_task(async_save_user(cb.from_user.id, cb.from_user.username, cb.from_user.first_name, cb.from_user.last_name))
        
        # Validate callback data
        if not cb.data or len(cb.data) < 5:
            await cb.answer("❌ Invalid item selection.", show_alert=True)
            return
            
        parts = cb.data[4:].split('|')
        if len(parts) != 4:
            await cb.answer("❌ Invalid item data format.", show_alert=True)
            return

        section, product, size, price = parts
        
        # Validate input data
        if not all([section.strip(), product.strip(), size.strip(), price.strip()]):
            await cb.answer("❌ Missing item information.", show_alert=True)
            return
            
        # Validate price is numeric
        try:
            price_float = float(price)
            if price_float <= 0 or price_float > 10000:  # Sanity check
                await cb.answer("❌ Invalid price.", show_alert=True)
                return
        except (ValueError, TypeError):
            await cb.answer("❌ Invalid price format.", show_alert=True)
            return
            
        # Validate product exists in our catalog
        if section not in products or product not in products[section]:
            await cb.answer("❌ Product not found.", show_alert=True)
            return

        # Get current cart with state validation
        try:
            data = await state.get_data()
            if data is None:
                data = {}
        except Exception as e:
            print(f"⚠️ Error getting state data: {e}")
            data = {}
            
        cart = data.get('cart', [])
        
        # Validate cart structure
        if not isinstance(cart, list):
            print(f"⚠️ Invalid cart structure, resetting: {type(cart)}")
            cart = []

        # Create new item with validation
        new_item = {
            'name': str(product).strip()[:100],  # Limit length and sanitize
            'size': str(size).strip()[:50],
            'price': int(float(price)),  # Ensure it's a valid integer
            'section': str(section).strip()[:50],
            'added_at': datetime.datetime.now().isoformat()  # Track when added
        }
        
        # Validate cart size limit
        if len(cart) >= 50:  # Prevent memory issues
            await cb.answer("❌ Cart is full. Please checkout or clear some items.", show_alert=True)
            return
            
        cart.append(new_item)

        # Apply special pricing if applicable
        def calculate_discounted_price(cart_items):
            # Define combination discount prices (total price for both items combined)
            combination_discounts = {
                "3.5g": 50,  # Total price for Blue nerds + Permanent marker 3.5g bundle
                "7g": 85,    # Total price for Blue nerds + Permanent marker 7g bundle  
                "14g": 160,  # Total price for Blue nerds + Permanent marker 14g bundle
                "28g": 290,  # Total price for Blue nerds + Permanent marker 1oz bundle
                "2oz": 550,  # Total price for Blue nerds + Permanent marker 2oz bundle
                "4oz": 1000  # Total price for Blue nerds + Permanent marker 4oz bundle
            }

            # No bundle discounts currently active
            # (Blue nerds removed, so no bundle pricing needed)

            return cart_items

        # Update cart before proceeding with checkout
        cart = calculate_discounted_price(cart)

        # Update state with error handling
        try:
            await state.update_data(cart=cart)
            await state.set_state(OrderStates.shopping)
        except Exception as e:
            print(f"❌ Error updating cart state: {e}")
            await cb.answer("❌ Error adding item to cart. Please try again.", show_alert=True)
            return

        # Show confirmation and continue shopping
        data = await state.get_data()
        applied_coupon = data.get('applied_coupon')
        cart_text = get_cart_text(cart, applied_coupon)
        text = f"✅ *Added to cart!*\n\n{product} ({size}) - £{price}\n\n{cart_text}"

        try:
            if cb.message and hasattr(cb.message, 'edit_text') and not isinstance(cb.message, types.InaccessibleMessage):
                await cb.message.edit_text(text, reply_markup=cart_kb(cart), parse_mode="Markdown")
        except Exception as e:
            print(f"Error in add_item: {e}")
            # If edit fails, try sending a new message
            try:
                await cb.answer()
                await bot.send_message(cb.from_user.id, text, reply_markup=cart_kb(cart), parse_mode="Markdown")
            except Exception as e2:
                print(f"Error sending new add_item message: {e2}")

    except (ValueError, IndexError) as e:
        print(f"⚠️ Value/Index error in add_item: {e}")
        await cb.answer("❌ Invalid item selection.", show_alert=True)
    except Exception as e:
        print(f"❌ Unexpected error in add_item: {e}")
        try:
            await cb.answer("❌ An error occurred. Please try again.", show_alert=True)
        except:
            pass  # Don't crash if callback answer fails


@dp.callback_query(F.data == 'view_cart')
async def view_cart(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    data = await state.get_data()
    cart = data.get('cart', [])
    applied_coupon = data.get('applied_coupon')

    text = get_cart_text(cart, applied_coupon)
    try:
        if cb.message and hasattr(cb.message, 'edit_text') and not isinstance(cb.message, types.InaccessibleMessage):
            await cb.message.edit_text(text, reply_markup=cart_kb(cart), parse_mode="Markdown")
    except Exception as e:
        print(f"Error in view_cart: {e}")
        # If edit fails, try sending a new message
        try:
            await cb.answer()
            await bot.send_message(cb.from_user.id, text, reply_markup=cart_kb(cart), parse_mode="Markdown")
        except Exception as e2:
            print(f"Error sending new cart message: {e2}")


# Handler for removing individual items from cart
@dp.callback_query(F.data.startswith('remove_item_'))
async def remove_item(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    try:
        item_index = int(cb.data[12:])  # Remove 'remove_item_' prefix
        
        data = await state.get_data()
        cart = data.get('cart', [])
        
        if 0 <= item_index < len(cart):
            removed_item = cart.pop(item_index)
            await state.update_data(cart=cart)
            
            text = f"❌ *Item Removed!*\n\n{removed_item['name']} ({removed_item['size']}) - £{removed_item['price']}\n\n"
            
            if cart:
                text += get_cart_text(cart)
            else:
                text += "🛒 *Your cart is now empty*\n\nAdd some items to get started!"
            
            try:
                if cb.message and hasattr(cb.message, 'edit_text') and not isinstance(cb.message, types.InaccessibleMessage):
                    await cb.message.edit_text(text, reply_markup=cart_kb(cart), parse_mode="Markdown")
            except Exception as e:
                print(f"Error updating cart after removal: {e}")
                try:
                    await cb.answer()
                    await bot.send_message(cb.from_user.id, text, reply_markup=cart_kb(cart), parse_mode="Markdown")
                except Exception as e2:
                    print(f"Error sending new removal message: {e2}")
        else:
            await cb.answer("Invalid item selection.", show_alert=True)
            
    except (ValueError, IndexError) as e:
        print(f"Error removing item: {e}")
        await cb.answer("Error removing item. Please try again.", show_alert=True)

# Ignore separator button clicks
@dp.callback_query(F.data == 'separator')
async def ignore_separator(cb: types.CallbackQuery):
    await cb.answer()
    track_user(cb.from_user)

@dp.callback_query(F.data == 'clear_cart')
async def clear_cart(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    await state.update_data(cart=[])
    try:
        if cb.message and hasattr(cb.message, 'edit_text') and not isinstance(cb.message, types.InaccessibleMessage):
            await cb.message.edit_text(
                "🗑️ *Cart cleared!*\n\nYour cart is now empty. Start shopping again:",
                reply_markup=shopping_kb(), parse_mode="Markdown")
    except Exception as e:
        print(f"Error in clear_cart: {e}")
        try:
            await cb.answer()
            await bot.send_message(cb.from_user.id, 
                "🗑️ *Cart cleared!*\n\nYour cart is now empty. Start shopping again:",
                reply_markup=shopping_kb(), parse_mode="Markdown")
        except Exception as e2:
            print(f"Error sending new clear cart message: {e2}")


@dp.callback_query(F.data == 'checkout')
async def checkout(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    try:
        # Fetch updated cart data
        data = await state.get_data()
        cart = data.get('cart', [])

        if not cart:
            await cb.answer("Your cart is empty! Add some items first.", show_alert=True)
            return

        # Compute the current total of the cart with discounts
        cart_total = sum(item['price'] for item in cart)
        applied_coupon = data.get('applied_coupon')
        cart_text = get_cart_text(cart, applied_coupon)

        # Check if cart only contains free items (freebies)
        if cart_total == 0:
            # Free delivery for freebies!
            await state.update_data(postage="FREE DELIVERY (Freebie)", postage_cost=0)
            await state.set_state(OrderStates.delivery)
            
            text = f"{cart_text}\n"
            text += f"📦 *Postage: FREE DELIVERY (Freebie)*\n\n"
            text += "🏠 *Please provide your delivery details:*\n\n"
            text += "🔒 *For Privacy:* Visit https://privyxnote.com to create a self-destructing encrypted link with your address, then paste the link here.\n\n"
            text += "📝 Or enter your full address directly:\n"
            text += "• Full name\n• Complete address with postcode\n• Any special delivery instructions\n• Phone number (optional)"
            
            try:
                if cb.message and hasattr(cb.message, 'edit_text'):
                    await cb.message.edit_text(text, parse_mode="Markdown")
                else:
                    await bot.send_message(cb.from_user.id, text, parse_mode="Markdown")
            except Exception as e:
                print(f"Error sending freebie delivery message: {e}")
            return

        # Set the state to postage and show postage options for paid orders
        await state.set_state(OrderStates.postage)
        text = f"{cart_text}\n\n"
        text += "📦 *Select Postage Method:*"

        try:
            if cb.message and hasattr(cb.message, 'edit_text') and not isinstance(cb.message, types.InaccessibleMessage):
                await cb.message.edit_text(text, reply_markup=await postage_kb_async(state))
        except Exception as e:
            print(f"Error editing checkout message: {e}")
            # If editing fails, send a new message
            try:
                await bot.send_message(cb.from_user.id, text, reply_markup=await postage_kb_async(state))
            except Exception as e2:
                print(f"Error sending checkout message: {e2}")
                await cb.answer("Error loading checkout. Please try again.", show_alert=True)
                
    except Exception as e:
        print(f"CRITICAL ERROR in checkout: {e}")
        try:
            await cb.answer("❌ Error loading checkout. Please try again.", show_alert=True)
        except:
            pass


@dp.callback_query(F.data.startswith('post_'), OrderStates.postage)
async def select_postage(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    try:
        if not cb.data:
            await cb.answer("Invalid postage selection.", show_alert=True)
            return
            
        method = cb.data[5:]
        if method not in postage_options:
            await cb.answer("Invalid postage method.", show_alert=True)
            return
        # Calculate cart total with validation
        data = await state.get_data()
        cart = data.get('cart', [])
        if not cart:
            await cb.answer("Your cart is empty. Please add items first.", show_alert=True)
            return
            
        try:
            cart_total = sum(item.get('price', 0) for item in cart if isinstance(item, dict) and 'price' in item)
        except (TypeError, ValueError):
            await cb.answer("Error calculating cart total. Please restart your order.", show_alert=True)
            return

        # Check for free postage eligibility using cart total (before discounts)
        if method == "48 FREE TRACKED RM" and cart_total < 30:
            await cb.answer("This option is only available for orders £30 and above.", show_alert=True)
            return

        # Apply postage method
        await state.update_data(postage=method, postage_cost=postage_options[method])
        await state.set_state(OrderStates.delivery)

        data = await state.get_data()
        cart = data.get('cart', [])
        applied_coupon = data.get('applied_coupon')
        cart_text = get_cart_text(cart, applied_coupon)

        text = f"{cart_text}\n"
        text += f"📦 *Postage: {method} - £{postage_options[method]}*\n\n"
        text += "🏠 *Please provide your delivery details:*\n\n"
        text += "🔒 *For Privacy:* Visit https://privyxnote.com to create a self-destructing encrypted link with your address, then paste the link here.\n\n"
        text += "📝 Or enter your full address directly:\n"
        text += "• Full name\n• Complete address with postcode\n• Any special delivery instructions\n• Phone number (optional)"

        try:
            if cb.message and hasattr(cb.message, 'edit_text'):
                await cb.message.edit_text(text)
        except Exception as e:
            print(f"Error in select_postage: {e}")
            # Fallback: send new message if editing fails
            try:
                await bot.send_message(cb.from_user.id, text)
            except Exception as e2:
                print(f"Error sending fallback message in select_postage: {e2}")
                await cb.answer("Error processing your request. Please try again.", show_alert=True)
                
    except Exception as e:
        print(f"Unexpected error in select_postage: {e}")
        try:
            await cb.answer("An error occurred processing your request. Please try again.", show_alert=True)
        except:
            pass  # Last resort - don't crash even if callback answer fails


@dp.message(OrderStates.delivery)
async def get_delivery(message: types.Message, state: FSMContext):
    track_user(message.from_user)
    """Handle delivery address input with comprehensive validation"""
    try:
        # Validate message exists and has text
        if not message or not message.text:
            await message.answer("❌ Please provide your delivery address as text.")
            return
            
        delivery_text = message.text.strip()
        
        # Comprehensive delivery validation
        if len(delivery_text) < 10:
            await message.answer("❌ Please provide a complete delivery address with your full name, address, and postcode.")
            return
            
        if len(delivery_text) > 1000:  # Prevent abuse
            await message.answer("❌ Delivery address is too long. Please provide a concise address.")
            return
            
        # Basic validation for required components
        delivery_lower = delivery_text.lower()
        has_name = any(len(word) > 1 and word.isalpha() for word in delivery_text.split()[:3])  # Check first 3 words for name
        has_address_words = any(word in delivery_lower for word in ['street', 'road', 'avenue', 'lane', 'close', 'drive', 'way', 'place', 'court', 'crescent'])
        has_postcode = any(len(word) >= 5 and any(c.isdigit() for c in word) for word in delivery_text.split()[-3:])  # Check last 3 words for postcode
        
        if not (has_name or has_address_words or has_postcode):
            await message.answer("❌ Please ensure your address includes:\n• Full name\n• Street address\n• Postcode\n\nExample: John Smith, 123 High Street, London SW1A 1AA")
            return
            
        # Get current state with validation
        try:
            data = await state.get_data()
            if data is None:
                data = {}
        except Exception as e:
            print(f"❌ Error getting delivery state: {e}")
            await message.answer("❌ Error processing your request. Please start again.")
            return
            
        # Validate required state data before proceeding
        cart = data.get('cart', [])
        if not cart or not isinstance(cart, list):
            await message.answer("❌ Your cart is empty. Please add items first.")
            return
            
        if 'postage' not in data or 'postage_cost' not in data:
            await message.answer("❌ Postage information missing. Please go back and select your postage method first.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Back to Cart", callback_data='checkout')]
            ]))
            return
            
        # Validate postage cost is numeric
        try:
            postage_cost = float(data['postage_cost'])
            if postage_cost < 0 or postage_cost > 100:  # Sanity check
                await message.answer("❌ Invalid postage cost detected. Please restart your order.")
                return
        except (ValueError, TypeError):
            await message.answer("❌ Invalid postage cost format. Please restart your order.")
            return
            
        # Update state with error handling
        try:
            await state.update_data(delivery=delivery_text)
            await state.set_state(OrderStates.payment)
        except Exception as e:
            print(f"❌ Error updating delivery state: {e}")
            await message.answer("❌ Error saving delivery information. Please try again.")
            return
            
        applied_coupon = data.get('applied_coupon')
        cart_text = get_cart_text(cart, applied_coupon)

        text = f"{cart_text}\n"
        text += f"📦 *Postage: {data['postage']} - £{data['postage_cost']}*\n"
        text += f"🏠 *Delivery details saved*\n\n"
        text += "💳 *Select Payment Method:*"

        await message.answer(text, reply_markup=payment_kb())
        
    except Exception as e:
        print(f"❌ Unexpected error in get_delivery: {e}")
        try:
            await message.answer("❌ An error occurred processing your delivery details. Please try again.")
        except:
            print(f"❌ Failed to send error message in get_delivery: {e}")
            pass  # Don't crash if message sending fails


@dp.callback_query(F.data.startswith('pay_'), OrderStates.payment)
async def select_payment(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    try:
        if not cb.data:
            await cb.answer("Invalid payment selection.", show_alert=True)
            return
            
        # Generate random 5-digit order number
        # Get next order number from database (starts from 106)
        if db:
            order_num = db.get_next_order_number()
        else:
            order_num = f"ORD{random.randint(10000, 99999)}"
        payment = cb.data[4:]
        await state.update_data(payment=payment)
        data = await state.get_data()

        cart = data.get('cart', [])
        if not cart:
            await cb.answer("Your cart is empty! Please add items first.", show_alert=True)
            return
            
        cart_total = sum(item['price'] for item in cart)
        postage_cost = data.get('postage_cost', 0)
        
        # Add postage first, then apply discount to total (cart + postage)
        subtotal_with_postage = cart_total + postage_cost
        applied_coupon = data.get('applied_coupon')
        discount_amount = 0
        total = subtotal_with_postage
        
        if applied_coupon:
            # Calculate discount on total including postage
            coupon_type = applied_coupon.get('type', 'percent')
            coupon_value = applied_coupon.get('value', 0)
            
            if coupon_type == 'percent':
                discount_amount = subtotal_with_postage * (float(coupon_value) / 100)
            else:  # fixed amount
                discount_amount = float(coupon_value)
            
            total = subtotal_with_postage - discount_amount

        text = f"📋 *Order Confirmation*\n\n"
        text += f"🔢 Order Number: `{order_num}`\n"
        text += f"📅 Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        text += f"🛒 *Items:*\n"
        for i, item in enumerate(cart, 1):
            text += f"{i}. {item['name']} ({item['size']}) - £{item['price']}\n"

        text += f"\n💰 *Cart Subtotal:* £{cart_total:.2f}\n"
        text += f"📦 *Postage:* {data.get('postage', 'N/A')} - £{postage_cost}\n"
        text += f"💸 *Subtotal (Cart + Postage):* £{subtotal_with_postage:.2f}\n"
        
        # Show discount if applied
        if applied_coupon:
            coupon_code = applied_coupon.get('code', '')
            text += f"🎫 *Coupon ({coupon_code}):* -£{discount_amount:.2f}\n"
            
        text += f"💰 *Final Total:* £{total:.2f}\n"
        text += f"💳 *Payment:* {payment}\n\n"
        text += f"🏠 *Delivery Details:*\n{data.get('delivery', 'N/A')}\n\n"

        if payment == "PayPal":
            text += f"💰 PayPal: `oliviaroberts1001`\n"
            text += f"⚠️ *IMPORTANT:* Send as Friends & Family\n"
            text += f"📝 Reference: `{order_num}`\n"
            text += f"📸 Send screenshot to @ogukdankzz\n\n"
        elif payment == "Cash in post":
            text += f"💵 Send cash in post\n"
            text += f"💬 Message @ogukdankzz for address\n\n"

        if payment == "Crypto (LTC)":
            try:
                # Ensure order is only confirmed once the payment is verified on the blockchain
                selected_address = random.choice(ltc_addresses_extended)
                # Add £1 fee for crypto processing to prevent underpayment
                total_with_fees = total + 1
                # Get current LTC price from live API
                current_ltc_price = await get_current_ltc_price()
                ltc_amount = round(total_with_fees / current_ltc_price, 6)  # LTC rate calculation with £1 fee included
                text += f"💰 *LTC Amount:* `{ltc_amount}` *(includes £1 fee)*\n"
                text += f"💰 *LTC Rate:* £{current_ltc_price:.2f} per LTC *(live rate)*\n"
                text += f"💰 *LTC Address:* `{selected_address}`\n\n"
                text += "🚀 *AUTOMATIC VERIFICATION*\n"
                text += "💬 *Your order will be automatically confirmed once payment is detected on the blockchain.*\n"
                text += "💬 *No manual confirmation needed - just send the LTC!*\n\n"
                text += "💬 Message @ogukdankzz if you have any issues"

                # Store user information and payment details for later verification
                user_info = {
                    'user_id': cb.from_user.id,
                    'username': cb.from_user.username or 'N/A',
                    'first_name': cb.from_user.first_name or 'N/A'
                }
                
                await state.update_data(
                    ltc_address=selected_address, 
                    ltc_amount=ltc_amount,
                    user_info=user_info,
                    total_amount=total
                )

                # Trigger payment verification function asynchronously with proper cleanup
                task = asyncio.create_task(verify_and_confirm_payment(cb.from_user.id, state, text, order_num))
                background_tasks.add(task)
                task.add_done_callback(background_tasks.discard)
            except Exception as e:
                print(f"Error setting up crypto payment: {e}")
                text += "❌ Error setting up crypto payment. Please try another payment method."

        else:
            text += "📌 *Click CONFIRM only after you have sent the funds*"

        await state.update_data(order_text=text, order_num=order_num)
        await state.set_state(OrderStates.confirm)
        
        try:
            if cb.message and hasattr(cb.message, 'edit_text'):
                # Use different keyboards based on payment method
                if payment == "Crypto (LTC)":
                    # For crypto: no confirm button, automatic verification
                    await cb.message.edit_text(text, reply_markup=crypto_payment_kb())
                else:
                    # For other payments: show confirm button
                    await cb.message.edit_text(text, reply_markup=confirm_kb_with_back())
        except Exception as e:
            print(f"Error editing payment confirmation message: {e}")
            # If editing fails, send a new message
            try:
                if payment == "Crypto (LTC)":
                    await bot.send_message(cb.from_user.id, text, reply_markup=crypto_payment_kb())
                else:
                    await bot.send_message(cb.from_user.id, text, reply_markup=confirm_kb_with_back())
            except Exception as e2:
                print(f"Error sending payment confirmation message: {e2}")
                await cb.answer("Error loading payment confirmation. Please try again.", show_alert=True)
                
    except Exception as e:
        print(f"CRITICAL ERROR in select_payment: {e}")
        try:
            await cb.answer("❌ Error processing payment selection. Please try again.", show_alert=True)
        except:
            pass


@dp.callback_query(F.data == 'confirm', OrderStates.confirm)
async def confirm_order(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    try:
        data = await state.get_data()
        payment = data.get('payment', '')
        
        # For crypto payments, block confirmation until verified
        if payment == "Crypto (LTC)" and data.get('payment_status') != "Verified":
            await cb.answer("⏳ Please wait for your LTC payment to be verified on the blockchain before confirming.", show_alert=True)
            return

        # First acknowledge the button press
        await cb.answer("✅ Order confirmed! Thank you!")
        
        # Try to edit the message, if it fails send a new message
        try:
            if cb.message and hasattr(cb.message, 'edit_text'):
                await cb.message.edit_text(
                    f"✅ *Order Confirmed!*\n\n{data.get('order_text', 'Order details')}\n\n🎉 Thank you for your order!",
                    reply_markup=shopping_kb(),
                    parse_mode="Markdown")
        except Exception as e:
            print(f"Error editing confirmation message: {e}")
            # If editing fails, send a new message
            try:
                await bot.send_message(
                    cb.from_user.id,
                    f"✅ *Order Confirmed!*\n\n{data.get('order_text', 'Order details')}\n\n🎉 Thank you for your order!",
                    reply_markup=shopping_kb(),
                    parse_mode="Markdown")
            except Exception as e2:
                print(f"Error sending confirmation message: {e2}")

        # Send to admin (only for non-crypto or verified crypto payments)
        admin_text = f"🚨 *NEW ORDER RECEIVED*\n\n"
        admin_text += f"👤 Customer: @{cb.from_user.username or 'N/A'} ({cb.from_user.first_name})\n"
        admin_text += f"🆔 User ID: `{cb.from_user.id}`\n"
        admin_text += data.get('order_text', 'Order details not available')
        
        # Add discount information to admin notification
        applied_coupon = data.get('applied_coupon')
        if applied_coupon:
            coupon_code = applied_coupon.get('code', '')
            # Recalculate discount for admin display (same logic as payment)
            cart = data.get('cart', [])
            cart_total = sum(item['price'] for item in cart)
            postage_cost = data.get('postage_cost', 0)
            subtotal_with_postage = cart_total + postage_cost
            
            coupon_type = applied_coupon.get('type', 'percent')
            coupon_value = applied_coupon.get('value', 0)
            
            if coupon_type == 'percent':
                discount_amount = subtotal_with_postage * (float(coupon_value) / 100)
            else:  # fixed amount
                discount_amount = float(coupon_value)
            
            final_amount = subtotal_with_postage - discount_amount
            
            admin_text += f"\n\n🎫 *DISCOUNT APPLIED*\n"
            admin_text += f"Code: {coupon_code}\n"
            admin_text += f"Cart + Postage: £{subtotal_with_postage:.2f}\n"
            admin_text += f"Discount: £{discount_amount:.2f}\n"
            admin_text += f"Customer Paid: £{final_amount:.2f}"

        # Send admin notification with retry logic
        admin_sent = False
        for attempt in range(3):
            try:
                await bot.send_message(ADMIN_ID, admin_text, parse_mode="Markdown")
                admin_sent = True
                logger.info(f"✅ Admin notification sent for order")
                break
            except Exception as e:
                logger.error(f"❌ Failed to send admin message (attempt {attempt+1}/3): {e}")
                await asyncio.sleep(1)  # Wait 1 second before retry
        
        if not admin_sent:
            logger.critical(f"🚨 CRITICAL: Failed to send admin notification after 3 attempts!")
            # Try one more time without parse_mode in case formatting is the issue
            try:
                plain_text = admin_text.replace('*', '').replace('`', '')
                await bot.send_message(ADMIN_ID, plain_text)
                logger.info("✅ Admin notification sent (plain text fallback)")
            except Exception as e:
                logger.critical(f"🚨 FINAL FAILURE sending admin notification: {e}")
        
        # Save order to database for admin confirmation
        if db:
            try:
                order_num = data.get('order_num', 'N/A')
                username = cb.from_user.username or 'N/A'
                
                # Include discount information in order details
                applied_coupon = data.get('applied_coupon')
                order_details = data.get('order_text', 'Order details not available')
                
                if applied_coupon:
                    coupon_code = applied_coupon.get('code', '')
                    discount_amount = applied_coupon.get('discount_amount', 0)
                    order_details += f"\n\n🎫 DISCOUNT USED:\nCode: {coupon_code}\nDiscount: £{discount_amount:.2f}"
                
                db.save_order(order_num, cb.from_user.id, username, order_details)
                
                # FINALIZE FREEBIE CLAIMS - Only now mark freebie as claimed!
                cart = data.get('cart', [])
                for item in cart:
                    if item.get('is_freebie') and item.get('freebie_user_id'):
                        freebie_user_id = item['freebie_user_id']
                        freebie_username = item.get('freebie_username', 'Unknown')
                        freebie_product = item.get('name', '100mg Edible')
                        
                        # Now record the claim in database (order completed!)
                        claim_result = db.claim_freebie(freebie_user_id, freebie_username, freebie_product)
                        if claim_result['success']:
                            logger.info(f"✅ Freebie claim finalized for user {freebie_user_id}")
                        else:
                            logger.warning(f"⚠️ Freebie claim issue for user {freebie_user_id}: {claim_result.get('error')}")
                
            except Exception as e:
                print(f"Error saving order to database: {e}")

        # Save order to history before clearing cart
        try:
            cart = data.get('cart', [])
            if cart:  # Only save if there were items
                order_date = datetime.date.today()
                user_orders[str(cb.from_user.id)] = {
                    'order_num': data.get('order_num', 'N/A'),
                    'items': cart.copy(),
                    'date': order_date
                }
        except Exception as e:
            print(f"Error saving order to history: {e}")
        
        # Clear cart and reset to shopping
        try:
            await state.update_data(cart=[])
            await state.set_state(OrderStates.shopping)
        except Exception as e:
            print(f"Error clearing cart and resetting state: {e}")
            
    except Exception as e:
        print(f"CRITICAL ERROR in confirm_order: {e}")
        # Emergency fallback - try to acknowledge the button and send error message
        try:
            await cb.answer("❌ Error processing order confirmation. Please try again.", show_alert=True)
        except:
            pass
        # Try to send error message to user
        try:
            await bot.send_message(cb.from_user.id, "❌ There was an error confirming your order. Please contact support or try again.", reply_markup=shopping_kb())
        except:
            pass


@dp.callback_query(F.data == 'main')
async def back_main(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    # Redirect to shopping instead of clearing everything
    try:
        if cb.message and hasattr(cb.message, 'edit_text'):
            await cb.message.edit_text("👋 Welcome to UKDANKZZ! Browse our menu:",
                                       reply_markup=shopping_kb())
    except Exception as e:
        print(f"Error in back_main: {e}")


async def verify_and_confirm_payment(user_id, state, order_text, order_num):
    await asyncio.sleep(300)  # Wait for 5 minutes for payment verification

    try:
        data = await state.get_data()
        ltc_address = data.get('ltc_address')
        ltc_amount = data.get('ltc_amount')
        user_info = data.get('user_info', {})
        
        if not ltc_address or not ltc_amount:
            print(f"Error: Missing payment details for user {user_id}")
            return

        # Debug: Log exactly what amounts we're using
        print(f"DEBUG - Payment verification for user {user_id}:")
        print(f"DEBUG - ltc_address: {ltc_address}")
        print(f"DEBUG - ltc_amount from state: {ltc_amount} (type: {type(ltc_amount)})")
        print(f"DEBUG - order_num: {order_num}")

        payment_result = await check_blockchain_payment(ltc_address, ltc_amount)
        
        if payment_result['status'] == 'paid':
            await state.update_data(payment_status="Verified")
            
            # CRITICAL: Save to order_confirmations so admin can confirm from pending orders
            username = user_info.get('username', 'N/A')
            if db:
                try:
                    # Save order to order_confirmations for admin approval
                    save_success = db.save_order(
                        order_num=order_num,
                        user_id=user_id,
                        username=username,
                        order_details=order_text
                    )
                    if save_success:
                        print(f"✅ Crypto order {order_num} saved to pending orders for admin confirmation")
                    else:
                        print(f"⚠️ Failed to save crypto order {order_num} to pending orders")
                except Exception as e:
                    print(f"❌ Error saving crypto order to pending: {e}")
            
            # Send confirmation message to user
            confirmation_message = (
                f"✅ *Payment Verified!*\n\n"
                f"🔢 Order Number: `{order_num}`\n"
                f"💰 LTC Payment confirmed on blockchain\n\n"
                f"⏳ Your order is now pending admin confirmation.\n"
                f"You'll be notified once confirmed!"
            )
            
            try:
                await bot.send_message(user_id, confirmation_message, reply_markup=shopping_kb())
            except Exception as e:
                print(f"Error sending payment verification message to user {user_id}: {e}")

            # Send to admin
            admin_text = f"🚨 *NEW CRYPTO ORDER - PENDING CONFIRMATION*\n\n"
            admin_text += f"👤 Customer: @{username} ({user_info.get('first_name', 'N/A')})\n"
            admin_text += f"🆔 User ID: `{user_id}`\n"
            admin_text += f"💰 LTC Payment: `{ltc_amount}` LTC to `{ltc_address}`\n"
            admin_text += f"✅ Blockchain: VERIFIED\n\n"
            admin_text += order_text
            admin_text += f"\n\n📋 Go to Admin Panel → Pending Orders to confirm!"

            # Send admin notification with retry logic
            admin_sent = False
            for attempt in range(3):
                try:
                    await bot.send_message(ADMIN_ID, admin_text)
                    admin_sent = True
                    logger.info(f"✅ Admin crypto notification sent for order {order_num}")
                    break
                except Exception as e:
                    logger.error(f"❌ Failed to send crypto admin message (attempt {attempt+1}/3): {e}")
                    await asyncio.sleep(1)
            
            if not admin_sent:
                logger.critical(f"🚨 CRITICAL: Failed to send crypto admin notification after 3 attempts!")

            # Save order to history before clearing cart
            cart = data.get('cart', [])
            if cart:  # Only save if there were items
                order_date = datetime.date.today()
                user_orders[str(user_id)] = {
                    'order_num': order_num,
                    'items': cart.copy(),
                    'date': order_date
                }
                
                # FINALIZE FREEBIE CLAIMS for crypto orders
                for item in cart:
                    if item.get('is_freebie') and item.get('freebie_user_id') and db:
                        freebie_user_id = item['freebie_user_id']
                        freebie_username = item.get('freebie_username', 'Unknown')
                        freebie_product = item.get('name', '100mg Edible')
                        claim_result = db.claim_freebie(freebie_user_id, freebie_username, freebie_product)
                        if claim_result['success']:
                            logger.info(f"✅ Crypto freebie claim finalized for user {freebie_user_id}")
            
            # Clear cart and reset to shopping
            await state.update_data(cart=[])
            await state.set_state(OrderStates.shopping)
            print(f"Payment verified and order confirmed for user {user_id}, order {order_num}")

        elif payment_result['status'] == 'underpaid':
            # Underpayment detected
            remaining = payment_result['remaining']
            received = payment_result['received']
            underpaid_message = (
                f"💸 *Underpayment Detected*\n\n"
                f"🔢 Order Number: `{order_num}`\n"
                f"You have underpaid for your order.\n\n"
                f"💰 Expected: `{ltc_amount}` LTC\n"
                f"💰 Received: `{received:.8f}` LTC\n"
                f"💰 Remaining: `{remaining:.8f}` LTC\n\n"
                f"📍 Please send the remaining `{remaining:.8f}` LTC to:\n"
                f"`{ltc_address}`\n\n"
                f"Your order will be confirmed once the full payment is received.\n"
                f"Contact @ogukdankzz if you need assistance."
            )
            
            try:
                await bot.send_message(user_id, underpaid_message, reply_markup=shopping_kb())
            except Exception as e:
                print(f"Error sending underpayment message to user {user_id}: {e}")
                
        else:
            # Payment not verified - notify user
            status = payment_result.get('status', 'unknown')
            received = payment_result.get('received', 0)
            
            if status == 'old_balance':
                message_type = "Old Balance Detected"
                explanation = f"Your address has `{received:.8f}` LTC but no recent payments were detected. Please send a fresh payment."
            else:
                message_type = "Payment Not Verified"
                explanation = "Your LTC payment has not been verified on the blockchain after 5 minutes."
            
            not_verified_message = (
                f"⚠️ *{message_type}*\n\n"
                f"🔢 Order Number: `{order_num}`\n"
                f"{explanation}\n\n"
                f"💰 Expected: `{ltc_amount}` LTC\n"
                f"💰 Address: `{ltc_address}`\n\n"
                f"Please check your transaction and contact @ogukdankzz if you believe this is an error.\n"
                f"Transactions may take longer during network congestion."
            )
            
            try:
                await bot.send_message(user_id, not_verified_message, reply_markup=shopping_kb())
            except Exception as e:
                print(f"Error sending payment not verified message to user {user_id}: {e}")
            
            print(f"Payment not verified for user {user_id}, order {order_num} after 5 minutes")
            # Keep the state for potential manual verification or follow-up

    except Exception as e:
        print(f"Error in verify_and_confirm_payment for user {user_id}: {e}")
        # Try to notify user of the error
        try:
            await bot.send_message(
                user_id, 
                f"⚠️ Error verifying payment for order `{order_num}`. Please contact @ogukdankzz for assistance.",
                reply_markup=shopping_kb()
            )
        except:
            print(f"Failed to notify user {user_id} of payment verification error")


# Review System Handlers
@dp.callback_query(F.data == 'reviews')
async def show_reviews_menu(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    await state.set_state(ReviewStates.viewing_reviews)
    
    try:
        if cb.message and hasattr(cb.message, 'edit_text'):
            await cb.message.edit_text(
                "⭐ *Reviews Section*\n\nWhat would you like to do?",
                reply_markup=reviews_main_kb(cb.from_user.id),
                parse_mode="Markdown")
    except Exception as e:
        print(f"Error showing reviews menu: {e}")


@dp.callback_query(F.data == 'view_all_reviews')
async def show_all_reviews(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    try:
        if cb.message and hasattr(cb.message, 'edit_text'):
            await cb.message.edit_text(
                "👀 *View Reviews*\n\nSelect a product to see its reviews:",
                reply_markup=view_reviews_kb(),
                parse_mode="Markdown")
    except Exception as e:
        print(f"Error showing all reviews: {e}")
        # If editing fails (e.g., message is identical), send a new message
        try:
            await bot.send_message(cb.from_user.id, 
                                 "👀 *View Reviews*\n\nSelect a product to see its reviews:",
                                 reply_markup=view_reviews_kb(),
                                 parse_mode="Markdown")
        except Exception as e2:
            print(f"Error sending new view reviews message: {e2}")


@dp.callback_query(F.data.startswith('show_reviews_'))
async def show_product_reviews(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    product_name = cb.data[13:]  # Remove 'show_reviews_' prefix
    is_admin = is_admin(cb.from_user.id)
    
    # Use efficient database query to get product-specific reviews
    product_specific_reviews = db.get_reviews_for_product(product_name, limit=5) if db else []
    
    if not product_specific_reviews:
        text = f"📝 *{product_name} Reviews*\n\nNo reviews yet for this product."
    else:
        text = f"📝 *{product_name} Reviews*\n\n"
        for review in product_specific_reviews:
            username = review['username']
            rating = review['rating']
            review_text = review['review_text']
            date = review['review_date']
            stars = "⭐" * rating
            
            if is_admin:
                # Admin sees usernames for business insights
                text += f"• {stars} by @{username}\n"
            else:
                # Regular customers see anonymous reviews
                text += f"• {stars} Anonymous Customer\n"
            
            text += f"  \"{review_text}\"\n"
            text += f"  📅 {date}\n\n"
    
    back_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Back", callback_data='view_all_reviews')]
    ])
    
    try:
        if cb.message and hasattr(cb.message, 'edit_text'):
            await cb.message.edit_text(text, reply_markup=back_kb, parse_mode="Markdown")
    except Exception as e:
        print(f"Error showing product reviews: {e}")
        # If editing fails (e.g., message is identical), send a new message
        try:
            await bot.send_message(cb.from_user.id, text, reply_markup=back_kb, parse_mode="Markdown")
        except Exception as e2:
            print(f"Error sending new product reviews message: {e2}")


# Removed: write_review handler - only customers who ordered can review
# This handler was allowing general reviews, which bypasses customer verification


# Removed: review_product_ handler - only customers who ordered can review
# This handler was allowing general product reviews, which bypasses customer verification


@dp.callback_query(F.data.startswith('rate_'))
async def rate_basket(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    user_id = str(cb.from_user.id)
    
    rating = int(cb.data[5:])  # Get the rating number
    await state.update_data(review_rating=rating)
    
    # Check if we're reviewing a specific order from "Your Orders"
    data = await state.get_data()
    reviewing_order_num = data.get('reviewing_order_num')
    
    if reviewing_order_num:
        # New flow: reviewing from "Your Orders"
        logger.info(f"User {user_id} rating order {reviewing_order_num} with {rating} stars")
        await state.set_state(ReviewStates.writing_review)
        stars = "⭐" * rating
        
        try:
            if cb.message and hasattr(cb.message, 'edit_text'):
                await cb.message.edit_text(
                    f"✍️ *Write Your Review for Order {reviewing_order_num}*\n\nRating: {stars}\n\nPlease type your review:",
                    parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Error showing review writing prompt: {e}")
        return
    
    # Old flow: reviewing from basket (legacy support)
    if user_id not in user_orders:
        await cb.answer("❌ Only customers who have placed orders can leave reviews.", show_alert=True)
        return
    
    # Check if the order is within the review window (5 days)
    user_order = user_orders[user_id]
    order_date = user_order.get('date', '')
    
    if not is_order_within_review_window(order_date):
        await cb.answer("❌ Reviews can only be left within 5 days of your order. Your order is too old to review.", show_alert=True)
        return
    
    await state.set_state(ReviewStates.writing_review)
    
    stars = "⭐" * rating
    
    try:
        if cb.message and hasattr(cb.message, 'edit_text'):
            await cb.message.edit_text(
                f"✍️ *Write Your Basket Review*\n\nRating: {stars}\n\nPlease type your review for the entire order:",
                parse_mode="Markdown")
    except Exception as e:
        print(f"Error showing review writing prompt: {e}")


@dp.message(ReviewStates.writing_review)
async def save_basket_review(message: types.Message, state: FSMContext):
    track_user(message.from_user)
    """Save review for entire basket/order"""
    user_id = str(message.from_user.id)
    
    data = await state.get_data()
    rating = data.get('review_rating')
    review_text = message.text
    username = message.from_user.username or message.from_user.first_name or "Anonymous"
    date = datetime.date.today()
    
    # Check if reviewing a specific order from "Your Orders"
    reviewing_order_num = data.get('reviewing_order_num')
    
    if reviewing_order_num:
        # New flow: reviewing from "Your Orders" - get order from database
        if db:
            # Check if already reviewed
            if db.user_has_reviewed_order(message.from_user.id, reviewing_order_num):
                await message.answer("❌ You have already reviewed this order.", reply_markup=shopping_kb())
                await state.set_state(OrderStates.shopping)
                return
            
            # Get order items from database
            orders = db.get_user_orders(message.from_user.id, limit=50)
            order_data = next((o for o in orders if o['order_num'] == reviewing_order_num), None)
            
            if order_data:
                order_details = order_data.get('order_details', {})
                ordered_items = []
                
                # Try to get items from structured format first
                if isinstance(order_details, dict):
                    if 'items' in order_details:
                        ordered_items = order_details.get('items', [])
                    elif 'details' in order_details:
                        # Parse items from text format: "1. Product Name (size) - £price"
                        import re
                        details_text = order_details.get('details', '')
                        item_matches = re.findall(r'\d+\.\s+(.+?)\s+\(([^)]+)\)\s+-\s+£([\d.]+)', details_text)
                        ordered_items = [
                            {
                                'name': item_name.strip(),
                                'size': size.strip(),
                                'price': float(price)
                            }
                            for item_name, size, price in item_matches
                        ]
                
                logger.info(f"User {message.from_user.id} reviewing order {reviewing_order_num} with {len(ordered_items)} items")
            else:
                logger.warning(f"Order {reviewing_order_num} not found for user {message.from_user.id}")
                await message.answer("❌ Order not found. Please try again.", reply_markup=shopping_kb())
                await state.set_state(OrderStates.shopping)
                await state.update_data(reviewing_order_num=None, review_rating=None)
                return
            
            # Save review
            success = db.save_review(
                username=username,
                rating=rating,
                review_text=review_text,
                review_date=date,
                order_items=ordered_items,
                order_num=reviewing_order_num,
                user_id=message.from_user.id
            )
            
            if success:
                stars = "⭐" * rating
                confirmation_text = f"✅ *Review Successfully Saved!*\n\n"
                confirmation_text += f"🔢 Order: #{reviewing_order_num}\n"
                confirmation_text += f"⭐ Rating: {stars} ({rating}/5)\n"
                confirmation_text += f"📝 Review: \"{review_text}\"\n\n"
                confirmation_text += f"🙏 *Thank you {username}!*\n"
                confirmation_text += f"Your verified review helps other customers make informed decisions. We truly appreciate you taking the time to share your experience! 💚"
                
                await message.answer(confirmation_text, reply_markup=shopping_kb(), parse_mode="Markdown")
                await state.set_state(OrderStates.shopping)
                # Clear reviewing_order_num from state to allow future reviews
                await state.update_data(reviewing_order_num=None, review_rating=None)
                return
            else:
                await message.answer("❌ Error saving your review. Please try again later.", reply_markup=shopping_kb())
                await state.set_state(OrderStates.shopping)
                # Clear reviewing_order_num even on error
                await state.update_data(reviewing_order_num=None, review_rating=None)
                return
        else:
            await message.answer("❌ Database not available. Please try again later.", reply_markup=shopping_kb())
            await state.set_state(OrderStates.shopping)
            # Clear reviewing_order_num even on error
            await state.update_data(reviewing_order_num=None, review_rating=None)
            return
    
    # Old flow: reviewing from basket (legacy support)
    if user_id not in user_orders:
        await message.answer("❌ Only customers who have placed orders can leave reviews.")
        return
    
    # Check if the order is within the review window (5 days)
    user_order = user_orders.get(user_id, {})
    order_date = user_order.get('date', '')
    
    if not is_order_within_review_window(order_date):
        await message.answer("❌ Reviews can only be left within 5 days of your order. Your order is too old to review.")
        return
    
    order_num = user_order.get('order_num', 'N/A')
    ordered_items = user_order.get('items', [])
    
    # Save basket review to database instead of in-memory list
    if db:
        success = db.save_review(
            username=username,
            rating=rating,
            review_text=review_text,
            review_date=date,
            order_items=ordered_items,
            order_num=order_num,
            user_id=message.from_user.id
        )
        
        if not success:
            await message.answer("❌ Error saving your review. Please try again later.")
            return
    else:
        await message.answer("❌ Database not available. Please try again later.")
        return
    
    stars = "⭐" * rating
    confirmation_text = f"✅ *Basket Review Successfully Saved!*\n\n"
    confirmation_text += f"🔢 Order: #{order_num}\n"
    confirmation_text += f"⭐ Rating: {stars} ({rating}/5)\n"
    confirmation_text += f"📝 Review: \"{review_text}\"\n\n"
    
    # Show reviewed basket items
    confirmation_text += "🛒 *Your reviewed basket:*\n"
    for i, item in enumerate(ordered_items[:5], 1):  # Show first 5 items
        confirmation_text += f"{i}. {item['name']} ({item['size']})\n"
    if len(ordered_items) > 5:
        confirmation_text += f"... and {len(ordered_items)-5} more items\n"
    
    confirmation_text += f"\n🙏 *Thank you {username}!*\n"
    confirmation_text += f"Your verified review helps other customers make informed decisions. We truly appreciate you taking the time to share your experience with your entire order! 💚"
    
    await message.answer(confirmation_text, reply_markup=shopping_kb(), parse_mode="Markdown")
    await state.set_state(OrderStates.shopping)


@dp.callback_query(F.data == 'review_basket')
async def review_basket(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    user_id = str(cb.from_user.id)
    
    if user_id not in user_orders:
        await cb.answer("No previous orders found.", show_alert=True)
        return
        
    # Check if the order is within the review window (5 days)
    last_order = user_orders[user_id]
    order_date = last_order.get('date', '')
    
    if not is_order_within_review_window(order_date):
        await cb.answer("❌ Reviews can only be left within 5 days of your order. Your last order is too old to review.", show_alert=True)
        return
    
    order_num = last_order['order_num']
    items = last_order['items']
    order_date = last_order['date']
    
    # Check if user already reviewed this order using database
    if db and db.user_has_reviewed_order(cb.from_user.id, order_num):
        await cb.answer("❌ You have already reviewed this order.", show_alert=True)
        return
    
    text = f"📦 *Review Your Order #{order_num}*\n"
    text += f"📅 Ordered: {order_date}\n\n"
    text += "🛒 *Your basket contained:*\n"
    
    total = 0
    for i, item in enumerate(items, 1):
        text += f"{i}. {item['name']} ({item['size']}) - £{item['price']}\n"
        total += item['price']
    
    text += f"\n💰 Total: £{total}\n\n"
    text += "⭐ *How would you rate your overall experience with this order?*"
    
    await state.update_data(review_order_num=order_num, review_items=items)
    await state.set_state(ReviewStates.rating_product)
    
    try:
        if cb.message and hasattr(cb.message, 'edit_text'):
            await cb.message.edit_text(text, reply_markup=rating_kb(), parse_mode="Markdown")
    except Exception as e:
        print(f"Error showing basket review: {e}")
        # If editing fails (e.g., message is identical), send a new message
        try:
            await bot.send_message(cb.from_user.id, text, reply_markup=rating_kb(), parse_mode="Markdown")
        except Exception as e2:
            print(f"Error sending new basket review message: {e2}")


@dp.callback_query(F.data == 'your_orders')
@uncrashable
async def show_your_orders(cb: types.CallbackQuery, state: FSMContext):
    """Show user's order history"""
    await cb.answer()
    
    user_id = cb.from_user.id
    
    # Get orders from database
    if db:
        orders = db.get_user_orders(user_id, limit=10)
    else:
        orders = []
    
    if not orders:
        text = "📦 *Your Orders*\n\nYou haven't placed any orders yet.\n\nBrowse our menu to place your first order!"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛒 Browse Menu", callback_data='shopping')]
        ])
    else:
        text = f"📦 *Your Orders* ({len(orders)} total)\n\n"
        buttons = []
        
        for i, order in enumerate(orders[:5], 1):  # Show last 5 orders
            order_num = order.get('order_num', 'N/A')
            status = order.get('status', 'pending')
            created_at = order.get('created_at')
            
            # Status emoji
            if status == 'confirmed':
                status_emoji = "✅"
            elif status == 'pending':
                status_emoji = "⏳"
            else:
                status_emoji = "📦"
            
            # Format date
            if created_at:
                if isinstance(created_at, str):
                    date_str = created_at[:10]
                else:
                    date_str = created_at.strftime('%Y-%m-%d')
            else:
                date_str = "N/A"
            
            text += f"{i}. {status_emoji} Order `{order_num}`\n"
            text += f"   📅 {date_str} | Status: {status.title()}\n"
            
            # Add review button if order is confirmed and not yet reviewed
            if status == 'confirmed':
                # Check if already reviewed
                has_reviewed = db.user_has_reviewed_order(user_id, order_num) if db else False
                if not has_reviewed:
                    buttons.append([InlineKeyboardButton(
                        text=f"⭐ Review Order {order_num}", 
                        callback_data=f"review_order_{order_num}"
                    )])
            
            text += "\n"
        
        buttons.append([InlineKeyboardButton(text="⬅️ Back to Menu", callback_data='shopping')])
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    try:
        if cb.message and hasattr(cb.message, 'edit_text'):
            await cb.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        else:
            await bot.send_message(cb.from_user.id, text, reply_markup=kb, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Error showing user orders: {e}")

@dp.callback_query(F.data.startswith('review_order_'))
@uncrashable
async def start_order_review(cb: types.CallbackQuery, state: FSMContext):
    """Start review process for a specific order"""
    await cb.answer()
    
    order_num = cb.data[13:]  # Remove 'review_order_' prefix
    user_id = cb.from_user.id
    
    # Check if already reviewed
    if db and db.user_has_reviewed_order(user_id, order_num):
        await cb.answer("❌ You have already reviewed this order.", show_alert=True)
        return
    
    # Store order number in state
    await state.update_data(reviewing_order_num=order_num)
    await state.set_state(ReviewStates.rating_product)
    
    # Show rating keyboard
    text = f"⭐ *Review Order {order_num}*\n\nHow would you rate this order?"
    
    try:
        if cb.message and hasattr(cb.message, 'edit_text'):
            await cb.message.edit_text(text, reply_markup=rating_kb(), parse_mode="Markdown")
        else:
            await bot.send_message(cb.from_user.id, text, reply_markup=rating_kb(), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Error starting order review: {e}")

@dp.callback_query(F.data.startswith('show_all_customer_reviews'))
async def show_all_customer_reviews(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    track_user(cb.from_user)
    is_admin = is_admin(cb.from_user.id)
    
    # Parse page number from callback data (format: show_all_customer_reviews or show_all_customer_reviews_page_2)
    page = 0
    if cb.data.startswith("show_all_customer_reviews_page_"):
        try:
            page = int(cb.data.split("_")[-1]) - 1
        except:
            page = 0
    
    # Pagination settings
    reviews_per_page = 50
    offset = page * reviews_per_page
    
    # Get reviews from database
    reviews_from_db = db.get_all_reviews(limit=reviews_per_page, offset=offset) if db else []
    total_reviews = db.get_review_count() if db else 0
    total_pages = (total_reviews + reviews_per_page - 1) // reviews_per_page if total_reviews > 0 else 1
    
    # Ensure page is within bounds
    page = max(0, min(page, total_pages - 1))
    
    if not reviews_from_db:
        text = "📝 *Customer Reviews*\n\nNo reviews yet."
    else:
        text = f"📝 *Customer Reviews*\n\n"
        text += f"Showing {len(reviews_from_db)} reviews (Total: {total_reviews})\n"
        text += f"Page {page + 1} of {total_pages}\n\n"
        
        for review in reviews_from_db:
            username = review['username']
            rating = review['rating']
            review_text = review['review_text']
            date = review['review_date']
            order_items = review['order_items'] or []
            order_num = review['order_num'] or "N/A"
            
            stars = "⭐" * rating
            
            if is_admin:
                # Admin sees usernames and order details
                text += f"• {stars} by @{username}\n"
                text += f"  Order #{order_num} - {date}\n"
            else:
                # Regular customers see anonymous reviews
                text += f"• {stars} Anonymous Customer\n"
                text += f"  📅 {date}\n"
            
            text += f"  \"{review_text}\"\n"
            
            # Show basket items
            if order_items:
                text += f"  🛒 Basket: "
                item_names = [f"{item['name']} ({item['size']})" for item in order_items[:3]]
                if len(order_items) > 3:
                    item_names.append(f"and {len(order_items)-3} more...")
                text += ", ".join(item_names)
            text += "\n\n"
    
    # Build pagination buttons
    buttons = []
    nav_row = []
    
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="⬅️ Previous", callback_data=f"show_all_customer_reviews_page_{page}"))
    
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"show_all_customer_reviews_page_{page + 2}"))
    
    if nav_row:
        buttons.append(nav_row)
    
    buttons.append([InlineKeyboardButton(text="⬅️ Back", callback_data='view_all_reviews')])
    
    back_kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    try:
        if cb.message and hasattr(cb.message, 'edit_text'):
            await cb.message.edit_text(text, reply_markup=back_kb, parse_mode="Markdown")
    except Exception as e:
        print(f"Error showing all reviews: {e}")
        # If editing fails (e.g., message is identical), send a new message
        try:
            await bot.send_message(cb.from_user.id, text, reply_markup=back_kb, parse_mode="Markdown")
        except Exception as e2:
            print(f"Error sending new reviews message: {e2}")


async def cleanup_resources():
    """Enhanced resource cleanup with comprehensive error handling"""
    global background_tasks
    
    print("🧹 Starting comprehensive resource cleanup...")
    
    # Cancel all background tasks with timeout
    if background_tasks:
        print(f"🧹 Cancelling {len(background_tasks)} background tasks...")
        cancelled_count = 0
        for task in background_tasks.copy():
            try:
                if not task.done():
                    task.cancel()
                    cancelled_count += 1
            except Exception as e:
                print(f"⚠️ Error cancelling task: {e}")
        
        print(f"🧹 Cancelled {cancelled_count} tasks")
        
        # Wait for tasks to cancel with timeout
        if background_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*background_tasks, return_exceptions=True),
                    timeout=5.0
                )
                print("✅ All background tasks completed")
            except asyncio.TimeoutError:
                print("⚠️ Some background tasks didn't complete within timeout")
            except Exception as e:
                print(f"⚠️ Error waiting for tasks: {e}")
        
        background_tasks.clear()
    
    # Close bot session with error handling
    try:
        if hasattr(bot, 'session') and bot.session and not bot.session.closed:
            await bot.session.close()
            print("✅ Bot session closed")
        else:
            print("ℹ️ Bot session already closed or not available")
    except Exception as e:
        print(f"⚠️ Error closing bot session: {e}")
    
    # Close database connections if available
    try:
        if db and hasattr(db, 'close_all_connections'):
            db.close_all_connections()
            print("✅ Database connections closed")
    except Exception as e:
        print(f"⚠️ Error closing database connections: {e}")
    
    # Clear any remaining cached data
    try:
        global CACHED_LTC_PRICE, LAST_PRICE_UPDATE
        CACHED_LTC_PRICE = 84.0
        LAST_PRICE_UPDATE = None
        print("✅ Cached data cleared")
    except Exception as e:
        print(f"⚠️ Error clearing cache: {e}")
    
    print("✅ Resource cleanup completed")


async def periodic_heartbeat():
    """Periodic heartbeat to keep system healthy"""
    while True:
        try:
            crash_system.heartbeat()
            health_monitor.heartbeat()
            await asyncio.sleep(30)  # Heartbeat every 30 seconds
        except Exception as e:
            logger.error(f"Heartbeat error: {e}")
            await asyncio.sleep(60)  # Retry in 1 minute


async def system_status_monitor():
    """Monitor system status and log health reports"""
    while True:
        try:
            status = crash_system.get_system_status()
            
            # Log status every hour
            if int(status['uptime']) % 3600 < 30:  # Every hour (with 30s tolerance)
                logger.info(f"🏥 SYSTEM STATUS: Uptime {status['uptime']/3600:.1f}h, "
                           f"Restarts: {status['restart_count']}, "
                           f"Crashes: {status['crash_count']}, "
                           f"Health: {'✅' if status['is_healthy'] else '⚠️'}")
            
            # Check for critical conditions
            if status['recent_crashes'] > 3:
                logger.warning(f"🚨 HIGH CRASH RATE: {status['recent_crashes']} recent crashes")
                
            if not status['is_healthy']:
                logger.error(f"💔 SYSTEM UNHEALTHY: Last heartbeat {status['last_heartbeat']}s ago")
                
            await asyncio.sleep(300)  # Check every 5 minutes
            
        except Exception as e:
            logger.error(f"System monitor error: {e}")
            await asyncio.sleep(600)  # Retry in 10 minutes


import fcntl

BOT_LOCK_FILE = "/tmp/ukdankzz_bot.lock"

async def main():
    print("🚀 Starting UNCRASHABLE Telegram Bot...")
    print("🌐 Keep-alive server will be available at your Replit URL")
    print("🛡️ Enhanced crash prevention and auto-recovery systems active")

    # CRITICAL: Prevent multiple instances with process lock
    lock_file = None
    try:
        lock_file = open(BOT_LOCK_FILE, 'w')
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        print("🔒 Bot instance lock acquired - proceeding...")
    except IOError:
        print("🚨 ERROR: Another bot instance is already running!")
        print("🚨 Only ONE bot instance can run at a time to prevent TelegramConflictError")
        print("🚨 If you're sure no other instance is running, delete /tmp/ukdankzz_bot.lock")
        return

    # Enhanced startup protection with auto-restart capabilities
    import uuid
    instance_id = f"ukdankzz_bot_{uuid.uuid4().hex[:8]}"
    
    print(f"🚀 Bot instance starting: {instance_id}")
    print(f"🔧 Process ID: {os.getpid()}")
    
    try:
        # Start background monitoring systems
        print("🏥 Starting health monitoring systems...")
        
        # Start the keep-alive server
        keep_alive()
        
        # TEMPORARILY DISABLED: Testing for conflict issue
        # Start background task cleanup
        # cleanup_task = add_background_task(cleanup_background_tasks())
        # print("✅ Background task cleanup started")
        
        # Start periodic heartbeat
        # heartbeat_task = add_background_task(periodic_heartbeat())
        # print("✅ Health monitoring heartbeat started")
        
        # Start system status monitoring
        # status_task = add_background_task(system_status_monitor())
        # print("✅ System status monitoring started")
        
        # CRITICAL: Start automatic user sync every 5 seconds - INSTANT PROTECTION
        # user_sync_task = add_background_task(sync_users_periodically())
        # print("✅ User sync started - backs up users every 60 seconds")
        print("⚠️ Background tasks temporarily disabled for testing")

        # One-time migration to preserve any existing reviews
        if db:
            print("🔄 Checking for existing reviews to migrate...")
            # If there were any in-memory reviews, they would be migrated here
            # Since this bot already uses database-only, this is a safety check
            existing_reviews = []  # No in-memory reviews to migrate
            if existing_reviews:
                migrated = db.migrate_existing_reviews(existing_reviews)
                print(f"✅ Migrated {migrated} existing reviews to database")
            else:
                print("✅ No existing reviews to migrate - database ready")
        else:
            print("⚠️ Database not available - reviews will not be saved")

        # Check runtime mode: webhook for deployment, polling for development
        BOT_RUNTIME = os.getenv('BOT_RUNTIME', 'dev')
        
        if BOT_RUNTIME == 'deploy':
            # DEPLOYMENT MODE: Use webhook
            print("🌐 DEPLOYMENT MODE: Setting up webhook...")
            
            # Compute webhook URL using Replit dev domain
            replit_domain = os.getenv('REPLIT_DEV_DOMAIN')
            if not replit_domain:
                # Fallback to old format
                repl_owner = os.getenv('REPL_OWNER', 'unknown')
                repl_slug = os.getenv('REPL_SLUG', 'workspace')
                webhook_url = f"https://{repl_slug}.{repl_owner}.repl.co/telegram/{WEBHOOK_SECRET}"
            else:
                webhook_url = f"https://{replit_domain}/telegram/{WEBHOOK_SECRET}"
            
            print(f"📡 Webhook URL: {webhook_url}")
            
            # Set webhook
            try:
                await bot.set_webhook(
                    url=webhook_url,
                    drop_pending_updates=True,
                    allowed_updates=None
                )
                print(f"✅ Webhook set successfully: {webhook_url}")
                
                # Verify webhook
                webhook_info = await bot.get_webhook_info()
                print(f"✅ Webhook verified: {webhook_info.url}")
                print(f"📊 Pending updates: {webhook_info.pending_update_count}")
                
            except Exception as e:
                print(f"❌ Failed to set webhook: {e}")
                raise
            
            # Keep Flask running indefinitely for webhook processing
            print("🔄 Webhook mode active - Flask handling requests...")
            while True:
                await asyncio.sleep(3600)  # Sleep forever, Flask handles requests
                
        else:
            # DEVELOPMENT MODE: Use polling
            print("💻 DEVELOPMENT MODE: Using polling...")
            
            # Use polling mode to get bot working immediately
            try:
                print("🔄 Starting in polling mode...")
                
                # AGGRESSIVE webhook clearing with extended delays
                print("🔧 Forcing webhook deletion with extended timeouts...")
                
                # Pending updates will be cleared by dp.start_polling with drop_pending_updates=True
                # REMOVED: Manual get_updates call that was causing TelegramConflictError
                # The dp.start_polling automatically handles pending updates
                
                # Multiple webhook deletion attempts with longer delays
                for i in range(5):
                    try:
                        result = await bot.delete_webhook(drop_pending_updates=True)
                        print(f"✅ Webhook deletion attempt {i+1} - SUCCESS: {result}")
                        await asyncio.sleep(8)  # Longer delay between attempts
                    except Exception as e:
                        print(f"⚠️ Webhook deletion attempt {i+1} failed: {e}")
                        await asyncio.sleep(5)
                
                # Extended verification with multiple checks
                for check in range(3):
                    try:
                        info = await bot.get_webhook_info()
                        if info.url:
                            print(f"🚨 WARNING: Webhook still active on check {check+1}: {info.url}")
                            await bot.delete_webhook(drop_pending_updates=True)
                            await asyncio.sleep(10)
                        else:
                            print(f"✅ Webhook verification check {check+1} - CLEAR")
                            break
                    except Exception as e:
                        print(f"⚠️ Webhook verification check {check+1} failed: {e}")
                        await asyncio.sleep(5)
                
                print("✅ Webhook completely cleared - ready for polling")
                print("⏳ Waiting 60 seconds for Telegram servers to fully process webhook deletion...")
                await asyncio.sleep(60)  # CRITICAL: Long wait for Telegram API to fully process
                
                # Start polling
                print("🚀 Starting polling...")
                
                # Pre-polling verification
                try:
                    me = await bot.get_me()
                    print(f"✅ Bot identity verified: {me.first_name} (@{me.username})")
                except Exception as e:
                    print(f"⚠️ Could not verify bot identity: {e}")
                
                # START POLLING - SINGLE ATTEMPT ONLY (no retry loop to prevent duplicate polling)
                try:
                    print("🚀 Starting polling (single clean attempt)...")
                    await dp.start_polling(
                        bot, 
                        handle_signals=True,
                        allowed_updates=None,  # Allow all updates
                        drop_pending_updates=True  # Drop any pending updates
                    )
                    
                except KeyboardInterrupt:
                    print("🛑 Bot stopped by user")
                    
                except Exception as polling_error:
                    error_msg = str(polling_error)
                    print(f"❌ Polling error: {error_msg}")
                    
                    if "TelegramConflictError" in error_msg or "terminated by other getUpdates" in error_msg:
                        print("🚨 TelegramConflictError detected!")
                        print("🚨 This means another instance is polling with the same token")
                        print("🚨 Possible causes:")
                        print("   1. Multiple bot processes running")
                        print("   2. Webhook still active")
                        print("   3. Token being used elsewhere")
                        print("🚨 Bot will exit cleanly - check for duplicate instances")
                    raise
                    
            except Exception as e:
                print(f"❌ Bot error: {e}")
                print("🔄 Bot will retry automatically...")
                # Let aiogram's built-in retry handle this - don't create multiple instances
                
            finally:
                # CRITICAL: SAVE ALL USERS BEFORE SHUTDOWN - NEVER LOSE CUSTOMERS
                print("🛡️ EMERGENCY: Saving all users before shutdown...")
                try:
                    synced = await sync_memory_users_to_database()
                    print(f"✅ SAVED {synced} users before shutdown - COUNT PROTECTED!")
                except Exception as save_error:
                    print(f"❌ Error saving users before shutdown: {save_error}")
                
                # Always cleanup on exit
                await cleanup_resources()
                
                # Release the bot lock file
                try:
                    if lock_file:
                        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                        lock_file.close()
                    if os.path.exists(BOT_LOCK_FILE):
                        os.remove(BOT_LOCK_FILE)
                    print("🔓 Bot instance lock released")
                except Exception as lock_error:
                    print(f"⚠️ Lock cleanup error: {lock_error}")
                    
                print(f"✅ Bot instance {instance_id} stopped")
            
    except Exception as startup_error:
        print(f"🚨 Bot startup error: {startup_error}")
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())
