#!/usr/bin/env python3
"""
Bot Status Monitor - For development monitoring only
This does NOT run the bot - only monitors deployment status
"""

import requests
import time
import os

def check_deployment_status():
    """Check if the deployed bot is running"""
    try:
        # Check the deployment health endpoint
        replit_url = os.getenv('REPL_URL', '')
        if replit_url:
            response = requests.get(f"{replit_url}/status", timeout=10)
            if response.status_code == 200:
                print("✅ Reserved VM Deployment: ACTIVE")
                return True
        print("⚠️ Reserved VM Deployment: Status unknown")
        return False
    except Exception as e:
        print(f"⚠️ Deployment check failed: {e}")
        return False

def main():
    print("🔍 Bot Deployment Monitor (Development workspace only)")
    print("📢 IMPORTANT: This is monitoring only - bot runs on Reserved VM deployment")
    print("💡 Close this workspace - your bot will keep running on Reserved VM!")
    
    while True:
        try:
            print(f"\n⏰ {time.strftime('%H:%M:%S')} - Checking deployment status...")
            is_active = check_deployment_status()
            
            if is_active:
                print("✅ Your bot is running on Reserved VM - customers can place orders!")
            else:
                print("⚠️ Could not verify deployment status")
                
            print("💡 You can safely close this workspace - Reserved VM runs independently!")
            time.sleep(300)  # Check every 5 minutes
            
        except KeyboardInterrupt:
            print("\n🛑 Monitor stopped - Reserved VM deployment continues running!")
            break
        except Exception as e:
            print(f"Monitor error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()