#!/usr/bin/env python3
"""
UKDANKZZ Bot Backup System
Automatically backs up critical data and provides recovery mechanisms
"""

import json
import os
import datetime
import shutil
import logging
from typing import Dict, Any

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('backup_system.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BackupSystem:
    def __init__(self):
        self.backup_dir = "backups"
        self.ensure_backup_directory()
        
    def ensure_backup_directory(self):
        """Ensure backup directory exists"""
        if not os.path.exists(self.backup_dir):
            os.makedirs(self.backup_dir)
            logger.info(f"Created backup directory: {self.backup_dir}")
    
    def create_backup(self, data: Dict[str, Any], backup_type: str = "auto"):
        """Create a timestamped backup of critical data"""
        try:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = f"{backup_type}_backup_{timestamp}.json"
            backup_path = os.path.join(self.backup_dir, backup_filename)
            
            backup_data = {
                "timestamp": timestamp,
                "backup_type": backup_type,
                "data": data,
                "version": "1.0"
            }
            
            with open(backup_path, 'w', encoding='utf-8') as f:
                json.dump(backup_data, f, indent=2, ensure_ascii=False)
                
            logger.info(f"✅ Backup created: {backup_path}")
            self.cleanup_old_backups()
            return backup_path
            
        except Exception as e:
            logger.error(f"❌ Backup creation failed: {e}")
            return None
    
    def restore_backup(self, backup_file: str) -> Dict[str, Any]:
        """Restore data from backup file"""
        try:
            backup_path = os.path.join(self.backup_dir, backup_file)
            
            if not os.path.exists(backup_path):
                logger.error(f"❌ Backup file not found: {backup_path}")
                return {}
                
            with open(backup_path, 'r', encoding='utf-8') as f:
                backup_data = json.load(f)
                
            logger.info(f"✅ Backup restored from: {backup_path}")
            return backup_data.get("data", {})
            
        except Exception as e:
            logger.error(f"❌ Backup restoration failed: {e}")
            return {}
    
    def get_latest_backup(self, backup_type: str = None) -> str:
        """Get the most recent backup file"""
        try:
            backup_files = [f for f in os.listdir(self.backup_dir) if f.endswith('.json')]
            
            if backup_type:
                backup_files = [f for f in backup_files if f.startswith(backup_type)]
            
            if not backup_files:
                return None
                
            # Sort by creation time (newest first)
            backup_files.sort(key=lambda f: os.path.getctime(os.path.join(self.backup_dir, f)), reverse=True)
            return backup_files[0]
            
        except Exception as e:
            logger.error(f"❌ Failed to get latest backup: {e}")
            return None
    
    def cleanup_old_backups(self, keep_count: int = 10):
        """Remove old backup files, keeping only the most recent ones"""
        try:
            backup_files = [f for f in os.listdir(self.backup_dir) if f.endswith('.json')]
            
            if len(backup_files) <= keep_count:
                return
                
            # Sort by creation time (oldest first)
            backup_files.sort(key=lambda f: os.path.getctime(os.path.join(self.backup_dir, f)))
            
            # Remove oldest files
            files_to_remove = backup_files[:-keep_count]
            for file_to_remove in files_to_remove:
                file_path = os.path.join(self.backup_dir, file_to_remove)
                os.remove(file_path)
                logger.info(f"🗑️ Removed old backup: {file_to_remove}")
                
        except Exception as e:
            logger.error(f"❌ Backup cleanup failed: {e}")
    
    def backup_critical_files(self):
        """Backup critical bot files"""
        try:
            critical_files = [
                "main.py",
                "database.py",
                "pricing_engine.py"
            ]
            
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_folder = os.path.join(self.backup_dir, f"files_backup_{timestamp}")
            os.makedirs(backup_folder, exist_ok=True)
            
            for file_name in critical_files:
                if os.path.exists(file_name):
                    shutil.copy2(file_name, backup_folder)
                    logger.info(f"📄 Backed up file: {file_name}")
                    
            logger.info(f"✅ File backup completed: {backup_folder}")
            return backup_folder
            
        except Exception as e:
            logger.error(f"❌ File backup failed: {e}")
            return None

# Global backup system instance
backup_system = BackupSystem()

def emergency_backup(user_states: Dict, orders: Dict, admin_data: Dict = None):
    """Create emergency backup of all critical data"""
    emergency_data = {
        "user_states": user_states,
        "orders": orders,
        "admin_data": admin_data or {},
        "emergency": True
    }
    
    backup_path = backup_system.create_backup(emergency_data, "emergency")
    if backup_path:
        logger.critical(f"🚨 EMERGENCY BACKUP CREATED: {backup_path}")
    return backup_path

def create_scheduled_backup(user_states: Dict, orders: Dict):
    """Create scheduled backup of bot data"""
    scheduled_data = {
        "user_states": user_states,
        "orders": orders,
        "scheduled": True
    }
    
    return backup_system.create_backup(scheduled_data, "scheduled")

if __name__ == "__main__":
    # Test backup system
    test_data = {"test": "data", "timestamp": datetime.datetime.now().isoformat()}
    backup_system.create_backup(test_data, "test")
    logger.info("✅ Backup system test completed")