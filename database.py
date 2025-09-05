import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

class BotDatabase:
    def __init__(self, db_path: str = "bots_database.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Инициализация базы данных"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Таблица пользователей для каждого бота
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    bot_id TEXT NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_blocked BOOLEAN DEFAULT 0,
                    is_premium BOOLEAN DEFAULT 0,
                    premium_checked_at TIMESTAMP,
                    UNIQUE(user_id, bot_id)
                )
            ''')
            
            # Добавляем колонку is_premium если её нет (для существующих БД)
            cursor.execute("PRAGMA table_info(users)")
            columns = [column[1] for column in cursor.fetchall()]
            if 'is_premium' not in columns:
                cursor.execute('ALTER TABLE users ADD COLUMN is_premium BOOLEAN DEFAULT 0')
                cursor.execute('ALTER TABLE users ADD COLUMN premium_checked_at TIMESTAMP')
                logger.info("Added premium columns to existing database")
            
            # Индексы для быстрого поиска
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_bot ON users(user_id, bot_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_bot_blocked ON users(bot_id, is_blocked)')
            
            conn.commit()
    
    def add_user(self, user_id: int, bot_id: str, username: str = None,
                 first_name: str = None, last_name: str = None, is_premium: bool = False) -> bool:
        """Добавление или обновление пользователя"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO users (user_id, bot_id, username, first_name, last_name, is_blocked, is_premium)
                    VALUES (?, ?, ?, ?, ?, 0, ?)
                ''', (user_id, bot_id, username, first_name, last_name, is_premium))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Ошибка добавления пользователя: {e}")
            return False
    
    def block_user(self, user_id: int, bot_id: str = None):
        """Пометить пользователя как заблокировавшего бота"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                if bot_id:
                    cursor.execute('''
                        UPDATE users SET is_blocked = 1 
                        WHERE user_id = ? AND bot_id = ?
                    ''', (user_id, bot_id))
                else:
                    # Блокировка для всех ботов
                    cursor.execute('''
                        UPDATE users SET is_blocked = 1 
                        WHERE user_id = ?
                    ''', (user_id,))
                conn.commit()
        except Exception as e:
            logger.error(f"Ошибка блокировки пользователя: {e}")
    
    def unblock_user(self, user_id: int, bot_id: str):
        """Разблокировать пользователя"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE users SET is_blocked = 0 
                    WHERE user_id = ? AND bot_id = ?
                ''', (user_id, bot_id))
                conn.commit()
        except Exception as e:
            logger.error(f"Ошибка разблокировки пользователя: {e}")
    
    def get_bot_users(self, bot_id: str, only_active: bool = True, only_premium: bool = False) -> List[Dict]:
        """Получить пользователей бота"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                if only_active:
                    base_query = '''
                        SELECT user_id, username, first_name, last_name, joined_at, is_premium
                        FROM users
                        WHERE bot_id = ? AND is_blocked = 0
                    '''
                else:
                    base_query = '''
                        SELECT user_id, username, first_name, last_name, joined_at, is_blocked, is_premium
                        FROM users
                        WHERE bot_id = ?
                    '''
                
                if only_premium:
                    base_query += ' AND is_premium = 1'
                
                base_query += ' ORDER BY joined_at DESC'
                
                cursor.execute(base_query, (bot_id,))
                
                columns = [col[0] for col in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Ошибка получения пользователей: {e}")
            return []
    
    def get_all_active_users(self) -> List[int]:
        """Получить всех активных пользователей (для общей рассылки)"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT DISTINCT user_id 
                    FROM users 
                    WHERE is_blocked = 0
                ''')
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Ошибка получения активных пользователей: {e}")
            return []
    
    def get_bot_stats(self, bot_id: str) -> Dict:
        """Получить статистику по боту"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Общее количество пользователей
                cursor.execute('''
                    SELECT COUNT(*) FROM users WHERE bot_id = ?
                ''', (bot_id,))
                total = cursor.fetchone()[0]
                
                # Активные пользователи
                cursor.execute('''
                    SELECT COUNT(*) FROM users WHERE bot_id = ? AND is_blocked = 0
                ''', (bot_id,))
                active = cursor.fetchone()[0]
                
                # Заблокировавшие
                cursor.execute('''
                    SELECT COUNT(*) FROM users WHERE bot_id = ? AND is_blocked = 1
                ''', (bot_id,))
                blocked = cursor.fetchone()[0]
                
                # С username
                cursor.execute('''
                    SELECT COUNT(*) FROM users WHERE bot_id = ? AND username IS NOT NULL
                ''', (bot_id,))
                with_username = cursor.fetchone()[0]
                
                # Премиум пользователи
                cursor.execute('''
                    SELECT COUNT(*) FROM users WHERE bot_id = ? AND is_premium = 1 AND is_blocked = 0
                ''', (bot_id,))
                premium = cursor.fetchone()[0]
                
                return {
                    'total': total,
                    'active': active,
                    'blocked': blocked,
                    'with_username': with_username,
                    'premium': premium
                }
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {'total': 0, 'active': 0, 'blocked': 0, 'with_username': 0, 'premium': 0}
    
    def get_global_stats(self) -> Dict:
        """Получить общую статистику по всем ботам"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Уникальные пользователи
                cursor.execute('SELECT COUNT(DISTINCT user_id) FROM users')
                unique_users = cursor.fetchone()[0]
                
                # Активные уникальные пользователи
                cursor.execute('SELECT COUNT(DISTINCT user_id) FROM users WHERE is_blocked = 0')
                active_users = cursor.fetchone()[0]
                
                # Премиум пользователи (уникальные)
                cursor.execute('SELECT COUNT(DISTINCT user_id) FROM users WHERE is_premium = 1 AND is_blocked = 0')
                premium_users = cursor.fetchone()[0]
                
                # Статистика по ботам
                cursor.execute('''
                    SELECT bot_id, COUNT(*) as count
                    FROM users
                    WHERE is_blocked = 0
                    GROUP BY bot_id
                ''')
                bot_stats = dict(cursor.fetchall())
                
                return {
                    'unique_users': unique_users,
                    'active_users': active_users,
                    'premium_users': premium_users,
                    'bot_stats': bot_stats
                }
        except Exception as e:
            logger.error(f"Ошибка получения глобальной статистики: {e}")
            return {'unique_users': 0, 'active_users': 0, 'premium_users': 0, 'bot_stats': {}}
    
    def migrate_from_json(self, users_json: Dict):
        """Миграция данных из старого JSON формата"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                for user_id, user_data in users_json.items():
                    cursor.execute('''
                        INSERT OR IGNORE INTO users (user_id, bot_id, username, first_name, last_name)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        int(user_id),
                        'main',  # Старые пользователи относятся к главному боту
                        user_data.get('username'),
                        user_data.get('first_name'),
                        user_data.get('last_name')
                    ))
                conn.commit()
                logger.info(f"Мигрировано {len(users_json)} пользователей")
        except Exception as e:
            logger.error(f"Ошибка миграции: {e}")
    
    def update_user_premium(self, user_id: int, bot_id: str, is_premium: bool) -> bool:
        """Обновить премиум статус пользователя"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE users
                    SET is_premium = ?, premium_checked_at = CURRENT_TIMESTAMP
                    WHERE user_id = ? AND bot_id = ?
                ''', (is_premium, user_id, bot_id))
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Ошибка обновления премиум статуса: {e}")
            return False
    
    def get_users_for_premium_check(self, bot_id: str = None, limit: int = 100) -> List[Dict]:
        """Получить пользователей для проверки премиум статуса"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                if bot_id:
                    # Пользователи конкретного бота, которых давно не проверяли
                    cursor.execute('''
                        SELECT user_id, bot_id, username
                        FROM users
                        WHERE bot_id = ? AND is_blocked = 0
                        AND (premium_checked_at IS NULL OR
                             datetime(premium_checked_at) < datetime('now', '-1 day'))
                        ORDER BY premium_checked_at ASC NULLS FIRST
                        LIMIT ?
                    ''', (bot_id, limit))
                else:
                    # Все пользователи, которых давно не проверяли
                    cursor.execute('''
                        SELECT user_id, bot_id, username
                        FROM users
                        WHERE is_blocked = 0
                        AND (premium_checked_at IS NULL OR
                             datetime(premium_checked_at) < datetime('now', '-1 day'))
                        ORDER BY premium_checked_at ASC NULLS FIRST
                        LIMIT ?
                    ''', (limit,))
                
                columns = [col[0] for col in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Ошибка получения пользователей для проверки: {e}")
            return []