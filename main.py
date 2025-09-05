import logging
import json
import os
import asyncio
import threading
import signal
import sys
import re
import io
import time
from datetime import datetime, timedelta
from typing import Dict, List
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot, InputFile
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from telegram.constants import ParseMode
from telegram.error import Forbidden, BadRequest
from database import BotDatabase
from broadcast_workers import OptimizedBroadcast

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# Конфигурация
MAIN_BOT_TOKEN = "YOUR_MAIN_BOT_TOKEN_HERE"  # Токен основного бота
ADMIN_ID = 123456789  # Главный админ, который получает все сообщения

# Файл для хранения данных
DATA_FILE = "bots_data.json"
ADMINS_FILE = "admins.json"
USERS_FILE = "users.json"  # Новый файл для хранения пользователей

# Словарь для хранения запущенных ботов и их потоков
# ВАЖНО: Используем уникальное имя, чтобы избежать конфликтов
active_bot_instances = {}  # Изменено имя для избежания конфликтов
bot_threads = {}

# Словарь для хранения связи между пересланными сообщениями и пользователями
# Формат: {message_id: {'user_id': user_id, 'bot_id': bot_id}}
# Добавляем ограничение на размер для предотвращения утечек памяти
forwarded_messages = {}
MAX_FORWARDED_MESSAGES = 1000  # Максимальное количество сохраненных сообщений

class BotManager:
    def __init__(self):
        self.ensure_files_exist()
        self.data = self.load_data()
        self.clean_invalid_bots()  # Очищаем некорректные записи
        self.admins = self.load_admins()
        self.users = self.load_users()
    
    def clean_invalid_bots(self):
        """Удаление некорректных записей ботов"""
        invalid_bots = []
        for bot_id, bot_data in self.data.get("bots", {}).items():
            # Проверяем некорректные ID
            if bot_id == "bot" or not bot_id:
                invalid_bots.append(bot_id)
                logger.warning(f"Обнаружен некорректный бот с ID '{bot_id}', удаляю...")
                continue
            
            # Проверяем, что у бота есть необходимые поля (кроме main)
            if bot_id != "main":
                if not bot_data.get("token"):
                    invalid_bots.append(bot_id)
                    logger.warning(f"Бот {bot_id} не имеет токена, удаляю...")
        
        # Удаляем некорректные записи
        for bot_id in invalid_bots:
            if bot_id in self.data["bots"]:
                del self.data["bots"][bot_id]
                logger.info(f"Удален некорректный бот: {bot_id}")
        
        # Сохраняем очищенные данные
        if invalid_bots:
            self.save_data()
            logger.info(f"Очищено {len(invalid_bots)} некорректных записей ботов")
    
    def ensure_files_exist(self):
        """Создание необходимых файлов при первом запуске"""
        # Создаем файл данных ботов если его нет
        if not os.path.exists(DATA_FILE):
            default_data = {
                "bots": {
                    "main": {
                        "name": "Основной бот (управление)",
                        "token": MAIN_BOT_TOKEN,
                        "status": "running",
                        "start_text": """🌸 *Добро пожаловать в наш магазин букетов!* 🌸

📂 *Каталоги букетов:*
• [До 6000 рублей](https://t.me/buketi_do_6000)
• [До 10000 рублей](https://t.me/buketi_do_10000)
• [До 15000 рублей](https://t.me/buketi_do_15000)

💬 Для заказа свяжитесь с нами или выберите подходящий каталог!

_Красивые букеты для особенных моментов_ ✨"""
                    }
                }
            }
            try:
                with open(DATA_FILE, 'w', encoding='utf-8') as f:
                    json.dump(default_data, f, ensure_ascii=False, indent=2)
                logger.info(f"Создан файл {DATA_FILE} с настройками по умолчанию")
            except Exception as e:
                logger.error(f"Ошибка создания {DATA_FILE}: {e}")
        
        # Создаем файл админов если его нет
        if not os.path.exists(ADMINS_FILE):
            try:
                with open(ADMINS_FILE, 'w') as f:
                    json.dump([ADMIN_ID], f)
                logger.info(f"Создан файл {ADMINS_FILE} с главным админом")
            except Exception as e:
                logger.error(f"Ошибка создания {ADMINS_FILE}: {e}")
        
        # Создаем файл запущенных ботов если его нет
        running_bots_file = "running_bots.json"
        if not os.path.exists(running_bots_file):
            try:
                with open(running_bots_file, 'w') as f:
                    json.dump([], f)
                logger.info(f"Создан файл {running_bots_file}")
            except Exception as e:
                logger.error(f"Ошибка создания {running_bots_file}: {e}")
        
        # Создаем файл пользователей если его нет
        if not os.path.exists(USERS_FILE):
            try:
                with open(USERS_FILE, 'w', encoding='utf-8') as f:
                    json.dump({}, f)
                logger.info(f"Создан файл {USERS_FILE}")
            except Exception as e:
                logger.error(f"Ошибка создания {USERS_FILE}: {e}")
    
    def load_data(self) -> Dict:
        """Загрузка данных из файла"""
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Ошибка загрузки данных: {e}")
        
        # Данные по умолчанию
        return {
            "bots": {
                "main": {
                    "name": "Основной бот (управление)",
                    "token": MAIN_BOT_TOKEN,
                    "status": "running",
                    "start_text": """🌸 *Добро пожаловать в наш магазин букетов!* 🌸

📂 *Каталоги букетов:*
• [До 6000 рублей](https://t.me/buketi_do_6000)
• [До 10000 рублей](https://t.me/buketi_do_10000)
• [До 15000 рублей](https://t.me/buketi_do_15000)

💬 Для заказа свяжитесь с нами или выберите подходящий каталог!

_Красивые букеты для особенных моментов_ ✨"""
                }
            }
        }
    
    def load_admins(self) -> List[int]:
        """Загрузка списка админов"""
        if os.path.exists(ADMINS_FILE):
            try:
                with open(ADMINS_FILE, 'r') as f:
                    admins = json.load(f)
                    logger.info(f"Загружены админы: {admins}")
                    return admins
            except Exception as e:
                logger.error(f"Ошибка загрузки админов: {e}")
        logger.info(f"Используется админ по умолчанию: {ADMIN_ID}")
        return [ADMIN_ID]  # По умолчанию только главный админ
    
    def load_users(self) -> Dict:
        """Загрузка списка пользователей"""
        if os.path.exists(USERS_FILE):
            try:
                with open(USERS_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Ошибка загрузки пользователей: {e}")
        return {}
    
    def save_users(self):
        """Сохранение списка пользователей"""
        try:
            with open(USERS_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.users, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения пользователей: {e}")
    
    def add_user(self, user_id: int, username: str = None, first_name: str = None, last_name: str = None):
        """Добавление нового пользователя (для совместимости)"""
        # Теперь используем базу данных
        return db.add_user(user_id, 'main', username, first_name, last_name)
    
    def get_all_users(self) -> List[str]:
        """Получение списка всех пользователей (для совместимости)"""
        # Возвращаем как строки для совместимости
        return [str(uid) for uid in db.get_all_active_users()]
    
    def save_admins(self):
        """Сохранение списка админов"""
        try:
            with open(ADMINS_FILE, 'w') as f:
                json.dump(self.admins, f)
        except Exception as e:
            logger.error(f"Ошибка сохранения админов: {e}")
    
    def add_admin(self, user_id: int) -> bool:
        """Добавление нового админа"""
        if user_id not in self.admins:
            self.admins.append(user_id)
            self.save_admins()
            return True
        return False
    
    def remove_admin(self, user_id: int) -> bool:
        """Удаление админа"""
        if user_id in self.admins and user_id != ADMIN_ID:
            self.admins.remove(user_id)
            self.save_admins()
            return True
        return False
    
    def get_admins_list(self) -> List[int]:
        """Получение списка админов"""
        return self.admins
    
    def save_data(self):
        """Сохранение данных в файл"""
        try:
            with open(DATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения данных: {e}")
    
    def get_bot_text(self, bot_id: str) -> str:
        """Получение текста для бота"""
        return self.data["bots"].get(bot_id, {}).get("start_text", "Текст не найден")
    
    def set_bot_text(self, bot_id: str, text: str):
        """Установка текста для бота"""
        # Защита от некорректных bot_id
        if not bot_id or bot_id == "bot":
            logger.error(f"Попытка установить текст для некорректного bot_id: '{bot_id}'")
            return
        
        if bot_id not in self.data["bots"]:
            logger.warning(f"Бот {bot_id} не существует, создаю новую запись")
            self.data["bots"][bot_id] = {"name": f"Бот {bot_id}"}
        self.data["bots"][bot_id]["start_text"] = text
        self.save_data()
    
    def add_bot(self, bot_id: str, name: str, token: str, username: str = None):
        """Добавление нового бота"""
        self.data["bots"][bot_id] = {
            "name": name,
            "token": token,
            "username": username,
            "status": "stopped",
            "start_text": f"🤖 *{name}*\n\nДобро пожаловать! Текст еще не настроен администратором."
        }
        self.save_data()
    
    def delete_bot(self, bot_id: str):
        """Удаление бота"""
        if bot_id in self.data["bots"] and bot_id != "main":
            del self.data["bots"][bot_id]
            self.save_data()
            return True
        return False
    
    def get_bots_list(self) -> List[tuple]:
        """Получение списка ботов"""
        return [(bot_id, bot_data["name"], bot_data.get("status", "stopped")) 
                for bot_id, bot_data in self.data["bots"].items()]
    
    def get_bot_token(self, bot_id: str) -> str:
        logger.info(f"\n\n\n\n\n\n\n\nget_bot_token: bot_id={bot_id}, available_bots={list(self.data['bots'].keys())}")
        return self.data["bots"].get(bot_id, {}).get("token", "")
        
    def set_bot_status(self, bot_id: str, status: str):
        """Установка статуса бота"""
        if bot_id in self.data["bots"]:
            self.data["bots"][bot_id]["status"] = status
            self.save_data()

# Инициализация менеджера и базы данных
bot_manager = BotManager()
db = BotDatabase()

# Выводим информацию о загруженных админах
logger.info(f"Система инициализирована. Админы: {bot_manager.admins}")

# Миграция старых данных при первом запуске
if os.path.exists(USERS_FILE):
    try:
        with open(USERS_FILE, 'r', encoding='utf-8') as f:
            old_users = json.load(f)
            if old_users:
                db.migrate_from_json(old_users)
                logger.info("Данные пользователей мигрированы в базу данных")
    except Exception as e:
        logger.error(f"Ошибка миграции данных: {e}")

def escape_markdown(text: str) -> str:
    """Экранирует специальные символы для безопасного отображения в Markdown"""
    if not text:
        return text
    # Список символов, которые нужно экранировать в Markdown V2
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special_chars:
        text = text.replace(char, '\\' + char)
    return text

def markdown_to_html(text: str) -> str:
    """Конвертация Markdown в HTML для Telegram"""
    # Словарь для хранения временных замен
    replacements = {}
    replacement_counter = 0
    
    def create_placeholder():
        nonlocal replacement_counter
        placeholder = f"§§§PLACEHOLDER{replacement_counter}§§§"
        replacement_counter += 1
        return placeholder
    
    # 1. Сохраняем блоки кода (тройные обратные кавычки)
    code_blocks = re.findall(r'```([^`]+)```', text)
    for code in code_blocks:
        placeholder = create_placeholder()
        replacements[placeholder] = f'<pre>{code}</pre>'
        text = text.replace(f'```{code}```', placeholder, 1)
    
    # 2. Сохраняем инлайн код (одинарные обратные кавычки)
    inline_codes = re.findall(r'`([^`]+)`', text)
    for code in inline_codes:
        placeholder = create_placeholder()
        replacements[placeholder] = f'<code>{code}</code>'
        text = text.replace(f'`{code}`', placeholder, 1)
    
    # 3. Сохраняем и обрабатываем ссылки
    link_pattern = r'\[([^\]]+)\]\(([^\)]+)\)'
    
    def process_link(match):
        link_text = match.group(1)
        url = match.group(2)
        
        # Обрабатываем форматирование внутри текста ссылки
        link_text_html = link_text
        link_text_html = re.sub(r'\*\*([^\*]+)\*\*', r'<b>\1</b>', link_text_html)
        link_text_html = re.sub(r'\*([^\*]+)\*', r'<b>\1</b>', link_text_html)
        link_text_html = re.sub(r'_([^_]+)_', r'<i>\1</i>', link_text_html)
        
        # Добавляем протокол если его нет
        if not url.startswith(('http://', 'https://', 'tg://', 'tme://')):
            if url.startswith('www.'):
                url = 'https://' + url
            elif not url.startswith('/'):
                url = 'https://' + url
        
        placeholder = create_placeholder()
        replacements[placeholder] = f'<a href="{url}">{link_text_html}</a>'
        return placeholder
    
    text = re.sub(link_pattern, process_link, text)
    
    # 4. Обрабатываем форматирование текста (порядок важен!)
    
    # Подчеркнутый текст (двойное подчеркивание)
    text = re.sub(r'__([^_]+)__', r'<u>\1</u>', text)
    
    # Жирный текст с ** (для вложенности)
    text = re.sub(r'\*\*([^\*]+)\*\*', r'<b>\1</b>', text)
    
    # Жирный текст с *
    text = re.sub(r'\*([^\*]+)\*', r'<b>\1</b>', text)
    
    # Курсив
    text = re.sub(r'_([^_]+)_', r'<i>\1</i>', text)
    
    # Зачеркнутый текст
    text = re.sub(r'~([^~]+)~', r'<s>\1</s>', text)
    
    # 5. Восстанавливаем все плейсхолдеры
    for placeholder, replacement in replacements.items():
        text = text.replace(placeholder, replacement)
    
    return text

def is_admin(user_id: int) -> bool:
    """Проверка на админа"""
    return user_id in bot_manager.admins

def save_running_bots():
    """Сохранение списка запущенных ботов"""
    running_bots_file = "running_bots.json"
    
    try:
        # Используем правильное имя переменной
        running_bot_ids = list(active_bot_instances.keys())
        with open(running_bots_file, 'w') as f:
            json.dump(running_bot_ids, f)
        logger.info(f"Сохранены запущенные боты: {running_bot_ids}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении списка запущенных ботов: {e}")

# Функции для создания дочерних ботов
async def create_child_bot_start_handler(bot_id: str):
    """Создание обработчика /start для дочернего бота"""
    async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        
        # Добавляем пользователя в базу для конкретного бота
        try:
            db.add_user(
                user.id,
                bot_id,  # Используем ID конкретного бота, а не 'main'
                username=user.username,
                first_name=user.first_name,
                last_name=user.last_name
            )
        except Exception as e:
            logger.error(f"Ошибка добавления пользователя в БД для бота {bot_id}: {e}")
        
        start_text = bot_manager.get_bot_text(bot_id)
        
        # Сначала пробуем конвертировать Markdown в HTML
        try:
            html_text = markdown_to_html(start_text)
            await update.message.reply_text(
                html_text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.warning(f"Ошибка отправки с HTML для бота {bot_id}: {e}")
            try:
                # Если не получилось, пробуем оригинальный Markdown
                await update.message.reply_text(
                    start_text,
                    parse_mode=ParseMode.MARKDOWN_V2,
                    disable_web_page_preview=True
                )
            except Exception as e2:
                logger.warning(f"Ошибка отправки с Markdown для бота {bot_id}: {e2}")
                try:
                    # Пробуем старый Markdown
                    await update.message.reply_text(
                        start_text,
                        parse_mode=ParseMode.MARKDOWN,
                        disable_web_page_preview=True
                    )
                except Exception as e3:
                    logger.warning(f"Ошибка отправки с старым Markdown для бота {bot_id}: {e3}")
                    # Если все не работает, отправляем без форматирования
                    await update.message.reply_text(
                        start_text,
                        disable_web_page_preview=True
                    )
    return start_handler

async def create_child_bot_message_handler(bot_id: str):
    """Создание обработчика сообщений для дочернего бота"""
    async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        message = update.message
        
        # Обработка фото для рассылки
        if message.photo and is_admin(user.id) and context.user_data.get('broadcast_step') == 'text' and context.user_data.get('broadcast_bot_id') == bot_id:
            photo = message.photo[-1]  # Берем фото в максимальном качестве
            caption = message.caption or ""
            
            # Сохраняем фото и подпись для рассылки
            context.user_data['broadcast_photo'] = photo.file_id
            context.user_data['broadcast_text'] = caption
            context.user_data['broadcast_type'] = 'photo'
            context.user_data['broadcast_step'] = 'confirm'
            
            # Получаем количество пользователей для этого бота
            bot_stats = db.get_bot_stats(bot_id)
            users_count = bot_stats['active']
            
            # Получаем информацию о боте
            bot_data = bot_manager.data["bots"].get(bot_id, {})
            bot_name = bot_data.get("name", "Неизвестный бот")
            
            await message.reply_photo(
                photo=photo.file_id,
                caption=f"📢 *Подтверждение рассылки с фото*\n\n"
                       f"🤖 *От бота:* {bot_name}\n"
                       f"👥 *Получателей:* {users_count} пользователей\n\n"
                       f"📝 *Подпись к фото:*\n{caption if caption else '(без подписи)'}\n\n"
                       f"Отправить рассылку? Напишите:\n"
                       f"/send - для отправки\n"
                       f"/cancel - для отмены",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Обработка текста для рассылки от админа
        if is_admin(user.id) and context.user_data.get('broadcast_step') == 'text' and context.user_data.get('broadcast_bot_id') == bot_id:
            broadcast_text = message.text
            context.user_data['broadcast_text'] = broadcast_text
            context.user_data['broadcast_type'] = 'text'  # Указываем тип рассылки
            context.user_data['broadcast_step'] = 'confirm'
            
            # Получаем количество пользователей для этого бота
            bot_stats = db.get_bot_stats(bot_id)
            users_count = bot_stats['active']
            
            # Экранируем текст рассылки для безопасного отображения
            escaped_text = escape_markdown(broadcast_text)
            await message.reply_text(
                f"📢 *Подтверждение рассылки*\n\n"
                f"👥 Получателей: *{users_count}* пользователей\n\n"
                f"📝 *Текст рассылки:*\n{escaped_text}\n\n"
                f"Отправить рассылку? Напишите:\n"
                f"/send - для отправки\n"
                f"/cancel - для отмены",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Обработка команд
        if message.text and message.text.startswith('/'):
            # Команда /broadcast для админов
            if message.text == '/broadcast' and is_admin(user.id):
                # Начинаем процесс рассылки для этого бота
                context.user_data['broadcast_bot_id'] = bot_id
                context.user_data['broadcast_step'] = 'text'
                
                bot_data = bot_manager.data["bots"].get(bot_id, {})
                bot_name = bot_data.get("name", "Неизвестный бот")
                
                # Получаем количество пользователей для этого бота
                bot_stats = db.get_bot_stats(bot_id)
                users_count = bot_stats['active']
                
                await message.reply_text(
                    f"📢 *Рассылка от бота: {bot_name}*\n\n"
                    f"👥 Получателей: *{users_count}* пользователей\n\n"
                    f"📤 Отправьте контент для рассылки:\n"
                    f"• 📝 Текст - обычное текстовое сообщение\n"
                    f"• 🖼 Фото с подписью - перетащите фото и добавьте текст\n\n"
                    f"Для отмены напишите /cancel",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            # Команда /cancel
            elif message.text == '/cancel' and context.user_data.get('broadcast_step'):
                # Проверяем, что отмена относится к этому боту
                if context.user_data.get('broadcast_bot_id') == bot_id:
                    context.user_data.clear()
                    await message.reply_text("❌ Рассылка отменена")
                return
            # Команда /send для подтверждения
            elif message.text == '/send' and context.user_data.get('broadcast_step') == 'confirm' and context.user_data.get('broadcast_bot_id') == bot_id:
                broadcast_text = context.user_data.get('broadcast_text')
                broadcast_type = context.user_data.get('broadcast_type', 'text')
                broadcast_photo = context.user_data.get('broadcast_photo')
                
                # Получаем пользователей конкретного бота
                users = db.get_bot_users(bot_id, only_active=True)
                user_ids = [user['user_id'] if isinstance(user, dict) else user for user in users]
                
                await message.reply_text(
                    f"📤 *Рассылка запущена*\n\n"
                    f"👥 Отправка {len(users)} пользователям...\n"
                    f"⚡ Используется ускоренная рассылка с воркерами",
                    parse_mode=ParseMode.MARKDOWN
                )
                
                # Создаем объект Bot для дочернего бота
                child_bot_token = bot_manager.get_bot_token(bot_id)
                
                # Подготавливаем фото если есть
                photo_bytes = None
                if broadcast_type == 'photo':
                    try:
                        logger.info(f"Скачиваю фото для рассылки в боте {bot_id}")
                        file = await context.bot.get_file(broadcast_photo)
                        photo_bytes = await file.download_as_bytearray()
                        logger.info(f"Фото успешно подготовлено для бота {bot_id}")
                    except Exception as e:
                        logger.error(f"Ошибка скачивания фото для рассылки в боте {bot_id}: {e}")
                        await message.reply_text(f"❌ Ошибка подготовки фото для рассылки: {e}")
                        context.user_data.clear()
                        return
                
                # Используем оптимизированную рассылку с воркерами
                start_time = datetime.now()
                
                # Callback для отображения прогресса
                async def progress_callback(current, total):
                    if current % 50 == 0:  # Обновляем каждые 50 сообщений
                        percent = (current / total * 100) if total > 0 else 0
                        logger.info(f"Прогресс рассылки: {current}/{total} ({percent:.1f}%)")
                
                # Запускаем рассылку с воркерами (синхронно для дочерних ботов в команде /send)
                result = await OptimizedBroadcast.send_broadcast(
                    bot_token=child_bot_token,
                    users=user_ids,
                    text=broadcast_text if broadcast_type == 'text' else None,
                    photo=photo_bytes if broadcast_type == 'photo' else None,
                    photo_caption=broadcast_text if broadcast_type == 'photo' else None,
                    parse_mode=ParseMode.HTML,
                    progress_callback=progress_callback,
                    auto_optimize=True,  # Автоматически выбираем количество воркеров
                    template_chat_id=ADMIN_ID  # Используем ID админа для шаблона
                )
                
                # Обновляем заблокированных пользователей в БД
                for res in result['results']:
                    if res.is_blocked:
                        db.block_user(res.user_id, bot_id)
                
                context.user_data.clear()
                
                # Формируем статистику
                percent = (result['success'] / result['total'] * 100) if result['total'] > 0 else 0
                speedup = result['workers_used']
                time_saved = result['total_time'] * (speedup - 1) / speedup  # Примерное время сэкономленное
                
                await message.reply_text(
                    f"✅ *Рассылка завершена!*\n\n"
                    f"📊 *Статистика:*\n"
                    f"✅ Успешно: *{result['success']}*\n"
                    f"❌ Ошибок: *{result['failed']}*\n"
                    f"🚫 Заблокировали: *{result['blocked']}*\n"
                    f"📈 Всего: *{result['total']}*\n\n"
                    f"⏱ Время: *{result['total_time']:.1f}* сек\n"
                    f"⚡ Ускорение: *{speedup}x* (воркеров: {result['workers_used']})\n"
                    f"💾 Сэкономлено: ~*{time_saved:.1f}* сек\n"
                    f"📨 Скорость: *{result['messages_per_second']:.1f}* сообщ/сек\n\n"
                    f"Процент доставки: *{percent:.1f}%*",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
            # Игнорируем остальные команды
            else:
                return
        
        # Обработка текста для рассылки от админа
        if is_admin(user.id) and context.user_data.get('broadcast_step') == 'text' and context.user_data.get('broadcast_bot_id') == bot_id:
            broadcast_text = message.text
            context.user_data['broadcast_text'] = broadcast_text
            context.user_data['broadcast_step'] = 'confirm'
            
            # Получаем количество пользователей для этого бота
            bot_stats = db.get_bot_stats(bot_id)
            users_count = bot_stats['active']
            
            # Экранируем текст рассылки для безопасного отображения
            escaped_text = escape_markdown(broadcast_text)
            await message.reply_text(
                f"📢 *Подтверждение рассылки*\n\n"
                f"📊 Получателей: *{users_count}* пользователей\n\n"
                f"📝 *Текст рассылки:*\n{escaped_text}\n\n"
                f"Отправить рассылку? Напишите:\n"
                f"/send - для отправки\n"
                f"/cancel - для отмены",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Проверяем, является ли это ответом ГЛАВНОГО админа на пересланное сообщение
        if user.id == ADMIN_ID and message.reply_to_message:
            reply_to_id = message.reply_to_message.message_id
            
            # Проверяем, есть ли информация о пересланном сообщении
            if reply_to_id in forwarded_messages:
                forward_info = forwarded_messages[reply_to_id]
                target_user_id = forward_info['user_id']
                target_bot_id = forward_info['bot_id']
                
                try:
                    # Отправляем ответ пользователю от имени текущего бота
                    await context.bot.send_message(
                        chat_id=target_user_id,
                        text=f"💬 *Ответ от администратора:*\n\n{message.text}",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    
                    # Подтверждаем главному админу, что сообщение отправлено
                    await message.reply_text(
                        f"✅ Ответ отправлен пользователю (ID: `{target_user_id}`)",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    
                    logger.info(f"Главный админ {user.id} ответил пользователю {target_user_id} через бота {bot_id}")
                    return
                    
                except Exception as e:
                    logger.error(f"Ошибка отправки ответа пользователю: {e}")
                    await message.reply_text(
                        f"❌ Ошибка отправки ответа: {e}",
                        parse_mode=ParseMode.MARKDOWN
                    )
                    return
        
        # Если это другой админ пытается ответить в дочернем боте
        elif is_admin(user.id) and user.id != ADMIN_ID and message.reply_to_message:
            reply_to_id = message.reply_to_message.message_id
            if reply_to_id in forwarded_messages:
                await message.reply_text(
                    "⛔ Только главный администратор может отвечать на сообщения пользователей",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
        
        # Если это обычное сообщение от админа, не пересылаем его
        if is_admin(user.id):
            return
        
        # Пересылаем сообщение админам (настоящая пересылка)
        bot_data = bot_manager.data["bots"].get(bot_id, {})
        bot_name = bot_data.get("name", "Неизвестный бот")
        
        # Пересылаем сообщение ТОЛЬКО ГЛАВНОМУ АДМИНУ
        logger.info(f"Пересылка сообщения от пользователя {user.id} главному админу: {ADMIN_ID}")
        
        # Формируем информацию о пользователе
        user_name = f"{user.first_name or ''} {user.last_name or ''}".strip() or "Без имени"
        user_username = user.username or "нет"
        
        # НЕ экранируем, так как отправляем БЕЗ parse_mode
        # Создаем текст сообщения
        info_text = f"📨 Новое сообщение в боте: {bot_name}\n"
        info_text += f"Bot ID: {bot_id}\n"
        info_text += f"От: {user_name}\n"
        info_text += f"Username: @{user_username}\n" if user.username else f"Username: {user_username}\n"
        info_text += f"User ID: {user.id}"
        
        # Отправляем ТОЛЬКО главному админу БЕЗ форматирования
        try:
            # Отправляем информационное сообщение без parse_mode
            info_msg = await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=info_text
            )
            logger.info(f"✅ Информация отправлена главному админу {ADMIN_ID}")
            
            # Пересылаем оригинальное сообщение
            forwarded_msg = await context.bot.forward_message(
                chat_id=ADMIN_ID,
                from_chat_id=message.chat_id,
                message_id=message.message_id
            )
            
            # Сохраняем связь с временной меткой для автоочистки
            # Проверяем размер словаря для предотвращения утечек памяти
            if len(forwarded_messages) > MAX_FORWARDED_MESSAGES:
                # Удаляем самые старые сообщения
                oldest_keys = sorted(forwarded_messages.keys())[:100]
                for key in oldest_keys:
                    del forwarded_messages[key]
                logger.info(f"Очищено {len(oldest_keys)} старых пересланных сообщений")
            
            forwarded_messages[forwarded_msg.message_id] = {
                'user_id': user.id,
                'bot_id': bot_id,
                'original_message_id': message.message_id,
                'timestamp': datetime.now()  # Добавляем временную метку
            }
            logger.info(f"✅ Сообщение переслано главному админу, ID: {forwarded_msg.message_id}")
            
        except Exception as error:
            logger.error(f"❌ Ошибка отправки главному админу {ADMIN_ID}: {error}")
        
        # Отправляем подтверждение пользователю
        await message.reply_text(
            "Ваше сообщение отправлено администратору! Ожидайте ответа!",
            parse_mode=ParseMode.MARKDOWN
        )
    
    return message_handler

async def create_child_bot_help_handler(bot_id: str):
    """Создание обработчика /help для дочернего бота"""
    async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        bot_name = bot_manager.data["bots"][bot_id]["name"]
        help_text = f"🤖 *{bot_name}*\n\nКоманды:\n• `/start` - Главное меню\n• `/help` - Эта справка"
        
        # Добавляем команду broadcast для админов
        if is_admin(user.id):
            help_text += "\n\n*Команды админа:*\n• `/broadcast` - Сделать рассылку"
        
        await update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)
    return help_handler

async def create_child_bot_broadcast_handler(bot_id: str):
    """Создание обработчика /broadcast для дочернего бота"""
    async def broadcast_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        
        if not is_admin(user.id):
            await update.message.reply_text("⛔ У вас нет доступа к этой команде")
            return
        
        # Начинаем процесс рассылки для этого бота
        context.user_data['broadcast_bot_id'] = bot_id
        context.user_data['broadcast_step'] = 'text'
        
        bot_data = bot_manager.data["bots"].get(bot_id, {})
        bot_name = bot_data.get("name", "Неизвестный бот")
        
        # Получаем количество пользователей для этого бота
        bot_stats = db.get_bot_stats(bot_id)
        users_count = bot_stats['active']
        
        await update.message.reply_text(
            f"📢 *Рассылка от бота: {bot_name}*\n\n"
            f"👥 Получателей: *{users_count}* пользователей\n\n"
            f"📤 Отправьте контент для рассылки:\n"
            f"• 📝 Текст - обычное текстовое сообщение\n"
            f"• 🖼 Фото с подписью - перетащите фото и добавьте текст\n\n"
            f"Для отмены напишите /cancel",
            parse_mode=ParseMode.MARKDOWN
        )
    
    return broadcast_handler

async def start_bot(bot_id: str, token: str):
    """Запуск дочернего бота"""
    try:
        # Используем правильное имя переменной
        if bot_id in active_bot_instances:
            logger.info(f"Бот {bot_id} уже запущен в этой сессии")
            return True
        
        logger.info(f"Начинаю запуск бота {bot_id}")
        
        # ВАЖНО: Добавляем большую задержку перед запуском для избежания конфликтов
        await asyncio.sleep(5)
        
        # Первая попытка - с очисткой webhook
        webhook_cleared = False
        try:
            logger.info(f"Попытка 1: Проверка бота {bot_id} с очисткой webhook...")
            test_bot = Bot(token=token)
            bot_info = await test_bot.get_me()
            logger.info(f"Бот {bot_id} доступен: @{bot_info.username}")
            
            # Пытаемся очистить webhook
            try:
                await test_bot.delete_webhook(drop_pending_updates=True)
                logger.info(f"Webhook успешно очищен для бота {bot_id}")
                webhook_cleared = True
            except Exception as webhook_error:
                error_str = str(webhook_error).lower()
                if "429" in error_str or "flood" in error_str or "too many requests" in error_str:
                    logger.warning(f"Лимит на очистку webhook для {bot_id}: {webhook_error}")
                    # Не считаем это критической ошибкой, продолжаем без очистки
                    webhook_cleared = False
                else:
                    logger.warning(f"Не удалось очистить webhook для {bot_id}: {webhook_error}")
            
            # Закрываем тестового бота
            await test_bot.close()
            
            # Задержка после первой попытки
            if webhook_cleared:
                await asyncio.sleep(10)  # Большая задержка если webhook был очищен
            else:
                await asyncio.sleep(3)   # Меньшая задержка если webhook не очищался
                
        except Exception as e:
            error_msg = str(e).lower()
            
            # Если это лимит запросов, пробуем вторую попытку без очистки webhook
            if "429" in error_msg or "flood" in error_msg or "too many requests" in error_msg:
                logger.warning(f"Лимит запросов при первой попытке для бота {bot_id}. Пробую вторую попытку без очистки webhook...")
                
                # Ждем немного перед второй попыткой
                await asyncio.sleep(5)
                
                # Вторая попытка - просто подключаемся без всяких проверок
                logger.info(f"Попытка 2: Простое подключение к боту {bot_id} без проверок...")
                await asyncio.sleep(5)  # Ждем 5 секунд перед второй попыткой
                # Продолжаем дальше без проверок - сразу к созданию Application
                    
            elif "conflict" in error_msg or "terminated by other" in error_msg:
                logger.error(f"Бот {bot_id} уже запущен в другом процессе!")
                logger.error(f"Решение: остановите все процессы Python и перезапустите")
                bot_manager.set_bot_status(bot_id, "error")
                return False
            else:
                logger.error(f"Не удалось подключиться к боту {bot_id}: {e}")
                bot_manager.set_bot_status(bot_id, "error")
                return False
            
        app = Application.builder().token(token).build()
        
        start_handler = await create_child_bot_start_handler(bot_id)
        help_handler = await create_child_bot_help_handler(bot_id)
        broadcast_handler = await create_child_bot_broadcast_handler(bot_id)
        message_handler = await create_child_bot_message_handler(bot_id)
        
        app.add_handler(CommandHandler("start", start_handler))
        app.add_handler(CommandHandler("help", help_handler))
        app.add_handler(CommandHandler("broadcast", broadcast_handler))
        
        # Добавляем обработчик команды /send для рассылки
        async def send_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
            if context.user_data.get('broadcast_step') == 'confirm' and context.user_data.get('broadcast_bot_id') == bot_id:
                # Выполняем логику отправки рассылки
                broadcast_text = context.user_data.get('broadcast_text')
                broadcast_type = context.user_data.get('broadcast_type', 'text')
                broadcast_photo = context.user_data.get('broadcast_photo')
                users = db.get_bot_users(bot_id, only_active=True)
                
                await update.message.reply_text(
                    f"📤 *Рассылка запущена*\n\n"
                    f"👥 Отправка {len(users)} пользователям...",
                    parse_mode=ParseMode.MARKDOWN
                )
                
                success_count = 0
                failed_count = 0
                
                # Создаем объект Bot для дочернего бота
                child_bot = Bot(token=token)
                
                # Для дочерних ботов нужно переслать фото через файл
                photo_data = None
                if broadcast_type == 'photo':
                    try:
                        # Скачиваем фото через текущего бота (который получил фото)
                        logger.info(f"Скачиваю фото для команды /send в боте {bot_id}")
                        file = await context.bot.get_file(broadcast_photo)
                        file_bytes = await file.download_as_bytearray()
                        # Создаем InputFile из байтов
                        photo_data = io.BytesIO(file_bytes)
                        photo_data.name = 'photo.jpg'
                        logger.info(f"Фото успешно подготовлено для бота {bot_id}")
                    except Exception as e:
                        logger.error(f"Ошибка скачивания фото для рассылки в боте {bot_id}: {e}")
                        await update.message.reply_text(f"❌ Ошибка подготовки фото для рассылки: {e}")
                        context.user_data.clear()
                        return
                
                for user in users:
                    user_id = user['user_id'] if isinstance(user, dict) else user
                    try:
                        if broadcast_type == 'photo':
                            # Перематываем поток в начало для каждого пользователя
                            photo_data.seek(0)
                            photo_to_send = InputFile(photo_data, filename='photo.jpg')
                            
                            # Отправляем фото с подписью через child_bot
                            try:
                                if broadcast_text:
                                    html_caption = markdown_to_html(broadcast_text)
                                    await child_bot.send_photo(
                                        chat_id=user_id,
                                        photo=photo_to_send,
                                        caption=html_caption,
                                        parse_mode=ParseMode.HTML
                                    )
                                else:
                                    await child_bot.send_photo(
                                        chat_id=user_id,
                                        photo=photo_to_send
                                    )
                            except Exception as format_error:
                                logger.warning(f"Ошибка форматирования для пользователя {user_id}: {format_error}")
                                # Если не получилось с форматированием, отправляем без него
                                photo_data.seek(0)
                                photo_to_send = InputFile(photo_data, filename='photo.jpg')
                                await child_bot.send_photo(
                                    chat_id=user_id,
                                    photo=photo_to_send,
                                    caption=broadcast_text if broadcast_text else None
                                )
                        else:
                            # Отправляем текстовое сообщение через child_bot
                            try:
                                html_text = markdown_to_html(broadcast_text)
                                await child_bot.send_message(
                                    chat_id=user_id,
                                    text=html_text,
                                    parse_mode=ParseMode.HTML,
                                    disable_web_page_preview=True
                                )
                            except:
                                # Если не получилось, отправляем без форматирования
                                await child_bot.send_message(
                                    chat_id=user_id,
                                    text=broadcast_text,
                                    disable_web_page_preview=True
                                )
                        success_count += 1
                    except Forbidden:
                        # Пользователь заблокировал бота
                        logger.info(f"Пользователь {user_id} заблокировал бота {bot_id}")
                        db.block_user(user_id, bot_id)
                        failed_count += 1
                    except Exception as e:
                        logger.error(f"Ошибка отправки пользователю {user_id}: {e}")
                        failed_count += 1
                    await asyncio.sleep(0.05)
                
                context.user_data.clear()
                
                percent = (success_count/len(users)*100) if len(users) > 0 else 0
                
                await update.message.reply_text(
                    f"✅ *Рассылка завершена!*\n\n"
                    f"📊 *Статистика:*\n"
                    f"✅ Успешно: *{success_count}*\n"
                    f"❌ Ошибок: *{failed_count}*\n"
                    f"📈 Всего: *{len(users)}*\n\n"
                    f"Процент доставки: *{percent:.1f}%*",
                    parse_mode=ParseMode.MARKDOWN
                )
        
        # Добавляем обработчик команды /cancel для отмены
        async def cancel_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
            if context.user_data.get('broadcast_step'):
                context.user_data.clear()
                await update.message.reply_text("❌ Рассылка отменена")
        
        app.add_handler(CommandHandler("send", send_command_handler))
        app.add_handler(CommandHandler("cancel", cancel_command_handler))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
        app.add_handler(MessageHandler(filters.PHOTO, message_handler))
        
        # НЕ ИСПОЛЬЗУЕМ ПОТОКИ! Запускаем бота в текущем event loop
        logger.info(f"Инициализация бота {bot_id}...")
        await app.initialize()
        await app.start()
        
        # Получаем username бота
        bot_info = await app.bot.get_me()
        if bot_info.username:
            bot_manager.data["bots"][bot_id]["username"] = bot_info.username
            bot_manager.save_data()
        
        # Сохраняем информацию о боте
        active_bot_instances[bot_id] = {
            'app': app,
            'loop': asyncio.get_event_loop(),
            'stop_event': None,
            'start_time': datetime.now()
        }
        bot_manager.set_bot_status(bot_id, "running")
        
        # Сохраняем список запущенных ботов
        save_running_bots()
        
        logger.info(f"Запускаю polling для бота {bot_id}...")
        
        # Запускаем polling в фоновой задаче, а не в потоке
        asyncio.create_task(app.updater.start_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True
        ))
        
        logger.info(f"✅ Бот {bot_id} запущен успешно и готов к работе")
        
        # ВАЖНО: Добавляем задержку после запуска каждого бота
        await asyncio.sleep(15)
        
        # Проверяем, что бот действительно запустился
        if bot_id in active_bot_instances:
            logger.info(f"✅ Бот {bot_id} успешно добавлен в активные экземпляры")
            return True
        else:
            logger.error(f"❌ Бот {bot_id} не появился в активных экземплярах после запуска")
            # Пытаемся очистить поток если он завис
            if bot_id in bot_threads:
                del bot_threads[bot_id]
            return False
        
    except Exception as e:
        logger.error(f"Ошибка запуска бота {bot_id}: {e}")
        bot_manager.set_bot_status(bot_id, "error")
        return False

async def stop_bot(bot_id: str):
    """Остановка дочернего бота"""
    try:
        if bot_id not in active_bot_instances:
            logger.info(f"Бот {bot_id} уже остановлен")
            return True
            
        bot_info = active_bot_instances[bot_id]
        app = bot_info.get('app')
        
        if app:
            try:
                logger.info(f"Останавливаю бота {bot_id}...")
                
                # Останавливаем updater
                if app.updater:
                    await app.updater.stop()
                    logger.info(f"Updater бота {bot_id} остановлен")
                
                # Останавливаем приложение
                await app.stop()
                await app.shutdown()
                logger.info(f"Приложение бота {bot_id} остановлено")
                
            except Exception as e:
                logger.warning(f"Ошибка при остановке бота {bot_id}: {e}")
        
        # Удаляем из списка запущенных ботов
        if bot_id in active_bot_instances:
            del active_bot_instances[bot_id]
        
        # Удаляем из bot_threads если есть (для совместимости)
        if bot_id in bot_threads:
            del bot_threads[bot_id]
        
        bot_manager.set_bot_status(bot_id, "stopped")
        logger.info(f"✅ Бот {bot_id} остановлен")
        
        # Обновляем список запущенных ботов
        save_running_bots()
        
        return True
    except Exception as e:
        logger.error(f"Ошибка при остановке бота {bot_id}: {e}")
        # Даже при ошибке пытаемся обновить статус
        bot_manager.set_bot_status(bot_id, "stopped")
        if bot_id in active_bot_instances:
            del active_bot_instances[bot_id]
        if bot_id in bot_threads:
            del bot_threads[bot_id]
        save_running_bots()
        return True  # Возвращаем True, так как бот все равно будет остановлен

# Обработчики основного бота
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start основного бота"""
    user = update.effective_user
    
    # Добавляем пользователя в базу для главного бота
    try:
        db.add_user(
            user.id,
            'main',  # Это главный бот
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name
        )
    except Exception as e:
        logger.error(f"Ошибка добавления пользователя в БД: {e}")
    
    # Очищаем состояние при старте
    context.user_data.clear()
    
    if is_admin(user.id):
        keyboard = [[InlineKeyboardButton("🔧 Админка", callback_data="admin_panel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "🤖 *Главный бот управления*\n\n"
            "Добро пожаловать в панель управления ботами!\n\n"
            "🔧 _Доступна админка для управления ботами_",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
    else:
        # показываем текст, который админ задал для главного бота
        start_text = bot_manager.get_bot_text("main")
        try:
            html_text = markdown_to_html(start_text)
            await update.message.reply_text(
                html_text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.warning(f"Ошибка отправки с HTML: {e}")
            try:
                await update.message.reply_text(
                    start_text,
                    parse_mode=ParseMode.MARKDOWN,
                    disable_web_page_preview=True
                )
            except Exception:
                # Если ошибка с Markdown, отправляем без форматирования
                await update.message.reply_text(
                    start_text,
                    disable_web_page_preview=True
                )

async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /admin - быстрый доступ к админке"""
    user = update.effective_user
    
    if not is_admin(user.id):
        await update.message.reply_text("⛔ У вас нет доступа к админ-панели")
        return
    
    # Очищаем состояние
    context.user_data.clear()
    
    keyboard = [
        [
            InlineKeyboardButton("🤖 Боты", callback_data="manage_bots"),
            InlineKeyboardButton("📝 Тексты", callback_data="edit_texts")
        ],
        [
            InlineKeyboardButton("📢 Рассылка", callback_data="broadcast_menu"),
            InlineKeyboardButton("👥 Админы", callback_data="manage_admins")
        ],
        [
            InlineKeyboardButton("📊 Статистика", callback_data="stats"),
            InlineKeyboardButton("ℹ️ Справка", callback_data="markdown_help")
        ],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_start")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "🔧 *Панель администратора*\n\nВыберите действие:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

async def check_database_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для проверки базы данных (только для главного админа)"""
    user = update.effective_user
    
    if user.id != ADMIN_ID:
        await update.message.reply_text("⛔ Только главный админ может проверять базу данных")
        return
    
    try:
        import sqlite3
        conn = sqlite3.connect('bots_database.db')
        cursor = conn.cursor()
        
        # Получаем статистику
        cursor.execute('SELECT COUNT(DISTINCT user_id) FROM users')
        unique_users = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM users WHERE is_blocked = 0')
        active_total = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM users WHERE is_blocked = 1')
        blocked_total = cursor.fetchone()[0]
        
        cursor.execute('''
            SELECT bot_id, COUNT(*) as count
            FROM users
            WHERE is_blocked = 0
            GROUP BY bot_id
        ''')
        bot_stats = cursor.fetchall()
        
        # Формируем сообщение
        message = "🗄 *База данных*\n\n"
        message += f"👥 Уникальных пользователей: *{unique_users}*\n"
        message += f"✅ Активных записей: *{active_total}*\n"
        message += f"🚫 Заблокировали: *{blocked_total}*\n\n"
        
        if bot_stats:
            message += "*Статистика по ботам:*\n"
            for bot_id, count in bot_stats:
                bot_name = bot_manager.data["bots"].get(bot_id, {}).get("name", bot_id)
                message += f"• {bot_name}: *{count}* активных\n"
        
        conn.close()
        
        keyboard = [[InlineKeyboardButton("🔙 К админ-панели", callback_data="admin_panel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            message,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Ошибка проверки базы данных: {e}")
        await update.message.reply_text(f"❌ Ошибка проверки базы данных: {e}")

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главная панель админа"""
    # Обрабатываем как callback query, так и обычные сообщения
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        user = query.from_user
        message_obj = query.message
        is_callback = True
    else:
        user = update.effective_user
        message_obj = update.message
        is_callback = False
    
    if not is_admin(user.id):
        if is_callback:
            await query.edit_message_text("⛔ Доступ запрещен")
        else:
            await message_obj.reply_text("⛔ Доступ запрещен")
        return
    
    # Очищаем состояние пользователя при входе в админку
    context.user_data.clear()
    
    keyboard = [
        [
            InlineKeyboardButton("🤖 Боты", callback_data="manage_bots"),
            InlineKeyboardButton("📝 Тексты", callback_data="edit_texts")
        ],
        [
            InlineKeyboardButton("📢 Рассылка", callback_data="broadcast_menu"),
            InlineKeyboardButton("👥 Админы", callback_data="manage_admins")
        ],
        [
            InlineKeyboardButton("📊 Статистика", callback_data="stats"),
            InlineKeyboardButton("ℹ️ Справка", callback_data="markdown_help")
        ],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_start")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    admin_text = "🔧 *Панель администратора*\n\nВыберите действие:"
    
    # Если это callback query, редактируем сообщение
    if is_callback:
        try:
            await query.edit_message_text(
                admin_text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
        except Exception as e:
            # Если не удалось отредактировать, отправляем новое
            await message_obj.reply_text(
                admin_text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
    else:
        # Если это обычное сообщение, отправляем ответ
        await message_obj.reply_text(
            admin_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )

async def handle_photo_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка фотографий для рассылки"""
    user_id = update.effective_user.id
    
    # Проверяем, что это админ в режиме рассылки
    if is_admin(user_id) and context.user_data.get('broadcast_step') == 'text':
        photo = update.message.photo[-1]  # Берем фото в максимальном качестве
        caption = update.message.caption or ""
        
        # Сохраняем фото и подпись для рассылки
        context.user_data['broadcast_photo'] = photo.file_id
        context.user_data['broadcast_text'] = caption
        context.user_data['broadcast_type'] = 'photo'
        context.user_data['broadcast_step'] = 'confirm'
        
        # Получаем информацию о боте
        bot_id = context.user_data.get('broadcast_bot_id', 'main')
        bot_name = context.user_data.get('broadcast_bot_name', 'Главный бот')
        
        # Получаем количество пользователей для этого бота
        bot_stats = db.get_bot_stats(bot_id)
        users_count = bot_stats['active']
        
        # Показываем превью с фото
        keyboard = [
            [
                InlineKeyboardButton("✅ Отправить", callback_data="broadcast_send"),
                InlineKeyboardButton("❌ Отменить", callback_data="broadcast_cancel_photo")
            ],
            [InlineKeyboardButton("✏️ Изменить", callback_data="broadcast_edit_photo")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Экранируем подпись для безопасного отображения
        escaped_caption = escape_markdown(caption) if caption else '(без подписи)'
        escaped_bot_name = escape_markdown(bot_name)
        
        # Отправляем превью фото с подписью
        await update.message.reply_photo(
            photo=photo.file_id,
            caption=f"📢 *Подтверждение рассылки с фото*\n\n"
                   f"🤖 *От бота:* {escaped_bot_name}\n"
                   f"🆔 *ID бота:* `{bot_id}`\n"
                   f"📊 *Получателей:* {users_count} пользователей\n\n"
                   f"━━━━━━━━━━━━━━━━━━━━\n\n"
                   f"📝 *Подпись к фото:*\n{escaped_caption}\n\n"
                   f"Отправить рассылку всем пользователей?",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
        return

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текстовых сообщений"""
    user_id = update.effective_user.id
    user = update.effective_user
    message = update.message
    
    # Проверяем, является ли это ответом ГЛАВНОГО админа на пересланное сообщение
    if user_id == ADMIN_ID and message.reply_to_message:
        reply_to_id = message.reply_to_message.message_id
        
        # Проверяем, есть ли информация о пересланном сообщении
        if reply_to_id in forwarded_messages:
            forward_info = forwarded_messages[reply_to_id]
            target_user_id = forward_info['user_id']
            bot_id = forward_info['bot_id']
            
            try:
                # Определяем, какого бота использовать для отправки
                if bot_id == "main":
                    # Используем главного бота
                    bot = context.bot
                else:
                    # Для дочерних ботов используем их токен
                    token = bot_manager.get_bot_token(bot_id)
                    if not token:
                        await message.reply_text("❌ Не удалось найти токен бота для отправки ответа")
                        return
                    bot = Bot(token=token)
                
                # Отправляем ответ пользователю от имени соответствующего бота
                await bot.send_message(
                    chat_id=target_user_id,
                    text=f"💬 *Ответ от администратора:*\n\n{message.text}",
                    parse_mode=ParseMode.MARKDOWN
                )
                
                # Подтверждаем главному админу, что сообщение отправлено
                await message.reply_text(
                    f"✅ Ответ отправлен пользователю (ID: `{target_user_id}`)",
                    parse_mode=ParseMode.MARKDOWN
                )
                
                logger.info(f"Главный админ {user_id} ответил пользователю {target_user_id} через бота {bot_id}")
                return
                
            except Exception as e:
                logger.error(f"Ошибка отправки ответа пользователю: {e}")
                await message.reply_text(
                    f"❌ Ошибка отправки ответа: {e}",
                    parse_mode=ParseMode.MARKDOWN
                )
                return
    
    # Если это другой админ пытается ответить в главном боте
    elif is_admin(user_id) and user_id != ADMIN_ID and message.reply_to_message:
        reply_to_id = message.reply_to_message.message_id
        if reply_to_id in forwarded_messages:
            await message.reply_text(
                "⛔ Только главный администратор может отвечать на сообщения пользователей",
                parse_mode=ParseMode.MARKDOWN
            )
            return
    
    # Если это НЕ админ, пересылаем сообщение главному админу
    if not is_admin(user_id):
        # Пересылаем сообщение ТОЛЬКО ГЛАВНОМУ АДМИНУ
        logger.info(f"Пересылка сообщения от пользователя {user.id} главному админу: {ADMIN_ID}")
        
        try:
            # Формируем информацию о пользователе
            user_name = f"{user.first_name or ''} {user.last_name or ''}".strip() or "Без имени"
            user_username = f"@{user.username}" if user.username else "без username"
            
            # Получаем username главного бота
            main_bot_data = bot_manager.data["bots"].get("main", {})
            bot_username = main_bot_data.get("username")
            if bot_username:
                bot_display_name = f"@{bot_username}"
            else:
                bot_display_name = "Главном боте"
            
            # Экранируем имя пользователя для безопасного отображения
            safe_user_name = escape_markdown(user_name)
            safe_bot_display = escape_markdown(bot_display_name)
            
            # Сначала отправляем информацию о боте и пользователе БЕЗ форматирования
            info_msg = await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"📨 Новое сообщение в {safe_bot_display}\n"
                     f"От пользователя: {safe_user_name} "
                     f"({user_username}, ID: {user.id})"
            )
            # Затем пересылаем само сообщение
            forwarded_msg = await context.bot.forward_message(
                chat_id=ADMIN_ID,
                from_chat_id=message.chat_id,
                message_id=message.message_id
            )
            # Сохраняем связь между пересланным сообщением и пользователем с временной меткой
            # Проверяем размер словаря для предотвращения утечек памяти
            if len(forwarded_messages) > MAX_FORWARDED_MESSAGES:
                # Удаляем самые старые сообщения
                oldest_keys = sorted(forwarded_messages.keys())[:100]
                for key in oldest_keys:
                    del forwarded_messages[key]
                logger.info(f"Очищено {len(oldest_keys)} старых пересланных сообщений")
            
            forwarded_messages[forwarded_msg.message_id] = {
                'user_id': user.id,
                'bot_id': 'main',
                'original_message_id': message.message_id,
                'timestamp': datetime.now()  # Добавляем временную метку
            }
            logger.info(f"✅ Сообщение переслано главному админу, ID: {forwarded_msg.message_id}")
        except Exception as e:
            logger.error(f"❌ Ошибка пересылки сообщения главному админу {ADMIN_ID}: {e}")
        
        # Отправляем подтверждение пользователю
        await message.reply_text(
            "Ваше сообщение отправлено администратору! Ожидайте ответа!",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # Далее обрабатываем сообщения от админов (добавление админа, редактирование текста и т.д.)
    
    # Добавление нового админа
    if context.user_data.get('adding_admin'):
        try:
            new_admin_id = int(update.message.text.strip())
            
            if bot_manager.add_admin(new_admin_id):
                await update.message.reply_text(
                    f"✅ Пользователь `{new_admin_id}` успешно добавлен в админы",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 К управлению админами", callback_data="manage_admins")
                    ]])
                )
            else:
                await update.message.reply_text(
                    f"⚠️ Пользователь `{new_admin_id}` уже является админом",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 К управлению админами", callback_data="manage_admins")
                    ]])
                )
            
            # Очищаем флаг после успешной обработки
            if 'adding_admin' in context.user_data:
                del context.user_data['adding_admin']
        except ValueError:
            await update.message.reply_text(
                "❌ Неверный формат ID. Введите числовой ID пользователя.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Отмена", callback_data="manage_admins")
                ]])
            )
            # НЕ удаляем флаг при ошибке, чтобы пользователь мог попробовать снова
        return
    
    # Редактирование текста бота
    if 'editing_bot' in context.user_data:
        bot_id = context.user_data['editing_bot']
        new_text = update.message.text
        
        # Проверка на корректность bot_id перед сохранением
        if not bot_id or bot_id == "bot":
            logger.error(f"Попытка сохранить текст для некорректного bot_id: '{bot_id}'")
            await update.message.reply_text(
                "❌ Ошибка: некорректный идентификатор бота",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 К списку ботов", callback_data="edit_texts")
                ]])
            )
            del context.user_data['editing_bot']
            return
        
        # Проверяем, существует ли бот
        if bot_id not in bot_manager.data["bots"]:
            logger.error(f"Бот {bot_id} не существует")
            await update.message.reply_text(
                f"❌ Бот с ID `{bot_id}` не найден",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 К списку ботов", callback_data="edit_texts")
                ]])
            )
            del context.user_data['editing_bot']
            return
        
        bot_manager.set_bot_text(bot_id, new_text)
        del context.user_data['editing_bot']
        
        # Получаем информацию о боте для ссылки
        bot_data = bot_manager.data["bots"].get(bot_id, {})
        bot_name = bot_data.get("name", "Неизвестный бот")
        username = bot_data.get("username")
        
        keyboard = [
            [InlineKeyboardButton("🧪 Протестировать", callback_data=f"test_bot_{bot_id}")],
        ]
        
        # Добавляем кнопку перехода к боту, если есть username
        if username:
            keyboard.append([
                InlineKeyboardButton(f"🔗 Перейти к боту @{username}", url=f"https://t.me/{username}")
            ])
        
        keyboard.extend([
            [InlineKeyboardButton("📝 Редактировать еще", callback_data=f"edit_bot_{bot_id}")],
            [InlineKeyboardButton("🔙 К списку ботов", callback_data="edit_texts")]
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Отправляем подтверждение с информацией о боте
        await update.message.reply_text(
            f"✅ *Текст обновлен!*\n\n"
            f"🤖 Бот: *{bot_name}*\n"
            f"🆔 ID: `{bot_id}`\n\n"
            f"Текст бота был успешно изменен.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup,
            disable_web_page_preview=True
        )
        return
    
    # Создание нового бота - название
    elif context.user_data.get('creating_bot_step') == 'name':
        bot_name = update.message.text.strip()
        
        if len(bot_name) < 3 or len(bot_name) > 50:
            await update.message.reply_text(
                "⛔ *Ошибка*\n\nНазвание должно быть от 3 до 50 символов",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        context.user_data['new_bot_name'] = bot_name
        context.user_data['creating_bot_step'] = 'id'
        
        keyboard = [
            [InlineKeyboardButton("🔙 Отменить", callback_data="manage_bots")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            f"✅ *Название сохранено:* {bot_name}\n\n"
            f"**Шаг 2/3:** Введите ID бота (без пробелов)\n\n"
            f"Пример: `flowers_shop_bot`\n"
            f"Или: `pizza_delivery_bot`\n\n"
            f"ID должен содержать только буквы, цифры и подчеркивания",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
    
    # Создание нового бота - ID
    elif context.user_data.get('creating_bot_step') == 'id':
        bot_id = update.message.text.strip().lower()
        
        if not bot_id.replace('_', '').isalnum() or len(bot_id) < 3:
            await update.message.reply_text(
                "⛔ *Ошибка ID*\n\n"
                "ID должен содержать только буквы, цифры и подчеркивания, минимум 3 символа",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        if bot_id in bot_manager.data["bots"]:
            await update.message.reply_text(
                f"⛔ *ID уже занят*\n\n"
                f"Бот с ID `{bot_id}` уже существует. Выберите другой ID.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        context.user_data['new_bot_id'] = bot_id
        context.user_data['creating_bot_step'] = 'token'
        
        keyboard = [
            [InlineKeyboardButton("🔙 Отменить", callback_data="manage_bots")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            f"✅ *ID сохранен:* `{bot_id}`\n\n"
            f"**Шаг 3/3:** Введите токен бота\n\n"
            f"Получите токен у @BotFather:\n"
            f"1. Напишите `/newbot`\n"
            f"2. Введите название\n"
            f"3. Введите username\n"
            f"4. Скопируйте полученный токен\n\n"
            f"Токен выглядит примерно так:\n"
            f"`1234567890:ABCdefGHIjklMNOpqrsTUVwxyz`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
    
    # Обработка текста для рассылки
    elif context.user_data.get('broadcast_step') == 'text':
        broadcast_text = update.message.text
        
        # Проверяем, есть ли сохраненное фото (при редактировании)
        if 'saved_photo' in context.user_data:
            # Восстанавливаем фото и делаем рассылку с фото
            context.user_data['broadcast_photo'] = context.user_data['saved_photo']
            context.user_data['broadcast_text'] = broadcast_text
            context.user_data['broadcast_type'] = 'photo'
            context.user_data['broadcast_step'] = 'confirm'
            del context.user_data['saved_photo']  # Удаляем временное сохранение
            
            # Получаем информацию о боте
            bot_id = context.user_data.get('broadcast_bot_id', 'main')
            bot_name = context.user_data.get('broadcast_bot_name', 'Главный бот')
            
            # Получаем количество пользователей для этого бота
            bot_stats = db.get_bot_stats(bot_id)
            users_count = bot_stats['active']
            
            # Показываем превью с фото
            keyboard = [
                [
                    InlineKeyboardButton("✅ Отправить", callback_data="broadcast_send"),
                    InlineKeyboardButton("❌ Отменить", callback_data="broadcast_cancel_photo")
                ],
                [InlineKeyboardButton("✏️ Изменить", callback_data="broadcast_edit_photo")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Экранируем текст для безопасного отображения
            escaped_text = escape_markdown(broadcast_text) if broadcast_text else '(без подписи)'
            escaped_bot_name = escape_markdown(bot_name)
            
            await update.message.reply_photo(
                photo=context.user_data['broadcast_photo'],
                caption=f"📢 *Подтверждение рассылки с фото*\n\n"
                       f"🤖 *От бота:* {escaped_bot_name}\n"
                       f"🆔 *ID бота:* `{bot_id}`\n"
                       f"📊 *Получателей:* {users_count} пользователей\n\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n\n"
                       f"📝 *Подпись к фото:*\n{escaped_text}\n\n"
                       f"Отправить рассылку всем пользователям?",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
            return
        
        # Обычная текстовая рассылка
        context.user_data['broadcast_text'] = broadcast_text
        context.user_data['broadcast_type'] = 'text'  # Указываем тип рассылки
        context.user_data['broadcast_step'] = 'confirm'
        
        # Показываем превью и запрашиваем подтверждение
        keyboard = [
            [
                InlineKeyboardButton("✅ Отправить", callback_data="broadcast_send"),
                InlineKeyboardButton("❌ Отменить", callback_data="broadcast_cancel")
            ],
            [InlineKeyboardButton("✏️ Изменить текст", callback_data="broadcast_edit")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Получаем информацию о боте
        bot_id = context.user_data.get('broadcast_bot_id', 'main')
        bot_name = context.user_data.get('broadcast_bot_name', 'Главный бот')
        
        # Получаем количество пользователей для этого бота
        bot_stats = db.get_bot_stats(bot_id)
        users_count = bot_stats['active']
        
        # Экранируем текст рассылки для безопасного отображения
        escaped_text = escape_markdown(broadcast_text)
        escaped_bot_name = escape_markdown(bot_name)
        
        await update.message.reply_text(
            f"📢 *Подтверждение рассылки*\n\n"
            f"🤖 *От бота:* {escaped_bot_name}\n"
            f"🆔 *ID бота:* `{bot_id}`\n"
            f"📊 *Получателей:* {users_count} пользователей\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📝 *Текст рассылки:*\n{escaped_text}\n\n"
            f"Отправить рассылку всем пользователям?",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
    
    # Создание нового бота - токен
    elif context.user_data.get('creating_bot_step') == 'token':
        token = update.message.text.strip()
        
        # Проверяем формат токена
        if ':' not in token or len(token) < 40:
            await update.message.reply_text(
                "⛔ *Неверный формат токена*\n\n"
                "Токен должен быть в формате:\n"
                "`1234567890:ABCdefGHIjklMNOpqrsTUVwxyz`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Проверяем, не используется ли уже этот токен
        for existing_bot_id, bot_data in bot_manager.data["bots"].items():
            if bot_data.get("token") == token:
                existing_bot_name = bot_data.get("name", "Неизвестный бот")
                existing_username = bot_data.get("username")
                
                error_text = f"⛔ *Токен уже используется!*\n\n"
                error_text += f"Этот токен уже привязан к боту:\n"
                error_text += f"📋 Название: *{existing_bot_name}*\n"
                error_text += f"🆔 ID: `{existing_bot_id}`\n"
                if existing_username:
                    error_text += f"👤 Username: @{existing_username}\n"
                error_text += f"\n❗ Каждый бот должен иметь уникальный токен.\n"
                error_text += f"Получите новый токен у @BotFather или используйте другой."
                
                keyboard = [
                    [InlineKeyboardButton("🔙 Отменить", callback_data="manage_bots")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await update.message.reply_text(
                    error_text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=reply_markup
                )
                return
        
        bot_name = context.user_data['new_bot_name']
        bot_id = context.user_data['new_bot_id']
        
        # Функция для проверки токена с повторными попытками
        async def validate_token_with_retries(token: str, max_retries: int = 3):
            """Проверка токена с несколькими попытками"""
            webhook_cleared = False
            
            for attempt in range(max_retries):
                try:
                    logger.info(f"Попытка {attempt + 1}/{max_retries} проверки токена для бота {bot_id}...")
                    
                    # Небольшая задержка между попытками
                    if attempt > 0:
                        await asyncio.sleep(2)
                    
                    test_app = Application.builder().token(token).build()
                    await test_app.initialize()
                    
                    # Пытаемся получить информацию о боте
                    bot_info = await test_app.bot.get_me()
                    username = bot_info.username
                    logger.info(f"Токен валидный, username: @{username}")
                    
                    # На первой попытке пытаемся очистить webhook
                    if attempt == 0 and not webhook_cleared:
                        try:
                            await test_app.bot.delete_webhook(drop_pending_updates=True)
                            logger.info(f"Webhook очищен для нового бота @{username}")
                            webhook_cleared = True
                        except Exception as webhook_error:
                            error_str = str(webhook_error).lower()
                            if "429" in error_str or "flood" in error_str or "too many requests" in error_str:
                                logger.warning(f"Лимит на очистку webhook: {webhook_error}")
                                # Не считаем это критической ошибкой
                            else:
                                logger.warning(f"Не удалось очистить webhook: {webhook_error}")
                    elif attempt > 0:
                        logger.info(f"Пропускаю очистку webhook на попытке {attempt + 1}")
                    
                    await test_app.shutdown()
                    
                    return True, username, None
                    
                except Exception as e:
                    error_str = str(e).lower()
                    logger.error(f"Попытка {attempt + 1} не удалась: {e}")
                    
                    # Если это лимит и первая попытка, пробуем без очистки webhook
                    if ("429" in error_str or "flood" in error_str or "too many requests" in error_str) and attempt == 0:
                        logger.info("Обнаружен лимит запросов, следующая попытка будет без очистки webhook")
                        continue  # Переходим к следующей попытке
                    
                    # Если это последняя попытка, возвращаем ошибку
                    if attempt == max_retries - 1:
                        if "unauthorized" in error_str or "invalid token" in error_str or "404" in error_str:
                            error_message = "❌ *Неверный токен*\n\nТокен недействителен. Проверьте правильность токена."
                        elif "conflict" in error_str or "terminated by other" in error_str:
                            # Для конфликта пробуем освободить бота БЕЗ очистки webhook если был лимит
                            try:
                                logger.info("Пытаюсь освободить бота от другого процесса...")
                                temp_app = Application.builder().token(token).build()
                                await temp_app.initialize()
                                
                                # Очищаем webhook только если не было лимита
                                if not ("429" in error_str or "flood" in error_str):
                                    try:
                                        await temp_app.bot.delete_webhook(drop_pending_updates=True)
                                    except:
                                        pass
                                
                                await temp_app.shutdown()
                                await asyncio.sleep(3)
                                
                                # Еще одна попытка после освобождения
                                test_app = Application.builder().token(token).build()
                                await test_app.initialize()
                                bot_info = await test_app.bot.get_me()
                                username = bot_info.username
                                await test_app.shutdown()
                                logger.info(f"Бот успешно освобожден, username: @{username}")
                                return True, username, None
                            except:
                                error_message = "⚠️ *Бот уже используется*\n\nЭтот бот запущен в другом приложении. Остановите его там или используйте другого бота."
                        elif "not found" in error_str:
                            error_message = "❌ *Бот не найден*\n\nБот с таким токеном не существует. Создайте нового бота у @BotFather."
                        else:
                            error_message = f"❌ *Ошибка проверки токена*\n\n{str(e)}"
                        
                        return False, None, error_message
                    
                    # Продолжаем попытки
                    continue
            
            return False, None, "❌ Не удалось проверить токен после нескольких попыток"
        
        # Проверяем токен с повторными попытками
        token_valid, username, error_message = await validate_token_with_retries(token)
        
        # Если токен невалидный, показываем ошибку
        if not token_valid:
            keyboard = [
                [InlineKeyboardButton("🔄 Попробовать другой токен", callback_data="retry_token")],
                [InlineKeyboardButton("🔙 Отменить", callback_data="manage_bots")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                error_message or "❌ Не удалось проверить токен",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
            
            # Оставляем состояние для повторной попытки
            return
        
        # Добавляем бота в систему
        bot_manager.add_bot(bot_id, bot_name, token, username)
        
        # Очищаем состояние создания
        for key in ['creating_bot_step', 'new_bot_name', 'new_bot_id']:
            context.user_data.pop(key, None)
        
        keyboard = [
            [InlineKeyboardButton("▶️ Запустить бота", callback_data=f"start_bot_{bot_id}")],
            [InlineKeyboardButton("📝 Настроить текст", callback_data=f"edit_bot_{bot_id}")],
            [InlineKeyboardButton("🔙 К управлению ботами", callback_data="manage_bots")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            f"🎉 *Бот создан успешно!*\n\n"
            f"📋 **Информация о боте:**\n"
            f"• Название: *{bot_name}*\n"
            f"• ID: `{bot_id}`\n"
            f"• Статус: 🔴 Остановлен\n\n"
            f"Теперь вы можете запустить бота или настроить его текст.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )

async def manage_bots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Управление ботами"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    # Очищаем состояние при входе в меню управления ботами (включая создание бота)
    for key in ['creating_bot_step', 'new_bot_name', 'new_bot_id', 'adding_admin', 'editing_bot']:
        context.user_data.pop(key, None)
    
    bots_list = bot_manager.get_bots_list()
    text = "🤖 *Управление ботами*\n\n*Текущие боты:*\n"
    
    for bot_id, bot_name, status in bots_list:
        status_emoji = "🟢" if status == "running" else "🔴" if status == "stopped" else "🟡"
        # НЕ экранируем bot_id внутри обратных кавычек - Markdown не требует этого
        text += f"{status_emoji} `{bot_id}` - {bot_name} ({status})\n"
    
    keyboard = [
        [InlineKeyboardButton("➕ Создать нового бота", callback_data="create_new_bot")],
        [InlineKeyboardButton("🔄 Управлять существующими", callback_data="control_bots")],
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

async def create_new_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Создание нового бота - шаг 1: запрос названия"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    keyboard = [
        [InlineKeyboardButton("🔙 Назад", callback_data="manage_bots")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    context.user_data['creating_bot_step'] = 'name'
    
    await query.edit_message_text(
        "➕ *Создание нового бота*\n\n"
        "**Шаг 1/3:** Введите название бота\n\n"
        "Пример: `Цветочный магазин`\n"
        "Или: `Доставка пиццы`",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

async def retry_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Повторная попытка ввода токена"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    # Проверяем, что у нас есть сохраненные данные о боте
    if 'new_bot_name' not in context.user_data or 'new_bot_id' not in context.user_data:
        await query.edit_message_text(
            "❌ Данные о боте потеряны. Начните создание заново.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 К управлению ботами", callback_data="manage_bots")
            ]])
        )
        return
    
    bot_name = context.user_data['new_bot_name']
    bot_id = context.user_data['new_bot_id']
    
    keyboard = [
        [InlineKeyboardButton("🔙 Отменить", callback_data="manage_bots")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Возвращаемся к шагу ввода токена
    context.user_data['creating_bot_step'] = 'token'
    
    await query.edit_message_text(
        f"🔄 *Повторная попытка*\n\n"
        f"📋 Название: *{bot_name}*\n"
        f"🆔 ID: `{bot_id}`\n\n"
        f"**Шаг 3/3:** Введите другой токен бота\n\n"
        f"Получите токен у @BotFather:\n"
        f"1. Напишите `/newbot`\n"
        f"2. Введите название\n"
        f"3. Введите username\n"
        f"4. Скопируйте полученный токен\n\n"
        f"Токен выглядит примерно так:\n"
        f"`1234567890:ABCdefGHIjklMNOpqrsTUVwxyz`",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

async def control_bots(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Управление существующими ботами"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    bots_list = bot_manager.get_bots_list()
    keyboard = []
    
    text = "🔄 Управление существующими ботами\n\n"
    
    if len([b for b in bots_list if b[0] != "main"]) == 0:
        text += "У вас пока нет дочерних ботов\n\n"
    else:
        text += "📋 Список ваших ботов:\n\n"
    
    for bot_id, bot_name, status in bots_list:
        if bot_id != "main":  # Исключаем главный бот
            status_emoji = "🟢" if status == "running" else "🔴" if status == "stopped" else "🟡"
            bot_data = bot_manager.data["bots"].get(bot_id, {})
            username = bot_data.get("username")
            
            # НЕ экранируем, так как отправляем без parse_mode
            # Добавляем информацию в текст
            text += f"{status_emoji} {bot_name}\n"
            text += f"   └ ID: {bot_id}\n"
            if username:
                text += f"   └ Username: @{username}\n"
            text += f"   └ Статус: {'Запущен' if status == 'running' else 'Остановлен'}\n\n"
            
            # Если есть username, делаем кнопку с URL
            if username:
                keyboard.append([
                    InlineKeyboardButton(
                        f"{status_emoji} {bot_name}",
                        url=f"https://t.me/{username}"
                    ),
                    InlineKeyboardButton(
                        "⚙️ Управление",
                        callback_data=f"control_bot_{bot_id}"
                    )
                ])
            else:
                keyboard.append([InlineKeyboardButton(
                    f"{status_emoji} {bot_name}",
                    callback_data=f"control_bot_{bot_id}"
                )])
    
    if not keyboard:
        keyboard.append([InlineKeyboardButton("➕ Создать первого бота", callback_data="create_new_bot")])
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="manage_bots")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Отправляем без parse_mode чтобы избежать ошибок
    await query.edit_message_text(
        text,
        reply_markup=reply_markup
    )

async def control_single_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Управление конкретным ботом"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    # Правильно извлекаем bot_id, учитывая что он может содержать подчеркивания
    bot_id = query.data[len("control_bot_"):]
    bot_data = bot_manager.data["bots"].get(bot_id, {})
    bot_name = bot_data.get("name", "Неизвестный бот")
    status = bot_data.get("status", "stopped")
    
    status_text = "🟢 Запущен" if status == "running" else "🔴 Остановлен" if status == "stopped" else "🟡 Ошибка"
    
    keyboard = []
    
    if status == "running":
        keyboard.append([InlineKeyboardButton("⏹️ Остановить бота", callback_data=f"stop_bot_{bot_id}")])
    else:
        keyboard.append([InlineKeyboardButton("▶️ Запустить бота", callback_data=f"start_bot_{bot_id}")])
    
    keyboard.extend([
        [InlineKeyboardButton("📝 Редактировать текст", callback_data=f"edit_bot_{bot_id}")],
        [InlineKeyboardButton("🗑️ Удалить бота", callback_data=f"delete_bot_{bot_id}")],
        [InlineKeyboardButton("🔙 Назад", callback_data="control_bots")]
    ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    token_preview = bot_data.get('token', 'Не указан')
    if len(token_preview) > 20:
        token_preview = token_preview[:20] + "..."
    
    username = bot_data.get("username")
    
    # Формируем текст БЕЗ форматирования чтобы избежать ошибок с bot_id
    text = f"🤖 Бот: {bot_name}\n\n"
    text += f"🆔 ID: {bot_id}\n"
    if username:
        text += f"👤 Username: @{username}\n"
    text += f"📊 Статус: {status_text}\n"
    text += f"🔗 Токен: {token_preview}"
    
    # Отправляем БЕЗ parse_mode чтобы избежать ошибок с подчеркиваниями в bot_id
    await query.edit_message_text(
        text,
        reply_markup=reply_markup
    )

async def start_bot_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запуск бота"""
    query = update.callback_query
    await query.answer("⏳ Запускаю бота...")
    
    if not is_admin(query.from_user.id):
        return
    
    # Правильно извлекаем bot_id, учитывая что он может содержать подчеркивания
    bot_id = query.data[len("start_bot_"):]
    print(f"\n\n\nBOT\n\n\n\n\n\{bot_id}")
    token = bot_manager.get_bot_token(bot_id)
    bot_name = bot_manager.data["bots"].get(bot_id, {}).get("name", "Неизвестный бот")
    
    if not token:
        await query.edit_message_text("❌ Токен не найден")
        return
    
    # Сначала показываем, что бот запускается
    await query.edit_message_text(
        f"⏳ *Запуск бота*\n\n"
        f"Бот *{bot_name}* (`{bot_id}`) запускается...\n\n"
        f"Пожалуйста, подождите несколько секунд...",
        parse_mode=ParseMode.MARKDOWN
    )
    
    # Запускаем бота
    success = await start_bot(bot_id, token)
    
    # Ждем немного для инициализации
    await asyncio.sleep(2)
    
    # Обновляем сообщение с результатом
    if success and bot_id in active_bot_instances:
        keyboard = [
            [InlineKeyboardButton("⏹️ Остановить бота", callback_data=f"stop_bot_{bot_id}")],
            [InlineKeyboardButton("📝 Редактировать текст", callback_data=f"edit_bot_{bot_id}")],
            [InlineKeyboardButton("🗑️ Удалить бота", callback_data=f"delete_bot_{bot_id}")],
            [InlineKeyboardButton("🔙 Назад", callback_data="control_bots")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            f"✅ *Бот успешно запущен!*\n\n"
            f"🤖 *{bot_name}*\n"
            f"🆔 ID: `{bot_id}`\n"
            f"📊 Статус: 🟢 Запущен\n\n"
            f"Бот готов к работе и отвечает на команды!",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
    else:
        keyboard = [
            [InlineKeyboardButton("🔄 Попробовать снова", callback_data=f"start_bot_{bot_id}")],
            [InlineKeyboardButton("🔙 Назад", callback_data=f"control_bot_{bot_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            f"❌ *Ошибка запуска*\n\n"
            f"Не удалось запустить бота *{bot_name}* (`{bot_id}`)\n\n"
            f"Возможные причины:\n"
            f"• Неверный токен\n"
            f"• Бот уже запущен в другом месте\n"
            f"• Проблемы с сетью",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )

async def stop_bot_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Остановка бота"""
    query = update.callback_query
    await query.answer("⏳ Останавливаю бота...")
    
    if not is_admin(query.from_user.id):
        return
    
    # Правильно извлекаем bot_id, учитывая что он может содержать подчеркивания
    bot_id = query.data[len("stop_bot_"):]
    bot_name = bot_manager.data["bots"].get(bot_id, {}).get("name", "Неизвестный бот")
    
    # Показываем процесс остановки
    await query.edit_message_text(
        f"⏳ *Остановка бота*\n\n"
        f"Останавливаю бота *{bot_name}* (`{bot_id}`)...\n\n"
        f"Это может занять несколько секунд...",
        parse_mode=ParseMode.MARKDOWN
    )
    
    # Останавливаем бота
    success = await stop_bot(bot_id)
    
    # Ждем немного для полной остановки
    await asyncio.sleep(2)
    
    # Всегда показываем успешное сообщение, так как бот будет остановлен
    await query.edit_message_text(
        f"✅ *Бот остановлен*\n\n"
        f"Бот *{bot_name}* (`{bot_id}`) успешно остановлен!",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔙 Назад", callback_data=f"control_bot_{bot_id}")
        ]])
    )

async def delete_bot_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаление бота с подтверждением"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    # Правильно извлекаем bot_id, учитывая что он может содержать подчеркивания
    # delete_bot_<bot_id> - убираем префикс "delete_bot_"
    bot_id = query.data[len("delete_bot_"):]
    bot_name = bot_manager.data["bots"].get(bot_id, {}).get("name", "Неизвестный бот")
    
    keyboard = [
        [InlineKeyboardButton("⚠️ Да, удалить", callback_data=f"confirm_delete_{bot_id}")],
        [InlineKeyboardButton("🔙 Отмена", callback_data=f"control_bot_{bot_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"⚠️ *Подтвердите удаление*\n\n"
        f"Вы уверены, что хотите удалить бота:\n"
        f"*{bot_name}* (`{bot_id}`)\n\n"
        f"❗ Это действие необратимо!",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

async def confirm_delete_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение удаления бота"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    # Правильно извлекаем bot_id, учитывая что он может содержать подчеркивания
    # confirm_delete_<bot_id> - убираем префикс "confirm_delete_"
    prefix = "confirm_delete_"
    if not query.data.startswith(prefix):
        logger.error(f"Некорректный callback_data для подтверждения удаления: {query.data}")
        await query.edit_message_text(
            "❌ Ошибка: некорректный идентификатор бота",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 К управлению ботами", callback_data="manage_bots")
            ]])
        )
        return
    
    bot_id = query.data[len(prefix):]
    
    # Проверяем корректность bot_id
    if bot_id == "bot" or not bot_id:
        logger.error(f"Попытка подтвердить удаление бота с некорректным ID: '{bot_id}'")
        await query.edit_message_text(
            "❌ Ошибка: некорректный идентификатор бота",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 К управлению ботами", callback_data="manage_bots")
            ]])
        )
        return
    
    # Останавливаем бота если запущен
    if bot_id in active_bot_instances:
        await stop_bot(bot_id)
    
    # Удаляем из базы данных
    success = bot_manager.delete_bot(bot_id)
    
    keyboard = [
        [InlineKeyboardButton("🔙 К управлению ботами", callback_data="manage_bots")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if success:
        await query.edit_message_text(
            f"✅ *Бот удален успешно*\n\n"
            f"Бот `{bot_id}` был удален из системы.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
    else:
        await query.edit_message_text(
            f"❌ *Ошибка удаления*\n\n"
            f"Не удалось удалить бота `{bot_id}`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )

async def edit_texts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню редактирования текстов"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    # Очищаем состояние при входе в меню редактирования
    context.user_data.clear()
    
    bots_list = bot_manager.get_bots_list()
    keyboard = []
    
    # Формируем текст БЕЗ Markdown форматирования чтобы избежать ошибок
    text = "📝 Редактирование текстов ботов\n\n"
    text += "Выберите бота для редактирования приветственного сообщения:\n\n"
    
    for bot_id, bot_name, status in bots_list:
        bot_data = bot_manager.data["bots"].get(bot_id, {})
        username = bot_data.get("username")
        status_emoji = "🟢" if status == "running" else "🔴"
        
        # Добавляем информацию о боте в текст БЕЗ экранирования
        if bot_id == "main":
            # Получаем username главного бота
            main_bot_data = bot_manager.data["bots"].get("main", {})
            main_bot_username = main_bot_data.get("username")
            if main_bot_username:
                text += f"{status_emoji} @{main_bot_username} (главный)\n"
            else:
                text += f"{status_emoji} Главный бот\n"
        else:
            text += f"{status_emoji} {bot_name}\n"
        
        text += f"   └ ID: {bot_id}\n"
        if username:
            text += f"   └ Username: @{username}\n"
        text += "\n"
        
        # Создаем кнопки
        if username:
            keyboard.append([
                InlineKeyboardButton(f"✏️ {bot_name}", callback_data=f"edit_bot_{bot_id}"),
                InlineKeyboardButton(f"👁 Открыть", url=f"https://t.me/{username}")
            ])
        else:
            keyboard.append([InlineKeyboardButton(f"✏️ {bot_name}", callback_data=f"edit_bot_{bot_id}")])
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Отправляем БЕЗ parse_mode чтобы избежать ошибок парсинга
    await query.edit_message_text(
        text,
        reply_markup=reply_markup
    )

async def edit_bot_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Редактирование текста конкретного бота"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    # Правильно извлекаем bot_id, учитывая что он может содержать подчеркивания
    # edit_bot_<bot_id> - убираем префикс "edit_bot_"
    prefix = "edit_bot_"
    if not query.data.startswith(prefix):
        logger.error(f"Некорректный callback_data для редактирования: {query.data}")
        await query.edit_message_text(
            "❌ Ошибка: некорректный идентификатор бота",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Назад", callback_data="edit_texts")
            ]])
        )
        return
    
    bot_id = query.data[len(prefix):]
    
    # Дополнительная проверка
    if bot_id == "bot" or not bot_id:
        logger.error(f"Попытка редактировать бота с некорректным ID: '{bot_id}'")
        await query.edit_message_text(
            "❌ Ошибка: некорректный идентификатор бота",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Назад", callback_data="edit_texts")
            ]])
        )
        return
    
    # Проверяем, существует ли бот
    if bot_id not in bot_manager.data["bots"]:
        logger.error(f"Бот {bot_id} не найден в системе")
        await query.edit_message_text(
            f"❌ Бот с ID `{bot_id}` не найден",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Назад", callback_data="edit_texts")
            ]])
        )
        return
    
    current_text = bot_manager.get_bot_text(bot_id)
    bot_data = bot_manager.data["bots"].get(bot_id, {})
    bot_name = bot_data.get("name", "Неизвестный бот")
    username = bot_data.get("username")
    status = bot_data.get("status", "stopped")
    
    keyboard = [
        [InlineKeyboardButton("🧪 Протестировать", callback_data=f"test_bot_{bot_id}")],
    ]
    
    # Добавляем кнопку перехода к боту, если есть username
    if username:
        # НЕ экранируем username в кнопке
        keyboard.append([
            InlineKeyboardButton(f"👁 Открыть @{username}", url=f"https://t.me/{username}")
        ])
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="edit_texts")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Сохраняем ID бота для редактирования
    context.user_data['editing_bot'] = bot_id
    
    status_emoji = "🟢" if status == "running" else "🔴"
    
    # НЕ экранируем ID внутри обратных кавычек и username после @
    # Формируем информативный заголовок
    header = f"✏️ *Редактирование приветственного сообщения*\n\n"
    header += f"{status_emoji} *Бот:* {bot_name}\n"
    header += f"🆔 *ID:* `{bot_id}`\n"
    if username:
        header += f"👤 *Username:* @{username}\n"
    header += "\n━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # Показываем текст без Markdown форматирования, чтобы избежать ошибок
    await query.edit_message_text(
        f"{header}"
        f"📄 *Текущий текст:*\n```\n{current_text}\n```\n\n"
        f"📝 *Инструкция:*\nОтправьте новый текст сообщением.\n\n"
        f"🎨 *Поддерживаемое форматирование:*\n"
        f"• \\*жирный\\* → *жирный*\n"
        f"• \\_курсив\\_ → _курсив_\n"
        f"• \\[текст\\]\\(URL\\) → ссылка\n"
        f"• \\`код\\` → `код`\n\n"
        f"Бот автоматически определит формат.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

async def test_bot_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тестирование текста бота"""
    query = update.callback_query
    await query.answer()
    
    # Правильно извлекаем bot_id, учитывая что он может содержать подчеркивания
    # test_bot_<bot_id> - убираем префикс "test_bot_"
    bot_id = query.data[len("test_bot_"):]
    
    # Проверяем, существует ли бот
    if bot_id not in bot_manager.data["bots"]:
        await query.message.reply_text(
            f"❌ Бот с ID '{bot_id}' не найден",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 К списку ботов", callback_data="edit_texts")
            ]])
        )
        return
    
    text = bot_manager.get_bot_text(bot_id)
    bot_data = bot_manager.data["bots"].get(bot_id, {})
    bot_name = bot_data.get("name", "Неизвестный бот")
    username = bot_data.get("username")
    
    # Если текст не найден
    if text == "Текст не найден":
        await query.message.reply_text(
            f"⚠️ Текст для бота '{bot_name}' не настроен",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("📝 Настроить текст", callback_data=f"edit_bot_{bot_id}"),
                InlineKeyboardButton("🔙 К списку", callback_data="edit_texts")
            ]])
        )
        return
    
    # Формируем заголовок теста БЕЗ экранирования
    test_header = f"🧪 Тест бота: {bot_name}\n"
    test_header += f"🆔 ID: {bot_id}\n"
    if username:
        test_header += f"👤 Username: @{username}\n"
    test_header += "\n━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # Отправляем тест в том же формате, как это делает сам бот
    try:
        # Сначала пробуем конвертировать Markdown в HTML (как делает сам бот)
        html_text = markdown_to_html(text)
        await query.message.reply_text(
            test_header + "📝 Сообщение с форматированием HTML:\n\n" + html_text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.warning(f"Ошибка отправки с HTML для теста бота {bot_id}: {e}")
        try:
            # Если не получилось с HTML, пробуем без форматирования
            await query.message.reply_text(
                test_header + "📝 Сообщение без форматирования:\n\n" + text,
                disable_web_page_preview=True
            )
        except Exception as e2:
            logger.error(f"Ошибка при тестировании текста бота {bot_id}: {e2}")
            await query.message.reply_text(
                f"❌ Ошибка при отправке тестового сообщения: {e2}"
            )

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Статистика"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    # Очищаем состояние при просмотре статистики
    context.user_data.clear()
    
    bots_list = bot_manager.get_bots_list()
    total_bots = len(bots_list)
    running_bots_count = sum(1 for _, _, status in bots_list if status == "running")
    
    # Получаем глобальную статистику из БД
    global_stats = db.get_global_stats()
    
    keyboard = [
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Получаем список админов
    admins_count = len(bot_manager.admins)
    
    # Вычисляем процент премиум пользователей
    premium_percent = 0
    if global_stats['active_users'] > 0:
        premium_percent = (global_stats['premium_users'] / global_stats['active_users']) * 100
    
    await query.edit_message_text(
        f"📊 *Статистика системы*\n\n"
        f"🤖 *Боты:*\n"
        f"• Всего: {total_bots}\n"
        f"• Запущено: 🟢 {running_bots_count}\n"
        f"• Остановлено: 🔴 {total_bots - running_bots_count}\n\n"
        f"👥 *Пользователи:*\n"
        f"• Уникальных: {global_stats['unique_users']}\n"
        f"• Активных: {global_stats['active_users']}\n"
        f"• ⭐ Премиум: {global_stats['premium_users']} ({premium_percent:.1f}%)\n\n"
        f"👑 *Администрация:*\n"
        f"• Админов: {admins_count}\n"
        f"• Главный админ: `{ADMIN_ID}`",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

async def manage_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Управление админами"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    
    # Только главный админ может управлять другими админами
    if user_id != ADMIN_ID:
        await query.edit_message_text("⛔ Только главный админ может управлять другими админами")
        return
    
    # Очищаем флаг добавления админа при возврате в меню
    if 'adding_admin' in context.user_data:
        del context.user_data['adding_admin']
    
    admins_list = bot_manager.get_admins_list()
    
    text = "👥 *Управление админами*\n\n*Текущие админы:*\n"
    for admin_id in admins_list:
        if admin_id == ADMIN_ID:
            text += f"👑 `{admin_id}` (Главный админ)\n"
        else:
            text += f"👤 `{admin_id}`\n"
    
    keyboard = [
        [InlineKeyboardButton("➕ Добавить админа", callback_data="add_admin")],
        [InlineKeyboardButton("➖ Удалить админа", callback_data="remove_admin")],
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

async def add_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало процесса добавления админа"""
    query = update.callback_query
    await query.answer()
    
    if query.from_user.id != ADMIN_ID:
        return
    
    context.user_data['adding_admin'] = True
    
    keyboard = [
        [InlineKeyboardButton("🔙 Отмена", callback_data="manage_admins")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        "➕ *Добавление нового админа*\n\n"
        "Отправьте Telegram ID пользователя, которого хотите сделать админом.\n\n"
        "Пользователь может получить свой ID, отправив команду /start боту @userinfobot",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

async def remove_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаление админа"""
    query = update.callback_query
    await query.answer()
    
    if query.from_user.id != ADMIN_ID:
        return
    
    admins_list = [admin_id for admin_id in bot_manager.get_admins_list() if admin_id != ADMIN_ID]
    
    if not admins_list:
        await query.edit_message_text(
            "❌ Нет админов для удаления",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Назад", callback_data="manage_admins")
            ]])
        )
        return
    
    keyboard = []
    for admin_id in admins_list:
        keyboard.append([InlineKeyboardButton(
            f"❌ {admin_id}", 
            callback_data=f"confirm_remove_admin_{admin_id}"
        )])
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="manage_admins")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        "➖ *Удаление админа*\n\nВыберите админа для удаления:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

async def confirm_remove_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение удаления админа"""
    query = update.callback_query
    await query.answer()
    
    if query.from_user.id != ADMIN_ID:
        return
    
    # Правильно извлекаем admin_id
    # confirm_remove_admin_<admin_id> - убираем префикс
    admin_id = int(query.data[len("confirm_remove_admin_"):])
    
    if bot_manager.remove_admin(admin_id):
        await query.edit_message_text(
            f"✅ Админ `{admin_id}` успешно удален",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 К управлению админами", callback_data="manage_admins")
            ]])
        )
    else:
        await query.edit_message_text(
            "❌ Ошибка при удалении админа",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Назад", callback_data="manage_admins")
            ]])
        )

async def markdown_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Справка по Markdown разметке"""
    query = update.callback_query
    await query.answer()
    
    help_text = """ℹ️ *Справка по разметке*

📝 *Основное форматирование:*
• `*жирный текст*` → *жирный текст*
• `_курсив_` → _курсив_
• `` `код` `` → `код`
• `__подчеркнутый__` → подчеркнутый
• `~зачеркнутый~` → зачеркнутый

🔗 *Ссылки:*
• `[текст ссылки](https://example.com)`
• `[текст](www.site.com)` → добавит https://

🎨 *Комбинированное форматирование:*
• `[*жирная ссылка*](url)`
• `[_курсивная ссылка_](url)`
• `**альтернативный жирный**`

📌 *Пример использования:*
```
🌸 *Добро пожаловать!* 🌸

📋 *Наши услуги:*
• _Букеты на заказ_
• `Доставка` по городу

💐 *Популярные букеты:*
1. [*Розы*](https://example.com/roses)
2. [_Тюльпаны_](https://example.com/tulips)
```

💡 *Советы:*
• Бот автоматически конвертирует Markdown в HTML
• Если форматирование не работает, проверьте парные символы
• Можно комбинировать разные стили

"""
    
    keyboard = [
        [InlineKeyboardButton("🔙 Назад", callback_data="admin_panel")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        help_text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

async def broadcast_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 1):
    """Меню рассылки с пагинацией"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        await query.edit_message_text("⛔ Доступ запрещен")
        return
    
    logger.info(f"broadcast_menu called with page={page}")
    
    # Очищаем состояние
    context.user_data.clear()
    
    # Получаем общую статистику
    stats = db.get_global_stats()
    users_count = stats['active_users']
    bots_list = bot_manager.get_bots_list()
    
    # Фильтруем только запущенные боты
    running_bots = [(bot_id, bot_name, status) for bot_id, bot_name, status in bots_list if status == "running"]
    
    # Логируем информацию о ботах для отладки
    logger.info(f"Total bots: {len(bots_list)}, Running bots: {len(running_bots)}")
    for bot_id, bot_name, status in running_bots:
        logger.info(f"Running bot: {bot_id} ({bot_name}) - status: {status}")
    
    # Настройки пагинации
    BOTS_PER_PAGE = 3  # Количество ботов на странице
    total_bots = len(running_bots)
    total_pages = (total_bots + BOTS_PER_PAGE - 1) // BOTS_PER_PAGE  # Округление вверх
    
    # Проверяем корректность страницы
    if page < 1:
        page = 1
    elif page > total_pages:
        page = total_pages
    
    # Вычисляем индексы для текущей страницы
    start_idx = (page - 1) * BOTS_PER_PAGE
    end_idx = min(start_idx + BOTS_PER_PAGE, total_bots)
    
    # Боты для текущей страницы
    current_page_bots = running_bots[start_idx:end_idx]
    
    # Формируем кнопки для ботов на текущей странице
    keyboard = []
    
    for bot_id, bot_name, status in current_page_bots:
        bot_data = bot_manager.data["bots"].get(bot_id, {})
        username = bot_data.get("username")
        
        if bot_id == "main":
            # Главный бот
            main_bot_username = bot_data.get("username")
            if main_bot_username:
                button_text = f"📤 @{main_bot_username} (главный)"
            else:
                button_text = "📤 Главный бот"
            callback_data = "broadcast_start_main"
            logger.info(f"Creating button for MAIN bot with callback_data: {callback_data}")
            keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        else:
            # Дочерние боты
            if username:
                button_text = f"📤 @{username}"
            else:
                button_text = f"📤 {bot_name}"
            callback_data = f"broadcast_bot_{bot_id}"
            logger.info(f"Creating button for bot '{bot_id}' (name: {bot_name}) with callback_data: {callback_data}")
            keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
    
    # Добавляем кнопки навигации по страницам
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"broadcast_page_{page-1}"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton("Далее ▶️", callback_data=f"broadcast_page_{page+1}"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Добавляем дополнительные кнопки
    keyboard.extend([
        [InlineKeyboardButton("📊 Статистика пользователей", callback_data="user_stats")],
        [InlineKeyboardButton("🔙 К админ-панели", callback_data="admin_panel")]
    ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Формируем текст БЕЗ Markdown форматирования чтобы избежать ошибок
    text = f"📢 Панель рассылки\n\n"
    text += f"👥 Всего активных пользователей: {users_count}\n"
    text += f"🤖 Всего запущенных ботов: {total_bots}\n\n"
    
    if total_pages > 1:
        text += f"📋 Боты для рассылки (стр. {page}/{total_pages}):\n\n"
    else:
        text += f"📋 Боты для рассылки:\n\n"
    
    # Показываем информацию только о ботах на текущей странице
    for bot_id, bot_name, status in current_page_bots:
        bot_data = bot_manager.data["bots"].get(bot_id, {})
        username = bot_data.get("username")
        bot_stats = db.get_bot_stats(bot_id)
        
        if bot_id == "main":
            if username:
                text += f"🟢 @{username} (главный)\n"
            else:
                text += f"🟢 Главный бот\n"
        else:
            # НЕ экранируем, так как отправляем без parse_mode
            text += f"🟢 {bot_name}\n"
            if username:
                text += f"   └ @{username}\n"
        
        text += f"   └ Пользователей: {bot_stats['active']}\n\n"
    
    text += "💡 Подсказка: Выберите бота для рассылки"
    
    # Отправляем БЕЗ parse_mode чтобы избежать ошибок форматирования
    await query.edit_message_text(
        text,
        reply_markup=reply_markup
    )

async def broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало рассылки - запрос текста"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    # Логируем callback_data для отладки
    logger.info(f"broadcast_start called with callback_data: {query.data}")
    
    # Определяем от какого бота будет рассылка
    if query.data == "broadcast_start_main":
        bot_id = "main"
        # Получаем имя главного бота
        main_bot_data = bot_manager.data["bots"].get("main", {})
        bot_name = main_bot_data.get("name", "Главный бот")
    elif query.data.startswith("broadcast_bot_"):
        # Извлекаем bot_id после "broadcast_bot_"
        bot_id = query.data.replace("broadcast_bot_", "")
        logger.info(f"Extracted bot_id: '{bot_id}' from callback_data: '{query.data}'")
        logger.info(f"Available bots: {list(bot_manager.data['bots'].keys())}")
        
        # Проверяем, существует ли бот
        if bot_id not in bot_manager.data["bots"]:
            logger.error(f"Bot '{bot_id}' not found in bot_manager.data")
            logger.error(f"Available bot IDs: {list(bot_manager.data['bots'].keys())}")
            await query.edit_message_text(
                f"❌ Бот с ID `{bot_id}` не найден\n\nДоступные боты: {', '.join(bot_manager.data['bots'].keys())}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Назад", callback_data="broadcast_menu")
                ]])
            )
            return
            
        bot_data = bot_manager.data["bots"][bot_id]
        bot_name = bot_data.get("name", "Неизвестный бот")
        logger.info(f"Bot found: {bot_name} (ID: {bot_id})")
    else:
        # Неизвестный формат callback_data
        logger.error(f"Unknown callback_data format: {query.data}")
        await query.edit_message_text(
            "❌ Ошибка: неизвестный формат данных",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Назад", callback_data="broadcast_menu")
            ]])
        )
        return
    
    context.user_data['broadcast_step'] = 'text'
    context.user_data['broadcast_bot_id'] = bot_id
    context.user_data['broadcast_bot_name'] = bot_name
    
    keyboard = [
        [InlineKeyboardButton("🔙 Отмена", callback_data="broadcast_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Получаем количество пользователей для конкретного бота
    bot_stats = db.get_bot_stats(bot_id)
    users_count = bot_stats['active']
    
    await query.edit_message_text(
        f"📝 *Создание рассылки*\n\n"
        f"🤖 *От бота:* {bot_name}\n"
        f"🆔 *ID бота:* `{bot_id}`\n"
        f"👥 *Получателей:* {users_count}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📤 Отправьте контент для рассылки:\n\n"
        f"• 📝 *Текст* - обычное текстовое сообщение\n"
        f"• 🖼 *Фото с подписью* - перетащите фото и добавьте текст\n\n"
        f"🎨 *Поддерживается форматирование:*\n"
        f"• \\*жирный\\* → *жирный*\n"
        f"• \\_курсив\\_ → _курсив_\n"
        f"• \\[текст\\]\\(ссылка\\) → ссылка",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

async def broadcast_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправка рассылки"""
    query = update.callback_query
    await query.answer("⏳ Начинаю рассылку...")
    
    if not is_admin(query.from_user.id):
        return
    
    broadcast_text = context.user_data.get('broadcast_text')
    broadcast_type = context.user_data.get('broadcast_type', 'text')
    broadcast_photo = context.user_data.get('broadcast_photo')
    bot_id = context.user_data.get('broadcast_bot_id', 'main')
    # Получаем имя бота для отображения
    if bot_id == "main":
        main_bot_data = bot_manager.data["bots"].get("main", {})
        bot_name = main_bot_data.get("name", "Главный бот")
    else:
        bot_data = bot_manager.data["bots"].get(bot_id, {})
        bot_name = bot_data.get("name", "Неизвестный бот")
    
    if broadcast_type == 'text' and not broadcast_text:
        await query.edit_message_text("❌ Текст рассылки не найден")
        return
    elif broadcast_type == 'photo' and not broadcast_photo:
        # Для фото используем edit_message_caption вместо edit_message_text
        if query.message and query.message.caption:
            await query.edit_message_caption(caption="❌ Фото для рассылки не найдено")
        else:
            await query.edit_message_text(text="❌ Фото для рассылки не найдено")
        return
    
    # Получаем активных пользователей для этого бота
    try:
        users = db.get_bot_users(bot_id, only_active=True)
    except Exception as e:
        logger.error(f"Ошибка получения пользователей: {e}")
        await query.edit_message_text(f"❌ Ошибка получения списка пользователей: {e}")
        return
    
    user_ids = [user['user_id'] if isinstance(user, dict) else user for user in users]
    total_users = len(user_ids)
    
    # Показываем начальный прогресс БЕЗ кнопки остановки (подготовка)
    progress_text = f"⏳ *Подготовка рассылки...*\n\n" \
                    f"🤖 *От бота:* {bot_name}\n" \
                    f"👥 *Получателей:* {total_users}\n" \
                    f"⚙️ *Инициализация воркеров...*\n\n" \
                    f"_Пожалуйста, подождите начала рассылки_"
    
    # Сохраняем сообщение для обновления БЕЗ кнопки
    if broadcast_type == 'photo' and query.message and query.message.caption is not None:
        progress_message = await query.edit_message_caption(
            caption=progress_text,
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        progress_message = await query.edit_message_text(
            text=progress_text,
            parse_mode=ParseMode.MARKDOWN
        )
    
    # Сохраняем ID сообщения для обновления из другого callback
    context.user_data['broadcast_message_id'] = progress_message.message_id
    context.user_data['broadcast_chat_id'] = progress_message.chat.id
    
    # Определяем какого бота использовать для отправки
    if bot_id == "main":
        bot_token = MAIN_BOT_TOKEN
    else:
        # Для дочерних ботов используем их токен
        bot_token = bot_manager.get_bot_token(bot_id)
        if not bot_token:
            await query.edit_message_text("❌ Токен бота не найден")
            return
    
    # Подготавливаем фото если есть
    photo_bytes = None
    if broadcast_type == 'photo':
        try:
            logger.info(f"Скачиваю фото для рассылки в боте {bot_id}")
            file = await context.bot.get_file(broadcast_photo)
            photo_bytes = await file.download_as_bytearray()
            logger.info(f"Фото успешно подготовлено для бота {bot_id}, размер: {len(photo_bytes)} байт")
        except Exception as e:
            logger.error(f"Ошибка скачивания фото для рассылки: {e}")
            await query.edit_message_text(f"❌ Ошибка подготовки фото для рассылки: {e}")
            return
    
    # Создаем переменные для управления рассылкой
    context.user_data['broadcast_active'] = True
    context.user_data['broadcast_started'] = False  # Флаг начала рассылки
    
    # Переменные для отслеживания прогресса
    sent_count = 0
    last_update_time = time.time()
    first_message_sent = False  # Флаг первого сообщения
    
    # Callback для обновления прогресса с дополнительной информацией
    async def progress_callback(current, total, progress_info=None):
        nonlocal sent_count, last_update_time, first_message_sent
        
        # Проверяем, не была ли рассылка остановлена
        if context.user_data.get('broadcast_stopped', False):
            logger.info(f"Broadcast stop detected in progress callback at {current}/{total}")
            return
            
        sent_count = current
        context.user_data['last_processed'] = current  # Сохраняем для статистики при остановке
        remaining = total - current
        
        # При первом сообщении обновляем интерфейс и добавляем кнопку остановки
        if not first_message_sent:
            first_message_sent = True
            context.user_data['broadcast_started'] = True
            
            # Теперь показываем кнопку остановки
            stop_keyboard = [
                [InlineKeyboardButton("🛑 ОСТАНОВИТЬ РАССЫЛКУ", callback_data="stop_broadcast")]
            ]
            reply_markup = InlineKeyboardMarkup(stop_keyboard)
            
            initial_text = f"🚀 *Рассылка началась!*\n\n" \
                          f"🤖 *От бота:* {bot_name}\n" \
                          f"👥 *Всего пользователей:* {total}\n" \
                          f"✅ *Отправлено:* 1/{total}\n" \
                          f"📊 *Прогресс:* [▓░░░░░░░░░] начало...\n\n" \
                          f"_Теперь вы можете остановить рассылку_"
            
            try:
                if broadcast_type == 'photo' and progress_message.caption is not None:
                    await progress_message.edit_caption(
                        caption=initial_text,
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=reply_markup
                    )
                else:
                    await progress_message.edit_text(
                        text=initial_text,
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=reply_markup
                    )
            except:
                pass
        
        # Обновляем сообщение каждую секунду
        current_time = time.time()
        if current_time - last_update_time >= 1.0 or current == total:
            last_update_time = current_time
            
            progress_percent = (current / total * 100) if total > 0 else 0
            progress_bar = "▓" * int(progress_percent / 10) + "░" * (10 - int(progress_percent / 10))
            
            # Вычисляем скорость
            elapsed = current_time - start_time.timestamp() if hasattr(start_time, 'timestamp') else 1
            speed = current / elapsed if elapsed > 0 else 0
            
            # Используем дополнительную информацию если есть
            if progress_info:
                failed_count = progress_info.get('failed', 0)
                blocked_count = progress_info.get('blocked', 0)
                new_progress_text = f"📤 *Рассылка в процессе*\n\n" \
                                   f"🤖 *От бота:* {bot_name}\n" \
                                   f"👥 *Отправка {total} пользователям...*\n" \
                                   f"📊 *Осталось отправить:* {remaining}\n" \
                                   f"📈 *Прогресс:* [{progress_bar}] {progress_percent:.1f}%\n" \
                                   f"✅ *Отправлено:* {current}\n" \
                                   f"❌ *Ошибок:* {failed_count}\n" \
                                   f"🚫 *Заблокировали:* {blocked_count}\n" \
                                   f"⚡ *Скорость:* {speed:.1f} сообщ/сек\n" \
                                   f"🚀 *Метод:* Быстрая пересылка"
            else:
                new_progress_text = f"📤 *Рассылка в процессе*\n\n" \
                                   f"🤖 *От бота:* {bot_name}\n" \
                                   f"👥 *Отправка {total} пользователям...*\n" \
                                   f"📊 *Осталось отправить:* {remaining}\n" \
                                   f"📈 *Прогресс:* [{progress_bar}] {progress_percent:.1f}%\n" \
                                   f"✅ *Отправлено:* {current}\n" \
                                   f"⚡ *Скорость:* {speed:.1f} сообщ/сек\n" \
                                   f"🚀 *Метод:* Быстрая пересылка"
            
            # Кнопка остановки (показываем только если рассылка началась)
            if context.user_data.get('broadcast_started', False):
                stop_keyboard = [
                    [InlineKeyboardButton("🛑 ОСТАНОВИТЬ РАССЫЛКУ", callback_data="stop_broadcast")]
                ]
                reply_markup = InlineKeyboardMarkup(stop_keyboard)
            else:
                reply_markup = None
            
            try:
                if broadcast_type == 'photo' and progress_message.caption is not None:
                    await progress_message.edit_caption(
                        caption=new_progress_text,
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=reply_markup
                    )
                else:
                    await progress_message.edit_text(
                        text=new_progress_text,
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=reply_markup
                    )
            except Exception as e:
                # Игнорируем ошибки обновления
                pass
    
    # Используем оптимизированную рассылку с воркерами
    start_time = datetime.now()
    
    # Создаем флаг для управления рассылкой
    context.user_data['broadcast_active'] = True
    context.user_data['broadcast_stopped'] = False
    context.user_data['broadcast_results'] = []
    
    # Функция проверки остановки
    def check_if_stopped():
        return context.user_data.get('broadcast_stopped', False)
    
    # Запускаем рассылку асинхронно
    async def run_broadcast():
        try:
            # Запускаем рассылку с воркерами и функцией проверки остановки
            result = await OptimizedBroadcast.send_broadcast(
                bot_token=bot_token,
                users=user_ids,
                text=broadcast_text if broadcast_type == 'text' else None,
                photo=photo_bytes if broadcast_type == 'photo' else None,
                photo_caption=broadcast_text if broadcast_type == 'photo' else None,
                parse_mode=ParseMode.HTML,
                progress_callback=progress_callback,
                auto_optimize=True,  # Автоматически выбираем количество воркеров
                stop_check=check_if_stopped,  # Передаем функцию проверки остановки
                template_chat_id=ADMIN_ID,  # Используем ID админа как специальный чат для шаблона
                bot_name=bot_name  # Передаем имя бота для отчетов
            )
            
            # Проверяем, была ли рассылка остановлена
            if context.user_data.get('broadcast_stopped', False):
                logger.info("Broadcast was stopped by user")
                return None  # Возвращаем None если рассылка была остановлена
            
            # Обновляем заблокированных пользователей в БД
            for res in result['results']:
                if res.is_blocked:
                    db.block_user(res.user_id, bot_id)
            
            # Показываем финальную статистику
            await show_broadcast_results(progress_message, result, bot_name, broadcast_type, context)
            
            return result
        except Exception as e:
            logger.error(f"Ошибка в run_broadcast: {e}")
            # При ошибке показываем сообщение об ошибке
            try:
                error_text = f"❌ *Ошибка рассылки*\n\n{str(e)}"
                if broadcast_type == 'photo' and progress_message.caption is not None:
                    await progress_message.edit_caption(
                        caption=error_text,
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("🔙 К админ-панели", callback_data="admin_panel")
                        ]])
                    )
                else:
                    await progress_message.edit_text(
                        text=error_text,
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("🔙 К админ-панели", callback_data="admin_panel")
                        ]])
                    )
            except:
                pass
            raise
    
    # Функция для показа результатов рассылки
    async def show_broadcast_results(message, result, bot_name, broadcast_type, context):
        """Показывает финальные результаты рассылки"""
        # Проверяем, была ли рассылка остановлена пользователем
        was_cancelled = context.user_data.get('broadcast_stopped', False)
        
        # Очищаем данные рассылки
        keys_to_clear = ['broadcast_active', 'broadcast_stopped', 'broadcast_message_id',
                        'broadcast_chat_id', 'broadcast_text', 'broadcast_type',
                        'broadcast_photo', 'broadcast_bot_id', 'broadcast_bot_name']
        for key in keys_to_clear:
            context.user_data.pop(key, None)
        
        # После успешной рассылки показываем только кнопку возврата к админ-панели
        keyboard = [
            [InlineKeyboardButton("🔙 К админ-панели", callback_data="admin_panel")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Формируем статистику
        percent = (result['success'] / result['total'] * 100) if result['total'] > 0 else 0
        speedup = result['workers_used']
        time_saved = result['total_time'] * (speedup - 1) / speedup  # Примерное время сэкономленное
        
        # Определяем статус завершения
        if was_cancelled:
            status_text = "🛑 *Рассылка остановлена!*"
        else:
            status_text = "✅ *Рассылка завершена!*"
        
        # Финальное сообщение со статистикой с именем бота из результата
        bot_display = result.get('bot_name', bot_name)
        result_text = f"{status_text}\n\n" \
                      f"🤖 *Бот:* {bot_display}\n" \
                      f"📊 *Статистика:*\n" \
                      f"✅ Успешно: *{result['success']}*\n" \
                      f"❌ Ошибок: *{result['failed']}*\n" \
                      f"🚫 Заблокировали: *{result['blocked']}*\n" \
                      f"📈 Всего: *{result['total']}*\n\n" \
                      f"⏱ Время: *{result['total_time']:.1f}* сек\n" \
                      f"⚡ Ускорение: *{speedup}x* (воркеров: {result['workers_used']})\n" \
                      f"💾 Сэкономлено: ~*{time_saved:.1f}* сек\n" \
                      f"📨 Скорость: *{result['messages_per_second']:.1f}* сообщ/сек\n\n" \
                      f"Процент доставки: *{percent:.1f}%*"
        
        try:
            if broadcast_type == 'photo' and message.caption is not None:
                await message.edit_caption(
                    caption=result_text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=reply_markup
                )
            else:
                await message.edit_text(
                    text=result_text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=reply_markup
                )
        except:
            # Если не получилось отредактировать, отправляем новое сообщение
            await message.reply_text(
                result_text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
    
    # Запускаем рассылку асинхронно без блокировки
    asyncio.create_task(run_broadcast())
    
    # НЕ ждем результат - функция завершается сразу, позволяя боту продолжить работу
    logger.info("Broadcast task started asynchronously")

async def stop_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Остановка активной рассылки"""
    query = update.callback_query
    
    if not is_admin(query.from_user.id):
        try:
            await query.answer("❌ У вас нет доступа", show_alert=True)
        except:
            pass  # Игнорируем ошибки устаревшего query
        return
    
    # Проверяем, активна ли рассылка И она уже началась
    if context.user_data.get('broadcast_active') and context.user_data.get('broadcast_started') and not context.user_data.get('broadcast_stopped'):
        logger.info("Stop broadcast requested by admin")
        
        # Устанавливаем флаг остановки
        context.user_data['broadcast_stopped'] = True
        context.user_data['broadcast_active'] = False
        
        try:
            await query.answer(
                "🛑 Рассылка останавливается!\nПодождите несколько секунд...",
                show_alert=True
            )
        except:
            pass  # Игнорируем ошибки устаревшего query
        
        # Получаем информацию о сообщении для обновления
        message_id = context.user_data.get('broadcast_message_id')
        chat_id = context.user_data.get('broadcast_chat_id')
        
        if message_id and chat_id:
            try:
                # Ждем немного, чтобы воркеры успели остановиться
                await asyncio.sleep(3)
                
                # Получаем количество отправленных сообщений
                successful = context.user_data.get('last_processed', 0)
                
                # Кнопка возврата к админ панели
                back_keyboard = [
                    [InlineKeyboardButton("🔙 Вернуться к админ панели", callback_data="admin_panel")]
                ]
                reply_markup = InlineKeyboardMarkup(back_keyboard)
                
                # Получаем имя бота из контекста
                stopped_bot_name = context.user_data.get('broadcast_bot_name', 'Неизвестный бот')
                
                # Формируем сообщение о завершении с именем бота
                stop_time = datetime.now()
                final_text = (
                    f"🛑 **РАССЫЛКА ОСТАНОВЛЕНА!**\n\n"
                    f"🤖 **Бот:** {stopped_bot_name}\n"
                    f"📊 **Статистика на момент остановки:**\n"
                    f"├ 📤 Отправлено: **{successful}** сообщений\n"
                    f"├ ⏱ Остановлено: {stop_time.strftime('%H:%M:%S')}\n"
                    f"└ 📌 Статус: Остановлено администратором\n\n"
                    f"_Рассылка была прервана по вашему запросу._"
                )
                
                # Пробуем обновить сообщение
                try:
                    # Если это было сообщение с фото
                    if context.user_data.get('broadcast_type') == 'photo':
                        await query.bot.edit_message_caption(
                            chat_id=chat_id,
                            message_id=message_id,
                            caption=final_text,
                            parse_mode=ParseMode.MARKDOWN,
                            reply_markup=reply_markup
                        )
                    else:
                        # Обычное текстовое сообщение
                        await query.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=message_id,
                            text=final_text,
                            parse_mode=ParseMode.MARKDOWN,
                            reply_markup=reply_markup
                        )
                except Exception as edit_error:
                    logger.warning(f"Could not edit message: {edit_error}")
                    # Если не удалось отредактировать, отправляем новое
                    await query.message.reply_text(
                        final_text,
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=reply_markup
                    )
                
                logger.info(f"Broadcast stopped. Sent {successful} messages")
                
            except Exception as e:
                logger.error(f"Error in stop_broadcast: {e}")
                await query.message.reply_text(
                    f"⚠️ Рассылка остановлена, но произошла ошибка: {e}"
                )
        else:
            # Если нет информации о сообщении, просто уведомляем
            await query.message.reply_text(
                "🛑 Рассылка остановлена!",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 К админ панели", callback_data="admin_panel")
                ]])
            )
        
        # Очищаем все данные рассылки
        keys_to_remove = [
            'broadcast_active', 'broadcast_stopped', 'broadcast_message_id',
            'broadcast_chat_id', 'broadcast_results', 'last_processed',
            'broadcast_text', 'broadcast_type', 'broadcast_photo',
            'broadcast_bot_id', 'broadcast_bot_name'
        ]
        for key in keys_to_remove:
            context.user_data.pop(key, None)
        
    else:
        # Проверяем причину недоступности кнопки
        if not context.user_data.get('broadcast_started'):
            # Рассылка еще не началась
            try:
                await query.answer("⏳ Рассылка еще не началась, подождите...", show_alert=True)
            except:
                pass
        else:
            # Рассылка уже завершилась
            try:
                await query.answer("❌ Рассылка уже завершена", show_alert=True)
            except Exception as e:
                # Если query устарел, просто обновляем сообщение
                logger.warning(f"Query expired: {e}")
                try:
                    await query.message.reply_text(
                        "ℹ️ Рассылка уже завершена",
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("🔙 К админ панели", callback_data="admin_panel")
                        ]])
                    )
                except:
                    pass

async def broadcast_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена рассылки"""
    query = update.callback_query
    await query.answer("Рассылка отменена")
    
    context.user_data.clear()
    
    keyboard = [
        [InlineKeyboardButton("📢 Создать новую", callback_data="broadcast_menu")],
        [InlineKeyboardButton("🔙 К админ-панели", callback_data="admin_panel")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    cancel_text = "❌ *Рассылка отменена*\n\n" \
                  "Сообщение не было отправлено пользователям."
    
    # Определяем тип сообщения и используем правильный метод
    if query.message.text:
        # Это текстовое сообщение
        try:
            await query.edit_message_text(
                cancel_text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
        except Exception as e:
            logger.warning(f"Не удалось отредактировать текстовое сообщение при отмене: {e}")
            # Отправляем новое сообщение
            await query.message.reply_text(
                cancel_text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
            try:
                await query.message.delete()
            except:
                pass
    elif query.message.caption is not None:
        # Это сообщение с медиа (фото) и подписью
        try:
            await query.edit_message_caption(
                caption=cancel_text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
        except Exception as e:
            logger.warning(f"Не удалось отредактировать подпись при отмене: {e}")
            # Отправляем новое сообщение
            await query.message.reply_text(
                cancel_text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=reply_markup
            )
            try:
                await query.message.delete()
            except:
                pass
    else:
        # Неизвестный тип сообщения, просто отправляем новое
        await query.message.reply_text(
            cancel_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
        try:
            await query.message.delete()
        except:
            pass

async def broadcast_cancel_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена рассылки с фото"""
    query = update.callback_query
    await query.answer("Рассылка отменена")
    
    context.user_data.clear()
    
    keyboard = [
        [InlineKeyboardButton("📢 Создать новую", callback_data="broadcast_menu")],
        [InlineKeyboardButton("🔙 К админ-панели", callback_data="admin_panel")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    cancel_text = "❌ *Рассылка отменена*\n\n" \
                  "Сообщение не было отправлено пользователям."
    
    # Для фото используем edit_message_caption
    try:
        await query.edit_message_caption(
            caption=cancel_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.warning(f"Не удалось отредактировать подпись фото при отмене: {e}")
        # Если не получилось, отправляем новое сообщение
        await query.message.reply_text(
            cancel_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
        try:
            await query.message.delete()
        except:
            pass

async def broadcast_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Редактирование текста рассылки"""
    query = update.callback_query
    await query.answer()
    
    context.user_data['broadcast_step'] = 'text'
    
    keyboard = [
        [InlineKeyboardButton("🔙 Отмена", callback_data="broadcast_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    edit_text = "✏️ *Редактирование рассылки*\n\n" \
                "Отправьте новый контент для рассылки:\n" \
                "• Текстовое сообщение\n" \
                "• Фото с подписью"
    
    try:
        await query.edit_message_text(
            edit_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.warning(f"Не удалось отредактировать сообщение при редактировании: {e}")
        # Отправляем новое сообщение
        await query.message.reply_text(
            edit_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
        try:
            await query.message.delete()
        except:
            pass

async def broadcast_edit_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Редактирование рассылки с фото"""
    query = update.callback_query
    await query.answer()
    
    # Сохраняем фото для возможности повторного использования
    if 'broadcast_photo' in context.user_data:
        context.user_data['saved_photo'] = context.user_data['broadcast_photo']
    
    context.user_data['broadcast_step'] = 'text'
    
    keyboard = [
        [InlineKeyboardButton("🔙 Отмена", callback_data="broadcast_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Отправляем новое сообщение вместо редактирования
    await query.message.reply_text(
        "✏️ *Редактирование рассылки*\n\n"
        "Отправьте новый контент для рассылки:\n"
        "• Текстовое сообщение\n"
        "• Фото с подписью\n\n"
        "💡 _Для сохранения текущего фото просто отправьте новый текст_",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )
    
    # Удаляем старое сообщение с фото
    try:
        await query.message.delete()
    except:
        pass

async def user_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Статистика пользователей"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    # Получаем глобальную статистику
    global_stats = db.get_global_stats()
    
    # Формируем текст статистики без Markdown
    stats_text = f"📊 Общая статистика пользователей\n\n"
    stats_text += f"👥 Уникальных пользователей: {global_stats['unique_users']}\n"
    stats_text += f"✅ Активных: {global_stats['active_users']}\n"
    stats_text += f"🚫 Заблокировали: {global_stats['unique_users'] - global_stats['active_users']}\n\n"
    
    # Статистика по ботам
    if global_stats['bot_stats']:
        stats_text += "Статистика по ботам:\n"
        for bot_id, count in global_stats['bot_stats'].items():
            bot_name = bot_manager.data["bots"].get(bot_id, {}).get("name", bot_id)
            stats_text += f"• {bot_name}: {count} активных\n"
    
    # Получаем последних пользователей главного бота
    recent_users = db.get_bot_users('main', only_active=False)[:5]
    if recent_users:
        stats_text += "\n🆕 Последние пользователи (главный бот):\n"
        for user in recent_users:
            # Получаем данные пользователя
            first_name = user.get('first_name', '') or ''
            last_name = user.get('last_name', '') or ''
            name = f"{first_name} {last_name}".strip() or "Без имени"
            username = user.get('username')
            status = "🚫" if user.get('is_blocked') else "✅"
            
            # Формируем строку без форматирования
            if username:
                stats_text += f"{status} {name} - @{username}\n"
            else:
                stats_text += f"{status} {name} - без username\n"
    
    keyboard = [
        [InlineKeyboardButton("📢 Начать рассылку", callback_data="broadcast_start_main")],
        [InlineKeyboardButton("📊 Статистика по ботам", callback_data="bot_stats_menu")],
        [InlineKeyboardButton("🔙 Назад", callback_data="broadcast_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Отправляем без parse_mode чтобы избежать ошибок форматирования
    await query.edit_message_text(
        stats_text,
        reply_markup=reply_markup
    )

async def bot_stats_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню статистики по ботам"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    bots_list = bot_manager.get_bots_list()
    keyboard = []
    
    # Используем простое форматирование без Markdown
    text = "📊 Статистика по ботам\n\n"
    
    for bot_id, bot_name, status in bots_list:
        try:
            stats = db.get_bot_stats(bot_id)
        except Exception as e:
            logger.error(f"Ошибка получения статистики для бота {bot_id}: {e}")
            stats = {'total': 0, 'active': 0, 'blocked': 0, 'with_username': 0, 'premium': 0}
        
        premium_percent = (stats['premium'] / stats['active'] * 100) if stats['active'] > 0 else 0
        
        text += f"🤖 {bot_name}\n"
        text += f"   └ Всего: {stats['total']}\n"
        text += f"   └ Активных: {stats['active']}\n"
        text += f"   └ Заблокировали: {stats['blocked']}\n"
        text += f"   └ С username: {stats['with_username']}\n"
        text += f"   └ ⭐ Премиум: {stats['premium']} ({premium_percent:.1f}%)\n\n"
        
        if stats['total'] > 0:
            keyboard.append([
                InlineKeyboardButton(
                    f"📊 {bot_name}",
                    callback_data=f"bot_detailed_stats_{bot_id}"
                )
            ])
    
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="user_stats")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Отправляем без parse_mode чтобы избежать ошибок форматирования
    await query.edit_message_text(
        text,
        reply_markup=reply_markup
    )

async def bot_detailed_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Детальная статистика по конкретному боту"""
    query = update.callback_query
    await query.answer()
    
    if not is_admin(query.from_user.id):
        return
    
    # Правильно извлекаем bot_id, учитывая что он может содержать подчеркивания
    bot_id = query.data[len("bot_detailed_stats_"):]
    bot_name = bot_manager.data["bots"].get(bot_id, {}).get("name", "Неизвестный бот")
    
    try:
        stats = db.get_bot_stats(bot_id)
        users = db.get_bot_users(bot_id, only_active=False)[:10]
    except Exception as e:
        logger.error(f"Ошибка получения статистики для бота {bot_id}: {e}")
        await query.edit_message_text(
            f"❌ Ошибка получения статистики для бота {bot_name}",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Назад", callback_data="bot_stats_menu")
            ]])
        )
        return
    
    # Используем простое форматирование без Markdown
    premium_percent = (stats['premium'] / stats['active'] * 100) if stats['active'] > 0 else 0
    
    text = f"📊 Статистика бота: {bot_name}\n\n"
    text += f"📈 Общие показатели:\n"
    text += f"• Всего пользователей: {stats['total']}\n"
    text += f"• Активных: {stats['active']}\n"
    text += f"• Заблокировали: {stats['blocked']}\n"
    text += f"• С username: {stats['with_username']}\n"
    text += f"• ⭐ Премиум: {stats['premium']} ({premium_percent:.1f}%)\n\n"
    
    if users:
        text += "Последние 10 пользователей:\n"
        for user in users:
            # Получаем данные пользователя
            first_name = user.get('first_name', '') or ''
            last_name = user.get('last_name', '') or ''
            name = f"{first_name} {last_name}".strip() or "Без имени"
            username = user.get('username')
            status = "🚫" if user.get('is_blocked') else "✅"
            
            # Формируем строку без форматирования
            if username:
                text += f"{status} {name} - @{username}\n"
            else:
                text += f"{status} {name} - ID: {user['user_id']}\n"
    
    keyboard = [
        [InlineKeyboardButton(f"📢 Рассылка от {bot_name}", callback_data=f"broadcast_bot_{bot_id}")],
        [InlineKeyboardButton("🔙 Назад", callback_data="bot_stats_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Отправляем без parse_mode чтобы избежать ошибок форматирования
    await query.edit_message_text(
        text,
        reply_markup=reply_markup
    )

async def ignore_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик для кнопок, которые не должны ничего делать"""
    query = update.callback_query
    await query.answer()
    # Просто отвечаем на callback без изменений

async def back_to_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Возврат к стартовому сообщению"""
    query = update.callback_query
    await query.answer()
    
    # Очищаем все состояние пользователя при возврате к старту
    context.user_data.clear()
    
    user = query.from_user
    
    if is_admin(user.id):
        keyboard = [[InlineKeyboardButton("🔧 Админка", callback_data="admin_panel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Получаем username главного бота для отображения
        main_bot_data = bot_manager.data["bots"].get("main", {})
        bot_username = main_bot_data.get("username")
        if bot_username:
            bot_display_name = f"@{bot_username}"
        else:
            bot_display_name = "Бот управления"
        
        await query.edit_message_text(
            f"🤖 *{bot_display_name}*\n\n"
            "Добро пожаловать в панель управления ботами!\n\n"
            "🔧 _Доступна админка для управления ботами_",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=reply_markup
        )
    else:
        # показываем текст, который админ задал для главного бота
        start_text = bot_manager.get_bot_text("main")
        try:
            html_text = markdown_to_html(start_text)
            await query.edit_message_text(
                html_text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.warning(f"Ошибка отправки с HTML в back_to_start: {e}")
            try:
                await query.edit_message_text(
                    start_text,
                    parse_mode=ParseMode.MARKDOWN,
                    disable_web_page_preview=True
                )
            except Exception:
                # Если ошибка с Markdown, отправляем без форматирования
                await query.edit_message_text(
                    start_text,
                    disable_web_page_preview=True
                )

async def cleanup_old_forwarded_messages():
    """Периодическая очистка старых пересланных сообщений из памяти"""
    while True:
        try:
            await asyncio.sleep(1800)  # Очистка каждые 30 минут для лучшей производительности
            
            current_time = datetime.now()
            messages_to_remove = []
            
            # Находим сообщения старше 12 часов (уменьшаем время хранения)
            for message_id, message_info in list(forwarded_messages.items()):
                if 'timestamp' in message_info:
                    message_time = message_info['timestamp']
                    if current_time - message_time > timedelta(hours=12):
                        messages_to_remove.append(message_id)
            
            # Удаляем старые сообщения
            for message_id in messages_to_remove:
                if message_id in forwarded_messages:
                    del forwarded_messages[message_id]
            
            if messages_to_remove:
                logger.info(f"Очищено {len(messages_to_remove)} старых пересланных сообщений из памяти")
            
            # Также проверяем размер словаря
            if len(forwarded_messages) > MAX_FORWARDED_MESSAGES:
                # Сортируем по времени и удаляем самые старые
                sorted_messages = sorted(
                    forwarded_messages.items(),
                    key=lambda x: x[1].get('timestamp', datetime.min)
                )
                to_remove = len(forwarded_messages) - MAX_FORWARDED_MESSAGES + 100
                for msg_id, _ in sorted_messages[:to_remove]:
                    del forwarded_messages[msg_id]
                logger.info(f"Очищено {to_remove} сообщений из-за превышения лимита")
                
        except Exception as e:
            logger.error(f"Ошибка при очистке памяти: {e}")

async def restore_running_bots():
    """Восстановление работающих ботов при запуске"""
    running_bots_file = "running_bots.json"
    
    try:
        if os.path.exists(running_bots_file):
            with open(running_bots_file, 'r') as f:
                saved_running_bots = json.load(f)
                
            logger.info(f"Найдены сохраненные боты для восстановления: {saved_running_bots}")
            
            # ВАЖНО: Восстанавливаем боты последовательно с большими задержками
            for bot_id in saved_running_bots:
                if bot_id != "main" and bot_id in [b[0] for b in bot_manager.get_bots_list()]:
                    token = bot_manager.get_bot_token(bot_id)
                    if token:
                        logger.info(f"Восстанавливаю бота {bot_id}")
                        # Запускаем бота и ЖДЕМ завершения
                        success = await start_bot(bot_id, token)
                        if success:
                            logger.info(f"✅ Бот {bot_id} восстановлен")
                        else:
                            logger.error(f"❌ Не удалось восстановить бота {bot_id}")
                        # Большая задержка между ботами
                        await asyncio.sleep(20)
        else:
            logger.info("Нет сохраненных запущенных ботов для восстановления")
    except Exception as e:
        logger.error(f"Ошибка при восстановлении ботов: {e}")

def signal_handler(signum, frame):
    """Обработчик сигналов для корректного завершения"""
    logger.info("Получен сигнал завершения, останавливаю всех ботов...")
    
    # Останавливаем всех ботов
    for bot_id in list(active_bot_instances.keys()):
        try:
            if bot_id in active_bot_instances:
                bot_info = active_bot_instances[bot_id]
                app = bot_info['app']
                loop = bot_info['loop']
                
                # Планируем остановку в соответствующем loop
                if not loop.is_closed():
                    try:
                        asyncio.run_coroutine_threadsafe(app.updater.stop(), loop)
                        asyncio.run_coroutine_threadsafe(app.stop(), loop)
                        asyncio.run_coroutine_threadsafe(app.shutdown(), loop)
                    except Exception as stop_error:
                        logger.error(f"Ошибка при планировании остановки: {stop_error}")
        except Exception as e:
            logger.error(f"Ошибка остановки бота {bot_id}: {e}")
    
    active_bot_instances.clear()
    sys.exit(0)

async def main():
    """Главная функция"""
    try:
        # Устанавливаем обработчики сигналов
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        logger.info("Запуск системы управления ботами...")
        
        # Пытаемся очистить webhook главного бота перед запуском
        webhook_cleared = False
        try:
            temp_app = Application.builder().token(MAIN_BOT_TOKEN).build()
            await temp_app.initialize()
            
            # Первая попытка - с очисткой webhook
            try:
                await temp_app.bot.delete_webhook(drop_pending_updates=True)
                logger.info("Webhook главного бота успешно очищен")
                webhook_cleared = True
            except Exception as webhook_error:
                error_str = str(webhook_error).lower()
                if "429" in error_str or "flood" in error_str or "too many requests" in error_str:
                    logger.warning(f"Лимит на очистку webhook главного бота: {webhook_error}")
                    logger.info("Продолжаем запуск без очистки webhook")
                else:
                    logger.warning(f"Не удалось очистить webhook главного бота: {webhook_error}")
            
            bot_info = await temp_app.bot.get_me()
            logger.info(f"Главный бот: @{bot_info.username}")
            
            # Сохраняем username главного бота
            if bot_info.username:
                bot_manager.data["bots"]["main"]["username"] = bot_info.username
                bot_manager.save_data()
                logger.info(f"Username главного бота сохранен: @{bot_info.username}")
            
            await temp_app.shutdown()
            await asyncio.sleep(1)
            
        except Exception as e:
            error_str = str(e).lower()
            
            # Если лимит запросов при первой попытке, пробуем без очистки webhook
            if "429" in error_str or "flood" in error_str or "too many requests" in error_str:
                logger.warning("Лимит запросов при инициализации главного бота, пробуем без очистки webhook...")
                # Вторая попытка - просто продолжаем без проверок
                logger.info("Вторая попытка: продолжаем запуск главного бота без проверок...")
                await asyncio.sleep(5)
                # Продолжаем дальше без остановки программы
                    
            elif "conflict" in error_str:
                logger.error("❌ Главный бот уже запущен в другом процессе!")
                logger.error("Решение: Остановите другой процесс или используйте другой токен")
                sys.exit(1)
            else:
                logger.warning(f"Предупреждение при инициализации: {e}")
        
        # Создаем основное приложение
        main_app = Application.builder().token(MAIN_BOT_TOKEN).build()
        
        # Добавляем обработчики команд
        main_app.add_handler(CommandHandler("start", start))
        main_app.add_handler(CommandHandler("admin", admin_command))
        main_app.add_handler(CommandHandler("dbcheck", check_database_command))
        main_app.add_handler(CommandHandler("panel", admin_panel))  # Добавляем прямую команду для панели
        
        # Добавляем обработчики callback запросов
        main_app.add_handler(CallbackQueryHandler(admin_panel, pattern="^admin_panel$"))
        main_app.add_handler(CallbackQueryHandler(manage_bots, pattern="^manage_bots$"))
        main_app.add_handler(CallbackQueryHandler(create_new_bot, pattern="^create_new_bot$"))
        main_app.add_handler(CallbackQueryHandler(retry_token, pattern="^retry_token$"))
        main_app.add_handler(CallbackQueryHandler(control_bots, pattern="^control_bots$"))
        main_app.add_handler(CallbackQueryHandler(control_single_bot, pattern="^control_bot_"))
        main_app.add_handler(CallbackQueryHandler(start_bot_handler, pattern="^start_bot_"))
        main_app.add_handler(CallbackQueryHandler(stop_bot_handler, pattern="^stop_bot_"))
        main_app.add_handler(CallbackQueryHandler(delete_bot_handler, pattern="^delete_bot_"))
        main_app.add_handler(CallbackQueryHandler(confirm_delete_bot, pattern="^confirm_delete_"))
        main_app.add_handler(CallbackQueryHandler(edit_texts, pattern="^edit_texts$"))
        main_app.add_handler(CallbackQueryHandler(edit_bot_text, pattern="^edit_bot_"))
        main_app.add_handler(CallbackQueryHandler(test_bot_text, pattern="^test_bot_"))
        main_app.add_handler(CallbackQueryHandler(stats, pattern="^stats$"))
# Добавляем обработчики для управления админами
        main_app.add_handler(CallbackQueryHandler(manage_admins, pattern="^manage_admins$"))
        main_app.add_handler(CallbackQueryHandler(add_admin, pattern="^add_admin$"))
        main_app.add_handler(CallbackQueryHandler(remove_admin, pattern="^remove_admin$"))
        main_app.add_handler(CallbackQueryHandler(confirm_remove_admin, pattern="^confirm_remove_admin_"))
        
        # Добавляем обработчики для рассылки
        main_app.add_handler(CallbackQueryHandler(broadcast_menu, pattern="^broadcast_menu$"))
        main_app.add_handler(CallbackQueryHandler(lambda u, c: broadcast_menu(u, c, int(u.callback_query.data.split('_')[-1])), pattern="^broadcast_page_"))
        
        # Обработчики для начала рассылки - важно правильный порядок!
        main_app.add_handler(CallbackQueryHandler(broadcast_start, pattern="^broadcast_start_main$"))
        main_app.add_handler(CallbackQueryHandler(broadcast_start, pattern="^broadcast_bot_"))  # Упрощенный паттерн
        
        main_app.add_handler(CallbackQueryHandler(broadcast_send, pattern="^broadcast_send$"))
        main_app.add_handler(CallbackQueryHandler(stop_broadcast, pattern="^stop_broadcast$"))
        main_app.add_handler(CallbackQueryHandler(broadcast_cancel, pattern="^broadcast_cancel$"))
        main_app.add_handler(CallbackQueryHandler(broadcast_cancel_photo, pattern="^broadcast_cancel_photo$"))
        main_app.add_handler(CallbackQueryHandler(broadcast_edit, pattern="^broadcast_edit$"))
        main_app.add_handler(CallbackQueryHandler(broadcast_edit_photo, pattern="^broadcast_edit_photo$"))
        main_app.add_handler(CallbackQueryHandler(user_stats, pattern="^user_stats$"))
        main_app.add_handler(CallbackQueryHandler(bot_stats_menu, pattern="^bot_stats_menu$"))
        main_app.add_handler(CallbackQueryHandler(bot_detailed_stats, pattern="^bot_detailed_stats_"))
        
        # Добавляем обработчик для справки по разметке
        main_app.add_handler(CallbackQueryHandler(markdown_help, pattern="^markdown_help$"))
        main_app.add_handler(CallbackQueryHandler(back_to_start, pattern="^back_to_start$"))
        main_app.add_handler(CallbackQueryHandler(ignore_callback, pattern="^ignore$"))
        
        # Добавляем общий обработчик для отладки неизвестных callback_data
        async def debug_unknown_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
            query = update.callback_query
            logger.warning(f"Unknown callback_data received: {query.data}")
            await query.answer(f"Debug: callback_data = {query.data}", show_alert=True)
        
        # Этот обработчик должен быть последним, чтобы ловить все необработанные callback
        main_app.add_handler(CallbackQueryHandler(debug_unknown_callback))
        
        # Добавляем обработчики сообщений
        main_app.add_handler(MessageHandler(filters.PHOTO, handle_photo_message))
        main_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))
        
        # Инициализируем основное приложение
        await main_app.initialize()
        await main_app.start()
        
        # Восстанавливаем запущенных ботов с задержкой для стабильности
        logger.info("Начинаю восстановление ранее запущенных ботов...")
        await asyncio.sleep(2)  # Даем время основному боту полностью инициализироваться
        await restore_running_bots()
        
        # Запускаем периодическую очистку памяти
        asyncio.create_task(cleanup_old_forwarded_messages())
        
        logger.info("Основной бот запущен")
        
        # Запускаем polling
        await main_app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        
        # Держим приложение активным
        stop_event = asyncio.Event()
        await stop_event.wait()
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    finally:
        try:
            # Останавливаем всех дочерних ботов
            for bot_id in list(active_bot_instances.keys()):
                await stop_bot(bot_id)
            
            # Останавливаем основной бот
            await main_app.updater.stop()
            await main_app.stop()
            await main_app.shutdown()
            
            logger.info("Все боты остановлены")
        except Exception as e:
            logger.error(f"Ошибка при завершении: {e}")

if __name__ == '__main__':
    import argparse
    
    # Парсер аргументов командной строки
    parser = argparse.ArgumentParser(description='Telegram Bot Manager')
    parser.add_argument('--force', action='store_true',
                       help='Принудительно остановить конфликтующие процессы')
    parser.add_argument('--ignore-conflicts', action='store_true',
                       help='Игнорировать конфликты процессов')
    parser.add_argument('--silent', action='store_true',
                       help='Тихий режим без интерактивных запросов')
    args = parser.parse_args()
    
    try:
        # Проверяем, не запущен ли уже процесс
        try:
            import psutil
            current_pid = os.getpid()
            conflicting_processes = []
            
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if proc.info['pid'] != current_pid and proc.info['name'] and 'python' in proc.info['name'].lower():
                        cmdline = proc.info.get('cmdline', [])
                        if cmdline and any('main.py' in arg for arg in cmdline):
                            conflicting_processes.append({
                                'pid': proc.info['pid'],
                                'name': proc.info['name'],
                                'cmdline': ' '.join(cmdline[:3]) if len(cmdline) > 3 else ' '.join(cmdline)
                            })
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            
            if conflicting_processes:
                logger.warning(f"⚠️ Обнаружены конфликтующие процессы: {len(conflicting_processes)} шт.")
                
                # В тихом режиме или с флагом --force автоматически останавливаем
                if args.silent or args.force:
                    logger.info("Автоматическая остановка конфликтующих процессов...")
                    for proc_info in conflicting_processes:
                        try:
                            proc = psutil.Process(proc_info['pid'])
                            proc.terminate()
                            logger.info(f"Процесс {proc_info['pid']} остановлен")
                        except Exception as e:
                            logger.error(f"Не удалось остановить процесс {proc_info['pid']}: {e}")
                    
                    # Ждем завершения
                    import time
                    time.sleep(3)
                    
                    # Проверяем еще раз
                    still_running = []
                    for proc_info in conflicting_processes:
                        try:
                            if psutil.pid_exists(proc_info['pid']):
                                proc = psutil.Process(proc_info['pid'])
                                proc.kill()  # Принудительное завершение
                                logger.info(f"Процесс {proc_info['pid']} принудительно завершен")
                        except:
                            pass
                    
                    logger.info("Конфликтующие процессы остановлены, продолжаем запуск...")
                
                # Если флаг --ignore-conflicts, просто продолжаем
                elif args.ignore_conflicts:
                    logger.warning("Игнорируем конфликты процессов по запросу пользователя")
                
                # В обычном режиме спрашиваем пользователя
                else:
                    print("\n" + "="*60)
                    print("⚠️  ВНИМАНИЕ: Обнаружены конфликтующие процессы!")
                    print("="*60)
                    for proc in conflicting_processes:
                        print(f"PID: {proc['pid']} | Процесс: {proc['name']}")
                    print("-"*60)
                    print("\nИспользуйте параметры запуска:")
                    print("  python main.py --force           # Автоматически остановить конфликты")
                    print("  python main.py --ignore-conflicts # Игнорировать конфликты")
                    print("  python main.py --silent          # Тихий режим для автозапуска")
                    print("\nИли нажмите Enter для продолжения, Ctrl+C для отмены")
                    
                    try:
                        input()
                        logger.info("Пользователь выбрал продолжить")
                    except KeyboardInterrupt:
                        print("\nЗапуск отменен.")
                        sys.exit(0)
                    
        except ImportError:
            logger.info("Модуль psutil не установлен. Пропускаем проверку процессов.")
            if not args.silent:
                logger.info("Установите psutil для улучшенной проверки: pip install psutil")
        
        # Запускаем основную программу
        logger.info(f"Запуск бота (silent={args.silent}, force={args.force}, ignore_conflicts={args.ignore_conflicts})")
        asyncio.run(main())
        
    except KeyboardInterrupt:
        logger.info("Программа завершена пользователем (Ctrl+C)")
    except Exception as e:
        logger.error(f"Фатальная ошибка: {e}")
        if not args.silent:
            logger.error("Попробуйте перезапустить программу")
    finally:
        # Финальная очистка
        active_bot_instances.clear()
        logger.info("Завершение программы")
