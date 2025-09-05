"""
Модуль для многопоточной рассылки с воркерами
Ускоряет рассылку в 3-4 раза за счет параллельной отправки
"""

import asyncio
import logging
from typing import List, Dict, Optional, Callable
from dataclasses import dataclass
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor
from telegram import Bot, InputFile
from telegram.error import Forbidden, BadRequest, TelegramError
from telegram.constants import ParseMode
import io

logger = logging.getLogger(__name__)

@dataclass
class BroadcastTask:
    """Задача для рассылки"""
    user_id: int
    bot_token: str
    text: str = None
    photo: bytes = None
    photo_caption: str = None
    parse_mode: str = ParseMode.HTML
    disable_web_page_preview: bool = True
    message_id: int = None  # ID сообщения для пересылки
    from_chat_id: int = None  # Откуда пересылать

@dataclass
class BroadcastResult:
    """Результат рассылки"""
    user_id: int
    success: bool
    error: str = None
    is_blocked: bool = False
    send_time: float = 0

class BroadcastWorker:
    """Воркер для отправки сообщений"""
    
    def __init__(self, worker_id: int, bot_token: str):
        self.worker_id = worker_id
        self.bot_token = bot_token
        self.bot = None  # Будет создан при первом использовании
        self.sent_count = 0
        self.error_count = 0
        self.rate_limit_hits = 0
        self.last_rate_limit_time = 0
    
    async def _ensure_bot(self):
        """Ленивая инициализация Bot объекта"""
        if self.bot is None:
            self.bot = Bot(token=self.bot_token)
            logger.debug(f"Worker {self.worker_id}: Bot instance created")
        
    async def send_message(self, task: BroadcastTask) -> BroadcastResult:
        """Отправить одно сообщение с обработкой rate limits"""
        # Создаем Bot объект при первом использовании
        await self._ensure_bot()
        
        start_time = time.time()
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                # Если есть message_id, используем пересылку (copy_message)
                if task.message_id and task.from_chat_id:
                    # copy_message копирует сообщение без указания источника
                    await self.bot.copy_message(
                        chat_id=task.user_id,
                        from_chat_id=task.from_chat_id,
                        message_id=task.message_id
                    )
                elif task.photo:
                    # Отправка фото
                    photo_io = io.BytesIO(task.photo)
                    photo_io.name = 'photo.jpg'
                    
                    if task.photo_caption:
                        await self.bot.send_photo(
                            chat_id=task.user_id,
                            photo=InputFile(photo_io, filename='photo.jpg'),
                            caption=task.photo_caption,
                            parse_mode=task.parse_mode
                        )
                    else:
                        await self.bot.send_photo(
                            chat_id=task.user_id,
                            photo=InputFile(photo_io, filename='photo.jpg')
                        )
                else:
                    # Отправка текста
                    await self.bot.send_message(
                        chat_id=task.user_id,
                        text=task.text,
                        parse_mode=task.parse_mode,
                        disable_web_page_preview=task.disable_web_page_preview
                    )
                
                self.sent_count += 1
                send_time = time.time() - start_time
                
                return BroadcastResult(
                    user_id=task.user_id,
                    success=True,
                    send_time=send_time
                )
                
            except TelegramError as e:
                error_msg = str(e).lower()
                
                # Проверяем на rate limit (429 Too Many Requests)
                if "too many requests" in error_msg or "retry after" in error_msg or "429" in str(e):
                    self.rate_limit_hits += 1
                    self.last_rate_limit_time = time.time()
                    
                    # Извлекаем время ожидания из ошибки если есть
                    wait_time = 5  # По умолчанию 5 секунд
                    try:
                        # Пытаемся найти число в сообщении об ошибке
                        import re
                        match = re.search(r'retry after (\d+)', error_msg)
                        if match:
                            wait_time = int(match.group(1))
                    except:
                        pass
                    
                    logger.warning(f"Worker {self.worker_id} hit rate limit, waiting {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                    retry_count += 1
                    continue
                    
                elif "forbidden" in error_msg:
                    # Пользователь заблокировал бота
                    self.error_count += 1
                    return BroadcastResult(
                        user_id=task.user_id,
                        success=False,
                        error="User blocked bot",
                        is_blocked=True,
                        send_time=time.time() - start_time
                    )
                else:
                    # Другая ошибка Telegram
                    self.error_count += 1
                    return BroadcastResult(
                        user_id=task.user_id,
                        success=False,
                        error=str(e),
                        send_time=time.time() - start_time
                    )
                    
            except Exception as e:
                self.error_count += 1
                logger.error(f"Worker {self.worker_id} error sending to {task.user_id}: {e}")
                return BroadcastResult(
                    user_id=task.user_id,
                    success=False,
                    error=str(e),
                    send_time=time.time() - start_time
                )
        
        # Если все попытки исчерпаны
        self.error_count += 1
        return BroadcastResult(
            user_id=task.user_id,
            success=False,
            error="Max retries exceeded due to rate limit",
            send_time=time.time() - start_time
        )

class BroadcastManager:
    """Менеджер для управления рассылкой с несколькими воркерами"""
    
    def __init__(self, bot_token: str, num_workers: int = 4, bot_name: str = None):
        """
        Инициализация менеджера рассылки
        
        Args:
            bot_token: Токен бота
            num_workers: Количество воркеров (рекомендуется 3-4 для ускорения в 3-4 раза)
            bot_name: Имя бота для отображения в отчетах
        """
        self.bot_token = bot_token
        self.num_workers = num_workers
        self.bot_name = bot_name or "Неизвестный бот"
        self.workers: List[BroadcastWorker] = []
        self.results: List[BroadcastResult] = []
        self.start_time = None
        self.end_time = None
        self.total_rate_limit_hits = 0
        self.is_cancelled = False  # Флаг для отмены рассылки
        self.progress_info = {
            'total': 0,
            'sent': 0,
            'failed': 0,
            'blocked': 0,
            'current_speed': 0,
            'last_update_time': time.time()
        }
        self.workers_initialized = False  # Флаг инициализации воркеров
    
    async def initialize_workers(self):
        """Асинхронная инициализация воркеров"""
        if self.workers_initialized:
            return
        
        # Создаем воркеров асинхронно
        worker_creation_tasks = []
        for i in range(self.num_workers):
            # Создаем задачу для создания каждого воркера
            task = asyncio.create_task(self._create_worker(i))
            worker_creation_tasks.append(task)
        
        # Ждем создания всех воркеров параллельно
        created_workers = await asyncio.gather(*worker_creation_tasks)
        self.workers = created_workers
        self.workers_initialized = True
        logger.info(f"Initialized {len(self.workers)} workers asynchronously")
    
    async def _create_worker(self, worker_id: int) -> BroadcastWorker:
        """Асинхронное создание одного воркера"""
        # Небольшая задержка для распределения нагрузки
        await asyncio.sleep(worker_id * 0.01)
        worker = BroadcastWorker(worker_id, self.bot_token)
        logger.debug(f"Worker {worker_id} created")
        return worker
    
    def cancel(self):
        """Отменить рассылку"""
        self.is_cancelled = True
        logger.info("Broadcast cancellation requested")
    
    async def broadcast(
        self,
        users: List[int],
        text: str = None,
        photo: bytes = None,
        photo_caption: str = None,
        parse_mode: str = ParseMode.HTML,
        disable_web_page_preview: bool = True,
        progress_callback: Optional[Callable] = None,
        batch_delay: float = 0.02,  # Увеличиваем задержку для стабильности
        message_id: int = None,  # ID сообщения для пересылки
        from_chat_id: int = None,  # Откуда пересылать
        template_chat_id: int = None  # Специальный чат для создания шаблона (например, канал бота)
    ) -> Dict:
        """
        Выполнить рассылку
        
        Args:
            users: Список ID пользователей
            text: Текст сообщения
            photo: Байты фото (если есть)
            photo_caption: Подпись к фото
            parse_mode: Режим парсинга
            disable_web_page_preview: Отключить превью ссылок
            progress_callback: Функция для отслеживания прогресса
            batch_delay: Задержка между батчами (для избежания лимитов)
            message_id: ID сообщения для пересылки (если используем copy_message)
            from_chat_id: ID чата откуда пересылать
        
        Returns:
            Словарь с результатами рассылки
        """
        # Асинхронно инициализируем воркеров если еще не инициализированы
        await self.initialize_workers()
        
        self.start_time = datetime.now()
        self.results = []
        
        # ИСПРАВЛЕНИЕ: Создаем шаблон в специальном чате, а не у первого пользователя
        if not message_id and (text or photo):
            # Асинхронное создание шаблона сообщения
            template_result = await self._create_template_message(
                text, photo, photo_caption, parse_mode,
                disable_web_page_preview, template_chat_id
            )
            if template_result:
                message_id, from_chat_id = template_result
        
        # Создаем задачи
        tasks = []
        for user_id in users:
            task = BroadcastTask(
                user_id=user_id,
                bot_token=self.bot_token,
                text=text,
                photo=photo,
                photo_caption=photo_caption,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview,
                message_id=message_id,
                from_chat_id=from_chat_id
            )
            tasks.append(task)
        
        # Разделяем задачи между воркерами
        chunks = self._split_into_chunks(tasks, self.num_workers)
        
        # Запускаем воркеров параллельно
        worker_tasks = []
        for i, (worker, chunk) in enumerate(zip(self.workers, chunks)):
            worker_task = asyncio.create_task(
                self._worker_process(worker, chunk, progress_callback, batch_delay)
            )
            worker_tasks.append(worker_task)
        
        # Ждем завершения всех воркеров
        await asyncio.gather(*worker_tasks)
        
        self.end_time = datetime.now()
        
        # Подсчитываем результаты
        success_count = sum(1 for r in self.results if r.success)
        failed_count = sum(1 for r in self.results if not r.success)
        blocked_count = sum(1 for r in self.results if r.is_blocked)
        
        # Собираем статистику по rate limits
        for worker in self.workers:
            self.total_rate_limit_hits += worker.rate_limit_hits
        
        total_time = (self.end_time - self.start_time).total_seconds()
        avg_time = sum(r.send_time for r in self.results) / len(self.results) if self.results else 0
        
        return {
            'bot_name': self.bot_name,  # Добавляем имя бота
            'total': len(users),
            'success': success_count,
            'failed': failed_count,
            'blocked': blocked_count,
            'total_time': total_time,
            'avg_send_time': avg_time,
            'messages_per_second': len(users) / total_time if total_time > 0 else 0,
            'workers_used': self.num_workers,
            'rate_limit_hits': self.total_rate_limit_hits,
            'results': self.results,
            'was_cancelled': self.is_cancelled,
            'final_progress': self.progress_info  # Добавляем финальный прогресс
        }
    
    async def _worker_process(
        self,
        worker: BroadcastWorker,
        tasks: List[BroadcastTask],
        progress_callback: Optional[Callable],
        batch_delay: float
    ):
        """Процесс обработки задач воркером с динамической регулировкой скорости"""
        consecutive_successes = 0
        current_delay = batch_delay
        
        for i, task in enumerate(tasks):
            # Проверяем флаг отмены
            if self.is_cancelled:
                logger.info(f"Worker {worker.worker_id} stopped due to cancellation")
                break
                
            result = await worker.send_message(task)
            self.results.append(result)
            
            # Проверяем флаг отмены после каждого сообщения
            if self.is_cancelled:
                logger.info(f"Worker {worker.worker_id} stopping after message {i+1}")
                break
            
            # Динамическая регулировка задержки для максимальной скорости
            if result.success:
                consecutive_successes += 1
                # Более консервативное уменьшение задержки
                if consecutive_successes > 10 and current_delay > 0.01:
                    current_delay = max(0.01, current_delay * 0.9)  # Уменьшаем на 10%
                elif consecutive_successes > 20:
                    current_delay = 0.01  # Минимальная задержка после 20 успешных
            else:
                consecutive_successes = 0
                # При ошибке увеличиваем задержку
                if "rate" in (result.error or "").lower():
                    current_delay = min(0.1, current_delay * 2)  # Удваиваем при rate limit
                else:
                    current_delay = min(0.05, current_delay * 1.5)  # Увеличиваем на 50% при других ошибках
            
            # Обновляем информацию о прогрессе
            self.progress_info['sent'] = sum(1 for r in self.results if r.success)
            self.progress_info['failed'] = sum(1 for r in self.results if not r.success and not r.is_blocked)
            self.progress_info['blocked'] = sum(1 for r in self.results if r.is_blocked)
            
            # Вычисляем скорость
            current_time = time.time()
            time_diff = current_time - self.progress_info['last_update_time']
            if time_diff > 0:
                self.progress_info['current_speed'] = 1 / time_diff
            self.progress_info['last_update_time'] = current_time
            
            # Вызываем callback прогресса если есть
            if progress_callback:
                total_processed = len(self.results)
                # Передаем также информацию о прогрессе
                await progress_callback(total_processed, len(tasks) * self.num_workers, self.progress_info)
            
            # Минимальная задержка для максимальной скорости
            if i < len(tasks) - 1 and not self.is_cancelled:
                if current_delay > 0:
                    await asyncio.sleep(current_delay)
        
        logger.info(f"Worker {worker.worker_id} completed: sent={worker.sent_count}, errors={worker.error_count}, rate_limits={worker.rate_limit_hits}")
    
    def _split_into_chunks(self, lst: List, n: int) -> List[List]:
        """Разделить список на n примерно равных частей"""
        chunk_size = len(lst) // n
        remainder = len(lst) % n
        
        chunks = []
        start = 0
        
        for i in range(n):
            # Добавляем 1 к размеру чанка для первых remainder чанков
            current_chunk_size = chunk_size + (1 if i < remainder else 0)
            end = start + current_chunk_size
            
            if start < len(lst):
                chunks.append(lst[start:end])
            else:
                chunks.append([])
            
            start = end
        
        return chunks
    
    async def _create_template_message(
        self,
        text: str,
        photo: bytes,
        photo_caption: str,
        parse_mode: str,
        disable_web_page_preview: bool,
        template_chat_id: int
    ) -> Optional[tuple]:
        """
        Асинхронное создание шаблона сообщения для быстрой пересылки
        
        Returns:
            Tuple (message_id, from_chat_id) или None если не удалось создать
        """
        if not template_chat_id:
            return None
            
        try:
            # Используем первого воркера если он уже создан, иначе создаем временный Bot
            if self.workers and self.workers[0].bot:
                test_bot = self.workers[0].bot
                logger.debug("Using existing worker bot for template")
            else:
                # Создаем временный Bot асинхронно
                test_bot = Bot(token=self.bot_token)
                logger.debug("Created temporary bot for template")
            
            try:
                if photo:
                    # Отправляем фото в специальный чат
                    photo_io = io.BytesIO(photo)
                    photo_io.name = 'photo.jpg'
                    
                    # Используем asyncio.create_task для параллельной отправки
                    send_task = asyncio.create_task(
                        test_bot.send_photo(
                            chat_id=template_chat_id,
                            photo=InputFile(photo_io, filename='photo.jpg'),
                            caption=photo_caption if photo_caption else None,
                            parse_mode=parse_mode
                        )
                    )
                    
                    # Ждем с таймаутом
                    sent_msg = await asyncio.wait_for(send_task, timeout=5.0)
                else:
                    # Отправляем текст в специальный чат
                    send_task = asyncio.create_task(
                        test_bot.send_message(
                            chat_id=template_chat_id,
                            text=text,
                            parse_mode=parse_mode,
                            disable_web_page_preview=disable_web_page_preview
                        )
                    )
                    
                    # Ждем с таймаутом
                    sent_msg = await asyncio.wait_for(send_task, timeout=3.0)
                
                logger.info(f"Created template message {sent_msg.message_id} in special chat {template_chat_id} for fast forwarding")
                return (sent_msg.message_id, template_chat_id)
                
            except asyncio.TimeoutError:
                logger.warning("Timeout creating template message, will use direct send")
                return None
                
        except Exception as e:
            logger.warning(f"Could not create template message for forwarding: {e}")
            return None

class OptimizedBroadcast:
    """Оптимизированная рассылка с автоматическим выбором количества воркеров"""
    
    @staticmethod
    def calculate_optimal_workers(user_count: int) -> int:
        """
        Рассчитать оптимальное количество воркеров для ~20 сообщений/сек
        
        Args:
            user_count: Количество пользователей
            
        Returns:
            Оптимальное количество воркеров (от 2 до 8)
        """
        # Более консервативный подход к воркерам
        if user_count <= 4:
            return min(user_count, 2)  # Максимум 2 воркера для маленьких рассылок
        elif user_count < 10:
            return 2
        elif user_count < 50:
            return 3
        elif user_count < 100:
            return 3
        elif user_count < 500:
            return 4
        elif user_count < 1000:
            return 7
        else:
            return 10  # Максимум 6 воркеров для больших рассылок
    
    @staticmethod
    async def send_broadcast(
        bot_token: str,
        users: List[int],
        text: str = None,
        photo: bytes = None,
        photo_caption: str = None,
        parse_mode: str = ParseMode.HTML,
        progress_callback: Optional[Callable] = None,
        auto_optimize: bool = True,
        stop_check: Optional[Callable] = None,  # Функция проверки остановки
        template_chat_id: int = None,  # ID специального чата для шаблона
        bot_name: str = None  # Имя бота для отчетов
    ) -> Dict:
        """
        Отправить рассылку с автоматической оптимизацией
        
        Args:
            bot_token: Токен бота
            users: Список пользователей
            text: Текст сообщения
            photo: Фото (байты)
            photo_caption: Подпись к фото
            parse_mode: Режим парсинга
            progress_callback: Callback для прогресса
            auto_optimize: Автоматически выбрать количество воркеров
            stop_check: Функция проверки остановки
            
        Returns:
            Результаты рассылки
        """
        # Определяем количество воркеров
        if auto_optimize:
            num_workers = OptimizedBroadcast.calculate_optimal_workers(len(users))
        else:
            num_workers = 3  # По умолчанию 3 воркера для баланса скорости и стабильности
        
        logger.info(f"Starting broadcast to {len(users)} users with {num_workers} workers from bot: {bot_name}")
        
        # Создаем менеджер (воркеры будут созданы асинхронно при вызове broadcast)
        manager = BroadcastManager(bot_token, num_workers, bot_name)
        
        # Если есть функция проверки остановки, создаем обертку для callback
        if stop_check:
            original_callback = progress_callback
            check_counter = 0
            
            async def wrapped_callback(current, total, progress_info=None):
                nonlocal check_counter
                check_counter += 1
                
                # Проверяем, нужно ли остановить
                if stop_check():
                    logger.info(f"Broadcast stop requested at {current}/{total}, cancelling...")
                    manager.cancel()
                    # Не вызываем оригинальный callback при остановке
                    return
                
                # Вызываем оригинальный callback если есть
                if original_callback:
                    # Проверяем, сколько аргументов принимает оригинальный callback
                    import inspect
                    sig = inspect.signature(original_callback)
                    params = sig.parameters
                    if len(params) >= 3:
                        await original_callback(current, total, progress_info)
                    else:
                        await original_callback(current, total)
                
                # Логируем каждые 10 проверок для отладки
                if check_counter % 10 == 0:
                    logger.debug(f"Stop check #{check_counter}: broadcast continues ({current}/{total})")
            
            progress_callback = wrapped_callback
        
        result = await manager.broadcast(
            users=users,
            text=text,
            photo=photo,
            photo_caption=photo_caption,
            parse_mode=parse_mode,
            progress_callback=progress_callback,
            template_chat_id=template_chat_id  # Передаем ID специального чата
        )
        
        # Логируем результаты с именем бота
        bot_display_name = result.get('bot_name', 'Unknown bot')
        if result.get('was_cancelled'):
            logger.info(f"[{bot_display_name}] Broadcast was CANCELLED after {result['total_time']:.2f} seconds")
            logger.info(f"[{bot_display_name}] Sent {result['success']} messages before cancellation")
        else:
            logger.info(f"[{bot_display_name}] Broadcast completed in {result['total_time']:.2f} seconds")
            logger.info(f"[{bot_display_name}] Success: {result['success']}, Failed: {result['failed']}, Blocked: {result['blocked']}")
            logger.info(f"[{bot_display_name}] Speed: {result['messages_per_second']:.2f} messages/second")
            logger.info(f"[{bot_display_name}] Workers: {num_workers}, Rate limit hits: {result.get('rate_limit_hits', 0)}")
        
        return result

# Пример использования (закомментирован для безопасности)
# async def example_usage():
#     """Пример использования оптимизированной рассылки"""
#
#     bot_token = "YOUR_BOT_TOKEN"
#     users = [123456789, 987654321, 555555555]  # ID пользователей
#
#     # Простая текстовая рассылка
#     result = await OptimizedBroadcast.send_broadcast(
#         bot_token=bot_token,
#         users=users,
#         text="Привет! Это тестовая рассылка.",
#         auto_optimize=True  # Автоматически выберет количество воркеров
#     )
#
#     print(f"Рассылка завершена за {result['total_time']:.2f} секунд")
#     print(f"Успешно: {result['success']}, Ошибок: {result['failed']}")
#     print(f"Скорость: {result['messages_per_second']:.2f} сообщений/сек")
#
# if __name__ == "__main__":
#     # Запуск примера
#     asyncio.run(example_usage())
