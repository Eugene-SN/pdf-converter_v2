#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
✅ ДОПОЛНЕННЫЙ shared_utils.py с недостающим классом
Исправляет все ImportError в DAG файлах

ДОПОЛНЕНИЯ:
- ✅ Добавлен отсутствующий класс QualityAssuranceOperator (для совместимости)
- ✅ Добавлены дополнительные утилиты
- ✅ Улучшена обработка ошибок
"""

import os
import json
import logging
import hashlib
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
import requests
import time

# Настройка логирования
logger = logging.getLogger(__name__)

class SharedUtils:
    """✅ ИСПРАВЛЕНО: Общие утилиты для всех DAG"""

    @staticmethod
    def validate_input_file(file_path: str) -> bool:
        """Валидация входного PDF файла"""
        try:
            if not os.path.exists(file_path):
                logger.error(f"Файл не найден: {file_path}")
                return False

            # Проверка размера файла (макс 500MB)
            file_size = os.path.getsize(file_path)
            max_size = 500 * 1024 * 1024  # 500MB
            if file_size > max_size:
                logger.error(f"Файл слишком большой: {file_size} bytes")
                return False

            # Проверка PDF signature
            with open(file_path, 'rb') as f:
                header = f.read(4)
                if not header.startswith(b'%PDF'):
                    logger.error(f"Неверный формат PDF файла: {file_path}")
                    return False

            logger.info(f"✅ Файл валидирован: {file_path} ({file_size} bytes)")
            return True

        except Exception as e:
            logger.error(f"Ошибка валидации файла {file_path}: {e}")
            return False

    @staticmethod
    def create_work_directory(base_path: str, identifier: str) -> str:
        """Создание рабочей директории"""
        work_dir = Path(base_path) / f"work_{identifier}"
        work_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Создана рабочая директория: {work_dir}")
        return str(work_dir)

    @staticmethod
    def calculate_file_hash(file_path: str) -> str:
        """Вычисление хеша файла"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    @staticmethod
    def save_intermediate_result(data: Dict[str, Any], file_path: str) -> bool:
        """Сохранение промежуточного результата"""
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"✅ Промежуточный результат сохранен: {file_path}")
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения промежуточного результата: {e}")
            return False

    @staticmethod
    def load_intermediate_result(file_path: str) -> Optional[Dict[str, Any]]:
        """Загрузка промежуточного результата"""
        try:
            if not os.path.exists(file_path):
                logger.error(f"Промежуточный файл не найден: {file_path}")
                return None

            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f"✅ Промежуточный результат загружен: {file_path}")
            return data

        except Exception as e:
            logger.error(f"Ошибка загрузки промежуточного результата: {e}")
            return None

    @staticmethod
    def cleanup_temp_files(directory: str, max_age_hours: int = 24) -> int:
        """Очистка временных файлов старше указанного времени"""
        cleaned_count = 0
        current_time = time.time()
        max_age_seconds = max_age_hours * 3600

        try:
            for file_path in Path(directory).rglob("*"):
                if file_path.is_file():
                    file_age = current_time - file_path.stat().st_mtime
                    if file_age > max_age_seconds:
                        file_path.unlink()
                        cleaned_count += 1

            if cleaned_count > 0:
                logger.info(f"🧹 Очищено {cleaned_count} временных файлов")

        except Exception as e:
            logger.error(f"Ошибка очистки временных файлов: {e}")

        return cleaned_count

class NotificationUtils:
    """✅ ИСПРАВЛЕНО: Утилиты для уведомлений"""

    @staticmethod
    def send_success_notification(context: Dict[str, Any], result: Dict[str, Any]) -> bool:
        """Отправка уведомления об успехе"""
        try:
            dag_id = context.get('dag', {}).dag_id if hasattr(context.get('dag', {}), 'dag_id') else 'unknown'
            task_id = context.get('task_instance', {}).task_id if hasattr(context.get('task_instance', {}), 'task_id') else 'unknown'

            message = f"""
✅ УСПЕШНОЕ ЗАВЕРШЕНИЕ
DAG: {dag_id}
Task: {task_id}
Время: {datetime.now().isoformat()}
Результат: {result.get('message', 'Обработка завершена')}
            """

            logger.info(f"SUCCESS NOTIFICATION: {message}")

            # Здесь можно добавить отправку в Slack, email, webhook и т.д.
            webhook_url = os.getenv('SUCCESS_WEBHOOK_URL')
            if webhook_url:
                requests.post(webhook_url, json={'message': message})

            return True

        except Exception as e:
            logger.error(f"Ошибка отправки уведомления об успехе: {e}")
            return False

    @staticmethod
    def send_failure_notification(context: Dict[str, Any], exception: Exception) -> bool:
        """Отправка уведомления об ошибке"""
        try:
            dag_id = context.get('dag', {}).dag_id if hasattr(context.get('dag', {}), 'dag_id') else 'unknown'
            task_id = context.get('task_instance', {}).task_id if hasattr(context.get('task_instance', {}), 'task_id') else 'unknown'

            message = f"""
❌ ОШИБКА ВЫПОЛНЕНИЯ
DAG: {dag_id}
Task: {task_id}
Время: {datetime.now().isoformat()}
Ошибка: {str(exception)}
            """

            logger.error(f"FAILURE NOTIFICATION: {message}")

            # Здесь можно добавить отправку в системы мониторинга
            webhook_url = os.getenv('ERROR_WEBHOOK_URL')
            if webhook_url:
                requests.post(webhook_url, json={'message': message, 'severity': 'error'})

            return True

        except Exception as e:
            logger.error(f"Ошибка отправки уведомления об ошибке: {e}")
            return False

    @staticmethod
    def send_dag_completion_notification(context: Dict[str, Any], result: Dict[str, Any]) -> bool:
        """Отправка уведомления о завершении DAG"""
        try:
            success = result.get('success', False)
            if success:
                return NotificationUtils.send_success_notification(context, result)
            else:
                error = result.get('error', 'Unknown error')
                return NotificationUtils.send_failure_notification(context, Exception(error))

        except Exception as e:
            logger.error(f"Ошибка отправки уведомления о завершении DAG: {e}")
            return False

class ConfigUtils:
    """✅ ИСПРАВЛЕНО: Утилиты для работы с конфигурацией"""

    @staticmethod
    def get_vllm_config() -> Dict[str, Any]:
        """Получение конфигурации vLLM"""
        return {
            'server_url': os.getenv('VLLM_SERVER_URL', 'http://vllm-server:8000'),
            'content_model': os.getenv('VLLM_CONTENT_MODEL', 'Qwen/Qwen2.5-VL-7B-Instruct'),
            'translation_model': os.getenv('VLLM_TRANSLATION_MODEL', 'Qwen/Qwen2.5-72B-Instruct'),
            'api_key': os.getenv('VLLM_API_KEY', 'pdf-converter-secure-key-2024'),
            'timeout': int(os.getenv('VLLM_STANDARD_TIMEOUT', '1800')),
            'max_tokens': int(os.getenv('VLLM_MAX_TOKENS', '8192')),
            'temperature': float(os.getenv('VLLM_TEMPERATURE', '0.1'))
        }

    @staticmethod
    def get_docling_config() -> Dict[str, Any]:
        """Получение конфигурации Docling"""
        return {
            'model_path': os.getenv('DOCLING_MODEL_PATH', '/mnt/storage/models/docling'),
            'cache_dir': os.getenv('DOCLING_HOME', '/mnt/storage/models/docling'),
            'use_gpu': os.getenv('DOCLING_USE_GPU', 'true').lower() == 'true',
            'max_workers': int(os.getenv('DOCLING_MAX_WORKERS', '4')),
            'enable_ocr_by_default': False  # ✅ По умолчанию OCR отключен
        }

    @staticmethod
    def get_processing_paths() -> Dict[str, str]:
        """Получение путей для обработки"""
        return {
            'temp_dir': os.getenv('TEMP_DIR', '/app/temp'),
            'input_dir': os.getenv('INPUT_DIR', '/app/input'),
            'output_dir': os.getenv('OUTPUT_DIR', '/app/output'),
            'cache_dir': os.getenv('CACHE_DIR', '/app/cache'),
            'models_dir': os.getenv('HF_HOME', '/mnt/storage/models/huggingface')
        }

class VLLMUtils:
    """✅ ИСПРАВЛЕНО: Утилиты для работы с vLLM"""

    @staticmethod
    def check_vllm_health(server_url: str, timeout: int = 30) -> bool:
        """Проверка доступности vLLM сервера"""
        try:
            response = requests.get(f"{server_url}/health", timeout=timeout)
            is_healthy = response.status_code == 200

            if is_healthy:
                logger.info(f"✅ vLLM сервер доступен: {server_url}")
            else:
                logger.error(f"❌ vLLM сервер недоступен: {server_url} (код: {response.status_code})")

            return is_healthy

        except Exception as e:
            logger.error(f"❌ Ошибка проверки vLLM сервера: {e}")
            return False

    @staticmethod
    def get_available_models(server_url: str, api_key: str = None) -> List[str]:
        """Получение списка доступных моделей"""
        try:
            headers = {}
            if api_key:
                headers['Authorization'] = f'Bearer {api_key}'

            response = requests.get(f"{server_url}/v1/models", headers=headers, timeout=10)

            if response.status_code == 200:
                models_data = response.json()
                models = [model['id'] for model in models_data.get('data', [])]
                logger.info(f"✅ Доступные модели vLLM: {models}")
                return models
            else:
                logger.error(f"❌ Ошибка получения моделей: {response.status_code}")
                return []

        except Exception as e:
            logger.error(f"❌ Ошибка получения списка моделей: {e}")
            return []

class MetricsUtils:
    """✅ ИСПРАВЛЕНО: Утилиты для метрик и мониторинга"""

    @staticmethod
    def record_processing_metrics(
        dag_id: str,
        task_id: str,
        processing_time: float,
        pages_count: int = 0,
        success: bool = True
    ) -> bool:
        """Запись метрик обработки для Prometheus"""
        try:
            metrics_data = {
                'dag_id': dag_id,
                'task_id': task_id,
                'processing_time_seconds': processing_time,
                'pages_processed': pages_count,
                'success': success,
                'timestamp': datetime.now().isoformat()
            }

            logger.info(f"📊 Метрики: {metrics_data}")

            # Здесь можно добавить отправку метрик в Prometheus pushgateway
            pushgateway_url = os.getenv('PUSHGATEWAY_URL')
            if pushgateway_url:
                # Отправка метрик
                pass

            return True

        except Exception as e:
            logger.error(f"Ошибка записи метрик: {e}")
            return False

class ErrorHandlingUtils:
    """✅ Утилиты для обработки ошибок"""

    @staticmethod
    def handle_dag_error(context: Dict[str, Any], custom_message: str = "") -> Dict[str, Any]:
        """Централизованная обработка ошибок DAG"""
        dag_id = context.get('dag', {}).dag_id if hasattr(context.get('dag', {}), 'dag_id') else 'unknown'
        task_id = context.get('task_instance', {}).task_id if hasattr(context.get('task_instance', {}), 'task_id') else 'unknown'
        
        error_info = {
            'dag_id': dag_id,
            'task_id': task_id,
            'execution_date': str(context.get('execution_date', '')),
            'error': str(context.get('exception', 'Unknown error')),
            'custom_message': custom_message,
            'timestamp': datetime.now().isoformat()
        }

        logger.error(f"DAG Error: {error_info}")

        # Отправка уведомления
        NotificationUtils.send_failure_notification(context, context.get('exception'))

        return error_info

# ✅ НОВОЕ: Добавляем отсутствующий класс QualityAssuranceOperator для совместимости
class QualityAssuranceOperator:
    """
    ✅ СОВМЕСТИМОСТЬ: Заглушка для QualityAssuranceOperator
    
    ВАЖНО: Этот класс добавлен только для исправления ImportError.
    В реальности используйте микросервисную архитектуру через HTTP API!
    """
    
    def __init__(self, *args, **kwargs):
        logger.warning("⚠️ QualityAssuranceOperator устарел! Используйте микросервисную архитектуру")
        pass
    
    @staticmethod
    def validate_document(*args, **kwargs):
        """Заглушка для старого метода"""
        logger.warning("⚠️ Используйте новый quality_assurance_v2.py DAG вместо этого оператора")
        return {"success": False, "error": "Deprecated operator - use microservice architecture"}

# ✅ ДОПОЛНИТЕЛЬНЫЕ УТИЛИТЫ

class FileUtils:
    """✅ Утилиты для работы с файлами"""

    @staticmethod
    def ensure_directory_exists(directory: str) -> bool:
        """Создание директории если не существует"""
        try:
            Path(directory).mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            logger.error(f"Ошибка создания директории {directory}: {e}")
            return False

    @staticmethod
    def get_file_info(file_path: str) -> Dict[str, Any]:
        """Получение информации о файле"""
        try:
            file_stat = os.stat(file_path)
            return {
                'size_bytes': file_stat.st_size,
                'size_mb': file_stat.st_size / (1024 * 1024),
                'modified_time': datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
                'exists': True
            }
        except Exception as e:
            logger.error(f"Ошибка получения информации о файле {file_path}: {e}")
            return {'exists': False, 'error': str(e)}

# ✅ Экспорт всех утилит
__all__ = [
    'SharedUtils',
    'NotificationUtils',
    'ConfigUtils',
    'VLLMUtils',
    'MetricsUtils',
    'FileUtils',
    'ErrorHandlingUtils',
    'QualityAssuranceOperator'  # ✅ Добавлен для совместимости
]