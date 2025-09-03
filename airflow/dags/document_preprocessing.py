#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
✅ УЛУЧШЕННЫЙ DAG 1: Document Preprocessing via Microservice Architecture
ВЕРСИЯ 2.0 - Микросервисный подход с улучшенной функциональностью

УЛУЧШЕНИЯ:
- ✅ Микросервисная архитектура через HTTP API
- ✅ Улучшенная обработка ошибок с retry и exponential backoff
- ✅ Валидация схемы ответов от микросервисов
- ✅ Подробное логирование и мониторинг
- ✅ Timeout и SLA контроль
- ✅ Аутентификация и безопасность
- ✅ Метрики и performance tracking
- ✅ Гибкая конфигурация через переменные окружения
- ✅ Circuit breaker паттерн для отказоустойчивости
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
import os
import json
import logging
import time
import requests
from typing import Dict, Any, Optional
from pathlib import Path
from pydantic import BaseModel, ValidationError
import hashlib
from functools import wraps

# Импорт утилит
from shared_utils import (
    SharedUtils, NotificationUtils, ConfigUtils, 
    VLLMUtils, MetricsUtils, ErrorHandlingUtils
)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ НОВОЕ: Pydantic модели для валидации API ответов
class DocumentInfo(BaseModel):
    title: str
    total_pages: int
    processing_time: float
    status: str
    file_size_mb: Optional[float] = None
    content_hash: Optional[str] = None

class ProcessingStats(BaseModel):
    pages_processed: int
    ocr_used: bool
    processing_time_seconds: float
    tables_found: Optional[int] = 0
    images_found: Optional[int] = 0
    extraction_quality_score: Optional[float] = None

class DocumentProcessorResponse(BaseModel):
    success: bool
    document_info: Optional[DocumentInfo] = None
    intermediate_file: Optional[str] = None
    processing_stats: Optional[ProcessingStats] = None
    error: Optional[str] = None
    service_version: Optional[str] = None
    processing_id: Optional[str] = None

# ✅ НОВОЕ: Декоратор для retry с exponential backoff
def retry_with_backoff(max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 60.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise e
                    
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    logger.warning(f"Попытка {attempt + 1} неудачна: {e}. Повтор через {delay}с")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

# ✅ НОВОЕ: Circuit breaker для защиты от каскадных отказов
class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
    
    def call(self, func, *args, **kwargs):
        if self.state == 'OPEN':
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = 'HALF_OPEN'
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            self.reset()
            return result
        except Exception as e:
            self.record_failure()
            raise e
    
    def reset(self):
        self.failure_count = 0
        self.state = 'CLOSED'
        self.last_failure_time = None
    
    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'

# Глобальный circuit breaker для document-processor
doc_processor_cb = CircuitBreaker()

# Конфигурация DAG
DEFAULT_ARGS = {
    'owner': 'pdf-converter',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
    'sla': timedelta(hours=2),  # ✅ НОВОЕ: SLA мониторинг
}

dag = DAG(
    'document_preprocessing_v2',
    default_args=DEFAULT_ARGS,
    description='DAG 1: Document Preprocessing via Microservice Architecture v2.0',
    schedule_interval=None,
    max_active_runs=5,  # ✅ УВЕЛИЧЕНО: больше параллельных запусков
    catchup=False,
    tags=['pdf-converter', 'dag1', 'microservices', 'v2.0']
)

def validate_dag1_input(**context) -> Dict[str, Any]:
    """✅ УЛУЧШЕНО: Расширенная валидация входных данных"""
    start_time = time.time()
    
    try:
        dag_run_conf = context['dag_run'].conf or {}
        logger.info(f"🔍 DAG1: Получена конфигурация: {json.dumps(dag_run_conf, indent=2)}")
        
        # Обязательные параметры
        required_params = ['input_file', 'filename', 'timestamp', 'master_run_id']
        missing_params = [param for param in required_params if not dag_run_conf.get(param)]
        
        if missing_params:
            raise ValueError(f"DAG1: Отсутствуют обязательные параметры: {missing_params}")
        
        # Валидация файла через SharedUtils
        input_file = dag_run_conf['input_file']
        if not SharedUtils.validate_input_file(input_file):
            raise ValueError(f"DAG1: Некорректный файл: {input_file}")
        
        # ✅ НОВОЕ: Дополнительные проверки
        file_info = SharedUtils.calculate_file_hash(input_file)
        file_size = os.path.getsize(input_file)
        
        # Обогащаем конфигурацию
        enriched_config = {
            **dag_run_conf,
            'file_hash': file_info,
            'file_size_bytes': file_size,
            'file_size_mb': file_size / (1024 * 1024),
            'validation_timestamp': datetime.now().isoformat(),
            'dag_version': '2.0'
        }
        
        # ✅ НОВОЕ: Запись метрик
        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing_v2',
            task_id='validate_dag1_input',
            processing_time=time.time() - start_time,
            success=True
        )
        
        logger.info(f"✅ DAG1: Входные данные валидированы для файла: {dag_run_conf['filename']} "
                   f"({enriched_config['file_size_mb']:.2f} MB, hash: {file_info[:8]}...)")
        
        return enriched_config
        
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing_v2',
            task_id='validate_dag1_input',
            processing_time=time.time() - start_time,
            success=False
        )
        logger.error(f"❌ DAG1: Ошибка валидации: {e}")
        raise

@retry_with_backoff(max_retries=3, base_delay=2.0, max_delay=30.0)
def call_document_processor_service(request_data: Dict[str, Any]) -> requests.Response:
    """✅ НОВОЕ: Изолированная функция для HTTP-вызова с retry"""
    processor_url = os.getenv('DOCUMENT_PROCESSOR_URL', 'http://document-processor:8001')
    api_key = os.getenv('DOC_PROCESSOR_API_KEY')
    timeout = int(os.getenv('PROCESSING_TIMEOUT_MINUTES', '60')) * 60
    
    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Airflow-DAG-v2.0'
    }
    
    # ✅ НОВОЕ: Аутентификация
    if api_key:
        headers['Authorization'] = f'Bearer {api_key}'
    
    logger.info(f"🔄 DAG1: Отправляем запрос к {processor_url}/process")
    logger.debug(f"Request data: {json.dumps(request_data, indent=2)}")
    
    response = requests.post(
        f"{processor_url}/process",
        json=request_data,
        headers=headers,
        timeout=timeout
    )
    
    return response

def process_document_via_microservice(**context) -> Dict[str, Any]:
    """✅ УЛУЧШЕНО: Обработка документа через микросервис с полной обработкой ошибок"""
    start_time = time.time()
    dag_config = context['task_instance'].xcom_pull(task_ids='validate_dag1_input')
    
    try:
        # Подготовка запроса
        request_data = {
            'input_file': dag_config['input_file'],
            'filename': dag_config['filename'],
            'enable_ocr': dag_config.get('enable_ocr', False),
            'extract_tables': dag_config.get('extract_tables', True),
            'extract_images': dag_config.get('extract_images', True),
            'ocr_languages': dag_config.get('ocr_languages', 'eng'),
            'quality_level': dag_config.get('quality_level', 'high'),
            'timestamp': dag_config['timestamp'],
            'file_hash': dag_config['file_hash'],
            'request_id': f"dag1_{dag_config['master_run_id']}_{int(time.time())}"
        }
        
        # ✅ НОВОЕ: Circuit breaker защита
        response = doc_processor_cb.call(call_document_processor_service, request_data)
        
        # Проверка HTTP статуса
        if response.status_code != 200:
            error_msg = f"document-processor вернул статус {response.status_code}: {response.text}"
            logger.error(f"❌ DAG1: {error_msg}")
            
            return {
                'success': False,
                'error': error_msg,
                'http_status': response.status_code,
                'original_config': dag_config
            }
        
        # ✅ НОВОЕ: Валидация ответа через Pydantic
        try:
            response_data = response.json()
            validated_response = DocumentProcessorResponse(**response_data)
        except (json.JSONDecodeError, ValidationError) as e:
            error_msg = f"Некорректный ответ от document-processor: {e}"
            logger.error(f"❌ DAG1: {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'original_config': dag_config
            }
        
        if not validated_response.success:
            logger.error(f"❌ DAG1: document-processor сообщил об ошибке: {validated_response.error}")
            return {
                'success': False,
                'error': validated_response.error,
                'original_config': dag_config
            }
        
        # ✅ НОВОЕ: Проверка целостности результата
        if validated_response.intermediate_file:
            if not os.path.exists(validated_response.intermediate_file):
                error_msg = f"Промежуточный файл не найден: {validated_response.intermediate_file}"
                logger.error(f"❌ DAG1: {error_msg}")
                return {
                    'success': False,
                    'error': error_msg,
                    'original_config': dag_config
                }
        
        processing_time = time.time() - start_time
        
        # ✅ НОВОЕ: Метрики успешной обработки
        if validated_response.processing_stats:
            MetricsUtils.record_processing_metrics(
                dag_id='document_preprocessing_v2',
                task_id='process_document_via_microservice',
                processing_time=processing_time,
                pages_count=validated_response.processing_stats.pages_processed,
                success=True
            )
        
        result = {
            'success': True,
            'document_info': validated_response.document_info.dict() if validated_response.document_info else {},
            'intermediate_file': validated_response.intermediate_file,
            'original_config': dag_config,
            'processing_stats': validated_response.processing_stats.dict() if validated_response.processing_stats else {},
            'service_metadata': {
                'service_version': validated_response.service_version,
                'processing_id': validated_response.processing_id,
                'total_processing_time': processing_time
            }
        }
        
        logger.info(f"✅ DAG1: Документ обработан успешно за {processing_time:.2f}с")
        return result
        
    except requests.Timeout:
        error_msg = "Timeout при обращении к document-processor"
        logger.error(f"❌ DAG1: {error_msg}")
        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing_v2',
            task_id='process_document_via_microservice',
            processing_time=time.time() - start_time,
            success=False
        )
        return {
            'success': False,
            'error': error_msg,
            'error_type': 'timeout',
            'original_config': dag_config
        }
    
    except requests.ConnectionError:
        error_msg = "Не удается подключиться к document-processor"
        logger.error(f"❌ DAG1: {error_msg}")
        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing_v2',
            task_id='process_document_via_microservice',
            processing_time=time.time() - start_time,
            success=False
        )
        return {
            'success': False,
            'error': error_msg,
            'error_type': 'connection_error',
            'original_config': dag_config
        }
    
    except Exception as e:
        error_msg = f"Неожиданная ошибка при обращении к document-processor: {str(e)}"
        logger.error(f"❌ DAG1: {error_msg}")
        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing_v2',
            task_id='process_document_via_microservice',
            processing_time=time.time() - start_time,
            success=False
        )
        return {
            'success': False,
            'error': error_msg,
            'error_type': 'unexpected_error',
            'original_config': dag_config
        }

def prepare_for_dag2(**context) -> Dict[str, Any]:
    """✅ УЛУЧШЕНО: Подготовка данных для DAG2 с дополнительными проверками"""
    start_time = time.time()
    
    try:
        dag1_result = context['task_instance'].xcom_pull(task_ids='process_document_via_microservice')
        
        if not dag1_result.get('success'):
            error_details = {
                'error': dag1_result.get('error', 'Unknown error'),
                'error_type': dag1_result.get('error_type', 'unknown'),
                'http_status': dag1_result.get('http_status')
            }
            raise AirflowException(f"DAG1 завершился с ошибкой: {error_details}")
        
        # ✅ НОВОЕ: Проверка готовности промежуточного файла
        intermediate_file = dag1_result.get('intermediate_file')
        if intermediate_file:
            # Проверяем, что файл читаемый и содержит валидный JSON
            try:
                data = SharedUtils.load_intermediate_result(intermediate_file)
                if not data:
                    raise ValueError("Промежуточный файл пустой или поврежден")
            except Exception as e:
                raise AirflowException(f"Ошибка проверки промежуточного файла: {e}")
        
        # Подготовка конфигурации для DAG2
        dag2_config = {
            'intermediate_file': intermediate_file,
            'original_config': dag1_result['original_config'],
            'dag1_metadata': {
                **dag1_result.get('processing_stats', {}),
                **dag1_result.get('service_metadata', {}),
                'dag1_completion_time': datetime.now().isoformat()
            },
            'dag1_completed': True,
            'ready_for_transformation': True,
            'dag_version': '2.0'
        }
        
        # ✅ НОВОЕ: Метрики
        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing_v2',
            task_id='prepare_for_dag2',
            processing_time=time.time() - start_time,
            success=True
        )
        
        logger.info(f"✅ DAG1→DAG2: Данные подготовлены успешно")
        logger.info(f"📄 Промежуточный файл: {dag2_config['intermediate_file']}")
        logger.debug(f"DAG2 config: {json.dumps(dag2_config, indent=2, default=str)}")
        
        return dag2_config
        
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing_v2',
            task_id='prepare_for_dag2',
            processing_time=time.time() - start_time,
            success=False
        )
        logger.error(f"❌ DAG1: Ошибка подготовки данных для DAG2: {e}")
        raise

def notify_dag1_completion(**context) -> None:
    """✅ УЛУЧШЕНО: Расширенные уведомления с детальной статистикой"""
    try:
        dag1_result = context['task_instance'].xcom_pull(task_ids='process_document_via_microservice')
        dag2_config = context['task_instance'].xcom_pull(task_ids='prepare_for_dag2')
        
        if dag1_result and dag1_result.get('success'):
            stats = dag1_result.get('processing_stats', {})
            doc_info = dag1_result.get('document_info', {})
            service_meta = dag1_result.get('service_metadata', {})
            
            message = f"""
✅ DAG 1: DOCUMENT PREPROCESSING ЗАВЕРШЕН УСПЕШНО (v2.0)

📄 Файл: {dag1_result['original_config']['filename']}
📊 Страниц обработано: {stats.get('pages_processed', 'N/A')}
⏱️ Время обработки: {service_meta.get('total_processing_time', 0):.2f}с
🔍 OCR использован: {'Да' if stats.get('ocr_used') else 'Нет'}
📋 Таблиц найдено: {stats.get('tables_found', 'N/A')}
🖼️ Изображений найдено: {stats.get('images_found', 'N/A')}
⭐ Качество извлечения: {stats.get('extraction_quality_score', 'N/A')}
🏷️ Версия сервиса: {service_meta.get('service_version', 'N/A')}
🆔 ID обработки: {service_meta.get('processing_id', 'N/A')}

🔄 ГОТОВ К ПЕРЕДАЧЕ В DAG2 (Content Transformation)
📁 Промежуточный файл: {dag2_config.get('intermediate_file', 'N/A')}

✅ Микросервисная архитектура работает корректно
            """
            
            # Отправка уведомления об успехе
            NotificationUtils.send_dag_completion_notification(context, dag1_result)
            
        else:
            error = dag1_result.get('error', 'Unknown error') if dag1_result else 'No result received'
            error_type = dag1_result.get('error_type', 'unknown') if dag1_result else 'unknown'
            
            message = f"""
❌ DAG 1: DOCUMENT PREPROCESSING ЗАВЕРШЕН С ОШИБКОЙ (v2.0)

📄 Файл: {dag1_result['original_config']['filename'] if dag1_result else 'N/A'}
❌ Ошибка: {error}
🏷️ Тип ошибки: {error_type}
⏰ Время: {datetime.now().isoformat()}

🔧 Рекомендации по устранению:
1. Проверьте доступность document-processor сервиса
2. Проверьте корректность входного файла
3. Проверьте логи микросервиса
4. Проверьте конфигурацию сети и авторизации
            """
            
            # Отправка уведомления об ошибке
            error_obj = Exception(error) if dag1_result else Exception("No result from processing")
            NotificationUtils.send_failure_notification(context, error_obj)
        
        logger.info(message)
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки уведомления: {e}")

# ✅ НОВОЕ: Задача для проверки готовности микросервисов
def check_microservices_health(**context) -> Dict[str, Any]:
    """Проверка доступности всех необходимых микросервисов"""
    start_time = time.time()
    
    try:
        processor_url = os.getenv('DOCUMENT_PROCESSOR_URL', 'http://document-processor:8001')
        vllm_config = ConfigUtils.get_vllm_config()
        
        health_status = {
            'document_processor': False,
            'vllm_server': False,
            'all_healthy': False
        }
        
        # Проверка document-processor
        try:
            response = requests.get(f"{processor_url}/health", timeout=10)
            health_status['document_processor'] = response.status_code == 200
        except:
            pass
        
        # Проверка vLLM (для будущих DAG)
        health_status['vllm_server'] = VLLMUtils.check_vllm_health(
            vllm_config['server_url'], timeout=10
        )
        
        health_status['all_healthy'] = all(health_status.values())
        
        if not health_status['all_healthy']:
            logger.warning(f"⚠️ Некоторые сервисы недоступны: {health_status}")
        else:
            logger.info("✅ Все микросервисы доступны")
        
        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing_v2',
            task_id='check_microservices_health',
            processing_time=time.time() - start_time,
            success=health_status['all_healthy']
        )
        
        return health_status
        
    except Exception as e:
        logger.error(f"❌ Ошибка проверки здоровья сервисов: {e}")
        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing_v2',
            task_id='check_microservices_health',
            processing_time=time.time() - start_time,
            success=False
        )
        raise

# ✅ ОПРЕДЕЛЕНИЕ ЗАДАЧ DAG v2.0

# Задача 0: Проверка готовности микросервисов
health_check = PythonOperator(
    task_id='check_microservices_health',
    python_callable=check_microservices_health,
    execution_timeout=timedelta(minutes=2),
    dag=dag
)

# Задача 1: Валидация входных данных
validate_input = PythonOperator(
    task_id='validate_dag1_input',
    python_callable=validate_dag1_input,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

# Задача 2: Обработка документа через микросервис
process_document = PythonOperator(
    task_id='process_document_via_microservice',
    python_callable=process_document_via_microservice,
    execution_timeout=timedelta(hours=2),  # ✅ НОВОЕ: Timeout для больших файлов
    dag=dag
)

# Задача 3: Подготовка данных для DAG2
prepare_dag2 = PythonOperator(
    task_id='prepare_for_dag2',
    python_callable=prepare_for_dag2,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

# Задача 4: Уведомление о завершении
notify_completion = PythonOperator(
    task_id='notify_dag1_completion',
    python_callable=notify_dag1_completion,
    trigger_rule='all_done',  # ✅ НОВОЕ: Запускается в любом случае
    execution_timeout=timedelta(minutes=2),
    dag=dag
)

# ✅ НОВОЕ: Задача очистки временных файлов
def cleanup_temp_files(**context) -> None:
    """Очистка временных файлов после завершения обработки"""
    try:
        temp_dir = os.getenv('TEMP_DIR', '/app/temp')
        max_age_hours = int(os.getenv('TEMP_FILES_MAX_AGE_HOURS', '24'))
        
        cleaned_count = SharedUtils.cleanup_temp_files(temp_dir, max_age_hours)
        logger.info(f"🧹 Очищено {cleaned_count} временных файлов старше {max_age_hours} часов")
        
    except Exception as e:
        logger.error(f"❌ Ошибка очистки временных файлов: {e}")

cleanup_files = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=cleanup_temp_files,
    trigger_rule='all_done',
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

# ✅ ОПРЕДЕЛЕНИЕ ЗАВИСИМОСТЕЙ v2.0
health_check >> validate_input >> process_document >> prepare_dag2
[prepare_dag2, process_document] >> notify_completion
notify_completion >> cleanup_files

# ✅ УЛУЧШЕНО: Обработка ошибок с детальным анализом
def handle_dag1_failure(context):
    """Улучшенная обработка ошибок DAG1"""
    try:
        failed_task = context['task_instance'].task_id
        exception = context.get('exception')
        dag_run_conf = context.get('dag_run', {}).conf or {}
        
        error_details = ErrorHandlingUtils.handle_dag_error(context, 
            f"Критическая ошибка в задаче {failed_task}")
        
        # Дополнительная диагностика
        diagnostic_info = {
            'task_id': failed_task,
            'error_type': type(exception).__name__ if exception else 'Unknown',
            'file_processed': dag_run_conf.get('filename', 'Unknown'),
            'dag_version': '2.0',
            'circuit_breaker_state': doc_processor_cb.state,
            'failure_count': doc_processor_cb.failure_count
        }
        
        error_message = f"""
🔥 КРИТИЧЕСКАЯ ОШИБКА В DAG 1 v2.0: DOCUMENT PREPROCESSING

📋 Детали ошибки:
• Задача: {failed_task}
• Ошибка: {str(exception) if exception else 'Unknown'}
• Файл: {diagnostic_info['file_processed']}
• Circuit Breaker: {diagnostic_info['circuit_breaker_state']}

🔧 Возможные причины и решения:
1. document-processor недоступен → проверьте статус контейнера
2. Превышен timeout обработки → увеличьте лимиты или оптимизируйте
3. Некорректный входной файл → проверьте формат и размер
4. Проблемы сети → проверьте connectivity между сервисами
5. Переполнение ресурсов → проверьте memory/CPU usage

📊 Диагностическая информация:
{json.dumps(diagnostic_info, indent=2)}
        """
        
        logger.error(error_message)
        
        # Отправка детального уведомления об ошибке
        NotificationUtils.send_failure_notification(context, exception)
        
        # ✅ НОВОЕ: Метрики для ошибок
        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing_v2',
            task_id=failed_task,
            processing_time=0,
            success=False
        )
        
    except Exception as e:
        logger.error(f"❌ Ошибка в обработчике ошибок: {e}")

# Применение обработчика ошибок ко всем задачам
for task in dag.tasks:
    task.on_failure_callback = handle_dag1_failure