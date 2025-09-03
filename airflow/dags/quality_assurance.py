#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
✅ ИСПРАВЛЕННЫЙ DAG 3: Quality Assurance via Microservice Architecture
Исправляет ImportError: QualityAssuranceOperator

ИСПРАВЛЕНИЯ:
- ✅ Убран несуществующий импорт QualityAssuranceOperator
- ✅ Использует микросервисную архитектуру через HTTP API
- ✅ Использует только существующие классы из shared_utils
- ✅ Добавлена валидация и обработка ошибок
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
import os
import json
import logging
import time
import requests
from typing import Dict, Any, Optional

# ✅ ИСПРАВЛЕНО: Импортируем только существующие классы
from shared_utils import (
    SharedUtils, NotificationUtils, ConfigUtils, 
    VLLMUtils, MetricsUtils, ErrorHandlingUtils
)

# Настройка логирования
logger = logging.getLogger(__name__)

# Конфигурация DAG
DEFAULT_ARGS = {
    'owner': 'pdf-converter',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'quality_assurance_v2',
    default_args=DEFAULT_ARGS,
    description='DAG 3: Quality Assurance via Microservice Architecture',
    schedule_interval=None,
    max_active_runs=3,
    catchup=False,
    tags=['pdf-converter', 'dag3', 'quality-assurance', 'microservices']
)

def validate_qa_input(**context) -> Dict[str, Any]:
    """Валидация входных данных для QA"""
    try:
        dag_run_conf = context['dag_run'].conf or {}
        logger.info(f"🔍 QA: Получена конфигурация: {json.dumps(dag_run_conf, indent=2)}")
        
        # Обязательные параметры
        required_params = ['intermediate_file', 'original_config']
        missing_params = [param for param in required_params if not dag_run_conf.get(param)]
        
        if missing_params:
            raise ValueError(f"QA: Отсутствуют обязательные параметры: {missing_params}")
        
        # Проверка промежуточного файла
        intermediate_file = dag_run_conf['intermediate_file']
        if not os.path.exists(intermediate_file):
            raise ValueError(f"QA: Промежуточный файл не найден: {intermediate_file}")
        
        # Загрузка и валидация содержимого
        data = SharedUtils.load_intermediate_result(intermediate_file)
        if not data:
            raise ValueError("QA: Промежуточный файл пустой или поврежден")
        
        logger.info(f"✅ QA: Входные данные валидированы")
        return dag_run_conf
        
    except Exception as e:
        logger.error(f"❌ QA: Ошибка валидации: {e}")
        raise

def call_quality_assurance_service(**context) -> Dict[str, Any]:
    """Вызов микросервиса Quality Assurance через HTTP API"""
    start_time = time.time()
    dag_config = context['task_instance'].xcom_pull(task_ids='validate_qa_input')
    
    try:
        # URL сервиса из конфигурации
        qa_url = os.getenv('QUALITY_ASSURANCE_URL', 'http://quality-assurance:8002')
        api_key = os.getenv('QA_API_KEY')
        timeout = int(os.getenv('QA_VALIDATION_TIMEOUT', '600'))
        
        # Подготовка запроса
        request_data = {
            'intermediate_file': dag_config['intermediate_file'],
            'original_config': dag_config['original_config'],
            'qa_enabled': dag_config.get('qa_enabled', True),
            'qa_threshold': dag_config.get('qa_threshold', 0.95),
            'auto_correction_enabled': dag_config.get('auto_correction_enabled', True),
            'max_corrections': dag_config.get('max_corrections', 5),
            'timestamp': datetime.now().isoformat()
        }
        
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Airflow-QA-DAG'
        }
        
        if api_key:
            headers['Authorization'] = f'Bearer {api_key}'
        
        logger.info(f"🔄 QA: Отправляем запрос к {qa_url}/validate")
        
        # HTTP POST к микросервису
        response = requests.post(
            f"{qa_url}/validate",
            json=request_data,
            headers=headers,
            timeout=timeout
        )
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ QA: Валидация завершена успешно")
            
            # Метрики
            MetricsUtils.record_processing_metrics(
                dag_id='quality_assurance_v2',
                task_id='call_quality_assurance_service',
                processing_time=time.time() - start_time,
                success=True
            )
            
            return {
                'success': True,
                'qa_result': result.get('qa_result', {}),
                'corrections_made': result.get('corrections_made', []),
                'quality_score': result.get('quality_score', 0.0),
                'validated_file': result.get('validated_file'),
                'original_config': dag_config
            }
        else:
            error_msg = f"quality-assurance сервис вернул ошибку: {response.status_code}"
            logger.error(f"❌ QA: {error_msg}")
            
            MetricsUtils.record_processing_metrics(
                dag_id='quality_assurance_v2',
                task_id='call_quality_assurance_service',
                processing_time=time.time() - start_time,
                success=False
            )
            
            return {
                'success': False,
                'error': error_msg,
                'http_status': response.status_code,
                'original_config': dag_config
            }
            
    except requests.Timeout:
        error_msg = "Timeout при обращении к quality-assurance сервису"
        logger.error(f"❌ QA: {error_msg}")
        return {
            'success': False,
            'error': error_msg,
            'error_type': 'timeout',
            'original_config': dag_config
        }
    
    except requests.ConnectionError:
        error_msg = "Не удается подключиться к quality-assurance сервису"
        logger.error(f"❌ QA: {error_msg}")
        return {
            'success': False,
            'error': error_msg,
            'error_type': 'connection_error',
            'original_config': dag_config
        }
    
    except Exception as e:
        error_msg = f"Неожиданная ошибка при обращении к quality-assurance: {str(e)}"
        logger.error(f"❌ QA: {error_msg}")
        return {
            'success': False,
            'error': error_msg,
            'error_type': 'unexpected_error',
            'original_config': dag_config
        }

def prepare_qa_results(**context) -> Dict[str, Any]:
    """Подготовка результатов QA для следующих этапов"""
    start_time = time.time()
    
    try:
        qa_result = context['task_instance'].xcom_pull(task_ids='call_quality_assurance_service')
        
        if not qa_result.get('success'):
            raise AirflowException(f"QA завершился с ошибкой: {qa_result.get('error')}")
        
        # Подготовка данных для следующих DAG
        result_config = {
            'validated_file': qa_result.get('validated_file'),
            'quality_score': qa_result.get('quality_score', 0.0),
            'corrections_made': len(qa_result.get('corrections_made', [])),
            'qa_metadata': {
                'validation_timestamp': datetime.now().isoformat(),
                'qa_version': '2.0',
                'quality_threshold_met': qa_result.get('quality_score', 0.0) >= 0.95
            },
            'original_config': qa_result['original_config'],
            'qa_completed': True,
            'ready_for_next_stage': True
        }
        
        MetricsUtils.record_processing_metrics(
            dag_id='quality_assurance_v2',
            task_id='prepare_qa_results',
            processing_time=time.time() - start_time,
            success=True
        )
        
        logger.info(f"✅ QA: Результаты подготовлены. Качество: {result_config['quality_score']:.3f}")
        return result_config
        
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='quality_assurance_v2',
            task_id='prepare_qa_results',
            processing_time=time.time() - start_time,
            success=False
        )
        logger.error(f"❌ QA: Ошибка подготовки результатов: {e}")
        raise

def notify_qa_completion(**context) -> None:
    """Уведомление о завершении QA"""
    try:
        qa_result = context['task_instance'].xcom_pull(task_ids='call_quality_assurance_service')
        final_result = context['task_instance'].xcom_pull(task_ids='prepare_qa_results')
        
        if qa_result and qa_result.get('success'):
            message = f"""
✅ QA: QUALITY ASSURANCE ЗАВЕРШЕН УСПЕШНО

📊 Оценка качества: {final_result.get('quality_score', 0.0):.3f}
🔧 Исправлений сделано: {final_result.get('corrections_made', 0)}
✅ Порог качества достигнут: {'Да' if final_result.get('qa_metadata', {}).get('quality_threshold_met') else 'Нет'}
📁 Валидированный файл: {final_result.get('validated_file', 'N/A')}

🔄 ГОТОВ К СЛЕДУЮЩЕМУ ЭТАПУ ОБРАБОТКИ
            """
            NotificationUtils.send_success_notification(context, qa_result)
        else:
            error = qa_result.get('error', 'Unknown error') if qa_result else 'No result received'
            message = f"""
❌ QA: QUALITY ASSURANCE ЗАВЕРШЕН С ОШИБКОЙ

❌ Ошибка: {error}
⏰ Время: {datetime.now().isoformat()}

🔧 Проверьте доступность quality-assurance сервиса
            """
            NotificationUtils.send_failure_notification(context, Exception(error))
        
        logger.info(message)
        
    except Exception as e:
        logger.error(f"❌ QA: Ошибка отправки уведомления: {e}")

# ✅ ОПРЕДЕЛЕНИЕ ЗАДАЧ QA DAG

# Задача 1: Валидация входных данных
validate_input = PythonOperator(
    task_id='validate_qa_input',
    python_callable=validate_qa_input,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

# Задача 2: Вызов QA микросервиса
quality_check = PythonOperator(
    task_id='call_quality_assurance_service',
    python_callable=call_quality_assurance_service,
    execution_timeout=timedelta(minutes=30),
    dag=dag
)

# Задача 3: Подготовка результатов
prepare_results = PythonOperator(
    task_id='prepare_qa_results',
    python_callable=prepare_qa_results,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

# Задача 4: Уведомление о завершении
notify_completion = PythonOperator(
    task_id='notify_qa_completion',
    python_callable=notify_qa_completion,
    trigger_rule='all_done',
    execution_timeout=timedelta(minutes=2),
    dag=dag
)

# Определение зависимостей
validate_input >> quality_check >> prepare_results >> notify_completion

# Обработка ошибок
def handle_qa_failure(context):
    """Обработка ошибок QA DAG"""
    try:
        failed_task = context['task_instance'].task_id
        exception = context.get('exception')
        
        error_message = f"""
🔥 ОШИБКА В QA DAG: QUALITY ASSURANCE

Задача: {failed_task}
Ошибка: {str(exception) if exception else 'Unknown error'}

Возможные причины:
1. quality-assurance сервис недоступен
2. Поврежденный промежуточный файл
3. Превышен timeout валидации
4. Проблемы с сетевым соединением

Требуется проверка логов и состояния сервиса.
        """
        
        logger.error(error_message)
        NotificationUtils.send_failure_notification(context, exception)
        
    except Exception as e:
        logger.error(f"❌ Ошибка в обработчике ошибок QA: {e}")

# Применение обработчика ошибок ко всем задачам
for task in dag.tasks:
    task.on_failure_callback = handle_qa_failure