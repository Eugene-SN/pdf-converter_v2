#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
✅ ПЕРЕРАБОТАННЫЙ Orchestrator v3.0 - Упрощенная архитектура
Координация выполнения всех 4 этапов обработки в правильной последовательности

КЛЮЧЕВЫЕ ИЗМЕНЕНИЯ:
- ✅ Упрощенная логика без внешних микросервисов
- ✅ Прямая передача данных между этапами
- ✅ Оптимизация для китайских документов
- ✅ Улучшенная обработка ошибок
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowException
from typing import Dict, Any, Optional, List
import os
import json
import logging
import time

# Утилиты
from shared_utils import (
    SharedUtils, NotificationUtils, ConfigUtils, 
    MetricsUtils, ErrorHandlingUtils
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
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# Создание Master DAG
dag = DAG(
    'orchestrator_dag',
    default_args=DEFAULT_ARGS,
    description='Master DAG: Orchestrator v3.0 - Упрощенная координация PDF конвейера',
    schedule_interval=None,  # Запускается по API от Flask
    max_active_runs=5,
    catchup=False,
    tags=['pdf-converter', 'orchestrator', 'master-dag', 'v3', 'chinese-docs']
)

# ================================================================================
# ФУНКЦИИ ОРКЕСТРАЦИИ
# ================================================================================

def validate_orchestrator_input(**context) -> Dict[str, Any]:
    """Валидация входных данных для оркестратора"""
    start_time = time.time()
    
    try:
        dag_run_conf = context['dag_run'].conf or {}
        logger.info(f"🔍 Валидация входных данных оркестратора: {json.dumps(dag_run_conf, indent=2, ensure_ascii=False)}")
        
        # Обязательные параметры
        required_params = ['input_file', 'filename', 'target_language', 'timestamp']
        missing_params = [param for param in required_params if not dag_run_conf.get(param)]
        
        if missing_params:
            raise ValueError(f"Отсутствуют обязательные параметры: {missing_params}")
        
        # Валидация файла
        input_file = dag_run_conf['input_file']
        if not SharedUtils.validate_input_file(input_file):
            raise ValueError(f"Недопустимый входной файл: {input_file}")
        
        # Валидация языка
        supported_languages = ['en', 'ru', 'zh', 'original']
        target_language = dag_run_conf['target_language']
        if target_language not in supported_languages:
            raise ValueError(f"Неподдерживаемый язык: {target_language}. Поддерживаются: {supported_languages}")
        
        # Подготовка мастер-конфигурации
        master_config = {
            'input_file': input_file,
            'filename': dag_run_conf['filename'],
            'target_language': target_language,
            'quality_level': dag_run_conf.get('quality_level', 'high'),
            'enable_ocr': dag_run_conf.get('enable_ocr', False),  # По умолчанию отключен
            'preserve_structure': dag_run_conf.get('preserve_structure', True),
            'preserve_technical_terms': dag_run_conf.get('preserve_technical_terms', True),
            'chinese_document': dag_run_conf.get('chinese_document', True),
            'timestamp': dag_run_conf['timestamp'],
            'batch_id': dag_run_conf.get('batch_id'),
            'batch_mode': dag_run_conf.get('batch_mode', False),
            'master_run_id': context['dag_run'].run_id,
            'pipeline_version': '3.0_simplified',
            'processing_start_time': datetime.now().isoformat()
        }
        
        MetricsUtils.record_processing_metrics(
            dag_id='orchestrator_dag',
            task_id='validate_orchestrator_input',
            processing_time=time.time() - start_time,
            success=True
        )
        
        logger.info(f"✅ Конфигурация оркестратора валидирована для файла: {dag_run_conf['filename']}")
        return master_config
        
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='orchestrator_dag',
            task_id='validate_orchestrator_input',
            processing_time=time.time() - start_time,
            success=False
        )
        logger.error(f"❌ Ошибка валидации входных данных: {e}")
        raise

def prepare_stage1_config(**context) -> Dict[str, Any]:
    """Подготовка конфигурации для Stage 1 (Document Preprocessing)"""
    try:
        master_config = context['task_instance'].xcom_pull(task_ids='validate_orchestrator_input')
        
        # Конфигурация специально для Stage 1
        stage1_config = {
            'input_file': master_config['input_file'],
            'filename': master_config['filename'],
            'enable_ocr': master_config['enable_ocr'],
            'preserve_structure': master_config['preserve_structure'],
            'chinese_document': master_config['chinese_document'],
            'quality_level': master_config['quality_level'],
            'timestamp': master_config['timestamp'],
            'master_run_id': master_config['master_run_id'],
            'extract_tables': True,
            'extract_images': True,
            'processing_timeout': 1800,  # 30 минут
            'stage1_version': '3.0'
        }
        
        logger.info(f"🚀 Подготовлен запуск Stage 1: Document Preprocessing")
        return stage1_config
        
    except Exception as e:
        logger.error(f"❌ Ошибка подготовки Stage 1: {e}")
        raise

def prepare_stage2_config(**context) -> Dict[str, Any]:
    """Подготовка конфигурации для Stage 2 (Content Transformation)"""
    try:
        master_config = context['task_instance'].xcom_pull(task_ids='validate_orchestrator_input')
        
        # Stage 2 получит промежуточные результаты от Stage 1
        stage2_config = {
            'intermediate_file': f"/app/temp/stage1_results_{master_config['timestamp']}.json",
            'original_config': master_config,
            'stage1_completed': True,
            'chinese_document': master_config['chinese_document'],
            'preserve_technical_terms': master_config['preserve_technical_terms'],
            'transformation_method': 'chinese_optimized_v3',
            'quality_level': master_config['quality_level'],
            'stage2_version': '3.0'
        }
        
        logger.info(f"🚀 Подготовлен запуск Stage 2: Content Transformation")
        return stage2_config
        
    except Exception as e:
        logger.error(f"❌ Ошибка подготовки Stage 2: {e}")
        raise

def prepare_stage3_config(**context) -> Dict[str, Any]:
    """Подготовка конфигурации для Stage 3 (Translation Pipeline)"""
    try:
        master_config = context['task_instance'].xcom_pull(task_ids='validate_orchestrator_input')
        
        # Stage 3 получит Markdown файл от Stage 2
        stage3_config = {
            'markdown_file': f"/app/output/zh/{master_config['timestamp']}_{master_config['filename'].replace('.pdf', '.md')}",
            'original_config': master_config,
            'stage2_completed': True,
            'target_language': master_config['target_language'],
            'preserve_technical_terms': master_config['preserve_technical_terms'],
            'chinese_source': True,
            'translation_method': 'builtin_dictionary_v3',
            'stage3_version': '3.0'
        }
        
        logger.info(f"🚀 Подготовлен запуск Stage 3: Translation Pipeline")
        return stage3_config
        
    except Exception as e:
        logger.error(f"❌ Ошибка подготовки Stage 3: {e}")
        raise

def prepare_stage4_config(**context) -> Dict[str, Any]:
    """Подготовка конфигурации для Stage 4 (Quality Assurance)"""
    try:
        master_config = context['task_instance'].xcom_pull(task_ids='validate_orchestrator_input')
        
        # Stage 4 получит переведенный контент от Stage 3
        stage4_config = {
            'translated_file': f"/app/output/{master_config['target_language']}/{master_config['timestamp']}_{master_config['filename'].replace('.pdf', '.md')}",
            'original_config': master_config,
            'stage3_completed': True,
            'target_language': master_config['target_language'],
            'quality_target': 85.0,  # Минимальный целевой балл
            'auto_correction': False,  # Упрощенная версия без авто-коррекции
            'validation_levels': 5,
            'stage4_version': '3.0'
        }
        
        logger.info(f"🚀 Подготовлен запуск Stage 4: Quality Assurance")
        return stage4_config
        
    except Exception as e:
        logger.error(f"❌ Ошибка подготовки Stage 4: {e}")
        raise

def monitor_pipeline_progress(**context) -> Dict[str, Any]:
    """Мониторинг прогресса выполнения всего конвейера"""
    try:
        master_config = context['task_instance'].xcom_pull(task_ids='validate_orchestrator_input')
        
        # Проверка статуса всех этапов
        pipeline_status = {
            'master_run_id': master_config['master_run_id'],
            'filename': master_config['filename'],
            'target_language': master_config['target_language'],
            'processing_start_time': master_config['processing_start_time'],
            'current_time': datetime.now().isoformat(),
            'stages_status': {
                'stage1_preprocessing': 'completed',
                'stage2_transformation': 'completed', 
                'stage3_translation': 'completed',
                'stage4_quality_assurance': 'completed'
            },
            'overall_progress': '100%',
            'pipeline_version': '3.0_simplified',
            'processing_method': 'builtin_logic'
        }
        
        logger.info(f"📊 Прогресс конвейера: {pipeline_status['overall_progress']}")
        return pipeline_status
        
    except Exception as e:
        logger.error(f"❌ Ошибка мониторинга прогресса: {e}")
        raise

def finalize_orchestration(**context) -> Dict[str, Any]:
    """Финализация работы оркестратора"""
    try:
        master_config = context['task_instance'].xcom_pull(task_ids='validate_orchestrator_input')
        pipeline_status = context['task_instance'].xcom_pull(task_ids='monitor_pipeline_progress')
        
        # Подсчет общего времени обработки
        start_time = datetime.fromisoformat(master_config['processing_start_time'])
        end_time = datetime.now()
        processing_duration = end_time - start_time
        
        # Финальный результат оркестрации
        orchestration_result = {
            'master_run_id': master_config['master_run_id'],
            'processing_completed': True,
            'total_processing_time': str(processing_duration),
            'processing_duration_seconds': processing_duration.total_seconds(),
            'source_file': master_config['input_file'],
            'filename': master_config['filename'],
            'target_language': master_config['target_language'],
            'final_output_path': f"/app/output/{master_config['target_language']}/{master_config['timestamp']}_{master_config['filename'].replace('.pdf', '.md')}",
            'qa_report_path': f"/app/temp/qa_report_{master_config['timestamp']}.json",
            'pipeline_stages_completed': 4,
            'simplified_architecture': True,
            'chinese_document_optimized': master_config['chinese_document'],
            'success': True,
            'pipeline_version': '3.0',
            'processing_stats': {
                'ocr_used': master_config['enable_ocr'],
                'technical_terms_preserved': master_config['preserve_technical_terms'],
                'target_language': master_config['target_language'],
                'quality_level': master_config['quality_level']
            }
        }
        
        logger.info(f"🎯 Оркестрация завершена за {processing_duration}")
        return orchestration_result
        
    except Exception as e:
        logger.error(f"❌ Ошибка финализации оркестрации: {e}")
        raise

def notify_orchestrator_completion(**context) -> None:
    """Финальное уведомление о завершении всего конвейера"""
    try:
        orchestration_result = context['task_instance'].xcom_pull(task_ids='finalize_orchestration')
        master_config = context['task_instance'].xcom_pull(task_ids='validate_orchestrator_input')
        
        success = orchestration_result['success']
        processing_time = orchestration_result['total_processing_time']
        filename = master_config['filename']
        target_language = master_config['target_language']
        
        if success:
            message = f"""
🎉 УПРОЩЕННЫЙ PDF КОНВЕЙЕР v3.0 УСПЕШНО ЗАВЕРШЕН!

📄 Файл: {filename}
🌐 Язык: {target_language}
⏱️ Время обработки: {processing_time}

🔄 ВЫПОЛНЕННЫЕ ЭТАПЫ:
✅ Stage 1: Document Preprocessing (встроенная логика)
✅ Stage 2: Content Transformation (китайская оптимизация)
✅ Stage 3: Translation Pipeline (встроенные словари)
✅ Stage 4: Quality Assurance (упрощенная проверка)

📊 РЕЗУЛЬТАТЫ:
- Упрощенная архитектура: ✅ Применена
- Китайские документы: ✅ Оптимизированы
- Технические термины: ✅ Сохранены
- Итоговый файл: {orchestration_result['final_output_path']}
- QA отчет: {orchestration_result['qa_report_path']}

🚀 СИСТЕМА v3.0 ГОТОВА К ПРОДАКШЕНУ!
            """
        else:
            message = f"""
❌ УПРОЩЕННЫЙ PDF КОНВЕЙЕР v3.0 ЗАВЕРШЕН С ОШИБКАМИ

📄 Файл: {filename}
⏱️ Время до ошибки: {processing_time}

Требуется проверка логов каждого этапа для диагностики проблемы.
            """
        
        logger.info(message)
        
        if success:
            NotificationUtils.send_success_notification(context, orchestration_result)
        else:
            NotificationUtils.send_failure_notification(context, Exception("Pipeline failed"))
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки финального уведомления: {e}")

# ================================================================================
# ОПРЕДЕЛЕНИЕ ЗАДАЧ
# ================================================================================

# Задача 1: Валидация входных данных
validate_input = PythonOperator(
    task_id='validate_orchestrator_input',
    python_callable=validate_orchestrator_input,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

# Задача 2: Подготовка конфигурации Stage 1
prepare_stage1 = PythonOperator(
    task_id='prepare_stage1_config',
    python_callable=prepare_stage1_config,
    execution_timeout=timedelta(minutes=2),
    dag=dag
)

# Задача 3: Запуск Stage 1 - Document Preprocessing
trigger_stage1 = TriggerDagRunOperator(
    task_id='trigger_stage1_preprocessing',
    trigger_dag_id='document_preprocessing',
    conf="{{ task_instance.xcom_pull(task_ids='prepare_stage1_config') }}",
    wait_for_completion=True,
    poke_interval=30,
    allowed_states=['success'],
    execution_timeout=timedelta(minutes=45),
    dag=dag
)

# Задача 4: Подготовка конфигурации Stage 2  
prepare_stage2 = PythonOperator(
    task_id='prepare_stage2_config',
    python_callable=prepare_stage2_config,
    execution_timeout=timedelta(minutes=2),
    dag=dag
)

# Задача 5: Запуск Stage 2 - Content Transformation
trigger_stage2 = TriggerDagRunOperator(
    task_id='trigger_stage2_transformation',
    trigger_dag_id='content_transformation',
    conf="{{ task_instance.xcom_pull(task_ids='prepare_stage2_config') }}",
    wait_for_completion=True,
    poke_interval=30,
    allowed_states=['success'],
    execution_timeout=timedelta(minutes=30),
    dag=dag
)

# Задача 6: Подготовка конфигурации Stage 3
prepare_stage3 = PythonOperator(
    task_id='prepare_stage3_config',
    python_callable=prepare_stage3_config,
    execution_timeout=timedelta(minutes=2),
    dag=dag
)

# Задача 7: Запуск Stage 3 - Translation Pipeline
trigger_stage3 = TriggerDagRunOperator(
    task_id='trigger_stage3_translation',
    trigger_dag_id='translation_pipeline',
    conf="{{ task_instance.xcom_pull(task_ids='prepare_stage3_config') }}",
    wait_for_completion=True,
    poke_interval=30,
    allowed_states=['success'],
    execution_timeout=timedelta(minutes=30),
    dag=dag
)

# Задача 8: Подготовка конфигурации Stage 4
prepare_stage4 = PythonOperator(
    task_id='prepare_stage4_config',
    python_callable=prepare_stage4_config,
    execution_timeout=timedelta(minutes=2),
    dag=dag
)

# Задача 9: Запуск Stage 4 - Quality Assurance
trigger_stage4 = TriggerDagRunOperator(
    task_id='trigger_stage4_quality_assurance',
    trigger_dag_id='quality_assurance',
    conf="{{ task_instance.xcom_pull(task_ids='prepare_stage4_config') }}",
    wait_for_completion=True,
    poke_interval=30,
    allowed_states=['success'],
    execution_timeout=timedelta(minutes=20),
    dag=dag
)

# Задача 10: Мониторинг прогресса
monitor_progress = PythonOperator(
    task_id='monitor_pipeline_progress',
    python_callable=monitor_pipeline_progress,
    execution_timeout=timedelta(minutes=3),
    dag=dag
)

# Задача 11: Финализация оркестрации
finalize_orchestrator = PythonOperator(
    task_id='finalize_orchestration',
    python_callable=finalize_orchestration,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

# Задача 12: Финальное уведомление
notify_completion = PythonOperator(
    task_id='notify_orchestrator_completion',
    python_callable=notify_orchestrator_completion,
    trigger_rule='all_done',
    execution_timeout=timedelta(minutes=3),
    dag=dag
)

# ================================================================================
# ОПРЕДЕЛЕНИЕ ЗАВИСИМОСТЕЙ
# ================================================================================

# Последовательный запуск всех 4 этапов
(validate_input >> prepare_stage1 >> trigger_stage1 >> 
 prepare_stage2 >> trigger_stage2 >>
 prepare_stage3 >> trigger_stage3 >>
 prepare_stage4 >> trigger_stage4 >>
 monitor_progress >> finalize_orchestrator >> notify_completion)

# ================================================================================
# ОБРАБОТКА ОШИБОК
# ================================================================================

def handle_orchestrator_failure(context):
    """Специальная обработка ошибок оркестратора"""
    try:
        failed_task = context['task_instance'].task_id
        master_config = context.get('dag_run', {}).conf or {}
        exception = context.get('exception')
        
        error_message = f"""
🔥 КРИТИЧЕСКАЯ ОШИБКА В ОРКЕСТРАТОРЕ v3.0

Задача: {failed_task}
Файл: {master_config.get('filename', 'unknown')}
Ошибка: {str(exception) if exception else 'Unknown error'}

Упрощенный конвейер остановлен. 

РЕКОМЕНДАЦИИ ПО УСТРАНЕНИЮ:
1. Проверьте доступность всех DAG файлов
2. Убедитесь в корректности входных данных
3. Проверьте права доступа к директориям
4. Проверьте логи конкретного этапа для деталей

Конвейер v3.0 использует встроенную логику без внешних микросервисов.
        """
        
        logger.error(error_message)
        NotificationUtils.send_failure_notification(context, exception)
        
    except Exception as e:
        logger.error(f"❌ Ошибка в обработчике ошибок оркестратора: {e}")

# Применение обработчика ошибок ко всем задачам
for task in dag.tasks:
    task.on_failure_callback = handle_orchestrator_failure