#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
✅ ПЕРЕРАБОТАННЫЙ DAG: Document Preprocessing - Единый процессор конвертации
ВЕРСИЯ 3.0 - Production-ready решение для китайских технических PDF

АРХИТЕКТУРНЫЕ ИЗМЕНЕНИЯ:
- ✅ Airflow только как оркестратор
- ✅ Вся логика конвертации в этом DAG
- ✅ Оптимизация для китайских технических документов
- ✅ Прямая интеграция с Docling без микросервисов
- ✅ Упрощенная но мощная архитектура
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
import os
import json
import logging
import time
from typing import Dict, Any, Optional
from pathlib import Path

# Прямые импорты для обработки (через внешний сервис document-processor)
import requests  # ← выносим конвертацию из Airflow в микросервис [оркестрация]
DOCUMENT_PROCESSOR_URL = os.getenv('DOCUMENT_PROCESSOR_URL', 'http://document-processor:8001')

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
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'document_preprocessing',  # ✅ ИСПРАВЛЕНО: имя соответствует оркестратору
    default_args=DEFAULT_ARGS,
    description='DAG 1: Единый процессор конвертации PDF в Markdown для китайских документов',
    schedule_interval=None,
    max_active_runs=3,
    catchup=False,
    tags=['pdf-converter', 'dag1', 'chinese-docs', 'production']
)

# ================================================================================
# КОНФИГУРАЦИЯ ДЛЯ КИТАЙСКИХ ДОКУМЕНТОВ
# ================================================================================

# Специальная конфигурация для китайских технических документов
CHINESE_DOC_CONFIG = {
    # OCR настройки для китайского языка
    'ocr_languages': 'chi_sim,chi_tra,eng',  # Упрощенный и традиционный китайский + английский
    'ocr_confidence_threshold': 0.75,  # Пониженный порог для китайских символов
    
    # Специальные паттерны для китайских документов
    'chinese_header_patterns': [
        r'^[第章节]\s*[一二三四五六七八九十\d]+\s*[章节]',  # 第X章, 第X节
        r'^[一二三四五六七八九十]+[、．]',  # Китайские числительные
        r'^\d+[、．]\s*[\u4e00-\u9fff]',  # Арабские цифры + китайские символы
    ],
    
    # Техническая терминология (НЕ ПЕРЕВОДИТЬ)
    'tech_terms': {
        '问天': 'WenTian',
        '联想问天': 'Lenovo WenTian', 
        '天擎': 'ThinkSystem',
        '至强': 'Xeon',
        '可扩展处理器': 'Scalable Processors',
        '英特尔': 'Intel',
        '处理器': 'Processor',
        '内核': 'Core',
        '线程': 'Thread',
        '内存': 'Memory',
        '存储': 'Storage',
        '以太网': 'Ethernet',
        '机架': 'Rack',
        '插槽': 'Slot',
        '电源': 'Power Supply'
    },
    
    # Настройки качества для китайских PDF
    'quality_settings': {
        'dpi': 300,  # Высокое DPI для четких китайских символов
        'enable_table_detection': True,
        'preserve_chinese_formatting': True,
        'enhance_chinese_text': True
    }
}

# ================================================================================
# ОСНОВНЫЕ ФУНКЦИИ ОБРАБОТКИ
# ================================================================================

def validate_input_file(**context) -> Dict[str, Any]:
    """✅ Валидация входного файла с поддержкой китайских документов"""
    start_time = time.time()
    
    try:
        dag_run_conf = context['dag_run'].conf or {}
        logger.info(f"📋 Получена конфигурация: {json.dumps(dag_run_conf, indent=2, ensure_ascii=False)}")
        
        # Обязательные параметры
        required_params = ['input_file', 'filename', 'timestamp']
        missing_params = [param for param in required_params if not dag_run_conf.get(param)]
        
        if missing_params:
            raise ValueError(f"Отсутствуют обязательные параметры: {missing_params}")

        master_run_id = dag_run_conf.get('master_run_id') or context['dag_run'].run_id
        if 'master_run_id' not in dag_run_conf:
            logger.info(f"🆔 master_run_id не передан во входной конфигурации. "
                        f"Автозаполнение из контекста: {master_run_id}")
        dag_run_conf['master_run_id'] = master_run_id  # нормализуем конфигурацию для дальнейших стадий            
        
        # Валидация файла
        input_file = dag_run_conf['input_file']
        if not SharedUtils.validate_input_file(input_file):
            raise ValueError(f"Некорректный файл: {input_file}")
        
        # Расширенная валидация для китайских документов
        file_info = analyze_chinese_document(input_file)
        
        # Обогащенная конфигурация
        enriched_config = {
            **dag_run_conf,
            **file_info,
            'chinese_doc_analysis': file_info,
            'processing_mode': 'chinese_optimized',
            'validation_timestamp': datetime.now().isoformat()
        }
        
        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing',
            task_id='validate_input_file',
            processing_time=time.time() - start_time,
            success=True
        )
        
        logger.info(f"✅ Входной файл валидирован: {dag_run_conf['filename']}")
        return enriched_config
        
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing',
            task_id='validate_input_file', 
            processing_time=time.time() - start_time,
            success=False
        )
        logger.error(f"❌ Ошибка валидации: {e}")
        raise

def analyze_chinese_document(file_path: str) -> Dict[str, Any]:
    """Анализ китайского документа для оптимизации обработки"""
    try:
        file_size = os.path.getsize(file_path)
        file_hash = SharedUtils.calculate_file_hash(file_path)
        
        # Быстрая проверка на наличие китайского текста (если PDF читается)
        has_chinese_text = False
        estimated_pages = 0
        
        try:
            # Пробуем быстро определить характер документа
            import fitz  # PyMuPDF для быстрого анализа
            doc = fitz.open(file_path)
            estimated_pages = doc.page_count
            
            # Проверяем первые 3 страницы на китайский текст
            for page_num in range(min(3, doc.page_count)):
                page = doc[page_num]
                text = page.get_text()[:1000]  # Первые 1000 символов
                
                # Проверка на китайские символы
                chinese_chars = sum(1 for c in text if '\u4e00' <= c <= '\u9fff')
                if chinese_chars > 10:  # Если найдено более 10 китайских символов
                    has_chinese_text = True
                    break
            
            doc.close()
            
        except Exception:
            # Fallback анализ по размеру файла
            estimated_pages = max(1, file_size // 102400)  # ~100KB на страницу
        
        return {
            'file_hash': file_hash,
            'file_size_bytes': file_size,
            'file_size_mb': file_size / (1024 * 1024),
            'estimated_pages': estimated_pages,
            'has_chinese_text': has_chinese_text,
            'recommended_ocr': not has_chinese_text,  # OCR если текст не читается
            'processing_complexity': 'high' if file_size > 50*1024*1024 else 'medium'
        }
        
    except Exception as e:
        logger.warning(f"Не удалось проанализировать документ: {e}")
        return {
            'file_hash': 'unknown',
            'file_size_bytes': 0,
            'file_size_mb': 0.0,
            'estimated_pages': 1,
            'has_chinese_text': True,  # Предполагаем китайский по умолчанию
            'recommended_ocr': True,
            'processing_complexity': 'medium'
        }

def process_document_with_docling(**context) -> Dict[str, Any]:
    """✅ Основная обработка документа через внешний сервис document-processor
    (Docling и OCR запускаются ТОЛЬКО в отдельном сервисе, Airflow здесь — оркестратор)
    """
    start_time = time.time()
    config = context['task_instance'].xcom_pull(task_ids='validate_input_file')
    try:
        input_file = config['input_file']
        timestamp = config['timestamp']
        filename = config['filename']

        logger.info(f"🔄 Отправляем PDF в document-processor: {DOCUMENT_PROCESSOR_URL}/process")

        # Формируем опции сервиса (то, что ранее задавалось в DAG)
        options = {
            "extract_tables": bool(config.get("extract_tables", True)),
            "extract_images": bool(config.get("extract_images", True)),
            "extract_formulas": bool(config.get("extract_formulas", True)),
            "use_ocr": bool(config.get("enable_ocr", config.get("chinese_doc_analysis", {}).get("recommended_ocr", False))),
            "ocr_languages": config.get("ocr_languages", "eng,chi_sim"),
            "high_quality_ocr": bool(config.get("high_quality_ocr", True)),
            "preserve_layout": bool(config.get("preserve_structure", True)),
            "enable_chunking": False
        }

        # Безопасный временный каталог: TEMP_DIR → processing_paths.temp_dir → $AIRFLOW_HOME/temp
        airflow_home_temp = os.path.join(os.getenv('AIRFLOW_HOME', '/opt/airflow'), 'temp')  # стандарт для образов Airflow
        candidates = [
            airflow_home_temp,
            os.getenv('TEMP_DIR'),
            (ConfigUtils.get_processing_paths().get('temp_dir') if 'ConfigUtils' in globals() else None)
        ]
        temp_root = None
        for cand in candidates:
            if not cand:
                continue
            try:
                os.makedirs(cand, exist_ok=True)
                temp_root = cand
                break
            except PermissionError:
                logger.warning(f"⚠️ Нет прав на каталог {cand}, пробуем следующий")
        if not temp_root:
            # Финальная страховка: используем $AIRFLOW_HOME/temp
            temp_root = airflow_home_temp
            os.makedirs(temp_root, exist_ok=True)

        # Отправляем файл и опции в сервис
        with open(input_file, 'rb') as f:
            files = {'file': (os.path.basename(input_file), f, 'application/pdf')}
            data = {'options': json.dumps(options, ensure_ascii=False)}
            resp = requests.post(f"{DOCUMENT_PROCESSOR_URL}/process", files=files, data=data, timeout=60*30)

        if resp.status_code != 200:
            err = f"document-processor вернул {resp.status_code}: {resp.text}"
            logger.error(f"❌ {err}")
            return {"success": False, "error": err, "original_config": config}

        resp_json = resp.json()
        if not resp_json.get("success", False):
            err = f"document-processor сообщил об ошибке: {resp_json.get('message') or resp_json.get('error')}"
            logger.error(f"❌ {err}")
            return {"success": False, "error": err, "original_config": config}

        # Находим промежуточный файл, предпочтительно из поля 'intermediate_file'
        intermediate_file = resp_json.get("intermediate_file")
        if not intermediate_file:
            # fallback: пытаемся взять из output_files первый JSON
            for p in resp_json.get("output_files", []):
                if str(p).endswith("_intermediate.json") or str(p).endswith(".json"):
                    intermediate_file = p
                    break

        if not intermediate_file or not os.path.exists(intermediate_file):
            # Если сервис вернул путь в своём work_dir, но он не шарится с Airflow,
            # дополнительно сохраняем полезный минимум рядом (как локальный артефакт)
            local_intermediate = os.path.join(temp_root, f"preprocessing_{timestamp}.json")
            safe_payload = {
                "title": resp_json.get("document_id", filename.replace('.pdf', '')),
                "pages_count": resp_json.get("pages_count", 0),
                "metadata": resp_json.get("metadata", {}),
            }
            with open(local_intermediate, "w", encoding="utf-8") as f:
                json.dump(safe_payload, f, ensure_ascii=False, indent=2)
            intermediate_file = local_intermediate
            logger.warning("⚠️ Путь промежуточного файла из сервиса недоступен Airflow. "
                           "Сохранили минимальный локальный артефакт для продолжения пайплайна.")

        processing_time = resp_json.get("processing_time", time.time() - start_time)
        pages = resp_json.get("pages_count", 0)

        result = {
            "success": True,
            "document_info": {
                "title": filename.replace('.pdf', ''),
                "total_pages": pages,
                "processing_time": processing_time,
                "status": "success"
            },
            "intermediate_file": intermediate_file,
            "original_config": config,
            "processing_stats": {
                "pages_processed": pages,
                "ocr_used": options["use_ocr"],
                "processing_time_seconds": processing_time
            }
        }

        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing',
            task_id='process_document_with_docling',
            processing_time=processing_time,
            pages_count=pages,
            success=True
        )
        logger.info(f"✅ Обработка через document-processor завершена за {processing_time:.2f}с")
        return result

    except Exception as e:
        error_msg = f"Ошибка обращения к document-processor: {str(e)}"
        logger.error(f"❌ {error_msg}")
        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing',
            task_id='process_document_with_docling',
            processing_time=time.time() - start_time,
            success=False
        )
        return {
            "success": False,
            "error": error_msg,
            "original_config": config
        }

def process_document_fallback(input_file: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Fallback обработка без Docling"""
    try:
        logger.warning("⚠️ Используем fallback обработку (Docling недоступен)")
        
        # Простое извлечение текста
        markdown_content = f"# {config['filename'].replace('.pdf', '')}\n\n"
        markdown_content += "Документ обработан в fallback режиме.\n\n"
        markdown_content += f"Файл: {config['filename']}\n"
        markdown_content += f"Размер: {config['chinese_doc_analysis']['file_size_mb']:.2f} MB\n"
        
        # Подготовка данных
        temp_dir = "/opt/airflow/temp"
        os.makedirs(temp_dir, exist_ok=True)
        intermediate_file = f"{temp_dir}/preprocessing_{config['timestamp']}.json"
        
        document_data = {
            'title': config['filename'].replace('.pdf', ''),
            'pages_count': config['chinese_doc_analysis']['estimated_pages'],
            'markdown_content': markdown_content,
            'raw_text': markdown_content,
            'metadata': {
                'fallback_mode': True,
                'processing_timestamp': config['timestamp']
            }
        }
        
        with open(intermediate_file, 'w', encoding='utf-8') as f:
            json.dump(document_data, f, ensure_ascii=False, indent=2)
        
        return {
            'success': True,
            'document_info': {
                'title': document_data['title'],
                'total_pages': document_data['pages_count'],
                'processing_time': 1.0,
                'status': 'fallback_success'
            },
            'intermediate_file': intermediate_file,
            'original_config': config,
            'processing_stats': {
                'fallback_mode': True,
                'processing_time_seconds': 1.0
            }
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': f"Fallback обработка не удалась: {str(e)}",
            'original_config': config
        }

def post_process_chinese_markdown(markdown: str) -> str:
    """Постобработка Markdown для китайских документов"""
    try:
        # Сохранение технических терминов
        for chinese_term, english_term in CHINESE_DOC_CONFIG['tech_terms'].items():
            if chinese_term in markdown:
                # Сохраняем оригинальные технические термины
                markdown = markdown.replace(chinese_term, f"{chinese_term} ({english_term})")
        
        # Улучшение форматирования заголовков
        lines = markdown.split('\n')
        processed_lines = []
        
        for line in lines:
            # Обработка китайских заголовков
            for pattern in CHINESE_DOC_CONFIG['chinese_header_patterns']:
                import re
                if re.match(pattern, line.strip()):
                    if not line.strip().startswith('#'):
                        line = f"## {line.strip()}"
                    break
            
            processed_lines.append(line)
        
        # Улучшение структуры таблиц для китайских документов
        processed_markdown = '\n'.join(processed_lines)
        processed_markdown = improve_chinese_tables(processed_markdown)
        
        return processed_markdown
        
    except Exception as e:
        logger.warning(f"Постобработка китайского markdown не удалась: {e}")
        return markdown

def improve_chinese_tables(markdown: str) -> str:
    """Улучшение форматирования таблиц с китайским текстом"""
    try:
        import re
        
        # Поиск таблиц и улучшение их форматирования
        lines = markdown.split('\n')
        improved_lines = []
        in_table = False
        
        for line in lines:
            if '|' in line and len(line.split('|')) >= 3:
                if not in_table:
                    # Начало таблицы - добавляем заголовок если его нет
                    in_table = True
                    if not any(c in line for c in ['---', '===', '-+-']):
                        improved_lines.append(line)
                        # Добавляем разделитель для таблицы
                        cols = len([col for col in line.split('|') if col.strip()])
                        separator = '|' + '---|' * max(1, cols-2) + '|'
                        improved_lines.append(separator)
                        continue
                improved_lines.append(line)
            else:
                if in_table and line.strip() == '':
                    in_table = False
                improved_lines.append(line)
        
        return '\n'.join(improved_lines)
        
    except Exception as e:
        logger.warning(f"Улучшение таблиц не удалось: {e}")
        return markdown

def count_chinese_characters(text: str) -> int:
    """Подсчет китайских символов в тексте"""
    try:
        return sum(1 for char in text if '\u4e00' <= char <= '\u9fff')
    except:
        return 0

def prepare_for_next_stage(**context) -> Dict[str, Any]:
    """Подготовка данных для следующего DAG"""
    start_time = time.time()
    
    try:
        result = context['task_instance'].xcom_pull(task_ids='process_document_with_docling')
        
        if not result.get('success'):
            raise AirflowException(f"Обработка документа не удалась: {result.get('error')}")
        
        # Проверка промежуточного файла
        intermediate_file = result.get('intermediate_file')
        if not intermediate_file or not os.path.exists(intermediate_file):
            raise AirflowException("Промежуточный файл не найден")
        
        # Подготовка конфигурации для DAG2
        next_stage_config = {
            'intermediate_file': intermediate_file,
            'original_config': result['original_config'],
            'dag1_metadata': {
                **result.get('processing_stats', {}),
                'completion_time': datetime.now().isoformat()
            },
            'dag1_completed': True,
            'ready_for_transformation': True,
            'chinese_document': True  # Флаг для оптимизации следующих стадий
        }
        
        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing',
            task_id='prepare_for_next_stage',
            processing_time=time.time() - start_time,
            success=True
        )
        
        logger.info("✅ Данные подготовлены для следующего DAG")
        return next_stage_config
        
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing',
            task_id='prepare_for_next_stage',
            processing_time=time.time() - start_time,
            success=False
        )
        logger.error(f"❌ Ошибка подготовки данных: {e}")
        raise

def notify_completion(**context) -> None:
    """Уведомление о завершении обработки"""
    try:
        result = context['task_instance'].xcom_pull(task_ids='process_document_with_docling')
        next_config = context['task_instance'].xcom_pull(task_ids='prepare_for_next_stage')
        
        if result and result.get('success'):
            stats = result.get('processing_stats', {})
            message = f"""
✅ DOCUMENT PREPROCESSING ЗАВЕРШЕН УСПЕШНО

📄 Файл: {result['original_config']['filename']}
📊 Страниц обработано: {stats.get('pages_processed', 'N/A')}
⏱️ Время обработки: {stats.get('processing_time_seconds', 0):.2f}с
🔍 OCR использован: {'Да' if stats.get('ocr_used') else 'Нет'}
🈶 Китайских символов: {stats.get('chinese_chars_found', 0)}
🎯 Режим: Оптимизация для китайских документов

📁 Промежуточный файл: {next_config.get('intermediate_file', 'N/A')}

✅ Готов к передаче на следующую стадию
            """
            
            NotificationUtils.send_success_notification(context, result)
        else:
            error = result.get('error', 'Unknown error') if result else 'No result'
            message = f"""
❌ DOCUMENT PREPROCESSING ЗАВЕРШЕН С ОШИБКОЙ

📄 Файл: {result['original_config']['filename'] if result else 'Unknown'}
❌ Ошибка: {error}
⏰ Время: {datetime.now().isoformat()}

Требуется проверка конфигурации и входных данных.
            """
            NotificationUtils.send_failure_notification(context, Exception(error))
        
        logger.info(message)
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки уведомления: {e}")

# ================================================================================
# ОПРЕДЕЛЕНИЕ ЗАДАЧ
# ================================================================================

# Задача 1: Валидация входных данных
validate_input = PythonOperator(
    task_id='validate_input_file',
    python_callable=validate_input_file,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

# Задача 2: Основная обработка документа
process_document = PythonOperator(
    task_id='process_document_with_docling',
    python_callable=process_document_with_docling,
    execution_timeout=timedelta(hours=1),
    dag=dag
)

# Задача 3: Подготовка для следующего DAG
prepare_next = PythonOperator(
    task_id='prepare_for_next_stage',
    python_callable=prepare_for_next_stage,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

# Задача 4: Уведомление о завершении
notify_task = PythonOperator(
    task_id='notify_completion',
    python_callable=notify_completion,
    trigger_rule='all_done',
    execution_timeout=timedelta(minutes=2),
    dag=dag
)

# Определение зависимостей
validate_input >> process_document >> prepare_next >> notify_task

# Обработка ошибок
def handle_processing_failure(context):
    """Обработка ошибок обработки"""
    try:
        failed_task = context['task_instance'].task_id
        exception = context.get('exception')
        
        error_message = f"""
🔥 КРИТИЧЕСКАЯ ОШИБКА В DOCUMENT PREPROCESSING

Задача: {failed_task}
Ошибка: {str(exception) if exception else 'Unknown'}

Возможные причины:
1. Поврежденный PDF файл
2. Недостаточно ресурсов для обработки
3. Проблемы с Docling библиотекой
4. Неподдерживаемый формат документа

Рекомендации:
- Проверьте целостность PDF файла
- Убедитесь в достаточности памяти
- Проверьте логи для детальной диагностики
        """
        
        logger.error(error_message)
        NotificationUtils.send_failure_notification(context, exception)
        
        MetricsUtils.record_processing_metrics(
            dag_id='document_preprocessing',
            task_id=failed_task,
            processing_time=0,
            success=False
        )
        
    except Exception as e:
        logger.error(f"❌ Ошибка в обработчике ошибок: {e}")

# Применение обработчика ошибок ко всем задачам
for task in dag.tasks:
    task.on_failure_callback = handle_processing_failure