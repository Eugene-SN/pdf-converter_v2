#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
✅ ПЕРЕРАБОТАННЫЙ Content Transformation v3.0 - Упрощенная архитектура
Прямая обработка без микросервисов, оптимизированная для китайских документов

КЛЮЧЕВЫЕ ИЗМЕНЕНИЯ:
- ✅ Убрана зависимость от внешних микросервисов
- ✅ Встроенная логика трансформации
- ✅ Оптимизация для китайских технических документов
- ✅ Упрощенная но эффективная обработка
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
import os
import json
import logging
import time
import re
from typing import Dict, Any, Optional, List

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
    'content_transformation',
    default_args=DEFAULT_ARGS,
    description='DAG 2: Content Transformation v3.0 - Упрощенная обработка для китайских документов',
    schedule_interval=None,
    max_active_runs=2,
    catchup=False,
    tags=['pdf-converter', 'dag2', 'transformation', 'chinese-docs', 'v3']
)

# ================================================================================
# СПЕЦИАЛИЗИРОВАННАЯ ОБРАБОТКА ДЛЯ КИТАЙСКИХ ДОКУМЕНТОВ
# ================================================================================

CHINESE_TRANSFORMATION_CONFIG = {
    # Паттерны китайских заголовков
    'heading_patterns': [
        r'^[第章节]\s*[一二三四五六七八九十\d]+\s*[章节]',  # 第X章, 第X节
        r'^[一二三四五六七八九十]+[、．]',  # 中文数字
        r'^\d+[、．]\s*[\u4e00-\u9fff]',  # 数字 + 中文
        r'^[\u4e00-\u9fff]+[:：]',  # 中文标题后跟冒号
    ],
    
    # Технические термины для сохранения
    'preserve_terms': {
        '问天': 'WenTian',
        '联想问天': 'Lenovo WenTian',
        '天擎': 'ThinkSystem',
        'AnyBay': 'AnyBay',
        '至强': 'Xeon',
        '可扩展处理器': 'Scalable Processors',
        '英特尔': 'Intel',
        '处理器': 'Processor',
        '内核': 'Core',
        '线程': 'Thread',
        '睿频': 'Turbo Boost',
        '内存': 'Memory',
        '存储': 'Storage',
        '硬盘': 'Drive',
        '固态硬盘': 'SSD',
        '机械硬盘': 'HDD',
        '热插拔': 'Hot-swap',
        '冗余': 'Redundancy',
        '背板': 'Backplane',
        '托架': 'Tray',
        '以太网': 'Ethernet',
        '光纤': 'Fiber',
        '带宽': 'Bandwidth',
        '延迟': 'Latency',
        '网卡': 'Network Adapter',
        '英寸': 'inch',
        '机架': 'Rack',
        '插槽': 'Slot',
        '转接卡': 'Riser Card',
        '电源': 'Power Supply',
        '铂金': 'Platinum',
        '钛金': 'Titanium',
        'CRPS': 'CRPS'
    },
    
    # Настройки качества
    'quality_settings': {
        'preserve_chinese_structure': True,
        'enhance_technical_formatting': True,
        'improve_table_structure': True,
        'clean_whitespace': True
    }
}

# ================================================================================
# ОСНОВНЫЕ ФУНКЦИИ ТРАНСФОРМАЦИИ
# ================================================================================

def load_intermediate_data(**context) -> Dict[str, Any]:
    """Загрузка промежуточных данных от Stage 1"""
    start_time = time.time()
    
    try:
        dag_run_conf = context['dag_run'].conf or {}
        logger.info(f"📥 Загрузка данных для трансформации: {json.dumps(dag_run_conf, indent=2, ensure_ascii=False)}")
        
        # Получение промежуточного файла
        intermediate_file = dag_run_conf.get('intermediate_file')
        if not intermediate_file or not os.path.exists(intermediate_file):
            raise ValueError(f"Промежуточный файл не найден: {intermediate_file}")
        
        # Чтение данных
        with open(intermediate_file, 'r', encoding='utf-8') as f:
            document_data = json.load(f)
        
        if not document_data or 'markdown_content' not in document_data:
            raise ValueError("Данные документа некорректны или отсутствуют")
        
        # Подготовка сессии трансформации
        transformation_session = {
            'session_id': f"transform_{int(time.time())}",
            'document_data': document_data,
            'original_config': dag_run_conf.get('original_config', {}),
            'intermediate_file': intermediate_file,
            'chinese_document': dag_run_conf.get('chinese_document', True),
            'preserve_technical_terms': dag_run_conf.get('preserve_technical_terms', True),
            'transformation_start_time': datetime.now().isoformat()
        }
        
        content_length = len(document_data.get('markdown_content', ''))
        logger.info(f"✅ Данные загружены: {content_length} символов для трансформации")
        
        MetricsUtils.record_processing_metrics(
            dag_id='content_transformation',
            task_id='load_intermediate_data',
            processing_time=time.time() - start_time,
            success=True
        )
        
        return transformation_session
        
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='content_transformation',
            task_id='load_intermediate_data',
            processing_time=time.time() - start_time,
            success=False
        )
        logger.error(f"❌ Ошибка загрузки данных: {e}")
        raise

def transform_chinese_content(**context) -> Dict[str, Any]:
    """Основная трансформация контента с китайской оптимизацией"""
    start_time = time.time()
    session = context['task_instance'].xcom_pull(task_ids='load_intermediate_data')
    
    try:
        logger.info("🔄 Начинаем трансформацию китайского контента")
        
        document_data = session['document_data']
        markdown_content = document_data.get('markdown_content', '')
        
        if not markdown_content.strip():
            raise ValueError("Нет контента для трансформации")
        
        # ✅ Применяем специализированные трансформации
        transformed_content = apply_chinese_transformations(markdown_content)
        
        # ✅ Улучшение структуры документа
        structured_content = improve_document_structure(transformed_content)
        
        # ✅ Финальная очистка и форматирование
        final_content = finalize_content_formatting(structured_content)
        
        # Расчет качества трансформации
        quality_score = calculate_transformation_quality(markdown_content, final_content)
        
        transformation_results = {
            'transformed_content': final_content,
            'original_length': len(markdown_content),
            'transformed_length': len(final_content),
            'quality_score': quality_score,
            'chinese_chars_preserved': count_chinese_characters(final_content),
            'technical_terms_preserved': count_preserved_terms(final_content),
            'transformation_stats': {
                'processing_time_seconds': time.time() - start_time,
                'transformation_method': 'chinese_optimized_v3',
                'content_improved': True
            }
        }
        
        MetricsUtils.record_processing_metrics(
            dag_id='content_transformation',
            task_id='transform_chinese_content',
            processing_time=time.time() - start_time,
            success=True
        )
        
        logger.info(f"✅ Трансформация завершена. Качество: {quality_score:.1f}%")
        return transformation_results
        
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='content_transformation',
            task_id='transform_chinese_content',
            processing_time=time.time() - start_time,
            success=False
        )
        logger.error(f"❌ Ошибка трансформации: {e}")
        raise

def apply_chinese_transformations(content: str) -> str:
    """Применение специализированных трансформаций для китайского контента"""
    try:
        # 1. Сохранение технических терминов
        for chinese_term, english_term in CHINESE_TRANSFORMATION_CONFIG['preserve_terms'].items():
            if chinese_term in content:
                # Заменяем на комбинацию китайский + английский
                content = content.replace(chinese_term, f"{chinese_term} ({english_term})")
        
        # 2. Улучшение заголовков
        content = improve_chinese_headings(content)
        
        # 3. Улучшение таблиц
        content = enhance_chinese_tables(content)
        
        # 4. Очистка форматирования
        content = clean_chinese_formatting(content)
        
        return content
        
    except Exception as e:
        logger.warning(f"Ошибка применения китайских трансформаций: {e}")
        return content

def improve_chinese_headings(content: str) -> str:
    """Улучшение форматирования китайских заголовков"""
    try:
        lines = content.split('\n')
        improved_lines = []
        
        for line in lines:
            line_stripped = line.strip()
            if not line_stripped:
                improved_lines.append(line)
                continue
            
            # Проверяем паттерны китайских заголовков
            heading_level = detect_chinese_heading_level(line_stripped)
            
            if heading_level > 0 and not line_stripped.startswith('#'):
                # Добавляем markdown заголовок
                markdown_prefix = '#' * heading_level + ' '
                improved_lines.append(f"{markdown_prefix}{line_stripped}")
            else:
                improved_lines.append(line)
        
        return '\n'.join(improved_lines)
        
    except Exception as e:
        logger.warning(f"Ошибка улучшения заголовков: {e}")
        return content

def detect_chinese_heading_level(text: str) -> int:
    """Определение уровня китайского заголовка"""
    for pattern in CHINESE_TRANSFORMATION_CONFIG['heading_patterns']:
        if re.match(pattern, text):
            # Определяем уровень по паттерну
            if '第' in text and ('章' in text):
                return 1  # Главы
            elif '第' in text and ('节' in text):
                return 2  # Разделы
            elif re.match(r'^[一二三四五六七八九十]+[、．]', text):
                return 3  # Подразделы
            elif re.match(r'^\d+[、．]', text):
                return 2  # Нумерованные разделы
            else:
                return 2  # По умолчанию
    
    return 0  # Не заголовок

def enhance_chinese_tables(content: str) -> str:
    """Улучшение структуры таблиц с китайским контентом"""
    try:
        lines = content.split('\n')
        enhanced_lines = []
        in_table = False
        
        for i, line in enumerate(lines):
            if '|' in line and len([cell for cell in line.split('|') if cell.strip()]) >= 2:
                if not in_table:
                    # Начало таблицы
                    in_table = True
                    enhanced_lines.append(line)
                    
                    # Проверяем, есть ли разделитель
                    if (i + 1 < len(lines) and 
                        not re.match(r'^\|[\s\-:|]+\|', lines[i + 1])):
                        # Добавляем разделитель
                        cols = len([cell for cell in line.split('|') if cell.strip()])
                        separator = '|' + ' --- |' * cols
                        enhanced_lines.append(separator)
                else:
                    enhanced_lines.append(line)
            else:
                if in_table and line.strip() == '':
                    in_table = False
                enhanced_lines.append(line)
        
        return '\n'.join(enhanced_lines)
        
    except Exception as e:
        logger.warning(f"Ошибка улучшения таблиц: {e}")
        return content

def clean_chinese_formatting(content: str) -> str:
    """Очистка форматирования китайского текста"""
    try:
        # Убираем лишние пробелы вокруг китайских символов
        content = re.sub(r'([\u4e00-\u9fff])\s+([\u4e00-\u9fff])', r'\1\2', content)
        
        # Исправляем пробелы вокруг знаков препинания
        content = re.sub(r'([\u4e00-\u9fff])\s*([，。；：！？])', r'\1\2', content)
        
        # Убираем множественные пустые строки
        content = re.sub(r'\n\s*\n\s*\n+', '\n\n', content)
        
        # Очищаем лишние пробелы в начале и конце строк
        lines = [line.rstrip() for line in content.split('\n')]
        content = '\n'.join(lines)
        
        return content.strip()
        
    except Exception as e:
        logger.warning(f"Ошибка очистки форматирования: {e}")
        return content

def improve_document_structure(content: str) -> str:
    """Улучшение общей структуры документа"""
    try:
        lines = content.split('\n')
        structured_lines = []
        
        # Добавляем title если нет
        has_title = any(line.strip().startswith('# ') for line in lines[:5])
        
        if not has_title and lines:
            # Ищем первый заголовок для превращения в title
            for i, line in enumerate(lines[:10]):
                if line.strip() and not line.startswith('#'):
                    structured_lines.append(f"# {line.strip()}")
                    lines[i] = ""  # Убираем дублирование
                    break
        
        # Добавляем остальные строки с улучшениями
        for line in lines:
            if line.strip():
                structured_lines.append(line)
            else:
                structured_lines.append(line)
        
        return '\n'.join(structured_lines)
        
    except Exception as e:
        logger.warning(f"Ошибка улучшения структуры: {e}")
        return content

def finalize_content_formatting(content: str) -> str:
    """Финальное форматирование контента"""
    try:
        # Финальная очистка
        content = content.strip()
        
        # Убираем лишние пустые строки в конце разделов
        content = re.sub(r'(\n#+.*?)\n\n+', r'\1\n\n', content)
        
        # Обеспечиваем правильное расстояние между заголовками и контентом
        content = re.sub(r'(#+\s+.*?)\n([^\n])', r'\1\n\n\2', content)
        
        return content
        
    except Exception as e:
        logger.warning(f"Ошибка финального форматирования: {e}")
        return content

def calculate_transformation_quality(original: str, transformed: str) -> float:
    """Расчет качества трансформации"""
    try:
        quality_score = 100.0
        
        # Проверка длины (не должна сильно измениться)
        length_ratio = len(transformed) / max(len(original), 1)
        if length_ratio < 0.8 or length_ratio > 1.3:
            quality_score -= 10
        
        # Проверка сохранения заголовков
        original_headers = len(re.findall(r'^#+\s', original, re.MULTILINE))
        transformed_headers = len(re.findall(r'^#+\s', transformed, re.MULTILINE))
        
        if transformed_headers < original_headers:
            quality_score -= 15
        
        # Проверка сохранения таблиц
        original_tables = len(re.findall(r'\|.*\|', original))
        transformed_tables = len(re.findall(r'\|.*\|', transformed))
        
        if original_tables > 0:
            table_preservation = transformed_tables / original_tables
            if table_preservation < 0.9:
                quality_score -= 10
        
        # Проверка сохранения китайских символов
        original_chinese = count_chinese_characters(original)
        transformed_chinese = count_chinese_characters(transformed)
        
        if original_chinese > 0:
            chinese_preservation = transformed_chinese / original_chinese
            if chinese_preservation < 0.9:
                quality_score -= 20
        
        return max(0, quality_score)
        
    except Exception:
        return 75.0  # Средняя оценка по умолчанию

def count_chinese_characters(text: str) -> int:
    """Подсчет китайских символов"""
    return len(re.findall(r'[\u4e00-\u9fff]', text))

def count_preserved_terms(text: str) -> int:
    """Подсчет сохраненных технических терминов"""
    count = 0
    for term in CHINESE_TRANSFORMATION_CONFIG['preserve_terms'].values():
        count += text.count(term)
    return count

def save_transformation_result(**context) -> Dict[str, Any]:
    """Сохранение результата трансформации"""
    start_time = time.time()
    
    try:
        session = context['task_instance'].xcom_pull(task_ids='load_intermediate_data')
        transformation_results = context['task_instance'].xcom_pull(task_ids='transform_chinese_content')
        
        original_config = session['original_config']
        timestamp = original_config.get('timestamp', int(time.time()))
        filename = original_config.get('filename', 'unknown.pdf')
        
        # Определение пути сохранения (китайский язык - источник)
        output_dir = f"/app/output/zh"
        os.makedirs(output_dir, exist_ok=True)
        
        markdown_filename = f"{timestamp}_{filename.replace('.pdf', '.md')}"
        output_path = f"{output_dir}/{markdown_filename}"
        
        # Сохранение трансформированного контента
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(transformation_results['transformed_content'])
        
        # Подготовка конфигурации для Stage 3
        stage3_config = {
            'markdown_file': output_path,
            'markdown_content': transformation_results['transformed_content'],
            'original_config': original_config,
            'stage2_completed': True,
            'transformation_metadata': {
                'quality_score': transformation_results['quality_score'],
                'transformation_method': 'chinese_optimized_v3',
                'chinese_chars_preserved': transformation_results['chinese_chars_preserved'],
                'technical_terms_preserved': transformation_results['technical_terms_preserved'],
                'completion_time': datetime.now().isoformat()
            }
        }
        
        MetricsUtils.record_processing_metrics(
            dag_id='content_transformation',
            task_id='save_transformation_result',
            processing_time=time.time() - start_time,
            success=True
        )
        
        logger.info(f"💾 Трансформированный контент сохранен: {output_path}")
        return stage3_config
        
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='content_transformation',
            task_id='save_transformation_result',
            processing_time=time.time() - start_time,
            success=False
        )
        logger.error(f"❌ Ошибка сохранения результата: {e}")
        raise

def notify_transformation_completion(**context) -> None:
    """Уведомление о завершении трансформации"""
    try:
        stage3_config = context['task_instance'].xcom_pull(task_ids='save_transformation_result')
        transformation_metadata = stage3_config['transformation_metadata']
        
        quality_score = transformation_metadata['quality_score']
        chinese_chars = transformation_metadata['chinese_chars_preserved']
        tech_terms = transformation_metadata['technical_terms_preserved']
        
        message = f"""
✅ CONTENT TRANSFORMATION ЗАВЕРШЕН УСПЕШНО

📄 Файл: {stage3_config['markdown_file']}
🎯 Качество трансформации: {quality_score:.1f}%
🈶 Китайских символов: {chinese_chars}
🔧 Технических терминов: {tech_terms}
📊 Метод: {transformation_metadata['transformation_method']}

✅ Готов к передаче на Stage 3 (Translation Pipeline)
        """
        
        logger.info(message)
        NotificationUtils.send_success_notification(context, stage3_config)
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки уведомления: {e}")

# ================================================================================
# ОПРЕДЕЛЕНИЕ ЗАДАЧ
# ================================================================================

# Задача 1: Загрузка промежуточных данных
load_data = PythonOperator(
    task_id='load_intermediate_data',
    python_callable=load_intermediate_data,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

# Задача 2: Трансформация китайского контента
transform_content = PythonOperator(
    task_id='transform_chinese_content',
    python_callable=transform_chinese_content,
    execution_timeout=timedelta(minutes=15),
    dag=dag
)

# Задача 3: Сохранение результата
save_result = PythonOperator(
    task_id='save_transformation_result',
    python_callable=save_transformation_result,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

# Задача 4: Уведомление о завершении
notify_completion = PythonOperator(
    task_id='notify_transformation_completion',
    python_callable=notify_transformation_completion,
    trigger_rule='all_done',
    execution_timeout=timedelta(minutes=2),
    dag=dag
)

# Определение зависимостей
load_data >> transform_content >> save_result >> notify_completion

# Обработка ошибок
def handle_transformation_failure(context):
    """Обработка ошибок трансформации"""
    try:
        failed_task = context['task_instance'].task_id
        exception = context.get('exception')
        
        error_message = f"""
🔥 ОШИБКА В CONTENT TRANSFORMATION

Задача: {failed_task}
Ошибка: {str(exception) if exception else 'Unknown'}

Возможные причины:
1. Поврежденные промежуточные данные
2. Проблемы с форматированием контента
3. Недостаток памяти для обработки
4. Ошибки в китайских трансформациях

Требуется проверка логов и входных данных.
        """
        
        logger.error(error_message)
        NotificationUtils.send_failure_notification(context, exception)
        
    except Exception as e:
        logger.error(f"❌ Ошибка в обработчике ошибок: {e}")

# Применение обработчика ошибок ко всем задачам
for task in dag.tasks:
    task.on_failure_callback = handle_transformation_failure