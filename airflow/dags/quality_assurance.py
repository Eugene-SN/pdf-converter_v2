#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
✅ ПЕРЕРАБОТАННЫЙ Quality Assurance v3.0 - Упрощенная архитектура
Встроенная логика контроля качества без внешних микросервисов

КЛЮЧЕВЫЕ ИЗМЕНЕНИЯ:
- ✅ Убрана зависимость от QA микросервиса
- ✅ Встроенные проверки качества
- ✅ Специализация для китайских документов
- ✅ Простая но эффективная валидация
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
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'quality_assurance',
    default_args=DEFAULT_ARGS,
    description='DAG 4: Quality Assurance v3.0 - Упрощенный контроль качества для китайских документов',
    schedule_interval=None,
    max_active_runs=2,
    catchup=False,
    tags=['pdf-converter', 'dag4', 'qa', 'chinese-docs', 'v3']
)

# ================================================================================
# КОНФИГУРАЦИЯ КОНТРОЛЯ КАЧЕСТВА
# ================================================================================

QA_RULES = {
    # Минимальные требования к документу
    'min_content_length': 100,  # минимум символов
    'min_headings': 1,  # минимум заголовков
    'max_chinese_chars_ratio': 0.3,  # максимум 30% китайских символов после перевода
    
    # Проверки структуры
    'require_title': True,
    'check_table_structure': True,
    'validate_markdown_syntax': True,
    
    # Проверки технических терминов
    'technical_terms_check': True,
    'preserve_brand_names': True,
    
    # Оценки качества
    'min_quality_score': 80.0,  # минимальный балл качества
    'excellent_quality_score': 95.0  # отличное качество
}

TECHNICAL_BRAND_TERMS = [
    'WenTian', 'Lenovo WenTian', 'ThinkSystem', 'AnyBay',
    'Xeon', 'Intel', 'Scalable Processors'
]

# ================================================================================
# ФУНКЦИИ КОНТРОЛЯ КАЧЕСТВА
# ================================================================================

def load_translated_document(**context) -> Dict[str, Any]:
    """Загрузка переведенного документа для проверки качества"""
    start_time = time.time()
    
    try:
        dag_run_conf = context['dag_run'].conf or {}
        logger.info(f"🔍 Начало контроля качества: {json.dumps(dag_run_conf, indent=2, ensure_ascii=False)}")
        
        # Получение переведенного файла
        translated_file = dag_run_conf.get('translated_file')
        if not translated_file or not os.path.exists(translated_file):
            raise ValueError(f"Переведенный файл не найден: {translated_file}")
        
        # Чтение переведенного контента
        with open(translated_file, 'r', encoding='utf-8') as f:
            translated_content = f.read()
        
        if not translated_content.strip():
            raise ValueError("Переведенный файл пустой")
        
        # Подготовка сессии QA
        qa_session = {
            'session_id': f"qa_{int(time.time())}",
            'translated_file': translated_file,
            'translated_content': translated_content,
            'original_config': dag_run_conf.get('original_config', {}),
            'translation_metadata': dag_run_conf.get('translation_metadata', {}),
            'qa_start_time': datetime.now().isoformat(),
            'target_quality': dag_run_conf.get('quality_target', 90.0),
            'auto_correction': dag_run_conf.get('auto_correction', True)
        }
        
        MetricsUtils.record_processing_metrics(
            dag_id='quality_assurance',
            task_id='load_translated_document',
            processing_time=time.time() - start_time,
            success=True
        )
        
        content_length = len(translated_content)
        logger.info(f"✅ Документ загружен для QA: {content_length} символов")
        return qa_session
        
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='quality_assurance',
            task_id='load_translated_document',
            processing_time=time.time() - start_time,
            success=False
        )
        logger.error(f"❌ Ошибка загрузки документа для QA: {e}")
        raise

def perform_quality_checks(**context) -> Dict[str, Any]:
    """Выполнение комплексных проверок качества"""
    start_time = time.time()
    qa_session = context['task_instance'].xcom_pull(task_ids='load_translated_document')
    
    try:
        translated_content = qa_session['translated_content']
        target_quality = qa_session['target_quality']
        
        logger.info(f"🔍 Выполняем проверки качества (цель: {target_quality}%)")
        
        qa_results = {
            'checks_performed': [],
            'issues_found': [],
            'scores': {},
            'overall_score': 0.0,
            'corrections_suggested': [],
            'quality_level': 'unknown'
        }
        
        # 1. Проверка базовой структуры
        structure_score = check_document_structure(translated_content, qa_results)
        qa_results['scores']['structure'] = structure_score
        
        # 2. Проверка содержимого
        content_score = check_content_quality(translated_content, qa_results)
        qa_results['scores']['content'] = content_score
        
        # 3. Проверка технических терминов
        terms_score = check_technical_terms(translated_content, qa_results)
        qa_results['scores']['technical_terms'] = terms_score
        
        # 4. Проверка перевода (китайские символы)
        translation_score = check_translation_quality(translated_content, qa_results)
        qa_results['scores']['translation'] = translation_score
        
        # 5. Проверка Markdown синтаксиса
        markdown_score = check_markdown_syntax(translated_content, qa_results)
        qa_results['scores']['markdown'] = markdown_score
        
        # Расчет общего балла
        overall_score = calculate_overall_score(qa_results['scores'])
        qa_results['overall_score'] = overall_score
        
        # Определение уровня качества
        qa_results['quality_level'] = determine_quality_level(overall_score)
        
        MetricsUtils.record_processing_metrics(
            dag_id='quality_assurance',
            task_id='perform_quality_checks',
            processing_time=time.time() - start_time,
            success=True
        )
        
        logger.info(f"✅ Проверки качества завершены. Общий балл: {overall_score:.1f}%")
        return qa_results
        
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='quality_assurance',
            task_id='perform_quality_checks',
            processing_time=time.time() - start_time,
            success=False
        )
        logger.error(f"❌ Ошибка проверки качества: {e}")
        raise

def check_document_structure(content: str, qa_results: Dict) -> float:
    """Проверка структуры документа"""
    score = 100.0
    qa_results['checks_performed'].append('document_structure')
    
    try:
        # Проверка длины контента
        if len(content) < QA_RULES['min_content_length']:
            qa_results['issues_found'].append(f"Документ слишком короткий: {len(content)} символов")
            score -= 30
        
        # Проверка заголовков
        headers = re.findall(r'^#+\s+', content, re.MULTILINE)
        if len(headers) < QA_RULES['min_headings']:
            qa_results['issues_found'].append(f"Мало заголовков: {len(headers)}")
            score -= 20
        
        # Проверка title (главного заголовка)
        if QA_RULES['require_title'] and not re.search(r'^#\s+', content, re.MULTILINE):
            qa_results['issues_found'].append("Отсутствует главный заголовок")
            score -= 15
        
        # Проверка таблиц
        if QA_RULES['check_table_structure']:
            tables = re.findall(r'\|.*\|', content)
            malformed_tables = 0
            
            for table_line in tables:
                if not re.search(r'\|[\s\-:|]+\|', table_line):  # Нет разделителя
                    malformed_tables += 1
            
            if malformed_tables > len(tables) * 0.3:  # Более 30% плохих таблиц
                qa_results['issues_found'].append(f"Неправильно оформленные таблицы: {malformed_tables}")
                score -= 10
        
        return max(0, score)
        
    except Exception as e:
        logger.warning(f"Ошибка проверки структуры: {e}")
        return 50.0

def check_content_quality(content: str, qa_results: Dict) -> float:
    """Проверка качества содержимого"""
    score = 100.0
    qa_results['checks_performed'].append('content_quality')
    
    try:
        # Проверка пустых разделов
        empty_sections = len(re.findall(r'^#+\s+.*\n\s*\n\s*#+', content, re.MULTILINE))
        if empty_sections > 0:
            qa_results['issues_found'].append(f"Пустые разделы: {empty_sections}")
            score -= empty_sections * 5
        
        # Проверка повторяющихся строк
        lines = content.split('\n')
        unique_lines = set(line.strip() for line in lines if line.strip())
        repetition_ratio = 1 - (len(unique_lines) / max(len(lines), 1))
        
        if repetition_ratio > 0.3:  # Более 30% повторов
            qa_results['issues_found'].append(f"Высокий уровень повторов: {repetition_ratio:.1%}")
            score -= 20
        
        # Проверка слишком длинных строк
        long_lines = [line for line in lines if len(line) > 200 and not line.startswith('|')]
        if len(long_lines) > len(lines) * 0.2:  # Более 20% длинных строк
            qa_results['issues_found'].append(f"Слишком много длинных строк: {len(long_lines)}")
            score -= 10
        
        return max(0, score)
        
    except Exception as e:
        logger.warning(f"Ошибка проверки содержимого: {e}")
        return 70.0

def check_technical_terms(content: str, qa_results: Dict) -> float:
    """Проверка технических терминов"""
    score = 100.0
    qa_results['checks_performed'].append('technical_terms')
    
    try:
        if not QA_RULES['technical_terms_check']:
            return score
        
        # Проверка сохранения брендов
        if QA_RULES['preserve_brand_names']:
            missing_brands = []
            for brand in TECHNICAL_BRAND_TERMS:
                if brand not in content and brand.lower() not in content.lower():
                    # Проверяем, есть ли этот бренд в китайском виде
                    chinese_brands = {'WenTian': '问天', 'Lenovo WenTian': '联想问天', 'ThinkSystem': '天擎'}
                    chinese_form = chinese_brands.get(brand)
                    if chinese_form and chinese_form in content:
                        missing_brands.append(f"{brand} (найден в китайском виде: {chinese_form})")
            
            if missing_brands:
                qa_results['issues_found'].append(f"Отсутствуют бренды: {missing_brands}")
                score -= len(missing_brands) * 15
        
        # Проверка технической терминологии
        tech_indicators = ['Processor', 'Memory', 'Storage', 'Network', 'Power', 'Rack', 'Server']
        found_indicators = sum(1 for indicator in tech_indicators if indicator in content)
        
        if found_indicators == 0:
            qa_results['issues_found'].append("Не найдены технические термины")
            score -= 20
        elif found_indicators < 3:
            qa_results['corrections_suggested'].append("Рекомендуется проверить полноту технических терминов")
            score -= 5
        
        return max(0, score)
        
    except Exception as e:
        logger.warning(f"Ошибка проверки технических терминов: {e}")
        return 80.0

def check_translation_quality(content: str, qa_results: Dict) -> float:
    """Проверка качества перевода"""
    score = 100.0
    qa_results['checks_performed'].append('translation_quality')
    
    try:
        # Подсчет китайских символов
        chinese_chars = len(re.findall(r'[\u4e00-\u9fff]', content))
        total_chars = len(content)
        
        if total_chars > 0:
            chinese_ratio = chinese_chars / total_chars
            max_allowed_ratio = QA_RULES['max_chinese_chars_ratio']
            
            if chinese_ratio > max_allowed_ratio:
                qa_results['issues_found'].append(
                    f"Слишком много китайских символов: {chinese_ratio:.1%} (лимит: {max_allowed_ratio:.1%})"
                )
                score -= (chinese_ratio - max_allowed_ratio) * 100
            
            # Если есть китайские символы, предлагаем исправления
            if chinese_chars > 0:
                qa_results['corrections_suggested'].append(
                    f"Найдено {chinese_chars} непереведенных китайских символов"
                )
        
        # Проверка качества перевода по структуре
        # Слишком короткий результат может означать потерю информации
        if total_chars < 500:  # Очень короткий документ
            qa_results['issues_found'].append("Документ может быть неполным после перевода")
            score -= 20
        
        return max(0, score)
        
    except Exception as e:
        logger.warning(f"Ошибка проверки качества перевода: {e}")
        return 75.0

def check_markdown_syntax(content: str, qa_results: Dict) -> float:
    """Проверка синтаксиса Markdown"""
    score = 100.0
    qa_results['checks_performed'].append('markdown_syntax')
    
    try:
        if not QA_RULES['validate_markdown_syntax']:
            return score
        
        # Проверка заголовков
        malformed_headers = re.findall(r'^#{7,}', content, re.MULTILINE)  # Более 6 #
        if malformed_headers:
            qa_results['issues_found'].append(f"Неправильные заголовки: {len(malformed_headers)}")
            score -= len(malformed_headers) * 5
        
        # Проверка списков
        malformed_lists = re.findall(r'^\d+\.\s*$', content, re.MULTILINE)  # Пустые пункты списков
        if malformed_lists:
            qa_results['issues_found'].append(f"Пустые пункты списков: {len(malformed_lists)}")
            score -= len(malformed_lists) * 2
        
        # Проверка ссылок
        broken_links = re.findall(r'\]\(\s*\)', content)  # Пустые ссылки
        if broken_links:
            qa_results['issues_found'].append(f"Пустые ссылки: {len(broken_links)}")
            score -= len(broken_links) * 3
        
        # Проверка таблиц
        table_lines = re.findall(r'^\|.*\|$', content, re.MULTILINE)
        separator_lines = re.findall(r'^\|[\s\-:|]+\|$', content, re.MULTILINE)
        
        # Должен быть хотя бы один разделитель на каждую таблицу
        if table_lines and not separator_lines:
            qa_results['issues_found'].append("Таблицы без разделителей заголовков")
            score -= 15
        
        return max(0, score)
        
    except Exception as e:
        logger.warning(f"Ошибка проверки Markdown: {e}")
        return 85.0

def calculate_overall_score(scores: Dict[str, float]) -> float:
    """Расчет общего балла качества"""
    if not scores:
        return 0.0
    
    # Веса для разных типов проверок
    weights = {
        'structure': 0.25,
        'content': 0.25,
        'technical_terms': 0.25,
        'translation': 0.20,
        'markdown': 0.05
    }
    
    total_weighted_score = 0.0
    total_weight = 0.0
    
    for check_type, score in scores.items():
        weight = weights.get(check_type, 0.1)  # По умолчанию 10%
        total_weighted_score += score * weight
        total_weight += weight
    
    if total_weight > 0:
        return total_weighted_score / total_weight
    else:
        return sum(scores.values()) / len(scores)

def determine_quality_level(score: float) -> str:
    """Определение уровня качества"""
    if score >= QA_RULES['excellent_quality_score']:
        return 'excellent'
    elif score >= QA_RULES['min_quality_score']:
        return 'good'
    elif score >= 60.0:
        return 'acceptable'
    else:
        return 'poor'

def generate_qa_report(**context) -> Dict[str, Any]:
    """Генерация отчета о контроле качества"""
    start_time = time.time()
    
    try:
        qa_session = context['task_instance'].xcom_pull(task_ids='load_translated_document')
        qa_results = context['task_instance'].xcom_pull(task_ids='perform_quality_checks')
        
        overall_score = qa_results['overall_score']
        quality_level = qa_results['quality_level']
        
        # Создание детального отчета
        qa_report = {
            'session_id': qa_session['session_id'],
            'document_file': qa_session['translated_file'],
            'qa_completion_time': datetime.now().isoformat(),
            'processing_duration_seconds': time.time() - start_time,
            
            # Результаты проверок
            'overall_score': overall_score,
            'quality_level': quality_level,
            'quality_passed': overall_score >= QA_RULES['min_quality_score'],
            'detailed_scores': qa_results['scores'],
            
            # Проблемы и рекомендации
            'checks_performed': qa_results['checks_performed'],
            'issues_found': qa_results['issues_found'],
            'corrections_suggested': qa_results['corrections_suggested'],
            
            # Метаданные
            'qa_rules_version': '3.0',
            'target_quality': qa_session['target_quality'],
            'auto_correction_enabled': qa_session['auto_correction'],
            
            # Статистика
            'stats': {
                'total_checks': len(qa_results['checks_performed']),
                'issues_count': len(qa_results['issues_found']),
                'suggestions_count': len(qa_results['corrections_suggested'])
            }
        }
        
        # Сохранение отчета
        report_file = f"/app/temp/qa_report_{qa_session['session_id']}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(qa_report, f, ensure_ascii=False, indent=2)
        
        qa_report['report_file'] = report_file
        
        MetricsUtils.record_processing_metrics(
            dag_id='quality_assurance',
            task_id='generate_qa_report',
            processing_time=time.time() - start_time,
            success=True
        )
        
        logger.info(f"📊 QA отчет создан: {overall_score:.1f}% ({quality_level})")
        return qa_report
        
    except Exception as e:
        MetricsUtils.record_processing_metrics(
            dag_id='quality_assurance',
            task_id='generate_qa_report',
            processing_time=time.time() - start_time,
            success=False
        )
        logger.error(f"❌ Ошибка создания QA отчета: {e}")
        raise

def finalize_qa_process(**context) -> Dict[str, Any]:
    """Финализация процесса контроля качества"""
    try:
        qa_session = context['task_instance'].xcom_pull(task_ids='load_translated_document')
        qa_report = context['task_instance'].xcom_pull(task_ids='generate_qa_report')
        
        overall_score = qa_report['overall_score']
        quality_passed = qa_report['quality_passed']
        
        # Подготовка финального результата
        final_result = {
            'qa_completed': True,
            'quality_score': overall_score,
            'quality_level': qa_report['quality_level'],
            'quality_passed': quality_passed,
            'final_document': qa_session['translated_file'],
            'qa_report': qa_report['report_file'],
            'processing_summary': {
                'checks_performed': qa_report['stats']['total_checks'],
                'issues_found': qa_report['stats']['issues_count'],
                'suggestions_made': qa_report['stats']['suggestions_count']
            },
            'pipeline_completed': quality_passed,
            'ready_for_delivery': quality_passed
        }
        
        logger.info(f"🎯 QA процесс завершен: {overall_score:.1f}% {'✅ PASSED' if quality_passed else '❌ FAILED'}")
        return final_result
        
    except Exception as e:
        logger.error(f"❌ Ошибка финализации QA: {e}")
        raise

def notify_qa_completion(**context) -> None:
    """Уведомление о завершении контроля качества"""
    try:
        final_result = context['task_instance'].xcom_pull(task_ids='finalize_qa_process')
        qa_session = context['task_instance'].xcom_pull(task_ids='load_translated_document')
        
        quality_score = final_result['quality_score']
        quality_level = final_result['quality_level']
        quality_passed = final_result['quality_passed']
        
        status_icon = "✅" if quality_passed else "❌"
        status_text = "ПРОЙДЕН" if quality_passed else "НЕ ПРОЙДЕН"
        
        message = f"""
{status_icon} QUALITY ASSURANCE ЗАВЕРШЕН

🎯 Общий балл качества: {quality_score:.1f}%
📊 Уровень качества: {quality_level}
✅ Контроль качества: {status_text}

📁 Итоговый документ: {final_result['final_document']}
📋 QA отчет: {final_result['qa_report']}

📈 СТАТИСТИКА:
- Проверок выполнено: {final_result['processing_summary']['checks_performed']}
- Проблем найдено: {final_result['processing_summary']['issues_found']}
- Рекомендаций дано: {final_result['processing_summary']['suggestions_made']}

🏁 КОНВЕЙЕР PDF v3.0 ЗАВЕРШЕН {'УСПЕШНО' if quality_passed else 'С ЗАМЕЧАНИЯМИ'}!
        """
        
        logger.info(message)
        
        if quality_passed:
            NotificationUtils.send_success_notification(context, final_result)
        else:
            NotificationUtils.send_failure_notification(context, Exception(f"Качество ниже требуемого: {quality_score:.1f}%"))
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки уведомления QA: {e}")

# ================================================================================
# ОПРЕДЕЛЕНИЕ ЗАДАЧ
# ================================================================================

# Задача 1: Загрузка переведенного документа
load_document = PythonOperator(
    task_id='load_translated_document',
    python_callable=load_translated_document,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

# Задача 2: Выполнение проверок качества
quality_checks = PythonOperator(
    task_id='perform_quality_checks',
    python_callable=perform_quality_checks,
    execution_timeout=timedelta(minutes=10),
    dag=dag
)

# Задача 3: Генерация QA отчета
generate_report = PythonOperator(
    task_id='generate_qa_report',
    python_callable=generate_qa_report,
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

# Задача 4: Финализация QA процесса
finalize_qa = PythonOperator(
    task_id='finalize_qa_process',
    python_callable=finalize_qa_process,
    execution_timeout=timedelta(minutes=3),
    dag=dag
)

# Задача 5: Уведомление о завершении
notify_completion = PythonOperator(
    task_id='notify_qa_completion',
    python_callable=notify_qa_completion,
    trigger_rule='all_done',
    execution_timeout=timedelta(minutes=2),
    dag=dag
)

# Определение зависимостей
load_document >> quality_checks >> generate_report >> finalize_qa >> notify_completion

# Обработка ошибок
def handle_qa_failure(context):
    """Обработка ошибок контроля качества"""
    try:
        failed_task = context['task_instance'].task_id
        exception = context.get('exception')
        
        error_message = f"""
🔥 ОШИБКА В QUALITY ASSURANCE

Задача: {failed_task}
Ошибка: {str(exception) if exception else 'Unknown'}

Возможные причины:
1. Отсутствует переведенный файл
2. Поврежденное содержимое документа
3. Ошибки в правилах QA
4. Проблемы с созданием отчета

Требуется проверка входных данных и конфигурации QA.
        """
        
        logger.error(error_message)
        NotificationUtils.send_failure_notification(context, exception)
        
    except Exception as e:
        logger.error(f"❌ Ошибка в обработчике ошибок QA: {e}")

# Применение обработчика ошибок ко всем задачам
for task in dag.tasks:
    task.on_failure_callback = handle_qa_failure