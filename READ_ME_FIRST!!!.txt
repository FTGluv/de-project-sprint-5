Для проверки задания:
1. Проверьте описание маппинга - documentation/api_entities.md
2. Выполните sql из ddl_sql для расширения схемы
3. Скопировать folder dags в ваш instance AirFlow
4. Дождаться отработки всех дагов
5. запустить отдельно dags/project_dags/sprint5_project.py (если вдруг он не отработал)
6. Для ясности я всю логику поместил в sprint5_project.py

Также есть следующие folder-ы
1. dml_sql - тут лежат sql, которые я вставил в dag для перемещения данных
2. test_load_api - драфт работы с API