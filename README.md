# Проект 7-го спринта

### Описание
Репозиторий предназначен для сдачи проекта 7-го спринта  
Даг запускается каждый день


### Структура хранилища
1. Stagging layer 
	* /user/master/data/geo/events - данные по событиям
2. ODS layer
	* /user/ilyasmolin/data/geo/events - обогащенные данные по событиям (гео метки, партиции)
	* /user/ilyasmolin/data/geo/city_coordinates - данные по гео меткам городов + временные зоны
3. CDM layer
	* /user/ilyasmolin/data/datamart/recomendation_zone_report - витрина для рекомендаций друзей по заданию, отчет запускется на всех данных
	* /user/ilyasmolin/data/datamart/week_zone_report - витрина в разрезе зон по заданию, отчет расчитывает данные только за последний месяц и перезаписывает витрину
	* /user/ilyasmolin/data/datamart/user_zone_report - витрина в разрезе пользователей по заданию, отчет запускется на всех данных
4. Analytics layer
	* /user/ilyasmolin/data/analytics - среда для аналитиков по умолчанию


### Запуск приложения
1. Поместите содержимое директории репозитория dags в директорию dags в Airflow
2. Запустите hadoop_dag
3. Если нужно прогнать hadoop_dag за все предыдущие даты, то измените параметр catchup=False на catchup=True (на выделенной машине Яндекса при catchup=True происходит зависание, поэтому по умолчанию стоит False)
