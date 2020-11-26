import json
import os

os.chdir('task_folder')
json_files = (os.listdir(path='event'))  # Собираем все JSON файлы в один кортеж
schema_files = (os.listdir(path='schema'))  # Собираем все JSON Schema файлы в один кортеж
readed_schema = {}  # Словарь для хэширования схем
types = {"null": None, "integer": int, "object": dict, "array": list, "string": str, "boolean": bool}  # Преобразование
                                                                                                       # типов для схемы


def check_required(schema_base, json_base, error_list, json_level):  # Проверяем присутствие обязательных элементов
    for req in schema_base:  # Проверяем, присутвует ли необходимый элемент в JSON
        try:
            if req not in json_base.keys():
                error_list.append(f"Отсутствует обязательный элемент {req} в {json_level}")
        except AttributeError:
            error_list.append(f"Отсутствует обязательный элемент {req} в {json_level}")


def check_properties(schema_base, json_base, error_list, json_level):  # Проверяем properties
    try:
        for data in json_base.keys():  # Если элемента нет в properties схемы, то он лишний
            if data not in schema_base.keys():
                error_list.append(f"Лишний элемент {data} в {json_level}")
    except AttributeError:
        error_list.append(f"Полностью отсутствуют данные в {json_level}")


def check_data(schema_dict, json_dict, error_list, json_level):  # Проверка данных на соответствие требованиям схемы
    check_required(schema_dict['required'], json_dict, error_list, json_level=json_level)  # Проверяем обязательные
    check_properties(schema_dict['properties'], json_dict, error_list,
                     json_level=json_level)  # Проверяем properties
    try:
        for prop, value in json_dict.items():
            if value is None:
                type_value = None
            else:
                type_value = type(value)
            try:
                if type(schema_dict['properties'][prop]['type']) is list:  # Если в схеме типы в виде списка
                    if type_value not in [types[tp] for tp in schema_dict['properties'][prop]['type']]:
                        error_list.append(f"Некорректный тип у значения '{value}' в {prop}")
                else:
                    if types[schema_dict['properties'][prop]['type']] is not type_value:
                        error_list.append(f"Некорректный тип у значения '{value}' в {prop}")
                if type_value is dict:  # Если значение словарь, проверяем данные в рекурсии
                    check_data(schema_dict['properties'][prop], json_dict[prop], error_list, json_level=prop)
                elif type_value is list:  # Если список, поэлементно проверяем данные в рекурсии
                    for list_prop in value:
                        check_data(schema_dict['properties'][prop]['items'], list_prop, error_list, json_level=prop)
            except (AttributeError, KeyError):
                continue
    except AttributeError:
        return


with open(os.path.join('..', 'log.txt'), 'w', encoding='utf-8') as log:  # Открываем файл для записи логов
    for json_file in json_files:
        error_list = list()  # Здесь будем хранить список обнаруженных ошибок в JSON файле
        with open(os.path.join('event', json_file)) as json_f:  # Открываем очередной JSON файл
            json_dict = json.loads(json_f.read())  # Считываем его
            if type(json_dict) is not dict:  # Если итоговый файл не словарь
                log.write(f"!!!{json_file} не валидный. \nСписок ошибок: \nНе является JSON файлом\n\n")
                continue
            try:
                schema_file = json_dict['event'] + ".schema"  # Проверяем, присутствует ли ключевое слово event
                                                              # для идентификации схемы

            except KeyError:
                log.write(f"!!!{json_file} не валидный. \nСписок ошибок: \nНе содержит ключевое слово 'event. "
                          f"Невозможно сопоставить схему\n\n")
                continue
            if schema_file in schema_files:  # Проверяем валидный ли event указан
                with open(os.path.join('schema', schema_file)) as schema_f:  # Открываем нужную схему
                    if schema_f in readed_schema.keys():  # Кэширование схем
                        schema_dict = readed_schema[schema_file]
                    else:
                        readed_schema[schema_file] = schema_dict = json.loads(schema_f.read())
                    check_data(schema_dict, json_dict['data'], error_list, json_level='data')  # Запускаем проверку
                    if len(error_list) > 0:  # Если для текущего JSON найдены ошибки
                        review = "\n".join(error_list)
                        log.write(f"!!!{json_file} не валидный. \nСписок ошибок: \n{review}\n\n")
                    else:  # Иначе все хорошо
                        log.write(f"{json_file} валидный\n\n")
            else:
                log.write(f"!!!{json_file} не валидный. \nСписок ошибок: \nНекорректный 'event'. "
                          f"Невозможно сопоставить схему \n\n")
