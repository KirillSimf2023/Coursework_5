import requests
import json
import time


"Класс для работы с API HH.ru"

class HhruJob():

    def __init__(self, list_employer):
        self.list_employer = list_employer


    def get_data_employer(self):
        result = []
        # i = 1
        for item in self.list_employer:
            url = 'https://api.hh.ru/employers/' + str(item)
            response = requests.get(url)
            if response.status_code == 200:
                data = response.content.decode()
                response.close()
                employer_data = json.loads(data)
                print('.', end='')

                #  Создание словарей работодателя
                employer = {
                    'id': employer_data['id'],
                    'name': employer_data['name'],
                    'alternate_url': employer_data['alternate_url'], # адрес страницы на HH.ru
                    'site_url': employer_data['site_url'], # адрес сайта
                    'vacancies_url': employer_data['vacancies_url'], # адрес страницы с вакансиями на HH.ru
                    'description': employer_data['description'] # описание
                }
                result.append(employer)
            else:
                print(f"Request failed with status code: {response.status_code}")

        return result


    def get_data_vacancies(self, data_employer):
        result = []
        # count = 1

        for item in data_employer:
            url = item['vacancies_url']
            params = {
                "area": 113,  # Россия
                "per_page": 100,  # количество вакансий на страницу
                "page": 0  # номер страницы
            }
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.content.decode()
                response.close()
                vacancies_data = json.loads(data)

                if vacancies_data['pages'] > 0 and vacancies_data['found'] > 0:
                    # print(item['id'] + ' ' + str(vacancies_data['pages']) + ' ' + str(vacancies_data['found']))

                    for i in range(vacancies_data['pages']):
                        # задержка
                        time.sleep(0.25)
                        print('.', end='')
                        params_pages = {
                                "area": 113,  # Россия
                                "per_page": 100,  # количество вакансий на страницу
                                "page": i  # номер страницы
                            }
                        response_vacancies = requests.get(url, params=params_pages)

                        if response_vacancies.status_code == 200:
                            data_vacancies = response_vacancies.content.decode()
                            response_vacancies.close()
                            vacancies_data = json.loads(data_vacancies)
                            for v_item in vacancies_data['items']:
                                # print(v_items)
                                # print('.', end='')
                                # count = count + 1
                                id_vacancy = v_item['id']
                                title = v_item['name']
                                link = v_item['alternate_url']
                                company_id = v_item['employer']['id']
                                description = v_item['snippet']['requirement']

                                if v_item['salary'] is not None:
                                    salary_min = v_item.get('salary').get('from')
                                    salary_max = v_item.get('salary').get('to')
                                    salary_currency = v_item.get('salary').get('currency')
                                else:
                                    salary_min = 0
                                    salary_max = 0
                                    salary_currency = 'нет'

                                #  Создание словарей из вакансий
                                job = {
                                        'id': id_vacancy,
                                        'title': title,
                                        'link': link,
                                        'salary_min': salary_min,
                                        'salary_max': salary_max,
                                        'salary_currency': salary_currency,
                                        'company_id': company_id,
                                        'description': description}

                                result.append(job)
            else:
                print(f"Request failed with status code: {response.status_code}")
        return result


