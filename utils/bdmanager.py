
import psycopg2

class DBManager():

    def __init__(self):
        pass

    def __str__(self):
        pass

    def __repr__(self):
        pass

    # получает список всех компаний и количество вакансий у каждой компании.
    def get_companies_and_vacancies_count():
        pass

    # получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию
    def get_all_vacancies():
        pass

    # получает среднюю зарплату по вакансиям.
    def get_avg_salary():
        pass

    # получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
    def get_vacancies_with_higher_salary():
        pass

    # получает список всех вакансий, в названии которых содержатся переданные в метод слова, например “python”.
    def get_vacancies_with_keyword(keyword -> str):
        pass

