from utils.dbmanager import DBManager
from utils.hhru import HhruJob
import psycopg2
from config import config



emp_list = (6189,1057,5060211,3131901,11036,160748,67611,901158,3529,212543,2000762,9311920,3207941,32570,5894850, 9498112, 78638, 2324020, 80, 1049556, 3388)




def main():
    my_hhru = HhruJob(emp_list)

    print('Начинаем запрос информации о компаниях:')
    data_employer = my_hhru.get_data_employer()
    print(f'\nСобрана информация по - {len(data_employer)} компаниям')

    print('Начинаем сбор информации о вакансиях в собранных компаниях:')
    data_vacancies = my_hhru.get_data_vacancies(data_employer)
    print(f'\nНайдено - {len(data_vacancies)} вакансии')

    my_db = DBManager('kirill_hhru')
    my_db.create_db()
    print('База данных создана.')
    my_db.create_tables()
    print('Таблицы созданы.')
    my_db.set_table_company(data_employer)
    my_db.set_table_vacancies(data_vacancies)
    print('Данные полученные с HH.ru сохранены в базу данных')

    print('Предлагаю поработать с собранной информацией:')
    while True:
        user_answer = input(
            'Что желаете сделать с полученными данными?\n'
            '7 - Выход\n'
            '1 - Вывести список всех компаний и количество вакансий у каждой компании\n'
            '2 - Вывести список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию\n'
            '3 - Вывести среднюю зарплату по вакансиям компании\n'
            '4 - Вывести список всех вакансий, у которых зарплата выше средней по всем вакансиям\n'
            '5 - Вывести список всех вакансий, в названии которых содержатся переданные в метод слова, например: java\n'
        )

        match user_answer:
            case '1':
                # Вывести список всех компаний и количество вакансий у каждой компании
                my_db.get_companies_and_vacancies_count()
                pass
            case '2':
               #  Вывести список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию
               my_db.get_all_vacancies()
               pass
            case '3':
                # Вывести среднюю зарплату по вакансиям компании
                my_db.get_avg_salary()
                pass
            case '4':
                # Вывести список всех вакансий, у которых зарплата выше средней по всем вакансиям
                my_db.get_vacancies_with_higher_salary()
                pass
            case '5':
                # Вывести список всех вакансий, в названии которых содержатся переданные в метод слова, например “java”
                my_db.get_vacancies_with_keyword('Java')
                pass
            case '7':
                print("Спасибо что использовал мою программу!!!")
                quit()
            case _:
                print("Не правильно ввел выбор! Попробуй еще раз.")


if __name__ == '__main__':
    main()

