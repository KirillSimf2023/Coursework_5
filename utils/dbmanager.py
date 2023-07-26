import psycopg2
from config import config


class DBManager():

    def __init__(self, db_name: str):
        self.db_name = db_name
        self.params = config()


    def create_db(self):
        """Создание базы данных и таблиц для сохранения данных."""

        conn = psycopg2.connect(dbname='postgres', **self.params)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute(f"DROP DATABASE IF EXISTS {self.db_name}")
        cur.execute(f"CREATE DATABASE {self.db_name}")

        cur.close()
        conn.close()

    def create_tables(self):

        conn_db = psycopg2.connect(dbname=self.db_name, **self.params)

        with conn_db.cursor() as cur:
            cur.execute("""
                  CREATE TABLE company (
                      company_id int PRIMARY KEY,
                      name VARCHAR NOT NULL,
                      alternate_url VARCHAR,
                      site_url VARCHAR,
                      vacancies_url VARCHAR,
                      description TEXT
                  )
              """)

        conn_db.commit()

        with conn_db.cursor() as cur:
            cur.execute("""
                  CREATE TABLE vacancies (
                      id_vacancy int PRIMARY KEY,
                      title VARCHAR NOT NULL,
                      link VARCHAR NOT NULL,
                      salary_min REAL,
                      salary_max REAL,
                      salary_currency varchar,
                      company_id INT REFERENCES company(company_id),
                      description TEXT
                  )
              """)

        conn_db.commit()


        #Triger
        with conn_db.cursor() as cur:
            cur.execute("""
                    CREATE OR REPLACE FUNCTION vacancies_insert_trigger_fnc()
                    RETURNS trigger AS
                    $$
                    BEGIN
                        if NEW.salary_min is NULL THEN
                            NEW.salary_min = 0;
                        end if;	
                        if NEW.salary_max is NULL THEN
                            NEW.salary_max = 0;
                        end if;
                        if NEW.salary_min > 0 AND NEW.salary_max = 0 THEN
                            NEW.salary_max = NEW.salary_min;
                        end if;    
                        if NEW.salary_max > 0 AND NEW.salary_min = 0 THEN
                            NEW.salary_min = NEW.salary_max;
                        end if;    
                        RETURN NEW;
                    END;
                    $$
                    LANGUAGE 'plpgsql';
                    CREATE TRIGGER vacancies_insert_trigger
                      BEFORE INSERT
                      ON "vacancies"
                      FOR EACH ROW
                      EXECUTE PROCEDURE vacancies_insert_trigger_fnc();
              """)

        conn_db.commit()
        conn_db.close()


    def set_table_company(self, company):

        conn_db = psycopg2.connect(dbname=self.db_name, **self.params)

        for item in company:
            with conn_db.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO  company (company_id, name, alternate_url, site_url, vacancies_url, description)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (item['id'], item['name'], item['alternate_url'], item['site_url'], item['vacancies_url'], item['description'])
                )
                conn_db.commit()

        conn_db.close()

    def set_table_vacancies(self, vacancies):
        conn_db = psycopg2.connect(dbname=self.db_name, **self.params)

        for item in vacancies:
            with conn_db.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO  vacancies (id_vacancy, title, link, salary_min, salary_max, salary_currency, company_id, description)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (item['id'], item['title'], item['link'], item['salary_min'], item['salary_max'],
                     item['salary_currency'], item['company_id'], item['description'])
                )
                conn_db.commit()
        conn_db.close()



    def get_companies_and_vacancies_count(self):
        """получает список всех компаний и количество вакансий у каждой компании."""
        conn = psycopg2.connect(dbname=self.db_name, **self.params)
        with conn.cursor() as cur:
            cur.execute(""" SELECT company.name, COUNT(*) as count_vacncy 
            	            FROM vacancies
            	            INNER JOIN company USING (company_id)
            	            group by company.name; 
            	            """)
            rows = cur.fetchall()
            for row in rows:
                print(f'Компания - {row[0]} \ всего вакансий - {row[1]}')



    def get_all_vacancies(self):
        """получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию"""
        conn = psycopg2.connect(dbname=self.db_name, **self.params)
        with conn.cursor() as cur:
            cur.execute(""" 
                        SELECT company.name, vacancies.title, (vacancies.salary_min + vacancies.salary_max)/2 as salary, vacancies.link
	                    FROM vacancies
	                    INNER JOIN company USING (company_id); 
                  	    """)
            rows = cur.fetchall()
            for row in rows:
                print(f'Компания - {row[0]} \ Вакансия - {row[1]} \ Зарплата - {row[2]} \ Ссылка - {row[3]}')


    def get_avg_salary(self):
        """получает среднюю зарплату по вакансиям."""
        conn = psycopg2.connect(dbname=self.db_name, **self.params)
        with conn.cursor() as cur:
            cur.execute("""
                    SELECT company.name, ROUND(AVG((vacancies.salary_min + vacancies.salary_max)/2))
	                FROM vacancies
	                INNER JOIN company USING (company_id)
                    where vacancies.salary_min<>0 and vacancies.salary_max<>0
                    group by company.name;
                    """)
            rows = cur.fetchall()
            for row in rows:
                print(f'Компания - {row[0]} \ Средняя зарплата - {row[1]}')


    def get_vacancies_with_higher_salary(self):
        """получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        conn = psycopg2.connect(dbname=self.db_name, **self.params)
        with conn.cursor() as cur:
            cur.execute("""
                        SELECT title, link, salary_min
	                    FROM public.vacancies
                        where salary_min > (SELECT ROUND(AVG((salary_min + salary_max)/2)) from vacancies where vacancies.salary_min<>0 and vacancies.salary_max<>0);
                        """)
            rows = cur.fetchall()
            for row in rows:
                print(f'Вакансия - {row[0]} \ Ссылка - {row[1]} \ Зарплата - {row[2]}')


    def get_vacancies_with_keyword(self, keyword):
        """получает список всех вакансий, в названии которых содержатся переданные в метод слова, например “Java”."""
        conn = psycopg2.connect(dbname=self.db_name, **self.params)
        with conn.cursor() as cur:
            cur.execute("""SELECT title, link, salary_min 
                            FROM vacancies
                            where title LIKE %s;                                
                                """, (f"%{keyword}%",))
            rows = cur.fetchall()
            for row in rows:
                print(f'Вакансия - {row[0]} \ Ссылка - {row[1]} \ Зарплата - {row[2]}')




