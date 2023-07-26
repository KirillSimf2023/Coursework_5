DROP DATABASE IF EXISTS {self.db_name}
CREATE DATABASE {self.db_name}

CREATE TABLE company (
                      company_id int PRIMARY KEY,
                      name VARCHAR NOT NULL,
                      alternate_url VARCHAR,
                      site_url VARCHAR,
                      vacancies_url VARCHAR,
                      description TEXT)


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



--  Тригер используется для верификации данных при добавдениии их в таблицу
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


-- добаление данных о компании
 """INSERT INTO  company (company_id, name, alternate_url, site_url, vacancies_url, description)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (item['id'], item['name'], item['alternate_url'], item['site_url'], item['vacancies_url'], item['description']))



-- добавление данных о вакансиях
 """INSERT INTO  vacancies (id_vacancy, title, link, salary_min, salary_max, salary_currency, company_id, description)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (item['id'], item['title'], item['link'], item['salary_min'], item['salary_max'],
                     item['salary_currency'], item['company_id'], item['description'])





-- get_companies_and_vacancies_count(): получает список всех компаний и количество вакансий у каждой компании.
SELECT company.name, COUNT(*) as count_vacncy
	FROM vacancies
	INNER JOIN company USING (company_id)
group by company.name;


--get_all_vacancies(): получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию.
SELECT company.name, vacancies.title, (vacancies.salary_min + vacancies.salary_max)/2 as salary, vacancies.link
	FROM vacancies
	INNER JOIN company USING (company_id);


-- get_avg_salary(): получает среднюю зарплату по вакансиям.
SELECT company.name, ROUND(AVG((vacancies.salary_min + vacancies.salary_max)/2))
	FROM vacancies
	INNER JOIN company USING (company_id)
where vacancies.salary_min<>0 and vacancies.salary_max<>0
group by company.name;


-- get_vacancies_with_higher_salary(): получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
SELECT title, link, salary_min
	FROM public.vacancies
where salary_min > (SELECT ROUND(AVG((salary_min + salary_max)/2)) from vacancies where vacancies.salary_min<>0 and vacancies.salary_max<>0);


--get_vacancies_with_keyword(): получает список всех вакансий, в названии которых содержатся переданные в метод слова, например “python”.
"""SELECT title, link, salary_min
                            FROM vacancies
                            where title LIKE %s;
                                """, (f"%{keyword}%",)


