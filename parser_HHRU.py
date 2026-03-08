import requests
import pandas
from bs4 import BeautifulSoup
import configparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import quote

def get_checks():
    duties = []
    requirements = []
    conditions = []
    professions = []
    code_words = []
    try:
        df = pandas.read_excel('Файлы/Опции.xlsx')
        professions_column = df['Профессии'].dropna()
        for profession in professions_column:
            professions.append(str(profession).strip())
        
        code_words_column = df['Кодовые слова'].dropna()
        for code_word in code_words_column:
            code_words.append(str(code_word).strip())


        requirements_column = df['Требования'].dropna()
        for requirement in requirements_column:
            requirements.append(str(requirement).strip())
        
        conditions_column = df['Условия'].dropna()
        for condition in conditions_column:
            conditions.append(str(condition).strip())
        
        duties_column = df['Обязанности'].dropna()
        for duty in duties_column:
            duties.append(str(duty).strip())

    except FileNotFoundError:
        print("Файл Опции.xlsx не найден.")


    keywords = {
        "обязанности": duties,
        "требования": requirements,
        "условия": conditions
    }

    return keywords,professions,code_words


def get_access_token():
    global access_token

    config = configparser.ConfigParser()
    config.read('Файлы/config.ini')
    access_token = config.get('Settings', 'access_token')


def parse_vacancies(date_from, date_to, professions, code_words, keywords):
    # num = 1
    ids = []
    items = []
    session = requests.Session()
    retry_strategy = Retry(
        total=5,  # Total number of retries
        backoff_factor=1,  # Waits 1 second between retries, then 2s, 4s, 8s...
        status_forcelist=[403, 404, 408, 429, 500, 502, 503, 504],  # Status codes to retry on
        allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    for profession in professions:
        try:
            page = 0
            while True:  # Пагинация
                url = 'https://api.hh.ru/vacancies'
                params = {
                    'text': quote(profession),
                    'date_from': f"{date_from}T00:00:00+00:00",
                    'date_to': f"{date_to}T23:59:59+00:00",
                    'only_with_salary': 'true',
                    'per_page': 100,
                    'area': 113,
                    'order_by': 'relevance',
                    'page': page  # Номер страницы
                }
                headers = {
                    'Authorization': f'Bearer {access_token}',
                }
                response = session.get(url, params=params, headers=headers)
                response.raise_for_status()

                if response.status_code == 200:
                    data = response.json()
                    # print(profession)
                    # print(f"Получено вакансий на странице {page}: {len(data['items'])}")
                    if not data['items']:
                        break  # Если вакансий на странице нет, выходим из цикла

                    for item in data['items']:
                        # print(item)
                        # print('-'*20)
                        id = item.get('id')

                        vacancy_is_archived = bool(item.get('archived'))
                        if vacancy_is_archived:
                            print(f'Вакансия с ID {id} в архиве, пропускаем')
                            continue

                        publish_date = str(item.get('published_at')).split('T')[0]

                        vacancy_name = str(item.get('name'))
                        is_true_professional = False
                        for word in code_words:
                            if word in vacancy_name or word in vacancy_name.lower():
                                is_true_professional = True
                        if not is_true_professional:
                            print(f'В вакансии с ID {id} нет нужных нам ключевых слов в названии, пропускаем')
                            continue    
                        
                        contacts = item.get('contacts')
                        print(contacts)
                        print(f'В вакансии с ID {id} нет контактов, пропускаем')
                        if contacts is None:
                            continue

                        fio = item['contacts']['name']
                        if fio is None:
                            print(f'В вакансии с ID {id} нет контактов, пропускаем')
                            continue

                        email = item['contacts']['email']
                        if email is None:
                            continue

                        try:
                            phone_number = item['contacts']['phones'][0]['formatted']
                            if phone_number is None:
                                continue
                        except:
                            continue

                        salary_min = item['salary']['from']
                        if salary_min is None:
                            salary_min = '-'

                        salary_max = item['salary']['to']
                        if salary_max is None:
                            salary_max = '-'

                        link = item['alternate_url']
                        if link is None:
                            link = '-'

                        employer_name = item['employer']['name']
                        if employer_name is None:
                            employer_name = '-'

                        city = item['area']['name']
                        if city is None:
                            city = '-'

                        url = f'https://api.hh.ru/vacancies/{id}'
                        response_desc = session.get(url, headers=headers)
                        response_desc.raise_for_status()
                        if response_desc.status_code == 200:
                            description = response_desc.json()['description']
                            soup = BeautifulSoup(description, "lxml")
                            strongs = soup.find_all('strong')
                            duties, requirements, conditions = '-', '-', '-'
                            if strongs:
                                for strong in strongs:
                                    strong_lower = str(strong.text).lower()
                                    for key in ['обязанности', 'требования', 'условия']:
                                        for word in keywords[key]:
                                            if word.lower() in strong_lower:
                                                ul_tag = strong.find_next('ul')
                                                if ul_tag:
                                                    if key == 'обязанности':
                                                        duties = ' '.join(li.get_text() for li in ul_tag.find_all('li'))
                                                    elif key == 'требования':
                                                        requirements = ' '.join(li.get_text() for li in ul_tag.find_all('li'))
                                                    elif key == 'условия':
                                                        conditions = ' '.join(li.get_text() for li in ul_tag.find_all('li'))

                            if duties == requirements or requirements == conditions or duties == conditions:
                                duties, requirements, conditions = '-', '-', '-'

                            description = soup.get_text()

                        else:
                            print(f'Вторичный запрос не удался, код ответа от сервера - {response_desc.status_code}')
                            print(f'Ошибка - {response_desc.text}')
                            continue

                        if id not in ids:
                            ids.append(id)
                            items.append([link, publish_date, vacancy_name, salary_min, salary_max, fio, email, phone_number,
                                          employer_name, city, duties, requirements, conditions, description])
                            print(f'Успешно добавлена вакансия с ID {id}')
                            # num += 1
                else:
                    print(f'Запрос не удался, код ответа от сервера - {response.status_code}')
                    print(f'Ошибка - {response.text}')
                    break

                page += 1  # Переход к следующей странице

        except requests.exceptions.ConnectionError as e:
            print(f"Error connecting to the server: {e}")
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
    return items



def save_to_excel(items):
    # df = pandas.DataFrame(items, columns=['Ссылка', 'Название вакансии','Зарплата от','Зарплата до','ФИО','Email','Номер телефона','Название компании','Город','Описание'])
    df = pandas.DataFrame(items, columns=['Ссылка','Дата публикации','Название вакансии','Зарплата от','Зарплата до','ФИО','Email','Номер телефона','Название компании','Город','Обязанности','Требования','Условия','Описание'])
    df.to_excel('Вакансии.xlsx', sheet_name='Вакансии')
    print('Файл успешно сохранен!')


def main():
    keywords,professions,code_words = get_checks()
    get_access_token()
    date_from = input('Введите начальную дату в формате ГГГГ-ММ-ДД:')
    date_to = input('Введите конечную дату в формате ГГГГ-ММ-ДД:')
    items = parse_vacancies(date_from,date_to,professions,code_words,keywords)
    save_to_excel(items)
    

if __name__ == "__main__":
    main()