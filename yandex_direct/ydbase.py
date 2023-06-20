from __future__ import annotations
import json
import requests
import curlify
import re
from time import sleep
from datetime import date
from datetime import timedelta
from common_constants import constants
from google_analytics.analyticsbase import DateDeque
from urllib.request import urlopen
from urllib3.exceptions import ProtocolError
import pickle
ENVI = constants.EnviVar(
    main_dir="/home/eugene/Yandex.Disk/localsource/yandex_direct/",
    cred_dir="/home/eugene/Yandex.Disk/localsource/credentials/"
)
logger = constants.logging.getLogger(__name__)


class YandexDirectError(constants.PySeaError): pass
class InternalYDServerError(YandexDirectError): pass
class LimitOfRetryError(YandexDirectError): pass
class IntegrityDataError(YandexDirectError): pass
class PeriodError(YandexDirectError): pass


def limit_by(nlim):  # конструктор декоратора (L залипает в замыкании)
    """
    Декоратор для использования постраничной выборки в вызовах API Яндекс Директ
    https://tech.yandex.ru/direct/doc/dg/best-practice/get-docpage/#page

    :param nlim: не более 10 000 объектов за один запрос. (для метода get)
    :return:
    """
    def deco_limit(f):  # собственно декоратор принимающий функцию для декорирования
        def constructed_function(self, *argp, **argn):  # конструируемая функция
            result = []
            self.limit_by = nlim

            data = f(self, *argp, **argn)
            result.extend(data[0])

            while data[1]:
                self.offset = data[1]
                data = f(self, *argp, **argn)
                result.extend(data[0])

            self.offset = 0  # не забываем вернуть пагенатор в исходное состояние для следующих вызовов
            return result
        return constructed_function
    return deco_limit


def dump_to(prefix, d=False):  # конструктор декоратора (n залипает в замыкании)
    """
    Декоратор для кеширования возврата функции.
    Применим к методам класса, в котором объявлены:
    self.directory - ссылка на каталог
    self.dump_file_prefix - файловый префикс
    self.cache - True - кеширование требуется / False
    На вход принимает префикс, который идентифицирует декорируемую функцию

    Кеш хранится в сериализованных файлах с помощью pickle

    :param prefix: идентифицирует декорируемую кешируемую функцию
    :param d: явно указанная дата в self.current_date или False для сегодняшней даты (для формирования имени файла)
    :return:
    """
    def deco_dump(f):  # собственно декоратор принимающий функцию для декорирования
        def constructed_function(self, *argp, **argn):  # конструируемая функция
            if 'dump_parts_flag' in self.__dict__:
                dump_file_prefix = f"{self.dump_file_prefix}_p{self.dump_parts_flag['part_num']}"
            else:
                dump_file_prefix = self.dump_file_prefix

            if not d:
                file_out = "{}/{}_{}_{}.pickle".format(self.directory, dump_file_prefix, prefix,
                                                       date.today()).replace("//", "/")
            else:
                file_out = "{}/{}_{}_{}.pickle".format(self.directory, dump_file_prefix, prefix,
                                                       self.current_date).replace("//", "/")
            read_data = ""

            if self.cache:  # если кеширование требуется
                try:  # пробуем прочитать из файла
                    with open(file_out, "rb") as file:
                        read_data = pickle.load(file)
                except Exception as err:
                    logger.debug(f"{err}\n Cache file {file_out} is empty, getting fresh...")

            if not read_data:  # если не получилось то получаем данные прямым вызовом функции
                read_data = f(self, *argp, **argn)
                if 'dump_parts_flag' in self.__dict__:
                    self.dump_parts_flag['len'] = len(read_data)

                with open(file_out, "wb") as file:  # записываем результат в файл
                    if 'dump_parts_flag' in self.__dict__:
                        pickle.dump(read_data[-self.dump_parts_flag['len']:], file, pickle.HIGHEST_PROTOCOL)
                    else:
                        pickle.dump(read_data, file, pickle.HIGHEST_PROTOCOL)
            return read_data
        return constructed_function
    return deco_dump


def connection_attempts(n=12, t=10):  # конструктор декоратора (N,T залипает в замыкании)
    """
    Декоратор задает n попыток для соединения с сервером в случае ряда исключений
    с задержкой t*2^i секунд

    :param n: количество попыток соединения с сервером [1, 15]
    :param t: количество секунд задержки на первой попытке попытке (на i'ом шаге t*2^i)
    :return:
    """
    def deco_connect(f):  # собственно декоратор принимающий функцию для декорирования
        def constructed_function(*argp, **argn):  # конструируемая функция
            retry_flag, pause_seconds = n, t
            try_number = 0

            if retry_flag < 0 or retry_flag > 15:
                retry_flag = 8
            if pause_seconds < 1 or pause_seconds > 30:
                pause_seconds = 10

            while True:
                try:
                    result = f(*argp, **argn)
                    # Обработка ошибки, если не удалось соединиться с сервером
                except (ConnectionError,
                        requests.exceptions.ConnectionError,
                        requests.exceptions.ChunkedEncodingError,
                        ProtocolError,
                        InternalYDServerError) as err:
                    logger.error(f"Ошибка соединения с сервером {err}. Осталось попыток {retry_flag - try_number}")
                    if try_number >= retry_flag:
                        raise LimitOfRetryError
                    sleep(pause_seconds * 2 ** try_number)
                    try_number += 1
                    continue
                else:
                    return result

            return None
        return constructed_function
    return deco_connect


def main_array_limit(nlim):  # конструктор декоратора (L залипает в замыкании)
    """
    Декоратор для генерации вызовов API с ограничением по количеству
    передаваемых CampaignIds, AdGroupIds и пр.
    Используется первый список передающийся в функцию

    :param nlim: количество CampaignIds в одном API запросе
    :return:
    """
    def deco_list_limit(f):  # собственно декоратор принимающий функцию для декорирования
        def constructed_function(self, lst, *argp, **argn):  # конструируемая функция
            result_list = []
            result_dict = {}

            if type(lst) is str:
                lst = [int(lst)]
            elif type(lst) is int:
                lst = [lst]

            parts = (lst[i:i+nlim] for i in range(0, len(lst), nlim))  # chunk, разбивает список на части по ~nlim шт.

            for n, i in enumerate(parts):
                if 'dump_parts_flag' in self.__dict__:
                    self.dump_parts_flag['part_num'] = n

                res = f(self, i, *argp, **argn)
                if type(res) is list:
                    result_list.extend(res)
                elif type(res) is dict:
                    result_dict.update(res)
                elif type(res) is requests.models.Response:
                    tmp = res.json()['result'].get("AddResults", False)
                    if tmp:
                        result_list.extend(tmp)
                    tmp = res.json()['result'].get("UpdateResults", False)
                    if tmp:
                        result_list.extend(tmp)
                    tmp = res.json()['result'].get("DeleteResults", False)
                    if tmp:
                        result_list.extend(tmp)

            if result_dict:
                return result_dict
            elif result_list:
                return result_list
            else:
                return []
        return constructed_function
    return deco_list_limit


class TSVReport:
    def __init__(self, tsv: str = "") -> None:
        self.__tsv = tsv
        self.data = []
        self.report_name = ""
        self.period_begin = None
        self.period_end = None
        if tsv:
            self._create_report_from_tsv()

    def _create_report_from_tsv(self) -> None:
        n = re.compile('Total rows: ([0-9]+)')
        d = re.compile('\(([0-9]{4}-[0-9]{2}-[0-9]{2}) - ([0-9]{4}-[0-9]{2}-[0-9]{2})\)')
        report = self.__tsv.split("\n")

        while True:
            curr = report.pop()
            rows_number = n.search(curr)
            if rows_number is not None:
                rows_number = rows_number.group(1)
                if len(report[2:]) != int(rows_number):
                    logger.error("Ошибка целостности отчета Яндекс Директ")
                    raise IntegrityDataError
                break

        self.report_name = report[0].replace("\"", "").split()[0]
        self.period_begin, self.period_end = map(date.fromisoformat, d.search(report[0]).groups())
        fields = report[1].split("\t")
        for i in report[2:]:
            line = dict(zip(fields, i.split("\t")))  # получили dict с именами полей
            # приведение типов для извесных полей
            for field in ['CampaignId', 'AdGroupId', 'CriteriaId', 'Impressions', 'Clicks', 'Cost']:
                if line.get(field, False):
                    if line[field]=="--":
                        line[field] = "undefined"
                    else:
                        line[field] = int(line[field])
            for field in ['AvgImpressionPosition', 'AvgClickPosition', 'AvgTrafficVolume']:
                if line.get(field, False):
                    if line[field].find("-") != -1:
                        line[field] = None
                    else:
                        line[field] = float(line[field])

            if line.get('Date', False):
                line['Date'] = date.fromisoformat(line['Date'])
            self.data.append(line)

    def search_field(self, field_name, field_value):
        if len(self.data) > 0:
            if self.data[0].get(field_name, "no_field") == "no_field":
                raise KeyError
            if type(self.data[0][field_name]) != type(field_value):
                raise TypeError

            for i in self.data:
                if i[field_name] == field_value:
                    return i
            else:
                raise IndexError

    def __str__(self) -> str:
        return f"Отчет Яндекс Директ {self.report_name} за период {self.period_begin} - {self.period_end}"


class TSVReportByDate(TSVReport):
    def __init__(self, tsv: str = "") -> None:
        if type(tsv) is str:
            super(TSVReportByDate, self).__init__(tsv)
        elif type(tsv) is TSVReport:
            self.__dict__ = tsv.__dict__

        self.ids_index = set()
        self.date_data = DateDeque()

        if self.data:
            self._create_date_report_from_data(self.data)

    def __getitem__(self, date_item: date) -> tuple:
        if type(date_item) is str:
            return self.date_data.get_by_date(date.fromisoformat(date_item))
        return self.date_data.get_by_date(date_item)

    def __iter__(self):
        """
        Объект иттерируем по датам отчета, при этом возвращается словарь с добавленной датой и идентификатором кампании
        :return: иттератор
        """
        for d in self.date_data:
            for j in d[1].items():  # d[0] - дата
                for k in j[1]:  # j[0] - CampaignId
                    out = {"Date": d[0], "CampaignId": j[0]}
                    out.update(k)
                    yield out

    def _create_date_report_from_data(self, d: list) -> None:
        if len(d) == 0:
            return None
        d.sort(key=lambda x: x['Date'], reverse=False)
        start_point = d[0]['Date']
        tmp_date = dict()

        for i in d:
            curr_date = i.pop('Date')
            curr_campaignid = i.pop('CampaignId')

            if curr_date != start_point:
                self.date_data.append((start_point, tmp_date))
                start_point = curr_date
                tmp_date = dict()

            if curr_campaignid not in tmp_date:
                tmp_date.update({curr_campaignid: []})

            tmp_date[curr_campaignid].append(i)

        if tmp_date:
            self.date_data.append((start_point, tmp_date))

    def build_index(self) -> None:
        """
        создает индекс по идентификаторам (CampaignId, AdGroupId, AdGroupName, CriteriaId, Criteria)
        :return:
        """
        for d in self.date_data:
            for j in d[1].items():  # d[0] - дата
                for k in j[1]:  # j[0] - CampaignId
                    out = (j[0], k['AdGroupId'], k['AdGroupName'], k['CriteriaId'],
                           re.sub("\s\-.*$", "", k['Criteria'])  # подчищаем минус слова
                           )

                    self.ids_index.add(out)

    def set_begin_date(self, begin_date: date) -> None:
        self.period_begin = begin_date
        self.date_data.clear_dates_before(begin_date)

    def add_data(self, d: TSVReport) -> None:
        if self.report_name and self.report_name != d.report_name:
            logger.error(f"Тип присоединяемого отчета {d.report_name} не совпадает с {self.report_name}")
            raise IntegrityDataError

        if self.period_end and self.period_end != d.period_begin - timedelta(1):
            logger.error(f"Дата основной статистики заканчивается на {self.period_end}\n"
                         f"дата присоединяемого периода статистики начинается с {d.period_begin}")
            raise PeriodError

        self.period_end = d.period_end
        self._create_date_report_from_data(d.data)

    def summ_stat(self, from_date: date = False, to_date: date = False,
                  campaign_id: int = False, adgroup_id: int = False, criteria_id: int = False) -> dict:
        """
        подсчитывает статистику в виде {"from_date": from_date, "to_date": to_date, "AdGroupId": adgroup_id,
        "CriteriaId": criteria_id, "Impressions": 0, "Clicks": 0, "Cost": 0}

        :param from_date: дата в формате YYYY-MM-DD или class 'datetime.date'
        :param to_date: дата в формате YYYY-MM-DD или class 'datetime.date'
        :param campaign_id: идентификатор кампании
        :param adgroup_id: идентификатор группы
        :param criteria_id: идентификатор критерия таргетинга / ключевого слова
        :return: dict со статистикой
        """
        d = tuple(i[0] for i in self.date_data)
        from_date = min(d) if from_date is False else from_date
        to_date = max(d) if to_date is False else to_date
        from_date = date.fromisoformat(from_date) if type(from_date) is str else from_date
        to_date = date.fromisoformat(to_date) if type(to_date) is str else to_date
        if type(campaign_id) is not int:
            campaign_id = int(campaign_id)
        if type(adgroup_id) is not int:
            adgroup_id = int(adgroup_id)
        if type(criteria_id) is not int:
            criteria_id = int(criteria_id)

        period = tuple(from_date + timedelta(days=x) for x in range(0, (to_date-from_date).days + 1))

        result = {"from_date": from_date, "to_date": to_date,
                  "CampaignId": campaign_id, "AdGroupId": adgroup_id, "CriteriaId": criteria_id,
                  "Impressions": 0, "Clicks": 0, "Cost": 0}

        for i in self.date_data:
            if i[0] in period:
                for j in i[1].items():
                    if not campaign_id or campaign_id == j[0]:
                        for k in j[1]:
                            if not adgroup_id or adgroup_id == k['AdGroupId']:
                                if not criteria_id or criteria_id == k['CriteriaId']:
                                    result['Impressions'] += k['Impressions']
                                    result['Clicks'] += k['Clicks']
                                    result['Cost'] += k['Cost']

        return result


class YandexDirectBase:
    service = {
        'Ads': "https://api.direct.yandex.com/json/v5/ads",
        'AdGroups': "https://api.direct.yandex.com/json/v5/adgroups",
        'Campaigns': "https://api.direct.yandex.com/json/v5/campaigns",
        'KeywordBids': "https://api.direct.yandex.com/json/v5/keywordbids",
        'Keywords': "https://api.direct.yandex.com/json/v5/keywords",
        'KeywordsResearch': "https://api.direct.yandex.com/json/v5/keywordsresearch",
        'Reports': "https://api.direct.yandex.com/json/v5/reports",
        'Sitelinks': "https://api.direct.yandex.com/json/v5/sitelinks",
        'AdImages': "https://api.direct.yandex.com/json/v5/adimages",
        'Dictionaries': "https://api.direct.yandex.com/json/v5/dictionaries",
        'AdExtensions': "https://api.direct.yandex.com/json/v5/adextensions",
        'v4live': "https://api.direct.yandex.ru/live/v4/json/",
    }

    def __init__(self, directory="./", dump_file_prefix="fooooo", cache=True, account="default", login="default"):
        self.selected_account_name = account if login=="default" else login
        self.headers = {"Authorization": "Bearer " + ENVI['PYSEA_YD_TOKEN'], "Accept-Language": "ru",}
        if account != "default":
            self.headers = {"Authorization": "Bearer " + ENVI[f'PYSEA_YD_{account.upper()}_TOKEN'], "Accept-Language": "ru", }
        if login != "default":
            self.headers.update({"Client-Login": login})
        # переменные настраивающие кеширование запросов к API
        self.directory = directory
        self.dump_file_prefix = f"{dump_file_prefix}_{self.selected_account_name}"
        self.cache = cache

        # переменные устанавливают постраничные запросы к API
        self.limit_by = 200
        self.offset = 0

    def cache_enabled(self):
        self.cache = True

    def cache_disabled(self):
        self.cache = False

    def select_account(self, account_name, login="default"):
        self.headers = {"Authorization": "Bearer " + ENVI[f'PYSEA_YD_{account_name.upper()}_TOKEN'], "Accept-Language": "ru", }
        if login != "default":
            self.headers.update({"Client-Login": login})
        self.selected_account_name = account_name if login=="default" else login
        self.dump_file_prefix = f"{self.dump_file_prefix}_{self.selected_account_name}"

        return self

    def send_request(self, body, srv_type):
        """
        Выполняет непосредственно запрос к серверу API
        Принимает на входе сформированное тело запроса и тип запроса

        :param body: тело запроса к API Яндекс Директ
        :param srv_type: тип запроса (метка URL запроса, описана в YandexDirectBase.service)
        :return: возврящает полный ответ сервера
        """
        mutate_method = f"{body['method'].capitalize()}Results"
        if body['method'] == "get":
            mutate_method = ""
        elif body['method'] == "hasSearchVolume":
            mutate_method = "HasSearchVolumeResults"


        # Кодирование тела запроса в JSON
        json_body = json.dumps(body, ensure_ascii=False).encode('utf8')

        # Выполнение запроса
        try:
            result = requests.post(self.service[srv_type], json_body, headers=self.headers)
            # Распечатывает отладочную информацию
            self.print_request_info(result)

            # Обработка запроса
            # https://tech.yandex.ru/direct/doc/dg/concepts/errors-docpage/
            if result.status_code != 200 or result.json().get("error", False):
                logger.error(f"Произошла ошибка при обращении к серверу API Директа.\n"
                             f"Код ошибки: {result.json()['error']['error_code']}\n"
                             f"Ошибка: {result.json()['error']['error_string']}\n"
                             f"Описание ошибки: {result.json()['error']['error_detail']}\n"
                             f"RequestId: {result.headers.get('RequestId', False)}")
                if result.json()['error']['error_code'] == 1000 and \
                        result.json()['error']['error_string'] == "Сервис временно недоступен":
                    raise InternalYDServerError
                else:
                    raise YandexDirectError
            else:
                logger.info(f"RequestId: {result.headers.get('RequestId', False)} "
                            f"Информация о баллах: {result.headers.get('Units', False)}")

            # https://tech.yandex.ru/direct/doc/dg/best-practice/modify-docpage/
            if mutate_method:
                for mutate_result in result.json()['result'][mutate_method]:
                    if mutate_result.get('Errors', False):
                        logger.error(mutate_result)
                        if mutate_method == 'DeleteResults':
                            if mutate_result['Errors'][0]['Code'] == 6000 and \
                                    mutate_result['Errors'][0]['Details'] == \
                                    'Указанный набор быстрых ссылок используется и не может быть удалён':
                                continue
                            if mutate_result['Errors'][0]['Code'] == 8800 and \
                                    mutate_result['Errors'][0]['Message'] == 'Объект не найден':
                                continue

                        logger.error(f"{mutate_method}\n{body}\n{result}")
                        raise YandexDirectError

                    if mutate_result.get('Warnings', False):
                        logger.warning(mutate_result)

            else:
                if srv_type == "Sitelinks":
                    logger.info(f"Кол-во записей в ответе: {len(result.json()['result']['SitelinksSets'])}")
                else:
                    logger.info(f"Кол-во записей в ответе: {len(result.json()['result'].get(srv_type, ()))}")

        except ConnectionError:
            logger.error("ConnectionError во время обращения к Яндекс API")
            raise ConnectionError

        except Exception as ex:
            logger.error(f"Произошла непредвиденная ошибка во время обращения к Яндекс API (в send_request()) {ex}")
            raise YandexDirectError

        return result

    def send_request_report(self, body):
        """
        Выполняет непосредственно запрос к серверу API. Функция заточена для Report запросов.
        https://tech.yandex.ru/direct/doc/examples-v5/python3_requests_stat1-docpage/
        Реализует проверку готовности отчета
        https://tech.yandex.ru/direct/doc/reports/mode-docpage/

        :param body: тело запроса к API Яндекс Директ
        :return: возврящает отчет TSVReport
        """

        # Кодирование тела запроса в JSON
        body = json.dumps(body, indent=4)

        # --- Запуск цикла для выполнения запросов ---
        # Если получен HTTP-код 200, то выводится содержание отчета
        # Если получен HTTP-код 201 или 202, выполняются повторные запросы
        while True:
            try:
                result = requests.post(self.service['Reports'], body, headers=self.headers)
                # Распечатывает отладочную информацию
                self.print_request_info(result)

                result.encoding = 'utf-8'  # Принудительная обработка ответа в кодировке UTF-8
                if result.status_code == 400:
                    logger.error(f"{result.status_code} Параметры запроса указаны неверно "
                                 f"или достигнут лимит отчетов в очереди")
                    raise YandexDirectError
                elif result.status_code == 200:
                    logger.info("Отчет создан успешно.")
                    # logger.debug(f"Содержание отчета: \n{result.text}")
                    break
                elif result.status_code == 201:
                    retry_in = int(result.headers.get("retryIn", 60))
                    logger.info(f"Отчет успешно поставлен в очередь в режиме офлайн\n"
                                f"Повторная отправка запроса через {retry_in} секунд")
                    sleep(retry_in)
                elif result.status_code == 202:
                    retry_in = int(result.headers.get("retryIn", 60))
                    logger.info(f"Отчет формируется в режиме офлайн\n"
                                f"Повторная отправка запроса через {retry_in} секунд")
                    sleep(retry_in)
                elif result.status_code == 500:
                    logger.error("При формировании отчета произошла ошибка. Попробуйте повторить запрос позднее")
                    raise InternalYDServerError
                elif result.status_code == 502:
                    logger.error(f"Время формирования отчета превысило серверное ограничение.\n"
                                 f"Пожалуйста, попробуйте уменьшить период и количество запрашиваемых данных.")
                    raise YandexDirectError
                else:
                    logger.error(f"Произошла непредвиденная ошибка во время обращения "
                                 f"к Яндекс API (в send_request_report())")
                    raise YandexDirectError

            # Обработка ошибки, если не удалось соединиться с сервером API Директа
            except ConnectionError:
                # повторить запрос позднее
                raise ConnectionError

            # Если возникла какая-либо другая ошибка
            except Exception as ex:
                logger.error(f"Произошла непредвиденная ошибка во время обращения к Яндекс API (в send_request()) {ex}")
                raise YandexDirectError

        return TSVReport(result.text)

    def send_request_v4(self, body):
        """
        https://yandex.ru/dev/direct/doc/dg-v4/concepts/Versions-docpage/
        Выполняет непосредственно запрос к серверу API v4live
        Принимает на входе сформированное тело запроса

        :param body: тело запроса к API Яндекс Директ
        :return: возврящает полный ответ сервера
        """

        body.update({
            'token': ENVI['PYSEA_YD_TOKEN'],
            'locale': 'ru'
        })

        # Кодирование тела запроса в JSON
        json_body = json.dumps(body, ensure_ascii=False).encode('utf8')

        # Выполнение запроса
        try:
            response = urlopen(self.service['v4live'], json_body)
            response = json.loads(response.read().decode('utf8'))

            if response.get("error_code", False):
                logger.error(f"Произошла ошибка при обращении к серверу API Директа.\n {response}")
                raise YandexDirectError

        except ConnectionError:
            # повторить запрос позднее
            raise ConnectionError

        except Exception as ex:
            logger.error(f"Произошла непредвиденная ошибка (в send_request_v4()), {ex}")
            raise YandexDirectError

        return response

    @staticmethod
    def print_request_info(result):
        """
        Распечатывает отладочную информацию для одного запроса к API
        :return:
        """
        logger.debug(f"Заголовки запроса: {result.request.headers}\n"
                     f"Запрос: {result.request.body}\n"
                     f"Заголовки ответа: {result.headers}\n"
                     f"RequestId: {result.headers.get('RequestId', False)}\n"
                     f"Ответ: {result.text}\n"
                     f"CURL: {curlify.to_curl(result.request)}")
