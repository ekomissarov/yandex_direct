from common_constants import constants
from yandex_direct import ydbase
import re
ENVI = constants.EnviVar(
    main_dir="/home/eugene/Yandex.Disk/localsource/yandex_direct/",
    cred_dir="/home/eugene/Yandex.Disk/localsource/credentials/"
)
logger = constants.logging.getLogger(__name__)


class YCampaigns(ydbase.YandexDirectBase):
    def __init__(self, directory=None, dump_file_prefix="ycmpg", cache=False):
        if directory is None:
            directory = f"{ENVI['MAIN_PYSEA_DIR']}alldata/cache"
        super(YCampaigns, self).__init__(directory=directory, dump_file_prefix=dump_file_prefix, cache=cache)
        self.data = self.__get_campaigns()
        self.ids_enabled = {i['Id'] for i in self.data if i['State'] == 'ON'}

    def __str__(self):
        return f"<<Кампании Яндекс Директ {len(self.data)} шт.>>"

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    @ydbase.dump_to("campaigns")  # кешируем в файл
    @ydbase.limit_by(500)  # получаем ответ по страницам
    @ydbase.connection_attempts()  # делает доп попытки в случае возникновения ConnectionError
    def __get_campaigns(self):
        """
        Реализует метод API:
        https://tech.yandex.ru/direct/doc/ref-v5/campaigns/get-docpage/

        :return:
        """

        # Создание тела запроса
        body = {"method": 'get',
                "params":  {
                            "SelectionCriteria": {
                                "States": ['ON', 'SUSPENDED']
                            },
                            "FieldNames": ['Id', 'Name', 'State', 'DailyBudget'],
                            "Page": {"Limit": self.limit_by, "Offset": self.offset}
                           }
                }

        result = self.send_request(body, "Campaigns")
        if not result.json()['result']:
            return {}, False
        return result.json()['result']['Campaigns'], result.json()['result'].get('LimitedBy', False)

    def search_by_id(self, campaign_id, ret_field=None):
        for i in self.data:
            if i['Id'] == campaign_id:
                if ret_field:
                    return i[ret_field]
                else:
                    return i
        return False

    def search(self, item, ret_field='Id'):
        ptr = re.compile(item)
        if ret_field:
            return [i[ret_field] for i in self.data if ptr.search(i['Name']) is not None]
        else:
            return [i for i in self.data if ptr.search(i['Name']) is not None]

    def search_enabled(self, item, ret_field="Id"):
        ptr = re.compile(item)
        if ret_field:
            return [i[ret_field] for i in self.data if ptr.search(i['Name']) is not None and i['State'] == 'ON']
        else:
            return [i for i in self.data if ptr.search(i['Name']) is not None and i['State'] == 'ON']


    def pop_enabled(self, item):
        ptr = re.compile(item)
        result = []
        for i in self.data[:]:
            if ptr.search(i['Name']) is not None and i['State'] == 'ON':
                result.append(i['Id'])
                self.data.remove(i)
        return result

    def pop_all(self, item):
        ptr = re.compile(item)
        result = []
        for i in self.data[:]:
            if ptr.search(i['Name']) is not None:
                result.append(i['Id'])
                self.data.remove(i)
        return result

    def filter(self, key=lambda x: x):
        self.data = list(filter(key, self.data))
        self.ids_enabled = {i['Id'] for i in self.data if i['State'] == 'ON'}


class YGroups(ydbase.YandexDirectBase):
    def __init__(self, campaign_ids, directory=None, dump_file_prefix="ygroups", cache=False):
        if directory is None:
            directory = f"{ENVI['MAIN_PYSEA_DIR']}alldata/cache"
        super(YGroups, self).__init__(directory=directory, dump_file_prefix=dump_file_prefix, cache=cache)

        self.campaign_ids = campaign_ids
        self.data = self.__get_adgroups(campaign_ids)

    def __str__(self):
        return f"<<Кампании Яндекс Директ {len(self.data)} для кампаний {self.campaign_ids}>>"

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def __add__(self, other):
        if type(other) is type(self):
            return self.data + other.data
        else:
            return self.data + other

    @ydbase.dump_to("groups")
    @ydbase.main_array_limit(1)
    @ydbase.limit_by(200)
    @ydbase.connection_attempts()
    def __get_adgroups(self, campaign_ids):
        """
        Реализует метод API:
        https://tech.yandex.ru/direct/doc/ref-v5/adgroups/get-docpage/

        :return:
        """

        # Создание тела запроса
        body = {"method": 'get',
                "params":  {
                            "SelectionCriteria": {
                                "CampaignIds": campaign_ids,
                            },
                            "FieldNames": ["CampaignId", "Id", "Name"],
                            "Page": {"Limit": self.limit_by, "Offset": self.offset}
                           }
                }

        result = self.send_request(body, "AdGroups")
        if not result.json()['result']:
            return {}, False
        return result.json()['result']['AdGroups'], result.json()['result'].get('LimitedBy', False)

    def search(self, item=""):
        if item:
            ptr = re.compile(item)
            return [i['Id'] for i in self.data if ptr.search(i['Name']) is not None]
        return [i['Id'] for i in self.data]


if __name__ == '__main__':
    yc = YCampaigns()
    ids = yc.search_enabled("_msk_brand_cian")
    yg = YGroups(ids)
    print(yg.search())
    print("QKRQ!")
