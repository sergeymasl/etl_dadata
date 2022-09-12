#=============================================класс для запроса==================================================================

# класс запроса с счетчиком
class dadata_find_id:
    '''
    Класс реализующий запросы в api dadata для нахождения информации о компании по ИНН
    
    
    Methods:
    .find - запрос в дадату
    '''
    
    # инициализация
    def __init__(self, system_file = 'system.xlsx',
                count_request_sec_max = 30, count_request_total_max = 10000):
        '''
        Params:
        count_request_sec: int
            количество запросов в секунду
        count_request_total: int 
            максимальное количестов запросов на один токен
        system_file: str
            Путь к файлу excel с лимитами по запросам
        '''
        import pandas as pd
        import numpy as np
        from dadata import Dadata
        from datetime import datetime
        
        self.system_file = system_file
        # считывание системного файла
        self.system = pd.read_excel(system_file)
        # количество токенов
        self.count_token = range(self.system.shape[0])
        # номер токена
        self.num_token = 0
        # счетчики лимитов в секунду и день
        self.count_request_sec_max = count_request_sec_max
        self.count_request_total_max = count_request_total_max
        # инициализациия в dadate
        self.token = self.system.loc[self.num_token, 'token']
        self.dadata = Dadata(self.token)
        # начальный счетчик для суточного лимита
        # условие на проверку наличия сегодншних лимитов
        if self.system[(self.system['date'].apply(lambda x: x.date()) == datetime.now().date()) & (self.system['token'] == self.token)].shape[0] > 0:
            
            self.count_request_total = self.system.loc[self.num_token, 'limit']
        else:
            self.count_request_total = 0
        # начальный счетчик в сек
        self.count_request_sec = 0
    
    # запрос в дадату
    def find(self, inn):
        '''
        Запрос в дадату и выгрузка
        
        '''
        import pandas as pd
        import numpy as np
        from datetime import datetime
        import time
        import json
        from dadata import Dadata
        
        # селектор для зацикливания и смены токенов пока они есть
        selector = True
        # запрос и смена токена при привышении лимита
        
        
        while selector == True:
            # проверки на лимит в день           
            if self.count_request_total < self.count_request_total_max:
                # проверка на количество запросов в секунду
                if self.count_request_sec < self.count_request_sec_max:
                    self.count_request_sec += 1
                    self.count_request_total += 1
                    # ответ
                    answer =  self.dadata.find_by_id("party", inn)
                    
                    # проверка в случае не отработки по инн
                    if not answer:
                        answer = 'not found'
                        
                    selector = False
                    
                elif self.count_request_sec == self.count_request_sec_max:
                    time.sleep(1)
                    self.count_request_sec = 1
                    self.count_request_total += 1
                    
                    answer =  self.dadata.find_by_id("party", inn)
                    
                    # проверка в случае не отработки по инн
                    if not answer:
                        answer = 'not found'
                    
                    selector = False
            else:            
                # если суточный лимит превышен записываем отработанный лимит в system
                # сначала вписываем сегодняшнюю дату
                self.system.loc[self.system['token'] == self.token, 'date'] = datetime.now().date()
                # вписываем лимит
                self.system.loc[self.system['token'] == self.token, 'limit'] = self.count_request_total
                
                #  меняем токен
                if self.num_token + 1 in self.count_token:
                    self.num_token += 1
                    self.token = self.system.loc[self.num_token, 'token']
                    self.dadata = Dadata(self.token)
                    # начальный счетчик для суточного лимита
                    # условие на проверку наличия сегодншних лимитов для смены токена
                    if self.system[(self.system['date'] == datetime.now().date()) & (self.system['token'] == self.token)].shape[0] > 0:
                    
                        self.count_request_total = self.system.loc[self.num_token, 'limit']
                    else:
                        self.count_request_total = 0
                    # токен сменен
                # если токенов уже не осталось возвращяем False
                else:

                    selector = False
                    answer = False
        # возврат ответа          
        return answer
    
    def write_to_system(self):
        '''
        Вписываем текущие значения в system и выгружаем их
        '''
        from datetime import datetime
        # сначала вписываем сегодняшнюю дату
        self.system.loc[self.system['token'] == self.token, 'date'] = datetime.now().date()
        # вписываем лимит
        self.system.loc[self.system['token'] == self.token, 'limit'] = self.count_request_total
        
        # выгружаем
        self.system.to_excel(self.system_file, index = False)



#=============================================функция для работы==================================================================


def dadata_inn_parce(work_file = 'source.xlsx', column = 'userInn'):
    '''
    Функция по считыванию и запросу в дададу значений о компаниях по ИНН
    
    Params:
    work_file: str
        Путь к файлу excel с исходными данными для анализа
    column: str
        Строка с названием столбца ИНН
    '''
    # imports
    import pandas as pd
    import numpy as np
    from tqdm import tqdm
    import telebot
    from datetime import datetime
    from pandas.io.excel import ExcelWriter
    from bot_info inport bot_token, channel_id
    
    bot = telebot.TeleBot(bot_token)
    
    # считывам source_file
    df_source = pd.read_excel(work_file, dtype = 'str')
    df_itog = pd.read_excel(work_file, sheet_name = 'отработано', dtype = 'str')
    
    # добавляем столбец 'отработано' если его нет в файле
    if not 'отработано' in df_source.columns:
        df_source['отработано'] = np.nan
    # сортируем еще не отработанные данные для работы
    df_work = df_source.loc[df_source['отработано'] != df_source['отработано']]
    df_work.drop_duplicates(inplace = True)
    # данные для работы считаны
    # создаем объет для поиска
    f = dadata_find_id()
    # столбцы для возврата
    list_of_columns = [
        'data.inn', 
        'data.name.full_with_opf', 
        'data.name.short_with_opf', 
        'data.name.full', 
        'data.name.short',
        'data.opf.full', 
        'data.opf.short',
        'data.type', 
        'data.state.status',
        'data.branch_type', 
        'data.management.name', 
        'data.management.post',
        'data.kpp', 
        'data.ogrn', 
        'data.okpo', 
        'data.okato', 
        'data.oktmo', 
        'data.okogu', 
        'data.okfs',
        'data.okved', 
        'data.okveds',  
        'data.address.unrestricted_value', 
        'data.address.data.postal_code', 
        'data.address.data.country',
        'data.address.data.federal_district', 
        'data.address.data.region_fias_id', 
        'data.address.data.region_kladr_id',
        'data.address.data.region_iso_code', 
        'data.address.data.region_with_type', 
        'data.address.data.region_type',
        'data.address.data.region_type_full', 
        'data.address.data.region', 
        'data.address.data.city_type', 
        'data.address.data.city_type_full', 
        'data.address.data.city'
    ]
    
    
    df_answer = pd.DataFrame(columns = list_of_columns, dtype = 'str')
    
    # -----------------------------------------начинаем поиск--------------------------------
    try:
        for i, inn in enumerate(tqdm(df_work[column])):
            answer = f.find(inn = inn)

            if answer != False and answer != 'not found':

                answer = pd.json_normalize(answer)
                df_answer = df_answer.append(answer[[column for column in answer.columns if column in list_of_columns]])        

            elif answer == 'not found':
                continue

            else:
                break

        # сохраняем и выгружаем лимиты
        f.write_to_system()
        # сохраняем текущие итоги
        # вывод в excel промежуточных итогов
        path = (f'{datetime.now().date()}_inn_dadata_{df_answer.shape[0]}_lines.xlsx')
        df_answer.to_excel(path, index = False, sheet_name="отработало")

        # сообщаем о результатах на сегодня
        bot.send_message(chat_id = channel_id, 
                         text = f'inn_dadata перестал отрабатывать запросы на <b>{df_answer["data.inn"].nunique()}</b> позиции',
                         parse_mode='HTML')
        
        # добавляем к отработанным сегодняшние результаты
        df_itog = df_itog.append(df_answer)
        
        # отмечаем все итоги в df_source
        df_source = pd.DataFrame(df_source[column]).merge(df_itog.rename(columns = {'data.inn' : column, 'data.name.full_with_opf' : 'отработано'})[[column, 'отработано']].drop_duplicates(subset = [column]), how = 'left')

        # в общем файле отмечаем итоги в листе 'Лист1'
        with ExcelWriter(work_file, mode="a", if_sheet_exists = 'replace') as writer: 
            df_source.to_excel(writer, sheet_name="Лист1", index = False)

        # в общий файл загружаем все отработааные данные
        with ExcelWriter(work_file, mode="a", if_sheet_exists = 'replace') as writer: 
            df_itog.to_excel(writer, sheet_name="отработано", index = False)

        # отправляем общие итоги в телеграмм
        bot.send_message(chat_id = channel_id, 
                         text = f'общий итог <b>inn_dadata</b> на сегодня <b>{df_itog["data.inn"].nunique()}</b> из <b>{df_source[column].nunique()}</b>',
                             parse_mode='HTML')

    except Exception as ex:
        # оповестить о падении
        exc_mes = type(ex).__name__
        exc_arg = ex.args
        bot.send_message(chat_id = channel_id, text = f'''
        <b>inn_dadata</b> погиб смертью храбрых в {datetime.now()}
        Ошибка:
        {exc_mes}
        {exc_arg}
        ''', parse_mode='HTML')
        
#=============================================запуск==================================================================


dadata_inn_parce() 
