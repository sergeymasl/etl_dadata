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
    from bot_info import bot_token, channel_id 
    
    
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
    # создаем объект для поиска
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
    
