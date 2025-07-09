import logging
from logging.handlers import RotatingFileHandler
from io import BytesIO
import pandas as pd
from telegram import InputFile
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.ext import ApplicationBuilder

API_TOKEN = 'тут_токен'
ADMIN_ID = 214183717

#ЛОГИ
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler('bot.log', maxBytes=5 * 1024 * 1024, backupCount=3),
        logging.StreamHandler()  # Для вывода в консоль
    ]
)

#Основной логгер
logger = logging.getLogger(__name__)

#Уровень для специфичных логгеров
logging.getLogger("httpx").setLevel(logging.WARNING)  # Отключаем HTTP-логи уровня INFO

#Фильтр для исключения неинформативных сообщений
class CategoryLogFilter(logging.Filter):
    def filter(self, record):
        # Логи ошибок и предупреждений оставляем в файле, INFO в файл не пишем
        return record.levelno >= logging.WARNING or 'таблиц' in record.msg.lower()

for handler in logger.handlers:
    if isinstance(handler, RotatingFileHandler):
        handler.addFilter(CategoryLogFilter())
        
#Обработчик отправки логов
async def send_logs(update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.from_user.id == ADMIN_ID:
        try:
            with open('bot.log', 'rb') as log_file:
                await update.message.reply_document(document=log_file, filename="bot.log")
            logger.info("Логи отправлены администратору.")
        except Exception as e:
            logger.error(f"Ошибка отправки логов: {e}")
            await update.message.reply_text("Не удалось отправить логи.")
    else:
        await update.message.reply_text("У вас нет прав на запрос логов.")
        
#Стартовая команда
async def start(update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear() # Очистка данных пользователя при запуске
    await update.message.reply_text(("Привет! Отправь первый файл с остатками 1С в формате Excel. Пожалуйста, каждый цикл сравнения начинай с отправки мне команды /start"))

async def handle_file(update, context: ContextTypes.DEFAULT_TYPE):
    user_data = context.user_data
    file = update.message.document
    
    #Проверка расширения файла
    if not (file.file_name.endswith('.xlsx') or file.file_name.endswith('.xls')):
        await update.message.reply_text("Пожалуйста, загрузите файл в формате .xls или .xlsx.")
        return

    try:
        file_obj = await file.get_file()
        file_bytes = await file_obj.download_as_bytearray()
    except Exception as e:
        await update.message.reply_text("Ошибка при скачивании файла. Пожалуйста, попробуйте снова.")
        print(f"Ошибка скачивания файла: {e}")
        return

    try:
        #Чтение Excel файла
        df = pd.read_excel(BytesIO(file_bytes))
        if df.empty:
            await update.message.reply_text("Файл пустой или содержит некорректные данные.")
            return
    except ValueError as ve:
        await update.message.reply_text("Ошибка при чтении файла: неверный формат Excel. Проверьте, что файл содержит корректные данные.")
        print(f"Ошибка чтения файла: {ve}")
        return
    except Exception as e:
        await update.message.reply_text("Произошла ошибка при чтении файла. Попробуйте снова.")
        print(f"Ошибка чтения файла: {e}")
        return

    if 'df' not in user_data:
        user_data['df'] = df
        await update.message.reply_text("Первый файл успешно загружен. Загрузите второй файл с остатками СОЛВО в формате Excel.")
    elif 'gn' not in user_data:
        user_data['gn'] = df
        await update.message.reply_text("Файлы успешно загружены, начало обработки")
        await process_files(update, context)
    else:
        await update.message.reply_text("Вы уже загрузили два файла. Чтобы начать заново, используйте команду /start.")
        
#Обработка таблиц 
async def process_files(update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_data = context.user_data
    df = user_data.get('df')
    gn = user_data.get('gn')

    if df is None or gn is None:
        await update.message.reply_text("Отсутствуют необходимые данные. Пожалуйста, начните заново с команды /start.")
        return

    try:
        print("Начинаем обработку файла 1С")
        df.fillna(0, inplace=True)
        df = df.drop(index=range(8), errors='ignore')

        df = df.rename(columns={
            'Unnamed: 0': 'Артикул',
            'Unnamed: 3': 'Код товара',
            'Unnamed: 4': 'Номенклатура',
            'Unnamed: 6': 'Категория',
            'Unnamed: 7': 'Остаток',
            'Unnamed: 8': 'Резерв',
            'Unnamed: 9': 'Ожидается'
        })

        expected_columns = ['Артикул', 'Код товара', 'Номенклатура', 'Остаток']
        if not all(col in df.columns for col in expected_columns):
            await update.message.reply_text("Файл 1С не содержит необходимые столбцы.")
            return

        df = df.drop(columns=['Unnamed: 1', 'Unnamed: 2', 'Unnamed: 5', 'Резерв', 'Ожидается'], errors='ignore')

        unwanted_values = [
            "Юнилевер ООО", "ЭЛИТНЫЕ КАМИНЫ ООО", "ТЕХИНТЕГРА ООО", "Терра-Строй ООО",
            "Т2 Мобайл Коммерция Маркетинг", "Т2 Мобайл", "Стройметиз ООО", "Статио Проджект ООО",
            "Соловьёв Д.Ю ИП", "СКЛ ООО", "СИРИУС ООО", "СЕРВИС ЛОГИСТИКА ООО",
            "Сен-Гобен, ООО", "С.С.В.", "Пуролат ООО", "ПРОТЕИН ПЛЮС",
            "Полисан НТФФ ООО", "Поликом-Сервис ООО", "Пилар ООО", "Паровые системы ООО",
            "ПАО Сбербанк", "ОЛСО ООО", "Мултон Партнерс ООО", "МС АГРО ООО",
            "Милликом НТК АО", "Мелстон Инжиниринг ООО", "МАПЕД РУС ООО", "Магна ООО",
            "Комплекс Парадная №КП-24/09000/00020/Р", "Комплекс Парадная (820)", 
            "Комплекс Парадная (814)", "КОЛЕР РУС ООО", "Клинкманн СПб АО",
            "Инженерные технологии ООО", "Дрогери ритейл", "Доминанта Групп",
            "Вирс ООО", "Велес Трейд ООО", "Велес ООО", "Бакальдрин",
            "Аэро-Трейд", "АС Групп Плюс ООО", "Арт Фэшен ООО", "Амтел ООО",
            "Альфа Омега Трейд ООО", "Аквафор ООО", "Айнхелль ООО", "Август ООО"
        ]

        df = df[~df['Артикул'].isin(unwanted_values)]
        
        #Пробелы чистим
        def normalize_spaces(x):
            if isinstance(x, str):
                return ' '.join(x.split())
            return x
        
        for col in df.select_dtypes(include=[object]):
            df.loc[:, col] = df[col].apply(normalize_spaces)
    
        category_map = {
            'Хранение 45': "Норма",
            'Хранение НЕКОНДИЦИЯ 45': "Некондиция",
            'Транзитный ХРАНЕНИЕ 45': "Норма",
            'Хранение КАРАНТИН 45': "Карантин",
            'Склад 404': "404",
            '2 категория': "2 категория",
            '3 категория': "3 категория",
            '4 категория': "4 категория",
            'Брак': "Брак",
            '2780.W006':'2780.W006',
            '2780.T002':'2780.T002',
            '2780.T888':'2780.T888',
            '2780.T000':'2780.T000',
            '2780.T0E0':'2780.T0E0',
            '2780.T0W0':'2780.T0W0',
            '2780.T0S0':'2780.T0S0',
            '2780.W500':'2780.W500',
            '2780.T0N0':'2780.T0N0',
            '2780.T001':'2780.T001',
            '2780.TP01':'2780.TP01',
            '2780.T0W1':'2780.T0W1',
            '2780.T0N1':'2780.T0N1',
            '2780.TNEK':'2780.TNEK',
            '2780.T0E1':'2780.T0E1',
            '2780.TDR1':'2780.TDR1',
            '2780.Z71E':'2780.Z71E',
            '2780.Z71N':'2780.Z71N',
            '2780.Z71S':'2780.Z71S',
            '2780.Z71W':'2780.Z71W',
            '2780.TC51':'2780.TC51',
            '2780.TDRN':'2780.TDRN',
            '2780.T0S2':'2780.T0S2',
            '2780.T0S1':'2780.T0S1',
            '2780.T51S':'2780.T51S',
            '2780.T51W':'2780.T51W',
            '2780.T51N':'2780.T51N',
            '2780.W090':'2780.W090',
            '2780.Z710':'2780.Z710',
            '2780.T51E':'2780.T51E',
            '2780.TADS':'2780.TADS',
            '2780.T0W2':'2780.T0W2',
            '2780.T0E2':'2780.T0E2',
            '2780.T0N2':'2780.T0N2',
            '2780.TDRS':'2780.TDRS',
            '2780.TDRE':'2780.TDRE',
            '2780.TDRW':'2780.TDRW',
            'KZRN':'KZRN',
            'C3PL':'C3PL',
            'C508':'C508',
            'EMR1':'EMR1',
            'ESM1':'ESM1',
            'T781':'T781',
            'B781.T780':'B781.T780',
            '2780.Z731':'2780.Z731',
            '2780.TA01':'2780.TA01',
            '2780.TN01':'2780.TN01',
            '2780.TW01':'2780.TW01',
            '2780.TREE':'2780.TREE',
            '2780.TRER':'2780.TRER',
            '2780.TRES':'2780.TRES',
            '2780.TRET':'2780.TRET',
            '2780.BUCN':'2780.BUCN',
            'C509':'C509',
            'A780.T780':'A780.T780',
            '2780.TU01':'2780.TU01',
            'A780.T788':'A780.T788',
            '2780.TCOD':'2780.TCOD',
            '2780.T0RF':'2780.T0RF',
            'B781.TN01':'B781.TN01',
            'C510':'C510',
            'ERR1':'ERR1',
            'С00К':'С00К',
            '2780.TEN1':'2780.TEN1',
            '2780.D888':'2780.D888',
            '2780.W600':'2780.W600',
        }

        #Переименовываем категорию как в WMS
        df.loc[:, 'Категория'] = df['Категория'].replace(category_map)
        df = df[~df['Артикул'].isin(category_map)]

        #Ожидаемые категории из category_map
        expected_categories = set(category_map.values())
        
        #Фактические категории в столбце 'Категория'
        actual_categories = set(df['Категория'].dropna().unique())
        
        #Проверка на хотя бы одно ожидаемое значение 
        if not expected_categories.intersection(actual_categories):
            await update.message.reply_text(
                "Пожалуйста, проверьте, что вы выбрали при выгрузке отчета из 1С вариант отчета 'Для сверки БД'. Ваш файл содержит неизвестные категории или в столбце 'Категория' отсутствуют ожидаемые значения."
            )
            return 
            
        for column in ['Остаток']:
            df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(int)

            
        print("Датафрейм df:")
        print(df.head(3))
        print("Файл 1С успешно обработан")

        #Обработка таблицы СОЛВО (WMS)
        gn = gn.drop(index=[0, 1])
        gn.columns = [''] * len(gn.columns)
        gncolumns = ['Код товара', 'Артикул', 'Номенклатура', 'Категория', 'Остаток']
        gn.columns = gncolumns
        gn = gn.iloc[:-3]
        grouped_gn = gn.groupby(['Код товара', 'Категория', 'Артикул', 'Номенклатура']).agg({'Остаток': 'sum'}).reset_index()
    

        #Приводим к типу строки, чтобы избежать ошибки при объединении
        key_columns = ['Код товара', 'Артикул', 'Категория']
        for col in key_columns:
            df[col] = df[col].astype(str)
            grouped_gn[col] = grouped_gn[col].astype(str)
            
        #Еще раз чистим пробелы для солво
        for col in grouped_gn.select_dtypes(include=[object]):
            grouped_gn.loc[:, col] = grouped_gn[col].apply(normalize_spaces)
            
        print("Датафрейм grouped_gn:")
        print(grouped_gn.head(3))

        #Объединение таблиц 1С и СОЛВО
        print("Начинаем объединение данных")
        ost = pd.merge(df, grouped_gn[['Код товара', 'Артикул', 'Категория', 'Остаток','Номенклатура']], 
                        on=['Код товара', 'Артикул', 'Категория'], how='outer', suffixes=('_1C', '_SLV'))
        
        #Добавляем столбец "Разница Остаток"
        ost['Разница Остаток'] = ost['Остаток_1C'] - ost['Остаток_SLV']

        print("Датафрейм ost:")
        print(ost.head(3))
        
        #Находим расхождения в остатках
        diff_ost = ost[ost['Разница Остаток'] != 0]

        #Заполняем NaN в столбцах Номенклатура_1C и Номенклатура_SLV, оставляем только Номенклатура
        diff_ost.loc[:, 'Номенклатура_1C'] = diff_ost.loc[:,'Номенклатура_1C'].fillna(diff_ost.loc[:,'Номенклатура_SLV'])
        diff_ost.loc[:,'Номенклатура_SLV'] = diff_ost.loc[:,'Номенклатура_SLV'].fillna(diff_ost.loc[:,'Номенклатура_1C'])
        
        #Создаем полную копию DataFrame
        diff_ost = diff_ost.copy()
        
        #Теперь безопасно изменяем значения
        diff_ost.loc[:, 'Номенклатура'] = diff_ost['Номенклатура_1C']

        diff_ost = diff_ost.drop(columns=['Номенклатура_1C', 'Номенклатура_SLV'])

        # Заполняем NaN в столбце Остаток_1C значением "Нет данных в 1С"
        diff_ost['Остаток_1C'] = diff_ost['Остаток_1C'].fillna("Нет данных в 1С")
        
        # Заполняем NaN в столбце Остаток_SLV значением "Нет данных в Солво"
        diff_ost['Остаток_SLV'] = diff_ost['Остаток_SLV'].fillna("Нет данных в Солво")

        print("Датафрейм diff_ost:")
        print(diff_ost.head(3))

        #Проверяем, есть ли расхождения
        if diff_ost.empty:
            await update.message.reply_text("Нет расхождений между таблицами.")
            return
        
        #Порядок столбцов для финального файла
        new_columns_order = ['Код товара', 'Артикул', 'Номенклатура', 'Категория', 'Остаток_1C', 'Остаток_SLV', 'Разница Остаток']
        existing_columns = [col for col in new_columns_order if col in diff_ost.columns]
        diff_ost = diff_ost[existing_columns]
        
        #Сохраняем таблицу в файл
        output = BytesIO()
        diff_ost.to_excel(output, index=False)
        output.seek(0)
        
        #Отправляем файл пользователю
        await update.message.reply_document(document=InputFile(output, filename="Сравнение остатков.xlsx"))
        output.close()
        print("Файл успешно отправлен пользователю")

        user_data.clear()  #Очистка данных пользователя
    except KeyError as ke:
        await update.message.reply_text(f"Ошибка в структуре данных: отсутствует необходимый столбец ({ke}). Проверьте, что файлы содержат корректные столбцы.")
        print(f"Ошибка KeyError: {ke}")
    except Exception as e:
        await update.message.reply_text("Произошла ошибка при обработке файлов. Попробуйте снова.")
        print(f"Ошибка обработки: {e}")

        
#Основная функция
def main():
    app = Application.builder().token(API_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("getlogs", send_logs))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_file))

    logger.info("Бот запущен.")
    app.run_polling()

if __name__ == "__main__":
    main()
