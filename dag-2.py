import pandas as pd
from datetime import timedelta
from datetime import datetime
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

year = 1994 + hash(f'v-petrova-25') % 23

path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

default_args = {
    'owner': 'v-petrova-25',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 20),
    'schedule_interval': '0 0 * * *'
}

CHAT_ID = -894758326
try:
    BOT_TOKEN = 'token'
except:
    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Congratulations! DAG {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

@dag(default_args=default_args, catchup=False)
def homework_3():
    @task()
    def get_data():
        vgsales = pd.read_csv(path).query('Year == @year')
        return vgsales

    @task()
    def top_sales(vgsales):
    #Какая игра была самой продаваемой в этом году во всем мире?
        top_sales_name = vgsales.sort_values('Global_Sales', ascending = False).head(1)['Name'].values[0]
        return top_sales_name

    @task()
    def top_EU(vgsales):
    #Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
        EU_genre = vgsales.groupby('Genre', as_index = False).agg({'EU_Sales': 'sum'})
        top_EU_genre = EU_genre.query('EU_Sales == EU_Sales.max()').Genre.to_list()
        return top_EU_genre

    @task()
    def top_NA(vgsales):
    #На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? 
    #Перечислить все, если их несколько
        NA_platform = vgsales.query('NA_Sales > 1').groupby('Platform').agg({'Name': 'count'})
        top_NA_platform = NA_platform.query('Name == Name.max()').index.to_list()
        return top_NA_platform

    @task()
    def publisher_JP(vgsales):
    #У какого издателя самые высокие средние продажи в Японии?
    #Перечислить все, если их несколько
        publisher_JP_avg = vgsales.groupby('Publisher', as_index = False).agg({'JP_Sales': 'mean'})
        top_publisher_JP = publisher_JP_avg.query('JP_Sales == JP_Sales.max()').Publisher.to_list()
        return top_publisher_JP
       
    @task()
    def EU_JP(vgsales):
    #Сколько игр продались лучше в Европе, чем в Японии?
        EU_JP_count = vgsales.query('JP_Sales < EU_Sales').shape[0]
        return EU_JP_count 
    

    @task(on_success_callback=send_message)
    def print_data(top_sales_name, top_EU_genre, top_NA_platform, top_publisher_JP, EU_JP_count, year):

        print(f'''{top_sales_name} had the most global sales in {year}''')

        print(f'''{', '.join(top_EU_genre)} was the most sold genre in Europe in {year}''')
        
        print(f'''Most of the games that had more than 1M sales in North America in {year} were for {', '.join(top_NA_platform)}''')

        print(f'''{', '.join(top_publisher_JP)} had the biggest average sales in Japan in {year}''')
        
        print(f'''{EU_JP_count} games had better sales in Europe than in Japan in {year}''')

    vgsales = get_data()

    top_sales_name = top_sales(vgsales)
    top_EU_genre = top_EU(vgsales)
    top_NA_platform = top_NA(vgsales)
    top_publisher_JP = publisher_JP(vgsales)
    EU_JP_count = EU_JP(vgsales)

    print_data(top_sales_name, top_EU_genre, top_NA_platform, top_publisher_JP, EU_JP_count, year)

homework_3 = homework_3()