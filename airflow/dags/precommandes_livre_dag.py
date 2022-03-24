# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from albert.launching_scripts.precommandes.precommandes_livres import crawl_amazon, crawl_fnac, filter, integrate

start_date = datetime(2018, 4, 29)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'retries': 3
}
dag_name = 'precommandes_livre'
schedule_interval = '00 7 * * MON'
mail_recipients = ["dl-fd-referencement-livres2@fnac.com", "frederique.pie@fnacdarty.com"]
amazon_column_name_prefix = "Amazon : "
scraped_columns = {
    "sku": amazon_column_name_prefix + "ASIN", "title": amazon_column_name_prefix + "Titre",
    "authors": amazon_column_name_prefix + "Auteurs", "format": amazon_column_name_prefix + "Format",
    "availability": amazon_column_name_prefix + "Disponibilité",
    "cart": amazon_column_name_prefix + "Panier",
    "image_url": amazon_column_name_prefix + "URL du visuel",
    "genre": amazon_column_name_prefix + "Genre",
    "genre_featured_rank": amazon_column_name_prefix + "Popularité Genre"
}

script = PrecommandesScript("precommandes_livre", mail_recipients, "Précommandes Livre",
                                             scraped_columns, upload_on_fnac_deposit=True)

dag = DAG(dag_name, default_args=default_args, schedule_interval=schedule_interval, catchup=False,
          max_active_runs=1)

spider_amazon = {
    "amazon_precommandes_livre": {"project_name": script.project_name, "crawl_name": "",
                                  "input_filename": "", "base_directory": script.base_directory,
                                  "today_ymd": script.today_ymd, "exe_env": script.exe_env}
}
spider_fnac = {
    "fnac_result": {"project_name": script.project_name}
}

with dag:
    filter = PythonOperator(task_id='filter',
                            python_callable=filter,
                            op_kwargs={'precommandes_script': script})

    crawl_amazon = PythonOperator(task_id='crawl_amazon',
                           python_callable=crawl_amazon,
                           op_kwargs={'precommandes_script': script,
                                      'spiders': spider_amazon})

    crawl_fnac = PythonOperator(task_id='crawl_fnac',
                           python_callable=crawl_fnac,
                           op_kwargs={'precommandes_script': script,
                                      'spiders': spider_fnac})

    integrate = PythonOperator(task_id='integrate',
                               python_callable=integrate,
                               op_kwargs={'precommandes_script': script})

    # crawl_fnac >> integrate
    filter >> crawl_amazon >> crawl_fnac >> integrate
