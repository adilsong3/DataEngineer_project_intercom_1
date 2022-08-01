from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import requests
import json
import boto3
import pymongo
from sqlalchemy import create_engine
from airflow.models import Variable

# Pegando as senhas cadastradas no airflow já criptografado
aws_access_key_id = Variable.get("aws_access_key_id")
aws_secret_access_key = Variable.get("aws_secret_access_key")
secret_intercom = Variable.get("secret_intercom")
token = {'Authorization': f'{secret_intercom}'}

# criando client para conectar a aws
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# definir default_args
default_args = {
    "owner" : "Adilson Gustavo",
    "depends_on_past" : False,
    "start_date" : datetime(2022, 7, 29)
}

@dag(default_args=default_args, schedule_interval="@once", catchup=False, description="Projeto Intercom", tags=["HILAB","INTERCOM","python","airflow"])
def intercom_project():

    """
    Flow para obter dados do INTERCOM via api e depositar em um DW local PostgreSQL
    """

    @task
    def extract_api_and_save_s3():
        data_path = "/tmp/data_operators.csv"
        url = 'https://api.intercom.io/contacts'
        r = requests.get(url, headers=token)
        r_json = json.loads(r.text)
        df = pd.DataFrame(r_json)
        df.to_csv(data_path, index=False, encoding="utf-8")

        s3_client.upload_file(data_path, "datalake-adilson-project-intercom", f"raw-data/project_intercom/{data_path}",)

    @task
    def tranform_and_save_postgres_s3(file_name):

        url = 'https://api.intercom.io/contacts'
        r = requests.get(url, headers=token)
        r_json = json.loads(r.text)

        list_operators = []
        operator = {}
        intercom = r_json
        total_pages = intercom["pages"]["total_pages"]

        for x in range(0, 10):
            proc_id = intercom["data"][x]["id"]
            proc_name = intercom["data"][x]["name"]
            proc_phone = intercom["data"][x]["phone"]
            proc_email = intercom["data"][x]["email"]
            proc_role = intercom["data"][x]["role"]
            proc_cpf = intercom["data"][x]["custom_attributes"]["CPF"]
            proc_status = intercom["data"][x]["custom_attributes"]["status"]
            proc_profile = intercom["data"][x]["custom_attributes"]["profile"]
            operator.update(
                {
                    "id": proc_id,
                    "name": proc_name,
                    "cpf": proc_cpf,
                    "phone": proc_phone,
                    "email": proc_email,
                    "status": proc_status,
                    "profile": proc_profile,
                    "role": proc_role
                }
            )

            list_operators.append(operator)

        starting_after = intercom["pages"]["next"]["starting_after"]

        cont = 1
        while cont < total_pages:
            page_contact = requests.get(
                f'https://api.intercom.io/contacts?starting_after={starting_after}',
                headers=token)
            new_page = page_contact.json()
            starting_after = new_page["pages"]["next"]["starting_after"]

            # if starting_after is None:
            #    break

            for x in range(0, 10):
                proc_id = intercom["data"][x]["id"]
                proc_name = intercom["data"][x]["name"]
                proc_phone = intercom["data"][x]["phone"]
                proc_email = intercom["data"][x]["email"]
                proc_role = intercom["data"][x]["role"]
                proc_cpf = intercom["data"][x]["custom_attributes"]["CPF"]
                proc_status = intercom["data"][x]["custom_attributes"]["status"]
                proc_profile = intercom["data"][x]["custom_attributes"]["profile"]
                operator.update(
                    {
                        "id": proc_id,
                        "name": proc_name,
                        "cpf": proc_cpf,
                        "phone": proc_phone,
                        "email": proc_email,
                        "status": proc_status,
                        "profile": proc_profile,
                        "role": proc_role
                    }
                )
                list_operators.append(operator)
            cont += 1

            engine = create_engine("postgresql://postgres:postgres@postgres:5432/postgres")
            df = pd.DataFrame(list_operators)
            df.to_sql("transform_"+file_name, engine, if_exists="replace", index=False, method="multi", chunksize=1000)
            s3_client.upload_file("transform_"+file_name, "datalake-adilson-project-intercom", f"zone-bronze/project_intercom/{file_name}",)

    api = extract_api_and_save_s3()
    saveapi = tranform_and_save_postgres_s3(api)

execucao = intercom_project()
