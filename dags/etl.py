from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
import os, json,html
TABLES_CREATION_QUERY = """CREATE TABLE IF NOT EXISTS job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(225),
    industry VARCHAR(225),
    description TEXT,
    employment_type VARCHAR(125),
    date_posted DATE
);

CREATE TABLE IF NOT EXISTS company (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    name VARCHAR(225),
    link TEXT,
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS education (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    required_credential VARCHAR(225),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS experience (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    months_of_experience INTEGER,
    seniority_level VARCHAR(25),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS salary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    currency VARCHAR(3),
    min_value NUMERIC,
    max_value NUMERIC,
    unit VARCHAR(12),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS location (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    country VARCHAR(60),
    locality VARCHAR(60),
    region VARCHAR(60),
    postal_code VARCHAR(25),
    street_address VARCHAR(225),
    latitude NUMERIC,
    longitude NUMERIC,
    FOREIGN KEY (job_id) REFERENCES job(id)
)
"""

def load_and_clean(path_to_data):
    data = pd.read_csv(path_to_data)
    data.dropna(axis=0, inplace=True)

    return data

def text_decoder(text):
    decoded_text = html.unescape(text)
    utf8_text = decoded_text.encode('utf-8')

    return utf8_text.decode('utf-8')

def get_data_as_json():
    data_as_json = []
    files = os.listdir(os.path.join('staging', 'extracted'))
    files = [file for file in files if file.split('.')[1] == 'txt']
    for file in files:
        with open(os.path.join('staging', 'extracted', file), 'r') as f:
            # Load the JSON data from the file
            data = json.load(f)
            for key in data.keys():
                if isinstance(data[key], str):
                    data[key] = text_decoder(data[key])
            data_as_json.append(data)
        f.close()
    return data_as_json

def get_values(data_as_json):
    mapping = {
        'job': {
            'date_posted': 'datePosted',
            'description': 'description',
            'employment_type': 'employmentType',
            'industry': 'industry',
            'title': 'title',
        },
        'location': {
            'country': 'jobLocation.address.addressCountry',
            'latitude': 'jobLocation.latitude',
            'locality': 'jobLocation.address.addressLocality',
            'longitude': 'jobLocation.longitude',
            'postal_code': 'jobLocation.address.postalCode',
            'region': 'jobLocation.address.addressRegion',
            'street_address': 'jobLocation.address.streetAddress',
        },
        'company': {
            'link': 'hiringOrganization.sameAs',
            'name': 'hiringOrganization.name',
        },
        'salary': {
            'currency': 'estimatedSalary.currency',
            'max_value': 'estimatedSalary.value.maxValue',
            'min_value': 'estimatedSalary.value.minValue',
            'unit': 'estimatedSalary.value.unitText',
        },
        'education': {
            'required_credential': 'educationRequirements.credentialCategory',
        },
        'experience': {
            'months_of_experience': 'experienceRequirements.monthsOfExperience',
            'seniority_level': "",
        },
    }
    transformed_data = []
    for data in data_as_json:
        db = {
            "job": {
                "title": None,
                "industry": None,
                "description": None,
                "employment_type": None,
                "date_posted": None,
            },
            "company": {
                "name": None,
                "link": None,
            },
            "education": {
                "required_credential": None,
            },
            "experience": {
                "months_of_experience": None,
                "seniority_level": None,
            },
            "salary": {
                "currency": None,
                "min_value": None,
                "max_value": None,
                "unit": None,
            },
            "location": {
                "country": None,
                "locality": None,
                "region": None,
                "postal_code": None,
                "street_address": None,
                "latitude": None,
                "longitude": None,
            },
        }
        for section, section_mapping in mapping.items():
            for key_in_db, key_in_data in section_mapping.items():
                keys = key_in_data.split('.')
                value = data
                for key in keys:
                    if key in value:
                        value = value[key]
                    else:
                        break
                else:
                    db[section][key_in_db] = value
        transformed_data.append(db)
    return transformed_data

def get_transformed_data():
    transformed_data = []
    files = os.listdir(os.path.join('staging', 'transformed'))
    files = [file for file in files if file.split('.')[1] == 'json']
    for file in files:
        with open(os.path.join('staging', 'transformed', file), 'r') as f:
            data = json.load(f)
            transformed_data.append(data)
        f.close()

    return transformed_data
@task()
def extract():
        """Extract data from jobs.csv."""
    data = load_and_clean(os.path.join('source', 'jobs.csv'))
    for i, row in data.iterrows():
        with open(os.path.join('staging', 'extracted', f'enrg{i}.txt'), 'w') as f:
            f.write(row['context'])
        f.close()


@task()
def transform():
    """Clean and convert extracted elements to json."""
    data_as_json = get_data_as_json()
    transformed_data = get_values(data_as_json)
    i = 0
    for data in transformed_data:
        with open(os.path.join('staging', 'transformed', f'enrg{i}.json'), 'w') as f:
            json.dump(data, f, indent=4)
        f.close()
        i += 1

@task()
def load():
    """Load data to sqlite database."""
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    transformed_data = get_transformed_data()
    prepare_data = {}
    for key in transformed_data[0].keys():
        prepare_data[key] = []
    for data in transformed_data:
        for table, row in data.items():
            prepare_data[table].append(row)

    for table, load_data in prepare_data.items():
        sqlite_hook.insert_rows(table=table, rows=load_data, target_fields=load_data[0].keys())

DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)
def etl_dag():
    """ETL pipeline"""

    create_tables = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY
    )

    create_tables >> extract() >> transform() >> load()

etl_dag()