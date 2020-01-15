from datetime import timedelta, datetime

from airflow import DAG
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

default_args = {
    'owner': 'Jorricks',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 5),
    'email': ['jorricks3@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


class DownloadHTTPOperator(BaseOperator):
    template_fields = ['storage_location', 'endpoint']

    @apply_defaults
    def __init__(self,
                 http_conn_id,
                 method,
                 storage_location,
                 endpoint,
                 data=None,
                 headers=None,
                 extra_options=None,
                 *args, **kwargs):

        super(DownloadHTTPOperator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.method = method

        self.data = data or {}
        self.headers = headers or {}
        self.extra_options = extra_options or {'stream': True}

        self.storage_location = storage_location
        self.endpoint = endpoint

    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)

        self.log.info("Calling HTTP method")

        response = http.run(self.endpoint,
                            self.data,
                            self.headers,
                            self.extra_options)
        if response.status_code == 200:
            with open(self.storage_location, 'wb') as f:
                f.write(response.raw.read())
        else:
            raise ConnectionError('Status code was not 200')

        return "success"


with DAG(
        dag_id='gh_archive',
        default_args=default_args,
        schedule_interval='0 * * * *',
        catchup=True,
        max_active_runs=1,
) as dag:
    get = DownloadHTTPOperator(
        http_conn_id='hg_archive',
        method='GET',
        storage_location='day_utc={{ ds }}/{{ execution_date.strftime("%Y%m%dT%H%M") }}.json.gz',
        endpoint='{{execution_date.strftime("%Y-%m-%d-%-H")}}.json.gz',
        headers={'Accept-Encoding': 'deflate'},
        task_id='get_the_gh_stuff',
    )
