from datetime import timedelta, datetime

from airflow import DAG
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.contrib.sensors.python_sensor import PythonSensor
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults
from airflow.sensors.http_sensor import HttpSensor

# Things to do before running this:
# - Connections
#   - 'hg_archive', 'https://data.gharchive.org/'
#   - 'slack', see https://medium.com/datareply/integrating-slack-alerts-in-airflow-c9dcd155105

default_args = {
    'owner': 'Jorricks',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 14),
    'email': ['jorricks3@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

SLACK_CONN_ID = 'slack'
GH_ARCHIVE_CONN_ID = 'hg_archive'


class DownloadGZIPOperator(BaseOperator):
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

        super(DownloadGZIPOperator, self).__init__(*args, **kwargs)
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
            import os
            file = str(os.path.realpath(self.storage_location))
            self.log.info('Storing response to {}'.format(str(file)))
            parent_dir = os.path.dirname(self.storage_location)
            if not os.path.exists(parent_dir):
                self.log.info('Creating folder {}'.format(str(parent_dir)))
                os.makedirs(parent_dir)

            with open(self.storage_location, 'wb') as f:
                f.write(response.raw.read())
                self.log.info('Done. File was stored at {}'.format(str(file)))

            return file
        else:
            self.log.error('Status code was not 200')
            raise ConnectionError('Status code was not 200')


def extract_release_events(gz_file) -> str:
    full_file = gz_file.replace('.gz', '')
    release_event_str = '"type":"ReleaseEvent"'

    import os, json
    parent_dir = os.path.dirname(full_file)
    filename = os.path.basename(full_file)
    release_filename = 'release_' + filename
    release_path = os.path.join(parent_dir, release_filename)

    with open(full_file, 'r') as read_file, open(release_path, 'w+') as write_file:
        for line in read_file:
            if release_event_str in line:
                r_json = json.loads(line)
                write_file.write(
                    "New release, {}:{} at {}.\n".format(
                        r_json['repo']['name'],
                        r_json['payload']['release']['tag_name'],
                        r_json['created_at']
                    )
                )

    return str(release_path)


def check_if_file_is_empty(task_id_to_get_full_path, **context) -> bool:
    file = context['ti'].xcom_pull(task_ids=task_id_to_get_full_path)
    import os
    return os.path.getsize(file) > 0


def send_release_to_slack(release_file, **context):
    with open(release_file, 'r') as msg_file:
        slack_msg = msg_file.read()

    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_send_message = SlackWebhookOperator(
        task_id='slack_send_message',
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return slack_send_message.execute(context=context)


with DAG(
        dag_id='get_gh_archive_2',
        default_args=default_args,
        schedule_interval='0 * * * *',
        catchup=True,
        max_active_runs=1,
) as dag:
    sensor = HttpSensor(
        task_id='check_if_present',
        http_conn_id=GH_ARCHIVE_CONN_ID,
        endpoint='{{execution_date.strftime("%Y-%m-%d-%-H")}}.json.gz',
        method='HEAD',
        poke_interval=5,
        dag=dag,
    )

    get_gh_archive = DownloadGZIPOperator(
        task_id='download',
        http_conn_id=GH_ARCHIVE_CONN_ID,
        endpoint='{{execution_date.strftime("%Y-%m-%d-%-H")}}.json.gz',
        method='GET',
        storage_location='day_utc={{ ds }}/{{ execution_date.strftime("%Y-%m-%d-%-H") }}.json.gz',
        headers={'Accept-Encoding': 'deflate'},
    )

    unzip = BashOperator(
        task_id='unzip',
        bash_command='gunzip {{ ti.xcom_pull(task_ids="download") }}',
    )

    extract_release = PythonOperator(
        task_id='extract_release',
        python_callable=extract_release_events,
        op_kwargs={'gz_file': '{{ ti.xcom_pull(task_ids="download") }}'},
    )

    check_release_present = PythonSensor(
        task_id="check_release_present",
        python_callable=check_if_file_is_empty,
        provide_context=True,
        op_kwargs={'task_id_to_get_full_path': 'extract_release'},
        poke_interval=30,
    )

    send_releases = PythonOperator(
        task_id="send_release_to_slack",
        python_callable=send_release_to_slack,
        op_kwargs={'release_file': '{{ ti.xcom_pull(task_ids="extract_release") }}'},
    )

    sensor >> get_gh_archive >> unzip >> extract_release >> check_release_present >> send_releases
