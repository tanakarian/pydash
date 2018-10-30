from urllib.parse import urljoin
import json
import time
import os
import requests
import pandas as pd


class QueryResult(object):
    def __init__(self, rows, query):
        self.query = query
        self.rows = rows

    @classmethod
    def create(cls, res):
        rows = res['query_result']['data']['rows']
        query = res['query_result']['query']
        return QueryResult(rows, query)

    def to_dataframe(self):
        df = pd.DataFrame(self.rows)
        return df


class Client(object):
    def __init__(self, host, api_key, data_source_id):
        self.api_base = urljoin(host, 'api')
        self.api_key = api_key
        self.data_source_id = data_source_id

    def _api_get(self, resource, params={}):
        params.update({'api_key': self.api_key})
        # BasicAuth
        if os.environ.get('USER') and os.environ.get('PASSWORD'):
            return requests.get(self.api_base + '/' + resource,
                                params=params,
                                auth=(os.environ.get('USER'), os.environ.get('PASSWORD')))
        else:
            return requests.get(self.api_base + '/' + resource,
                                params=params)

    def _api_post(self, resource, data, params={}):
        params.update({'api_key': self.api_key})
        # BasicAuth
        if os.environ.get('USER') and os.environ.get('PASSWORD'):
            return requests.post(self.api_base + '/' + resource,
                                params=params,
                                auth=(os.environ.get('USER'), os.environ.get('PASSWORD')),
                                data=json.dumps(data))
        else:
            return requests.post(self.api_base + '/' + resource,
                                 params=params,
                                 data=json.dumps(data))

    def data_sources(self):
        return self._api_get('data_sources').json()

    def all_queries(self):
        queries = []
        page = 1
        res = self._api_get('queries', {'page': page})

        while res.status_code == 200:
            queries += res.json()['results']
            page += 1
            res = self._api_get('queries', {'page': page})

        return queries

    def query(self, query, retry_num=30, interval_sec=1, data_source_id=None):
        if data_source_id:
            self.data_source_id = data_source_id
        res_j = self._post_query(query).json()
        retried = 0

        while not self._query_completed(res_j):
            time.sleep(interval_sec)
            res_j = self._post_query(query).json()
            retried += 1
            if retried > retry_num:
                raise Exception('Max retry num reached.')
        return QueryResult.create(res_j)

    def _post_query(self, query):
        data = {
            'query': query,
            'data_source_id': self.data_source_id
        }
        return self._api_post('query_results', data=data)

    def _has_result(self, res_json):
        return ('query_result' in res_json) and ('retrieved_at' in res_json['query_result'])

    def _query_completed(self, res_json):
        if self._has_result(res_json):
            return True
        uncompleted_job = self.job(res_json['job']['id'])
        if self._job_has_error(uncompleted_job):
            raise Exception(uncompleted_job['job']['error'])
        return False

    def _job_has_error(self, res_json):
        if res_json['job']['error']:
           return True
        return False

    def job(self, jid):
        return self._api_get('jobs/%s' % jid).json()
