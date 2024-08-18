from connectors import HttpConnector
from connectors import S3Connector


endpoints = {
    'data': [
        {'data': 'statistic_page', 'url': 'https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/user/Angry Birds/daily/20240801/20240831'},
        {'data': 'articles_length', 'url': 'https://en.wikipedia.org/w/api.php?action=query&prop=info&titles=Angry Birds&format=json'},
        {'data': 'reviews', 'url': 'https://en.wikipedia.org/w/api.php?action=query&prop=revisions&titles=Angry_Birds&rvlimit=500&format=json'},
        {'data': 'contributors_by_language', 'url': 'https://en.wikipedia.org/w/api.php?action=query&titles=Angry_Birds&prop=langlinks&lllimit=500&format=json'}
    ]
}

def get_contributors_by_language(url_lang):
    langs = HttpConnector(method='GET', url=url_lang)
    api_result = langs.request_data()

    pages = api_result.get('query', {}).get('pages', {})
    
    langlinks = {}
    
    for page_id, page_data in pages.items():
        if 'langlinks' in page_data:
            for link in page_data['langlinks']:
                langlinks[link['lang']] = link['*']

    langlinks_list = list(langlinks.keys())
    lang_base = {}
    lang_base["language"] = ["language", "count"]
    lang_base["values"] = []

    for lang in langlinks_list:
        url=f"https://{lang}.wikipedia.org/w/api.php?action=query&prop=contributors&titles=Angry Birds&format=json&pclimit=500"
        get_contributors = HttpConnector(method='GET', url=url)
        api_result = get_contributors.request_data()

        pages = api_result.get('query', {}).get('pages', {})

        for page_id, page_data in pages.items():
           contributors = page_data.get('contributors', [])
           lang_base["values"].append([lang, len(contributors)])
    
    return lang_base
    
def landing_ingestion(local_path, landing_data, file_name):
    try:
        ingestion = S3Connector(local_path=local_path, landing_data=landing_data, file_name=file_name)
        ingestion.landing_ingestion_local()
        return True
    except Exception as err:
        print(err)
        return False


for ed in endpoints['data']:
    if ed['data'] != 'contributors_by_language':
        file_name = ed['data']
        url = ed['url']
        run = HttpConnector(method='GET', url=url)
        api_result = run.request_data()
        
        ingestion = landing_ingestion('./lake/landing', api_result, file_name)
        
    elif ed['data'] == 'contributors_by_language':
        file_name = ed['data']
        url_lang = ed['url']
        contributors_by_language = get_contributors_by_language(url_lang)
        
        ingestion = landing_ingestion('./lake/landing', contributors_by_language, file_name)