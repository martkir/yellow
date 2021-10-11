import concurrent.futures
import functools
import time
import concurrent.futures
import logging
import requests
from fake_useragent import UserAgent
from requests.adapters import HTTPAdapter


logger = logging.getLogger()
ua = UserAgent()


_configs = {
    "hydraproxy": {
        "random": {
            "username": "mart5483qvmk13316",
            "password": "jcREQyB4GFqE9QBy_country-UnitedStates",
            "host": "isp2.hydraproxy.com",
            "port": 9989
        },
        "sticky": {

        }
    },
    "packetstream": {
        "random": {
            "username": "martkir",
            "password": "J8cQ7WxnGP24cdab_country-UnitedStates",
            "host": "proxy.packetstream.io",
            "port": 31112
        },
        "sticky": {
            "username": "martkir",
            "password": "J8cQ7WxnGP24cdab_country-UnitedStates_session",
            "host": "proxy.packetstream.io",
            "port": 31112
        }
    }
}


def get_proxy_config(provider="hydraproxy", kind="random"):
    proxy = _configs[provider][kind]
    return proxy


def get_proxy_urls(provider="hydraproxy", kind="random"):
    config = get_proxy_config(provider, kind)
    proxy_dict = None
    if kind == "random":
        proxy_dict = {
            "http": f"http://{config['username']}:{config['password']}@{config['host']}:{config['port']}",
            "https": f"http://{config['username']}:{config['password']}@{config['host']}:{config['port']}"
        }
    elif kind == "sticky":
        proxy_dict = config
    return proxy_dict


def batch(arr, batch_size):
    arr = iter(arr)
    while True:
        batch = []
        for _ in range(batch_size):
            try:
                obs = next(arr)
                batch.append(obs)
            except:
                break
        if len(batch) > 0:
            yield batch
        else:
            break


def mfetch_threading_requests_random(urls, proxy_urls=None, num_workers=10, req_delay=0.0, rotate_ua=True, timeout=5, max_retries=0):

    def fetch(session, idx, url, attempt):
        print(f"Fetching {url}...")
        logger.info(f"[Attempt-{attempt}] [{idx + 1}/{len(urls)}] Sending request to {url}...")
        headers = {"User-Agent": ua.random if rotate_ua else ua.chrome}
        # headers = {"User-Agent": ua.random, "Referer": "https://thesocialcounter.com/twitter/CashDarma"}
        try:
            res = session.get(url, proxies=proxy_urls, headers=headers, timeout=timeout)
            if res.status_code not in {429}:  # 429 = too many requests
                return res, (idx, url, attempt)
            logger.error(f"[Attempt-{attempt}] [{idx + 1}/{len(urls)}] Failed request got status code {res.status_code}")
        except Exception as e:
            logger.error(f"[Attempt-{attempt}] [{idx + 1}/{len(urls)}] Failed request got error `{str(e)}`")
        return None, (idx, url, attempt)

    def fetch_all(session, args_list, res_dict, num_workers):
        if len(args_list) == 0:
            return res_dict

        tasks = []
        for _ in range(num_workers):
            try:
                (i, url, attempt) = args_list.pop()
                tasks.append(functools.partial(fetch, session=session, idx=i, url=urls[i], attempt=attempt))
            except:
                break

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for task in tasks:
                future = executor.submit(task)
                futures.append(future)
                time.sleep(req_delay)

        # use `as_completed` else new thread gets started and requests start timing out
        for future in concurrent.futures.as_completed(futures):
            res, (i, url, attempt) = future.result()
            if res is None:
                args_list.append((i, url, attempt + 1))
            else:
                res_dict[i] = res

        return fetch_all(session, args_list, res_dict, num_workers)

    with requests.Session() as _session:
        _session.mount("http://", HTTPAdapter(pool_maxsize=num_workers, max_retries=max_retries))
        _session.mount("https://", HTTPAdapter(pool_maxsize=num_workers, max_retries=max_retries))
        _res_dict = fetch_all(
            session=_session,
            args_list=[(i, urls[i], 1) for i in range(len(urls))],  # (i, url, attempt)
            res_dict={},
            num_workers=num_workers
        )
        responses = [_res_dict[i] for i in range(len(_res_dict))]
        return responses


def sfetch(url, proxy_urls=None):
    responses = mfetch_threading_requests_random(
        urls=[url],
        proxy_urls=proxy_urls,
        num_workers=1,
        req_delay=0.1,
        rotate_ua=True
    )
    res = next(iter(responses))
    return res