from collections import deque
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from itertools import islice
from threading import Semaphore
from typing import Callable, Iterable, Iterator

def limit_concurrency(max_workers: int) -> Callable[[Callable], Callable]:
    """Wraps methods to implement concurrency limit

    Prefect task concurrency limits use a 30 second delay between each
    check for an available slot. This is a more performative approach
    using a threading.Semaphore.

    Prefect must be using a "local" task runner for this to work (the
    ConcurrentTaskRunner) and not a distributed task runner like Dask
    or Ray.

    Usage:
      from prefect import task

      @task
      @limit_concurrency(max_workers=5)
      def my_task():
          pass
    """

    semaphore = Semaphore(max_workers)

    def pseudo_decorator(func: Callable):
        @wraps(func)
        def limited_concurrent_func(*args, **kwargs):
            with semaphore:
                return func(*args, **kwargs)

        return limited_concurrent_func

    return pseudo_decorator

def map_concurrently(func: Callable, items: Iterable, max_workers: int) -> Iterator:
    """Maps func over items in a thread pool, yielding results in input order.

    Unlike ThreadPoolExecutor.map, only max_workers calls are ever in flight: the
    next item is submitted as each result is yielded. A caller that consumes and
    drops results one by one therefore holds a window of payloads, not the whole
    iterable.
    """

    pending = iter(items)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        window = deque(pool.submit(func, item) for item in islice(pending, max_workers))
        while window:
            result = window.popleft().result()
            for item in islice(pending, 1):
                window.append(pool.submit(func, item))
            yield result
