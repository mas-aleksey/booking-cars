import asyncio
from asyncio import Queue, Event, Semaphore
from collections import defaultdict
from typing import Optional, Any, Dict, Set

from app.const import MAX_PARALLEL_AGG_REQUESTS_COUNT, WORKERS_COUNT


class PipelineContext:
    def __init__(self, user_id: int, data: Optional[Any] = None):
        self._user_id = user_id
        self.data = data

    @property
    def user_id(self):
        return self._user_id


CURRENT_AGG_REQUESTS_COUNT = 0
BOOKED_CARS: Dict[int, Set[str]] = defaultdict(set)


async def get_offers(source: str) -> list[dict]:
    """
    Эта функция эмулирует асинхронных запрос по сети в сервис каршеринга source.
     Например source = "yandex" - запрашиваем список свободных автомобилей в сервисе yandex.

    Keyword arguments:
    source - сайт каршеринга
    """
    await asyncio.sleep(1)
    return [
        {"url": f"http://{source}/car?id=1", "price": 1_000, "brand": "LADA"},
        {"url": f"http://{source}/car?id=2", "price": 5_000, "brand": "MITSUBISHI"},
        {"url": f"http://{source}/car?id=3", "price": 3_000, "brand": "KIA"},
        {"url": f"http://{source}/car?id=4", "price": 2_000, "brand": "DAEWOO"},
        {"url": f"http://{source}/car?id=5", "price": 10_000, "brand": "PORSCHE"},
    ]


async def get_offers_from_sourses(sources: list[str]) -> list[dict]:
    """
    Эта функция агрегирует предложения из списка сервисов по каршерингу

    Keyword arguments:
    sources - список сайтов каршеринга ["yandex", "belka", "delimobil"]
    """

    global CURRENT_AGG_REQUESTS_COUNT
    if CURRENT_AGG_REQUESTS_COUNT >= MAX_PARALLEL_AGG_REQUESTS_COUNT:
        await asyncio.sleep(10.0)

    CURRENT_AGG_REQUESTS_COUNT += 1

    # TODO реализовать получение всех предложений. Вызвать get_offers для каждого элемента из sources

    CURRENT_AGG_REQUESTS_COUNT -= 1

    out = list()

    # TODO корректно оформить данные в out. Объединить данные из всех запросов get_offers.

    return out

async def service_worker(inbound: Queue[PipelineContext], outbound: Queue[PipelineContext], sem: Semaphore):
    while True:
        ctx = await inbound.get()
        async with sem:
            res = await get_offers_from_sourses(ctx.data)
            outbound.put_nowait(PipelineContext(ctx.user_id, res))
        await asyncio.sleep(0.1)

# TODO реализовать звено агрегации предложений о бронировании каршерингов
async def chain_combine_service_offers(
    inbound: Queue[PipelineContext], outbound: Queue[PipelineContext], **kw
):
    """
    Запускает N функций worker-ов для обработки данных из очереди inbound и передачи результата в outbound очередь.
    N worker-ов == WORKERS_COUNT (константа из app/const.py)

    Нужно подобрать такой примитив синхронизации, который бы ограничивал вызов функции get_offers_from_sourses для N воркеров.
    Ограничение количества вызовов - MAX_PARALLEL_AGG_REQUESTS_COUNT (константа из app/const.py)

    Keyword arguments:
    inbound: Queue[PipelineContext] - очередь данных для обработки
    Пример элемента PipelineContext из inbound:
    PipelineContext(
        user_id = 1,
        data = [
           "yandex.ru",
           "delimobil.ru",
           "belkacar.ru",
        ]
    )

    outbound: Queue[PipelineContext] - очередь для передачи обработанных данных
    Пример элемента PipelineContext в outbound:
    PipelineContext(
        user_id = 1,
        data = [
            {"url": "http://yandex.ru/car?id=1", "price": 1_000, "brand": "LADA"},
            {"url": "http://yandex.ru/car?id=2", "price": 5_000, "brand": "MITSUBISHI"},
            {"url": "http://yandex.ru/car?id=3", "price": 3_000, "brand": "KIA"},
            {"url": "http://yandex.ru/car?id=4", "price": 2_000, "brand": "DAEWOO"},
            {"url": "http://yandex.ru/car?id=5", "price": 10_000, "brand": "PORSCHE"},
    ]
    )
    """

    # do some logic


# TODO реализовать звено фильтрации предложений о бронировании каршерингов
async def chain_filter_offers(
    inbound: Queue,
    outbound: Queue,
    brand: Optional[str] = None,
    price: Optional[int] = None,
    **kw,
):
    """
    Функция обработывает данных из очереди inbound и передает результат в outbound очередь.
    Нужно при наличии параметров brand и price - отфильтровать список предожений.

    Keyword arguments:
    brand: Optional[str] - название бренда по которому фильруем предложение. Условие brand == offer["brand"]
    price: Optional[int] - максимальная стоимость предложения. Условие price >= offer["price"]

    inbound: Queue[PipelineContext] - очередь данных для обработки
    Пример элемента PipelineContext из inbound:
    PipelineContext(
        user_id = 1,
        data = [
            {"url": "http://yandex.ru/car?id=1", "price": 1_000, "brand": "LADA"},
            {"url": "http://yandex.ru/car?id=2", "price": 5_000, "brand": "MITSUBISHI"},
            {"url": "http://yandex.ru/car?id=3", "price": 3_000, "brand": "KIA"},
            {"url": "http://yandex.ru/car?id=4", "price": 2_000, "brand": "DAEWOO"},
            {"url": "http://yandex.ru/car?id=5", "price": 10_000, "brand": "PORSCHE"},
    ]
    )

    outbound: Queue[PipelineContext] - очередь для передачи обработанных данных
    Пример элемента PipelineContext в outbound:
    PipelineContext(
        user_id = 1,
        data = [
            {"url": "http://yandex.ru/car?id=1", "price": 1_000, "brand": "LADA"},
            {"url": "http://yandex.ru/car?id=2", "price": 5_000, "brand": "MITSUBISHI"},
            {"url": "http://yandex.ru/car?id=3", "price": 3_000, "brand": "KIA"},
            {"url": "http://yandex.ru/car?id=4", "price": 2_000, "brand": "DAEWOO"},
            {"url": "http://yandex.ru/car?id=5", "price": 10_000, "brand": "PORSCHE"},
    ]
    )
    """


async def cancel_book_request(user_id: int, offer: dict):
    """
    Эмулирует запрос отмены бронирования  авто
    """
    await asyncio.sleep(1)
    await asyncio.sleep(1)
    await asyncio.sleep(1)
    # print('cancel offer', offer)
    BOOKED_CARS[user_id].remove(offer.get("url"))


# TODO отловить исколючение asyncio.CancelledError и вызвать cancel_book_request в этом случае
async def book_request(user_id: int, offer: dict, event: Event) -> dict:
    """
    Эмулирует запрос бронирования авто. В случае отмены вызывает cancel_book_request.
    """
    await asyncio.sleep(1)
    BOOKED_CARS[user_id].add(offer.get("url"))
    if not event.is_set():
        event.set()
        print('return', offer)
        return offer
    else:
        await cancel_book_request(user_id, offer)


async def booking_worker(inbound: Queue[PipelineContext], outbound: Queue[PipelineContext]):
    while True:
        ctx = await inbound.get()
        event = Event()

        tasks = [book_request(ctx.user_id, offer, event) for offer in ctx.data]
        finished, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        await asyncio.gather(*pending)

        result_offer = finished.pop().result()
        outbound.put_nowait(PipelineContext(ctx.user_id, result_offer))


# TODO реализовать звено бронирования автомобиля
async def chain_book_car(
    inbound: Queue[PipelineContext], outbound: Queue[PipelineContext], **kw
):
    """
    Запускает N функций worker-ов для обработки данных из очереди inbound и передачи результата в outbound очередь.
    Worker должен параллельно вызвать book_request для каждого предложения. Первый отработавший запрос передать в PipelineContext.
    Остальные запросы нужно отменить и вызвать для них cancel_book_request.

    Keyword arguments:
    inbound: Queue[PipelineContext] - очередь данных для обработки
    Пример элемента PipelineContext из inbound:
    PipelineContext(
        user_id = 1,
        data = [
            {"url": "http://yandex.ru/car?id=1", "price": 1_000, "brand": "LADA"},
            {"url": "http://yandex.ru/car?id=2", "price": 5_000, "brand": "MITSUBISHI"},
            {"url": "http://yandex.ru/car?id=3", "price": 3_000, "brand": "KIA"},
            {"url": "http://yandex.ru/car?id=4", "price": 2_000, "brand": "DAEWOO"},
            {"url": "http://yandex.ru/car?id=5", "price": 10_000, "brand": "PORSCHE"},
    ]
    )

    outbound: Queue[PipelineContext] - очередь для передачи обработанных данных
    Пример элемента PipelineContext в outbound:
    PipelineContext(
        user_id = 1,
        data = {"url": "http://yandex.ru/car?id=1", "price": 1_000, "brand": "LADA"}
    )
    """


# TODO запустить цепочку работы звеньев и обмены данными между ними
def run_pipeline(inbound: Queue[PipelineContext], loop) -> Queue[PipelineContext]:
    """
    Необходимо создать asyncio.Task для функций:
    - chain_combine_service_offers
    - chain_filter_offers
    - chain_book_car

    Создать необходимые очереди для обмена данными между звеньями (chain-ами)
    -> chain_combine_service_offers -> chain_filter_offers -> chain_book_car ->

    Вернуть outbound очередь для звена chain_book_car

    Keyword arguments:
    inbound: Queue[PipelineContext] - очередь данных для обработки
    Пример элемента PipelineContext из inbound:
    PipelineContext(
        user_id = 1,
        data = [
           "yandex.ru",
           "delimobil.ru",
           "belkacar.ru",
        ]
    )

    -> Queue[PipelineContext] - очередь для передачи обработанных данных
    Пример элемента PipelineContext из возвращаемой очереди:
    PipelineContext(
        user_id = 1,
        data = {"url": "http://yandex.ru/car?id=1", "price": 1_000, "brand": "LADA"}
    )
    """
    outbound_combine_service = Queue()
    outbound_filter = Queue()
    outbound_book_car = Queue()

    return outbound_book_car
