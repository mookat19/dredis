from tests.helpers import fresh_redis


def test_flushall():
    r0 = fresh_redis(db=0)
    r1 = fresh_redis(db=1)

    r0.set('test1', 'value1')
    r1.set('test2', 'value2')

    assert r0.flushall() is True

    assert r0.keys('*') == []
    assert r1.keys('*') == []


def test_flush_db():
    r0 = fresh_redis(db=0)
    r1 = fresh_redis(db=1)

    r0.set('test1', 'value1')
    r1.set('test2', 'value2')

    assert r0.flushdb() is True

    assert r0.keys('*') == []
    assert r1.keys('*') == ['test2']


def test_ping():
    r = fresh_redis()

    assert r.execute_command('ping') == 'PONG'
    assert r.execute_command('ping', 'msg') == 'msg'


def test_dbsize():
    r0 = fresh_redis(db=0)
    r1 = fresh_redis(db=1)

    assert r0.dbsize() == 0
    assert r1.dbsize() == 0

    r0.set('test', 'value')
    assert r0.dbsize() == 1
    assert r1.dbsize() == 0
