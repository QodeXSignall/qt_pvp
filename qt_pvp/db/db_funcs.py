import sqlalchemy
from sqlalchemy.dialects.sqlite import insert
from hermessplitter.db import init_db
from hermessplitter.db import tables


def create_new_record(record_id: int,
                      clear_gross: int = None, hermes_gross: int = None,
                      final_gross: int = None,
                      clear_cargo: int = None, hermes_cargo: int = None,
                      final_cargo: int = None, tare: int = None,
                      test_mode: bool = None, kf_source_id: int = None,
                      notes: str = None, kf_must_be=None):
    ins = tables.records.insert().values(
        record_id=record_id,
        clear_gross=clear_gross,
        hermes_gross=hermes_gross,
        final_gross=final_gross,
        clear_cargo=clear_cargo,
        hermes_cargo=hermes_cargo,
        final_cargo=final_cargo,
        tare=tare,
        test_mode=test_mode,
        kf_source_id=kf_source_id,
        notes=notes,
        kf_must_be=kf_must_be
    )
    return init_db.engine.execute(ins)


def create_or_upd_client(name: str, ex_id: str):
    exist = sqlalchemy.select(tables.clients.c.ex_id).where(
        tables.clients.c.ex_id == ex_id)
    res = init_db.engine.execute(exist)
    res = res.fetchone()
    if not res:
        ins = insert(tables.clients).values(
            name=name,
            kf=0,
            ex_id=ex_id
        )
        return init_db.engine.execute(ins)
    else:
        ins = sqlalchemy.update(tables.clients).where(
            tables.clients.c.ex_id == ex_id).values(
            name=name,
        )
        return init_db.engine.execute(ins)


def create_or_upd_auto(car_number: str, ex_id: str):
    exist = sqlalchemy.select(tables.auto.c.ex_id).where(
        tables.auto.c.ex_id == ex_id)
    res = init_db.engine.execute(exist)
    res = res.fetchone()
    if not res:
        ins = insert(tables.auto).values(
            car_number=car_number,
            kf=0,
            ex_id=ex_id
        )
        return init_db.engine.execute(ins)
    else:
        ins = sqlalchemy.update(tables.auto).where(
            tables.auto.c.ex_id == ex_id).values(
            car_number=car_number,
        )
        return init_db.engine.execute(ins)


def get_client_kf_by_name(name):
    ins = sqlalchemy.select(tables.clients.c.kf).where(
        tables.clients.c.name == name)
    r = init_db.engine.execute(ins)
    return r.fetchone()


def get_auto_kf_by_car_number(car_number):
    ins = sqlalchemy.select(tables.auto.c.kf).where(
        tables.auto.c.car_number == car_number)
    r = init_db.engine.execute(ins)
    return r.fetchone()


def get_client_kf_by_ex_id(ex_id):
    ins = sqlalchemy.select(tables.clients.c.kf).where(
        tables.clients.c.ex_id == ex_id)
    r = init_db.engine.execute(ins)
    return r.fetchone()


def get_all_data(table):
    s = sqlalchemy.select(table)
    r = init_db.engine.execute(s)
    return r.fetchall()


def set_settings(**kwargs):
    for key, _value in kwargs.items():
        ins = insert(tables.settings).values(
            key=key,
            value=_value,
        )
        on_duplicate_key = ins.on_conflict_do_update(
            index_elements=['key'],
            set_=dict(value=_value)
        )
        return init_db.engine.execute(on_duplicate_key)


def get_test_mode():
    ins = sqlalchemy.select(tables.settings.c.value).where(
        tables.settings.c.key == 'test_mode'
    )
    r = init_db.engine.execute(ins)
    return r.fetchone()


def get_hermes_activity():
    ins = sqlalchemy.select(tables.settings.c.value).where(
        tables.settings.c.key == 'active')
    r = init_db.engine.execute(ins)
    result = r.fetchone()
    return result


def get_records():
    ins = sqlalchemy.select(tables.records.c)
    r = init_db.engine.execute(ins)
    return r.fetchall()


def get_few_records(record_ids: str):
    s = ("SELECT * FROM records WHERE record_id in ({})".format(record_ids))
    result = init_db.engine.execute(s).fetchall()
    return result


def get_few_records_dict(record_ids: str):
    s = (
        "SELECT record_id, hermes_gross, test_mode, notes, kf_must_be FROM records WHERE record_id in ({})".format(
            record_ids))
    result = init_db.engine.execute(s).fetchall()
    all_records_dict = {}
    for res in result:
        new_rec = {}
        new_rec[res[0]] = {'hermes': res[1], 'test_mode': res[2],
                           'notes': res[3], 'kf_must_be': res[4]}
        all_records_dict.update(new_rec)
    return all_records_dict


def get_record(record_id):
    ins = sqlalchemy.select(tables.records.c).where \
        (tables.records.c.record_id == record_id)
    r = init_db.engine.execute(ins)
    return r.fetchone()


def get_clients_info():
    req = sqlalchemy.select(tables.clients.c)
    r = init_db.engine.execute(req)
    return r.fetchall()


def get_auto_info():
    req = sqlalchemy.select(tables.auto.c)
    r = init_db.engine.execute(req)
    return r.fetchall()


def update_kf(client_id, new_kf):
    ins = sqlalchemy.update(tables.clients).where(
        tables.clients.c.id == client_id).values(
        kf=new_kf,
    )
    return init_db.engine.execute(ins)


def update_kf_auto(auto_id, new_kf):
    ins = sqlalchemy.update(tables.auto).where(
        tables.auto.c.id == auto_id).values(
        kf=new_kf,
    )
    return init_db.engine.execute(ins)


def switch_turn():
    activity = get_hermes_activity()
    activity = activity[0]
    if activity == '1':
        turn_activity('0')
    else:
        turn_activity('1')


def turn_activity(activity='1'):
    ins = sqlalchemy.update(tables.settings).where(
        tables.settings.c.key == 'active').values(
        value=activity,
    )
    return init_db.engine.execute(ins)
