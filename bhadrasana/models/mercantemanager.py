from ajna_commons.flask.conf import SQL_URI
from sqlalchemy import create_engine
from sqlalchemy import select, and_, join, or_
from sqlalchemy.orm import sessionmaker
from virasana.integracao.mercante.mercantealchemy import Conhecimento, NCMItem

engine = create_engine(SQL_URI)
Session = sessionmaker(bind=engine)
session = Session()


def mercanterisco(pfiltros: dict):
    # conhecimentos = session.query(Conhecimento).\
    #    join(NCMItem, Conhecimento.numeroCEmercante == NCMItem.numeroCEMercante).\
    #    limit(10).all()
    keys = ['dataEmissao', 'numeroCEmercante', 'consignatario', 'descricao',
            'embarcador', 'portoOrigemCarga', 'codigoConteiner', 'identificacaoNCM']
    portosorigem = pfiltros.get('portoOrigemCarga')
    filtros = and_()
    for key in keys:
        lista = pfiltros.get(key)
        if lista is not None:
            filtro = or_(*
                     [and_(getattr(Conhecimento, key).ilike(porto + '%'))
                      for porto in lista])
            filtros = and_(filtros, filtro)
    if pfiltros.get('ncm'):
        filtro = or_(*
                         [and_(NCMItem.identificacaoNCM.ilike(ncm + '%'))
                          for ncm in pfiltros.get('ncm')])
        filtros = and_(filtros, filtro)
    j = join(Conhecimento, NCMItem, Conhecimento.numeroCEmercante == NCMItem.numeroCEMercante)
    s = select([Conhecimento, NCMItem]).select_from(j). \
        where(filtros). \
        order_by(Conhecimento.numeroCEmercante, NCMItem.numeroSequencialItemCarga). \
        limit(100)
    resultproxy = session.execute(s)

    result = [keys]
    for row in resultproxy:
        print(row)
        print(list(row.keys()))
        print(dir(row))
        result.append([row[key] for key in keys])
    return result
