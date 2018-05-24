"""Modelo de dados necessário para app bhadrasana."""
import enum
import os

from sqlalchemy import (Column, Enum, ForeignKey, Integer, String, Table,
                        create_engine)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, scoped_session, sessionmaker
from werkzeug.security import generate_password_hash


class Filtro(enum.Enum):
    """Enumerado.

    Para escolha do tipo de filtro a ser
    aplicado no parâmetro de risco
    """

    igual = 1
    comeca_com = 2
    contem = 3


class MySession():
    """Sessão com BD.

    Para definir a sessão com o BD na aplicação. Para os
    testes, passando o parâmetro test=True, um BD na memória
    """

    def __init__(self, base, test=False):
        """Inicializa."""
        if test:
            path = ':memory:'
        else:
            path = os.path.join(os.path.dirname(
                os.path.abspath(__file__)), 'bhadrasana.db')
            print('***PATH', path)
            if os.name != 'nt':
                path = '/' + path
        self._engine = create_engine('sqlite:///' + path, convert_unicode=True)
        Session = sessionmaker(bind=self._engine)
        if test:
            self._session = Session()
        else:
            self._session = scoped_session(Session)
            base.metadata.bind = self._engine

    @property
    def session(self):
        """Session."""
        return self._session

    @property
    def engine(self):
        """Engine."""
        return self._engine


Base = declarative_base()


class SQLDBUser(Base):
    """Base de Usuários."""

    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String(20), unique=True)
    _password = Column(String(200))

    def __init__(self, username, password):
        """Inicializa."""
        self.username = username
        self._password = self.encript(password)

    @classmethod
    def encript(self, password):
        """Recebe uma senha em texto plano, retorna uma versão encriptada."""
        return generate_password_hash(password)

    @classmethod
    def get(cls, session, username, password=None):
        """Testa se usuário existe, e se passado, se a senha está correta.

        Returns:
            SQLDBUser ou None

        """
        if password:
            DBUser = session.query(SQLDBUser).filter(
                SQLDBUser.username == username,
                SQLDBUser._password == cls.encript(password)
            ).first()
        else:
            DBUser = session.query(SQLDBUser).filter(
                SQLDBUser.username == username,
            ).first()
        return DBUser


association_table = Table('basesorigem_padroesrisco', Base.metadata,
                          Column('left_id', Integer,
                                 ForeignKey('basesorigem.id')),
                          Column('right_id', Integer,
                                 ForeignKey('padroesrisco.id'))
                          )


class BaseOrigem(Base):
    """Metadado sobre as bases de dados disponíveis/integradas.

    Caminho: caminho no disco onde os dados da importação da base
    (normalmente arquivos csv) estão guardados
    """

    __tablename__ = 'basesorigem'
    id = Column(Integer, primary_key=True)
    nome = Column(String(20), unique=True)
    caminho = Column(String(200), unique=True)
    deparas = relationship('DePara', back_populates='base')
    visoes = relationship('Visao', back_populates='base')
    padroes = relationship(
        'PadraoRisco', secondary=association_table,
        back_populates='bases')

    def __init__(self, nome, caminho=None):
        """Inicializa."""
        self.nome = nome
        self.caminho = caminho


class PadraoRisco(Base):
    """Metadado sobre as bases de dados disponíveis/integradas.

    Caminho: caminho no disco onde os dados da importação da base
    (normalmente arquivos csv) estão guardados
    """

    __tablename__ = 'padroesrisco'
    id = Column(Integer, primary_key=True)
    nome = Column(String(20), unique=True)
    parametros = relationship('ParametroRisco', back_populates='padraorisco')
    base_id = Column(Integer, ForeignKey('basesorigem.id'))
    bases = relationship(
        'BaseOrigem', secondary=association_table,
        back_populates='padroes')

    def __init__(self, nome, base=None):
        """Inicializa."""
        self.nome = nome
        if base:
            self.base_id = base.id


class DePara(Base):
    """Renomeia os titulos das colunas ao importar uma base."""

    __tablename__ = 'depara'
    id = Column(Integer, primary_key=True)
    titulo_ant = Column(String(50))
    titulo_novo = Column(String(50))
    base_id = Column(Integer, ForeignKey('basesorigem.id'))
    base = relationship(
        'BaseOrigem', back_populates='deparas')

    def __init__(self, titulo_ant, titulo_novo, base):
        """Inicializa."""
        self.titulo_ant = titulo_ant
        self.titulo_novo = titulo_novo
        self.base_id = base.id


class ParametroRisco(Base):
    """Paramêtro de Risco.

    Nomeia um parâmetro de risco que pode ser aplicado
    como filtro em um Banco de Dados. Um parâmetro tem uma
    lista de valores que serão o filtro efetivo.
    """

    __tablename__ = 'parametrosrisco'
    id = Column(Integer, primary_key=True)
    nome_campo = Column(String(20))
    descricao = Column(String(200))
    valores = relationship('ValorParametro', back_populates='risco')
    padraorisco = relationship(
        'PadraoRisco', back_populates='parametros')
    padraorisco_id = Column(Integer, ForeignKey('padroesrisco.id'))

    def __init__(self, nome, descricao='', padraorisco=None):
        """Inicializa."""
        self.nome_campo = nome
        self.descricao = descricao
        if padraorisco:
            self.padraorisco_id = padraorisco.id


class ValorParametro(Base):
    """Um valor de parametro.

    A ser aplicado como filtro em uma fonte de dados.

    nomecampo = nome do campo da fonte de dados a ser aplicado filtro

    tipofiltro = tipo de função de filtragem a ser realizada
    (ver enum TipoFiltro)
    """

    __tablename__ = 'valoresparametro'
    id = Column(Integer, primary_key=True)
    valor = Column(String(50), unique=True)
    tipo_filtro = Column(Enum(Filtro))
    risco_id = Column(Integer, ForeignKey('parametrosrisco.id'))
    risco = relationship(
        'ParametroRisco', back_populates='valores')

    def __init__(self, nome, tipo):
        """Inicializa."""
        self.valor = nome
        self.tipo_filtro = tipo


class Visao(Base):
    """Metadado sobre os csvs capturados.

    Para mapear relações entre os
    csvs capturados e permitir junção automática se necessário.
    Utilizado por "GerenteRisco.aplica_juncao()"
    Ver :py:func:`gerente_risco.aplica_juncao`
    """

    __tablename__ = 'visoes'
    id = Column(Integer, primary_key=True)
    nome = Column(String(50), unique=True)
    tabelas = relationship('Tabela', back_populates='visao')
    colunas = relationship('Coluna', back_populates='visao')
    base_id = Column(Integer, ForeignKey('basesorigem.id'))
    base = relationship(
        'BaseOrigem', back_populates='visoes')

    def __init__(self, csv):
        """Inicializa."""
        self.csv = csv


class Coluna(Base):
    """Metadado sobre os csvs capturados.

    Define os campos que serão exibidos na junção.
    Ver :py:func:`gerente_risco.aplica_juncao`
    """

    __tablename__ = 'colunas'
    id = Column(Integer, primary_key=True)
    nome = Column(String(50))
    visao_id = Column(Integer, ForeignKey('visoes.id'))
    visao = relationship(
        'Visao', back_populates='colunas')

    def __init__(self, csv):
        """Inicializa."""
        self.csv = csv


class Tabela(Base):
    """Metadado sobre os csvs capturados.

    Para mapear relações entre os
    csvs capturados e permitir junção automática se necessário.
    Utilizado por "GerenteRisco.aplica_juncao()"
    Ver :py:func:`gerente_risco.aplica_juncao`
    """

    __tablename__ = 'tabelas'
    id = Column(Integer, primary_key=True)
    csv = Column(String(20))
    descricao = Column(String(200))
    primario = Column(Integer)
    estrangeiro = Column(Integer)
    pai_id = Column(Integer, ForeignKey('tabelas.id'))
    filhos = relationship('Tabela')
    pai = relationship('Tabela', remote_side=[id])
    visao_id = Column(Integer, ForeignKey('visoes.id'))
    visao = relationship(
        'Visao', back_populates='tabelas')

    def __init__(self, csv, primario, estrangeiro, pai_id, visao_id):
        """Inicializa."""
        self.csv = csv
        self.primario = primario
        self.estrangeiro = estrangeiro
        self.pai_id = pai_id
        self.visao_id = visao_id

    @property
    def csv_file(self):
        """Acesso ao campo csv. Inclui extensão."""
        if self.csv.find('.csv') == -1:
            return self.csv + '.csv'
        return self.csv
