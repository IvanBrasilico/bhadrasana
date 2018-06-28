"""
Bhadrasana.

Módulo Bhadrasana - AJNA
========================

Interface do Usuário - WEB
--------------------------

Módulo responsável por gerenciar bases de dados importadas/acessadas pelo AJNA,
administrando estas e as cruzando com parâmetros de risco.

Serve para a administração, pré-tratamento e visualização dos dados importados,
assim como para acompanhamento de registros de log e detecção de problemas nas
conexões internas.

Adicionalmente, permite o merge entre bases, navegação de bases, e
a aplicação de filtros/parâmetros de risco.
"""
import datetime
import os
import shutil

from flask import (Flask, flash, jsonify, redirect, render_template, request,
                   session, url_for)
from flask_bootstrap import Bootstrap
# from flask_cors import CORS
from flask_login import current_user, login_required
from flask_nav import Nav
from flask_nav.elements import Navbar, View
from flask_session import Session
# from flask_sslify import SSLify
from flask_wtf.csrf import CSRFProtect
from werkzeug.utils import secure_filename

import ajna_commons.flask.login as login_ajna
from ajna_commons.flask.conf import ALLOWED_EXTENSIONS, SECRET, logo
from ajna_commons.flask.log import logger
from ajna_commons.utils.sanitiza import sanitizar, unicode_sanitizar
from bhadrasana.conf import APP_PATH, CSV_DOWNLOAD, CSV_FOLDER
from bhadrasana.models.models import (BaseOrigem, Coluna, DePara, PadraoRisco,
                                      ParametroRisco, Tabela, ValorParametro,
                                      Visao)
from bhadrasana.utils.gerente_base import Filtro, GerenteBase
from bhadrasana.utils.gerente_risco import GerenteRisco, SemHeaders, tmpdir
from bhadrasana.workers.tasks import (aplicar_risco, arquiva_base_csv,
                                      importar_base)

app = Flask(__name__, static_url_path='/static')
csrf = CSRFProtect(app)
Bootstrap(app)
nav = Nav()
nav.init_app(app)


def configure_app(sqllitedb, mongodb):
    """Configurações gerais e de Banco de Dados da Aplicação."""
    app.config['DEBUG'] = os.environ.get('DEBUG', 'None') == '1'
    if app.config['DEBUG'] is True:
        app.jinja_env.auto_reload = True
        app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.secret_key = SECRET
    app.config['SECRET_KEY'] = SECRET
    app.config['SESSION_TYPE'] = 'filesystem'
    Session(app)
    login_ajna.login_manager.init_app(app)
    login_ajna.configure(app)
    login_ajna.DBUser.dbsession = mongodb
    app.config['dbsession'] = sqllitedb
    app.config['mongodb'] = mongodb
    return app


@app.before_request
def log_every_request():
    """Envia cada requisição ao log."""
    name = 'No user'
    if current_user and current_user.is_authenticated:
        name = current_user.name
    logger.info(request.url + ' - ' + name)


def allowed_file(filename):
    """Checa extensões permitidas."""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/')
def index():
    """View retorna index.html ou login se não autenticado."""
    if current_user.is_authenticated:
        return render_template('index.html')
    else:
        return redirect(url_for('login'))


@app.route('/adiciona_base/<nome>')
@login_required
def adiciona_base(nome):
    """Cria nova instância de Base Origem com o nome passado."""
    dbsession = app.config.get('dbsession')
    logger.debug(nome)
    nova_base = BaseOrigem(nome)
    dbsession.add(nova_base)
    dbsession.commit()
    baseid = nova_base.id
    return redirect(url_for('importa_base', baseid=baseid))


@app.route('/edita_depara')
@login_required
def edita_depara():
    """Tela para configurar os titulos das bases a serem importadas.

    Args:
        baseid: ID da Base de Origem do arquivo
    """
    dbsession = app.config.get('dbsession')
    baseid = request.args.get('baseid')
    bases = dbsession.query(BaseOrigem).all()
    titulos = []
    headers1 = []
    headers2 = []
    user_folder = os.path.join(CSV_FOLDER, current_user.name)
    print('USER', current_user.name)
    if baseid:
        base = dbsession.query(BaseOrigem).filter(
            BaseOrigem.id == baseid
        ).first()
        if base:
            titulos = base.deparas
            gerente = GerenteRisco()
            headers1 = gerente.get_headers_base(
                baseid, path=user_folder)
            for padrao_risco in base.padroes:
                for parametro in padrao_risco.parametros:
                    headers2.append(parametro.nome_campo)
            if len(headers1) == 0:
                flash('Aviso: nenhuma base exemplo ou configuração muda títulos '
                      'encontrada para sugestão de campo título anterior.')
            if len(headers2) == 0:
                flash('Aviso: nenhum parâmetro de risco '
                      'encontrado para sugestão de campo título novo.')
    return render_template('muda_titulos.html', bases=bases,
                           baseid=baseid,
                           titulos=titulos,
                           lista_autocomplete1=headers1,
                           lista_autocomplete2=headers2)


@app.route('/adiciona_depara')
def adiciona_depara():
    """De_para - troca títulos.

    Função que permite unificar o nome de colunas que possuem o mesmo
    conteúdo.

    Esta função realiza a troca do titulo de uma coluna por outro, permitindo
    que duas colunas que tragam a mesma informação em bases diferentes sejam
    filtradas por um único parâmetro de risco.

    Args:
        baseid: ID da Base de Origem do arquivo

        titulo_antigo: Titulo original da base a ser importada

        titulo_novo: Titulo unificado
    """
    dbsession = app.config.get('dbsession')
    baseid = request.args.get('baseid')
    padraoid = request.args.get('padraoid')
    titulo_antigo = sanitizar(request.args.get('antigo'),
                              norm_function=unicode_sanitizar)
    titulo_novo = sanitizar(request.args.get('novo'),
                            norm_function=unicode_sanitizar)
    if baseid:
        base = dbsession.query(BaseOrigem).filter(
            BaseOrigem.id == baseid
        ).first()
        depara = DePara(titulo_antigo, titulo_novo, base)
        dbsession.add(depara)
        dbsession.commit()
    return redirect(url_for('edita_depara', baseid=baseid,
                            padraoid=padraoid))


@app.route('/exclui_depara')
def exclui_depara():
    """Função que remove a troca de titulo selecionada.

    Esta função permite unificar o nome de colunas que possuem o mesmo
    conteúdo.

    Args:
        baseid: ID da Base de Origem do arquivo

        tituloid: ID do titulo a ser excluído
    """
    dbsession = app.config.get('dbsession')
    baseid = request.args.get('baseid')
    padraoid = request.args.get('padraoid')
    tituloid = request.args.get('tituloid')
    dbsession.query(DePara).filter(
        DePara.id == tituloid).delete()
    dbsession.commit()
    return redirect(url_for('edita_depara', baseid=baseid,
                            padraoid=padraoid))


@app.route('/importa_base', methods=['GET', 'POST'])
@login_required
def importa_base():
    """Função para upload do arquivo de uma extração ou outra fonte externa.

    Utiliza o :class: `bhadrasana.utils.gerenterisco.GerenteRisco`.
    Suporte por ora para csv com títulos e zip com sch (padrão Carga)
    Necessita um Servidor Celery para repassar responsabilidade.
    Ver também :func: `bhadrasana.workers.tasks.importar_base`

    Args:
        baseid: ID da Base de Origem do arquivo

        data: data inicial do período extraído (se não for passada,
        assume hoje)

        file: arquivo csv, sch+txt, ou conjunto deles em formato zip
    """
    dbsession = app.config.get('dbsession')
    # print('dbsession', dbsession)
    baseid = request.form.get('baseid')
    data = request.form.get('data')
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('Arquivo vazio. Selecionar arquivo válido'
                  ' e depois clicar em submeter!')
        else:
            file = request.files['file']
            filename = secure_filename(file.filename)
            if (not filename or not allowed_file(filename)):
                flash('Selecionar arquivo válido e depois clicar em submeter!')
                filename = None
            else:
                if baseid is None or baseid == 0:
                    flash('Selecionar base original e clicar em submeter!')
                else:  # Validado - tentar upload e procesamento
                    try:
                        abase = dbsession.query(BaseOrigem).filter(
                            BaseOrigem.id == baseid).first()
                        if abase is None:
                            raise ValueError('Informe uma base válida!!!')
                        if not data:
                            data = datetime.date.today().strftime('%Y-%m-%d')
                        logger.debug(data)
                        logger.debug(current_user.name)
                        tempfile_name = os.path.join(tmpdir, filename)
                        file.save(tempfile_name)
                        # Passa responsabilidade de processamento da base
                        # para o processo Celery
                        user_folder = os.path.join(CSV_FOLDER,
                                                   current_user.name)
                        task = importar_base.apply_async((user_folder,
                                                          abase.id,
                                                          data,
                                                          tempfile_name,
                                                          True))
                        return redirect(url_for('risco',
                                                baseid=baseid,
                                                task=task.id))
                    except Exception as err:
                        logger.error(err, exc_info=True)
                        flash(err)
    bases = dbsession.query(BaseOrigem).order_by(BaseOrigem.nome).all()
    return render_template('importa_base.html', bases=bases,
                           baseid=baseid, data=data)


@app.route('/api/task_progress/<taskid>')
@login_required
def task_progress(taskid):
    """Retorna um json do progresso da celery task."""
    task = importar_base.AsyncResult(taskid)
    response = {'state': task.state}
    if task.info:
        response['current'] = task.info.get('current', ''),
        response['status'] = task.info.get('status', '')
    return jsonify(response)


def get_planilhas_criadas_agendamento(path):
    """Lê o diretório e retorna nomes de arquivos .csv."""
    if not path:
        return []
    return [planilha.name for planilha in os.scandir(path)
            if planilha.is_file() and planilha.name.endswith('.csv')]


@app.route('/risco', methods=['POST', 'GET'])
@login_required
def risco():
    """Função para aplicar parâmetros de risco em arquivo(s) importados.

    Args:
        baseid: ID da Base de Origem do arquivo

        padraoid: ID do padrão de risco aplicável

        visaoid: ID do objeto Visao (junção de CSVs e seleção de campos)

        file: caminho do(s) csv(s) já processados e no diretório

        acao:
            'aplicar' - aplica_risco no diretório file
            'arquivar' - adiciona diretório ao BD e apaga dir
            'excluir' - apaga dir
            'mongo' - busca no banco de dados arquivado
    """
    dbsession = app.config.get('dbsession')
    mongodb = app.config.get('mongodb')
    static_path = os.path.join(APP_PATH,
                               app.config.get('STATIC_FOLDER', 'static'),
                               current_user.name)
    user_folder = os.path.join(CSV_FOLDER, current_user.name)
    try:
        os.mkdir(static_path)
    except FileExistsError:
        pass
    total_linhas = 0
    path = request.args.get('filename', '')
    acao = request.args.get('acao')
    baseid = request.args.get('baseid', '0')
    padraoid = request.args.get('padraoid', '0')
    visaoid = request.args.get('visaoid', '0')
    parametros_ativos = request.args.get('parametros_ativos')
    tasks = []
    # Lista de planilhas geradas pelo agendamento de aplica_risco
    planilhas = get_planilhas_criadas_agendamento(static_path)
    print(planilhas)
    task = request.args.get('task')
    if task:
        tasks.append(task)
    if parametros_ativos:
        parametros_ativos = parametros_ativos.split(',')
    bases = dbsession.query(BaseOrigem).order_by(BaseOrigem.nome).all()
    if baseid:
        abase = dbsession.query(BaseOrigem).filter(
            BaseOrigem.id == baseid).first()
        if abase:
            padroes = abase.padroes
            visoes = abase.visoes
        else:
            padroes = []
            visoes = []
    parametros = []
    if path:
        base_csv = os.path.join(user_folder, baseid, path)
    if acao == 'arquivar' or acao == 'excluir':
        try:
            if abase and base_csv:
                if acao == 'excluir':
                    shutil.rmtree(base_csv)
                    flash('Base excluída!')
                else:
                    # As três linhas antes de chamar a task são para remover
                    # a linha da base escolhida da tela, evitando que o Usuário
                    # tente efetuar novas ações na base que está sendo
                    # arquivada pelo Celery
                    basedir = os.path.basename(base_csv)
                    temp_base_csv = os.path.join(tmpdir, basedir)
                    os.rename(base_csv, temp_base_csv)
                    task = arquiva_base_csv.apply_async(
                        (abase.id, temp_base_csv))
            else:
                flash('Informe Base Original e arquivo!')
        except Exception as err:
            logger.error(err, exc_info=True)
            flash('Erro ao arquivar base! ' +
                  'Detalhes no log da aplicação.')
            flash(type(err))
            flash(err)
        return redirect(url_for('risco', baseid=baseid, task=task))
    lista_arquivos = []
    try:
        for ano in os.listdir(os.path.join(user_folder, baseid)):
            for mes in os.listdir(os.path.join(user_folder, baseid, ano)):
                for dia in os.listdir(os.path.join(user_folder,
                                                   baseid, ano, mes)):
                    lista_arquivos.append(ano + '/' + mes + '/' + dia)
    except FileNotFoundError:
        pass
    padrao = dbsession.query(PadraoRisco).filter(
        PadraoRisco.id == padraoid
    ).first()
    if padrao is not None:
        parametros = padrao.parametros

    gerente = GerenteRisco()
    lista_risco = []
    csv_salvo = ''
    try:
        if visaoid != '0':
            print(visaoid)
            avisao = dbsession.query(Visao).filter(
                Visao.id == visaoid).one()
        if acao == 'mongo':
            path = 'Arquivo ' + abase.nome if abase else base_csv
            if padrao:
                gerente.set_padraorisco(padrao)
            if visaoid == '0':
                lista_risco = gerente.load_mongo(
                    mongodb, base=abase,
                    parametros_ativos=parametros_ativos)
            else:
                lista_risco = gerente.aplica_juncao_mongo(
                    mongodb, avisao, filtrar=padrao is not None,
                    parametros_ativos=parametros_ativos)
        else:
            if acao == 'aplicar':
                lista_risco = gerente.aplica_risco_por_parametros(
                    dbsession, base_csv, padraoid, visaoid, parametros_ativos
                )
            elif acao == 'agendar':
                task = aplicar_risco.apply_async((
                    base_csv, padraoid, visaoid, parametros_ativos, static_path
                ))
                tasks.append(task.id)

    except Exception as err:
        logger.error(err, exc_info=True)
        flash('Erro ao aplicar risco! ' +
              'Detalhes no log da aplicação.')
        flash(type(err))
        flash(err)
    # Salvar resultado um arquivo para donwload
    # Limita resultados em 100 linhas na tela
    if lista_risco:
        csv_salvo = os.path.join(static_path, 'baixar.csv')
        gerente.save_csv(lista_risco, csv_salvo)
        total_linhas = len(lista_risco) - 1
        lista_risco = lista_risco[:100]
    return render_template('aplica_risco.html',
                           lista_arquivos=lista_arquivos,
                           bases=bases,
                           padroes=padroes,
                           visoes=visoes,
                           baseid=baseid,
                           padraoid=padraoid,
                           visaoid=visaoid,
                           parametros=parametros,
                           parametros_ativos=parametros_ativos,
                           filename=path,
                           csv_salvo=os.path.basename(csv_salvo),
                           lista_risco=lista_risco,
                           total_linhas=total_linhas,
                           tasks=tasks,
                           planilhas=planilhas)


@app.route('/exclui_planilha/<planilha>')
@login_required
def exclui_planilha(planilha):
    """Função que exclui uma planilha csv e retorna restantes.

    Args:
        planilha: Nome da planilha

    Returns:
        Lista com as planilhas restantes

    """
    static_path = os.path.join(APP_PATH,
                               app.config.get('STATIC_FOLDER', 'static'),
                               current_user.name)
    if planilha:
        # planilha = secure_filename(planilha)
        planilha = os.path.join(static_path, planilha)
        try:
            os.remove(planilha)
        except OSError as err:
            logger.error(str(err))
            logger.error('exclui_planilha falhou ao excluir: ' + planilha)
    return jsonify(get_planilhas_criadas_agendamento(static_path))


@app.route('/valores')
@login_required
def valores():
    """Função que busca os valores dos parâmetros cadastrados.

    Args:
        parametroid: ID do parâmetro de risco a ser buscado

    Returns:
        Lista com os valores e seus respectivos filtros de busca

    """
    dbsession = app.config.get('dbsession')
    parametro_id = request.args.get('parametroid')
    result = []
    if parametro_id:
        paramrisco = dbsession.query(ParametroRisco).filter(
            ParametroRisco.id == parametro_id
        ).first()
        if paramrisco:
            valores = paramrisco.valores
            result.append([
                [item.valor, str(item.tipo_filtro)] for item in valores
            ])
    return jsonify(result)


@app.route('/edita_risco', methods=['POST', 'GET'])
@login_required
def edita_risco():
    """Editar Padrões e Valores de Risco.

    Tela para configurar os parâmetros de risco das bases importadas,
    permite a alteração e criação de novos parâmetros e seus dados.

    Args:
        padraoid: ID do padrão de risco criado e/ou escolhido para
        realizar a alteração

        riscoid: ID do objeto de risco para aplicar a edição
    """
    user_folder = os.path.join(CSV_FOLDER, current_user.name)
    dbsession = app.config.get('dbsession')
    padraoid = request.args.get('padraoid')
    baseid = request.args.get('baseid')
    padroes = dbsession.query(PadraoRisco).order_by(PadraoRisco.nome).all()
    bases = dbsession.query(BaseOrigem).order_by(BaseOrigem.nome).all()
    parametros = []
    headers = []
    basesid = []
    if padraoid:
        padrao = dbsession.query(PadraoRisco).filter(
            PadraoRisco.id == padraoid
        ).first()
        if padrao:
            basesid = padrao.bases
            parametros = padrao.parametros
    riscoid = request.args.get('riscoid')
    valores = []
    if riscoid:
        valor = dbsession.query(ParametroRisco).filter(
            ParametroRisco.id == riscoid
        ).first()
        if valor:
            valores = valor.valores
    headers = []
    if basesid:
        logger.debug(basesid)
        for base in basesid:
            gerente = GerenteRisco()
            logger.debug(base)
            base_id = base.id
            headers = gerente.get_headers_base(
                base_id, path=user_folder)
            headers = list(headers)
            base_headers = [depara.titulo_novo for depara in
                            dbsession.query(DePara).filter(
                                DePara.base_id == base_id
                            ).all()]
            base_headers = list(base_headers)
            headers.extend(base_headers)
        if len(headers) == 0:
            flash('Aviso: nenhuma base exemplo ou configuração muda títulos '
                  'encontrada para sugestão de campo parâmetro.')
        headers.sort()
        logger.debug(headers)
    return render_template('edita_risco.html',
                           padraoid=padraoid,
                           baseid=baseid,
                           padroes=padroes,
                           bases=bases,
                           basesid=basesid,
                           riscoid=riscoid,
                           parametros=parametros,
                           lista_autocomplete=headers,
                           valores=valores)


@app.route('/adiciona_padrao/<nome>')
@login_required
def adiciona_padrao(nome):
    """Função que adiciona um novo padrão de riscos.

    Args:
        nome: Nome do padrão a ser inserido no Banco de Dados
    """
    dbsession = app.config.get('dbsession')
    logger.debug(nome)
    novo_padrao = PadraoRisco(nome)
    dbsession.add(novo_padrao)
    dbsession.commit()
    padraoid = novo_padrao.id
    return redirect(url_for('edita_risco', padraoid=padraoid))


@app.route('/vincula_base')
@login_required
def vincula_base():
    """Função que vincula padrão de riscos e base origem.

    Args:
        padraoid: ID do padrão a ser atualizado
        baseid: ID da base a vincular
    """
    dbsession = app.config.get('dbsession')
    padraoid = request.args.get('padraoid')
    baseid = request.args.get('baseid')
    padrao = dbsession.query(PadraoRisco).filter(
        PadraoRisco.id == padraoid
    ).first()
    if padrao == None:
        flash('PadraoRisco %s não encontrada ' % padraoid)
    base = dbsession.query(BaseOrigem).filter(
        BaseOrigem.id == baseid
    ).first()
    if base == None:
        flash('Base %s não encontrada ' % baseid)
    if base and padrao:
        if base not in padrao.bases:
            padrao.bases.append(base)
        else:
            padrao.bases.remove(base)
        dbsession.merge(padrao)
        dbsession.commit()
    return redirect(url_for('edita_risco',
                            padraoid=padraoid,
                            baseid=baseid))


@app.route('/importa_csv/<padraoid>/<riscoid>', methods=['POST', 'GET'])
@login_required
def importa_csv(padraoid, riscoid):
    """Importar arquivo.

    Função que lê um arquivo csv contendo duas colunas(valor e tipo_filtro)
    e realiza a importação dos dados para um parâmetro de risco selecionado.

    Args:
        padraoid: ID do padrão de risco

        riscoid: ID do parâmetro de risco
    """
    dbsession = app.config.get('dbsession')
    if request.method == 'POST':
        if 'csv' not in request.files:
            flash('No file part')
            return redirect(request.url)
        csvf = request.files['csv']
        logger.info('FILE***' + csvf.filename)
        if csvf.filename == '':
            flash('No selected file')
            return redirect(request.url)
        risco = None
        if riscoid:
            risco = dbsession.query(ParametroRisco).filter(
                ParametroRisco.id == riscoid).first()
        if risco is None:
            flash('Não foi selecionado parametro de risco')
            return redirect(request.url)
        if (csvf and '.' in csvf.filename and
                csvf.filename.rsplit('.', 1)[1].lower() == 'csv'):
            # filename = secure_filename(csvf.filename)
            csvf.save(os.path.join(tmpdir, risco.nome_campo + '.csv'))
            logger.info(csvf.filename)
            gerente = GerenteRisco()
            gerente.parametros_fromcsv(risco.nome_campo, session=dbsession)
    return redirect(url_for('edita_risco', padraoid=padraoid,
                            riscoid=riscoid))


@app.route('/exporta_csv', methods=['POST', 'GET'])
@login_required
def exporta_csv():
    """Grava em arquivo parâmetros ativos.

    Função que cria um arquivo CSV contendo duas colunas(valor e tipo_filtro)
    e realiza a exportação dos dados do parâmetro de risco selecionado.

    Args:
        padraoid: ID do padrão de risco

        riscoid: ID do parâmetro de risco
    """
    dbsession = app.config.get('dbsession')
    padraoid = request.args.get('padraoid')
    riscoid = request.args.get('riscoid')
    gerente = GerenteRisco()
    gerente.parametro_tocsv(riscoid, path=CSV_DOWNLOAD, dbsession=dbsession)
    return redirect(url_for('edita_risco', padraoid=padraoid,
                            riscoid=riscoid))


@app.route('/adiciona_parametro')
def adiciona_parametro():
    """Função que adiciona um novo parâmetro de risco.

    Args:
        padraoid: ID do padrão de risco

        risco_novo: Nome do novo parâmetro

        lista: Lista com os nomes dos novos parâmetros
    """
    dbsession = app.config.get('dbsession')
    padraoid = request.args.get('padraoid')
    risco_novo = request.args.get('risco_novo')
    lista = request.args.get('lista')
    if risco_novo:
        sanitizado = sanitizar(risco_novo, norm_function=unicode_sanitizar)
        risco = ParametroRisco(sanitizado)
        risco.padraorisco_id = padraoid
        dbsession.add(risco)
        dbsession.commit()
    if lista:
        nova_lista = []
        nova_lista.append(lista)
        for item in nova_lista[0].split(','):
            sanitizado = sanitizar(item, norm_function=unicode_sanitizar)
            risco = ParametroRisco(sanitizado)
            risco.padraorisco_id = padraoid
            dbsession.add(risco)
        dbsession.commit()
    return redirect(url_for('edita_risco', padraoid=padraoid))


@app.route('/exclui_parametro')
def exclui_parametro():
    """Função que exclui um parâmetro de risco e seus respectivos valores.

    Args:
        padraoid: ID do padrão de risco

        riscoid: Nome do parâmetro a ser excluído
    """
    dbsession = app.config.get('dbsession')
    padraoid = request.args.get('padraoid')
    riscoid = request.args.get('riscoid')
    dbsession.query(ParametroRisco).filter(
        ParametroRisco.id == riscoid).delete()
    dbsession.query(ValorParametro).filter(
        ValorParametro.risco_id == riscoid).delete()
    dbsession.commit()
    return redirect(url_for('edita_risco', padraoid=padraoid))


@app.route('/adiciona_valor')
def adiciona_valor():
    """Função que adiciona um novo valor ao parâmetro de risco selecionado.

    Args:
        padraoid: ID do padrão de risco

        novo_valor: Nome do valor a ser inserido no parâmetro

        tipo_filtro: Filtro que este valor deverá ser buscado nas bases
    """
    dbsession = app.config.get('dbsession')
    padraoid = request.args.get('padraoid')
    novo_valor = request.args.get('novo_valor')
    tipo_filtro = request.args.get('filtro')
    valor = sanitizar(novo_valor, norm_function=unicode_sanitizar)
    filtro = sanitizar(tipo_filtro, norm_function=unicode_sanitizar)
    riscoid = request.args.get('riscoid')
    valor = ValorParametro(valor, filtro)
    valor.risco_id = riscoid
    dbsession.add(valor)
    dbsession.commit()
    return redirect(url_for('edita_risco', padraoid=padraoid,
                            riscoid=riscoid))


@app.route('/exclui_valor')
def exclui_valor():
    """Ecluir uma instância de ValorParametro.

    Função que exclui apenas o valor e o tipo_filtro de um parâmetro de
    risco.

    Args:
        padraoid: ID do padrão de risco

        riscoid: ID do parâmetro de risco

        valorid: ID do valor a ser excluído
    """
    dbsession = app.config.get('dbsession')
    padraoid = request.args.get('padraoid')
    riscoid = request.args.get('riscoid')
    valorid = request.args.get('valorid')
    dbsession.query(ValorParametro).filter(
        ValorParametro.id == valorid).delete()
    dbsession.commit()
    return redirect(url_for('edita_risco', padraoid=padraoid,
                            riscoid=riscoid))


@app.route('/navega_bases')
@login_required
def navega_bases():
    """Navega Bases. Deprecated."""
    selected_module = request.args.get('selected_module')
    selected_model = request.args.get('selected_model')
    selected_field = request.args.get('selected_field')
    filters = session.get('filters', [])
    gerente = GerenteBase()
    list_modulos = gerente.list_modulos
    list_models = []
    list_fields = []
    if selected_module:
        gerente.set_module(selected_module)
        list_models = gerente.list_models
        if selected_model:
            list_fields = gerente.dict_models[selected_model]['campos']
    return render_template('navega_bases.html',
                           selected_module=selected_module,
                           selected_model=selected_model,
                           selected_field=selected_field,
                           filters=filters,
                           list_modulos=list_modulos,
                           list_models=list_models,
                           list_fields=list_fields)


@app.route('/adiciona_filtro')
def adiciona_filtro():
    """Incluir Filtro. Deprecated."""
    selected_module = request.args.get('selected_module')
    selected_model = request.args.get('selected_model')
    selected_field = request.args.get('selected_field')
    filters = session.get('filters', [])
    tipo_filtro = request.args.get('filtro')
    valor = request.args.get('valor')
    afilter = Filtro(selected_field, tipo_filtro, valor)
    filters.append(afilter)
    session['filters'] = filters
    return redirect(url_for('navega_bases',
                            selected_module=selected_module,
                            selected_model=selected_model,
                            selected_field=selected_field,
                            filters=filters))


@app.route('/exclui_filtro')
def exclui_filtro():
    """Excluir Filtro. Deprecated."""
    selected_module = request.args.get('selected_module')
    selected_model = request.args.get('selected_model')
    selected_field = request.args.get('selected_field')
    filters = session.get('filters', [])
    index = request.args.get('index')
    if filters:
        filters.pop(int(index))
    session['filters'] = filters
    return redirect(url_for('navega_bases',
                            selected_module=selected_module,
                            selected_model=selected_model,
                            selected_field=selected_field,
                            filters=filters))


@app.route('/consulta_bases_executar')
def consulta_bases_executar():
    """Deprecated."""
    selected_module = request.args.get('selected_module')
    selected_model = request.args.get('selected_model')
    selected_field = request.args.get('selected_field')
    filters = session.get('filters', [])
    gerente = GerenteBase()
    gerente.set_module(selected_module, db='cargatest.db')
    dados = gerente.filtra(selected_model, filters)
    list_modulos = gerente.list_modulos
    list_models = []
    list_fields = []
    if selected_module:
        gerente.set_module(selected_module)
        list_models = gerente.list_models
        if selected_model:
            list_fields = gerente.dict_models[selected_model]['campos']
    return render_template('navega_bases.html',
                           selected_module=selected_module,
                           selected_model=selected_model,
                           selected_field=selected_field,
                           filters=filters,
                           list_modulos=list_modulos,
                           list_models=list_models,
                           list_fields=list_fields,
                           dados=dados)


@app.route('/arvore')
def arvore():
    """Àrvore GerenteBase. Deprecated."""
    gerente = GerenteBase()
    selected_module = request.args.get('selected_module')
    selected_model = request.args.get('selected_model')
    selected_field = request.args.get('selected_field')
    instance_id = request.args.get('instance_id')
    logger.info(selected_module)
    gerente.set_module(selected_module, db='cargatest.db')
    filters = []
    afilter = Filtro(selected_field, None, instance_id)
    filters.append(afilter)
    q = gerente.filtra(selected_model, filters, return_query=True)
    instance = q.first()
    string_arvore = ''
    logger.info(instance)
    pai = gerente.get_paiarvore(instance)
    logger.info(pai)
    if pai:
        lista = gerente.recursive_tree(pai)
        string_arvore = '\n'.join(lista)
    return render_template('arvore.html',
                           arvore=string_arvore)


@app.route('/arvore_teste')
def arvore_teste():
    """Deprecated."""
    gerente = GerenteBase()
    gerente.set_module('carga', db='cargatest.db')
    filters = []
    afilter = Filtro('Manifesto', None, 'M-2')
    filters.append(afilter)
    q = gerente.filtra('Manifesto', filters, return_query=True)
    manifesto = q.first()
    escala = gerente.get_paiarvore(manifesto)
    string_arvore = ''
    if escala:
        lista = gerente.recursive_tree(escala, child=manifesto)
        string_arvore = '\n'.join(lista)
    return render_template('arvore.html',
                           arvore=string_arvore)


@app.route('/juncoes')
@login_required
def juncoes():
    """Tela para cadastramento de junções.

    Args:
        baseid: ID da Base de Origem do arquivo

        visaoid: ID objeto de Banco de Dados que espeficica as configurações
        (metadados) da base
    """
    user_folder = os.path.join(CSV_FOLDER, current_user.name)
    dbsession = app.config.get('dbsession')
    baseid = request.args.get('baseid')
    visaoid = request.args.get('visaoid')
    bases = dbsession.query(BaseOrigem).all()
    visoes = dbsession.query(Visao).order_by(Visao.nome).all()
    tabelas = []
    colunas = []
    headers = []
    arquivos = []
    if baseid:
        gerente = GerenteRisco()
        arquivos = gerente.get_headers_base(baseid, user_folder, csvs=True)
        headers = gerente.get_headers_base(baseid, user_folder)
        list(headers)
    if visaoid:
        tabelas = dbsession.query(Tabela).filter(
            Tabela.visao_id == visaoid
        ).all()
        colunas = dbsession.query(Coluna).filter(
            Coluna.visao_id == visaoid
        ).all()
    return render_template('gerencia_juncoes.html',
                           baseid=baseid,
                           bases=bases,
                           visaoid=visaoid,
                           visoes=visoes,
                           colunas=colunas,
                           tabelas=tabelas,
                           lista_colunas=headers,
                           lista_arquivos=arquivos)


@app.route('/adiciona_visao')
def adiciona_visao():
    """Função que permite a criação de um novo objeto Visão.

    Args:
        baseid: ID da Base de Origem do arquivo

        visao_novo: Nome do objeto de Banco de Dados que espeficica as
        configurações (metadados) da base
    """
    dbsession = app.config.get('dbsession')
    baseid = request.args.get('baseid')
    visao_novo = request.args.get('visao_novo')
    visao = Visao(visao_novo)
    visao.nome = visao_novo
    dbsession.add(visao)
    dbsession.commit()
    visao = dbsession.query(Visao).filter(
        Visao.nome == visao_novo
    ).first()
    return redirect(url_for('juncoes',
                            visaoid=visao.id,
                            baseid=baseid))


@app.route('/exclui_visao')
def exclui_visao():
    """Função que permite a remoção de um objeto Visão.

    Args:
        visaoid: ID do objeto de Banco de Dados que espeficica as
        configurações (metadados) da base
    """
    dbsession = app.config.get('dbsession')
    visaoid = request.args.get('visaoid')
    dbsession.query(Visao).filter(
        Visao.id == visaoid).delete()
    dbsession.query(Coluna).filter(
        Coluna.visao_id == visaoid).delete()
    dbsession.commit()
    return redirect(url_for('juncoes'))


@app.route('/adiciona_coluna')
def adiciona_coluna():
    """Função inserir uma coluna ao objeto Visão.

    Args:
        visaoid: ID do objeto de Banco de Dados que espeficica as
        configurações (metadados) da base

        col_nova: Nome da coluna que será inserida no objeto Visão
    """
    dbsession = app.config.get('dbsession')
    visaoid = request.args.get('visaoid')
    col_nova = request.args.get('col_nova')
    coluna = Coluna(col_nova)
    coluna.nome = col_nova
    coluna.visao_id = visaoid
    dbsession.add(coluna)
    dbsession.commit()
    return redirect(url_for('juncoes', visaoid=visaoid))


@app.route('/exclui_coluna')
def exclui_coluna():
    """Função que permite a remoção de uma coluna do objeto Visão.

    Args:
        visaoid: ID do objeto de Banco de Dados que espeficica as
        configurações (metadados) da base

        colunaid: ID da coluna a ser excluída
    """
    dbsession = app.config.get('dbsession')
    visaoid = request.args.get('visaoid')
    colunaid = request.args.get('colunaid')
    dbsession.query(Coluna).filter(
        Coluna.id == colunaid).delete()
    dbsession.commit()
    return redirect(url_for('juncoes',
                            visaoid=visaoid))


@app.route('/adiciona_tabela')
def adiciona_tabela():
    """Função para inserir uma tabela ao objeto Visão.

    Args:
        visaoid: ID do objeto de Banco de Dados que espeficica as
        configurações (metadados) da base

        csv: Nome do CSV a ser inserido

        primario:

        estrangeiro:
    """
    dbsession = app.config.get('dbsession')
    visaoid = request.args.get('visaoid')
    csv = request.args.get('csv')
    primario = request.args.get('primario')
    estrangeiro = request.args.get('estrangeiro')
    pai_id = request.args.get('pai_id')
    desc = request.args.get('descricao')
    tabela = Tabela(csv, primario, estrangeiro, pai_id, visaoid)
    if desc:
        tabela.descricao = desc
    dbsession.add(tabela)
    dbsession.commit()
    return redirect(url_for('juncoes',
                            visaoid=visaoid))


@app.route('/exclui_tabela')
def exclui_tabela():
    """Função para remover uma tabela do objeto Visão.

    Args:
        visaoid: ID do objeto de Banco de Dados que espeficica as
        configurações (metadados) da base

        tabelaid: ID da tabela a ser excluída
    """
    dbsession = app.config.get('dbsession')
    visaoid = request.args.get('visaoid')
    tabelaid = request.args.get('tabelaid')
    dbsession.query(Tabela).filter(
        Tabela.id == tabelaid).delete()
    dbsession.commit()
    return redirect(url_for('juncoes',
                            visaoid=visaoid))


@nav.navigation()
def mynavbar():
    """Menu da aplicação."""
    items = [View('Home', 'index'),
             View('Importar Base', 'importa_base'),
             View('Aplicar Risco', 'risco'),
             View('Editar Riscos', 'edita_risco'),
             View('Editar Visão', 'juncoes'),
             View('Editar Titulos', 'edita_depara'),
             # View('Navega Bases', 'navega_bases')
             ]
    if current_user.is_authenticated:
        items.append(View('Sair', 'logout'))
    return Navbar(logo, *items)
