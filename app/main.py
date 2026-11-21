import importlib
from flask import Flask,jsonify,request,redirect,Response
from app.config import MODULES
from app.errors import DomainError
from app.workflow import load
from app import store
from app.modules.m7_archive.export_out import bundle

def create_app():
    app=Flask(__name__);app.config['MAX_CONTENT_LENGTH']=2_000_000
    for entry in MODULES:app.register_blueprint(importlib.import_module('app.modules.'+entry[0]+'.handler').bp)
    @app.errorhandler(DomainError)
    def domain_error(error):return jsonify(code=error.code,message=error.message),error.status
    @app.errorhandler(404)
    def missing(error):return jsonify(code='NOT_FOUND',message='入口不存在'),404
    @app.get('/')
    def index():return redirect('/'+MODULES[0][0])
    @app.get('/healthz')
    def health():return jsonify(status='ok')
    @app.post('/api/runs')
    def submit():
        from app.web import submit_payload
        state=submit_payload()
        response=jsonify(state)
        if not state.get('publish',{}).get('ok',True):response.status_code=503
        return response
    @app.get('/api/runs')
    def list_runs():return jsonify(store.history())
    @app.get('/api/runs/<run_id>')
    def get_run(run_id):return jsonify(store.get(run_id))
    @app.post('/api/runs/<run_id>/review')
    def review(run_id):
        data=request.get_json(silent=True) or {}
        if not isinstance(data.get('version'),int):raise DomainError('VERSION','请提交复核版本')
        state=store.review(run_id,data['version'],data.get('decision'))
        # Every arrival must end with a destination.  Settling only rewrites
        # reception dispositions; it never adds a calculation row or a sign-off
        # version.
        if len(state.get('receptions',[]))>1:
            store.settle(run_id)
            state=store.get(run_id)
        return jsonify(state)
    @app.get('/api/archive/<run_id>/export.csv')
    def download(run_id):
        state=store.get(run_id)
        if not state.get('reconcile',{}).get('balanced'):raise DomainError('UNBALANCED','对账未平')
        # Pending receptions still appear in the bundle with their disposition,
        # so the export never hides an arrival; the page marks the batch unready.
        return Response(bundle(state),mimetype='text/csv; charset=utf-8',headers={'Content-Disposition':'attachment; filename=evidence.csv'})
    @app.get('/api/receptions')
    def list_receptions():
        batch=request.args.get('batch_id') or None
        return jsonify(store.receptions(batch))
    @app.post('/api/receptions/identify')
    def identify_receptions():
        data=request.get_json(silent=True) or {}
        if not isinstance(data,dict):raise DomainError('PAYLOAD','请求必须是对象')
        batch_id=data.get('batch_id') or None
        reception_id=data.get('reception_id')
        if reception_id is not None and not isinstance(reception_id,int):raise DomainError('RECEPTION','接收编号无效')
        if batch_id is not None and not isinstance(batch_id,str):raise DomainError('BATCH','批次编号无效')
        return jsonify(store.identify(batch_id=batch_id,reception_id=reception_id))
    @app.get('/api/receptions/<int:reception_id>/source')
    def reception_source(reception_id):
        filename,body=store.reception_source(reception_id)
        safe=''.join(c for c in filename if c.isalnum() or c in '._-()（）') or 'source.csv'
        return Response(body,mimetype='application/octet-stream',headers={
            'Content-Disposition':'attachment; filename="reception-%d-%s"'%(reception_id,safe)})
    with app.app_context():
        store.migrate_legacy()
        if not store.history():
            store.receive(load('dirty.csv'),{},'样例','dirty.csv')
    return app

app=create_app()
if __name__=='__main__':app.run(host='0.0.0.0',port=8000)
