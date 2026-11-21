import base64
from flask import request,render_template,jsonify
from app.errors import DomainError
from app.config import TITLE,MODULES,SHORT
from app.workflow import run,load
from app import store

def payload():
    data=request.get_json(silent=True) or {}
    if not isinstance(data,dict):raise DomainError('PAYLOAD','请求必须是对象')
    if 'csv' in data and not isinstance(data['csv'],str):raise DomainError('CSV','CSV内容必须是文本')
    sample=data.get('sample',request.args.get('sample','dirty.csv'))
    content=data.get('csv')
    return {'content':content.encode() if isinstance(content,str) else load(sample),
            'parameters':data.get('parameters',{}),
            'source':data.get('source',request.headers.get('X-Source-Channel')),
            'filename':data.get('filename') or (sample if content is None else None)}

def submit_payload():
    data=payload()
    return store.receive(data['content'],data['parameters'],data['source'],data['filename'])

def api(stage):
    data=payload();return jsonify(run(data['content'],data['parameters'],stage))

def page(stage,template):
    latest=store.history();state=store.get(latest[0]['id']) if latest else run(load('dirty.csv'),{})
    source=load(request.args['sample']) if 'sample' in request.args else base64.b64decode(state['source_b64']) if state.get('source_b64') else load('dirty.csv')
    data=run(source,state.get('manifest',{}).get('parameters',{}),stage)
    if data['run_id']==state['run_id']:
        # Page and closing download describe the same batch: identity,
        # reception ledger and archive checksum all come from the stored batch.
        data['receptions']=state.get('receptions',[])
        data['reception_count']=state.get('reception_count',0)
        data['adoption_basis']=state.get('adoption_basis',[])
        data['pending_reception_count']=state.get('pending_reception_count',0)
        data['batch_id']=state['run_id']
        if state.get('archive'):
            data['archive']=state['archive']
        data['publish']=state.get('publish')
    else:
        # Previewing another sample: never borrow the visible batch's identity
        # or its reception ledger.
        data['batch_id']=data['run_id'];data['receptions']=[]
        data['reception_count']=0;data['adoption_basis']=[];data['pending_reception_count']=0
        data['preview']={'sample':request.args.get('sample'),'visible_batch':state['run_id']}
        if data.get('archive'):
            data['archive']['ready']=False
    return render_template(template,title=TITLE,short=SHORT,modules=MODULES,current=stage,data=data,run=state,
        history=latest,reception_queue=store.receptions())
