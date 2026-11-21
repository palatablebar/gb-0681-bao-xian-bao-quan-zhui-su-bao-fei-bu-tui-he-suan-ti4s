import io,csv,json,hashlib

def bundle(state):
    out=io.StringIO(newline='');writer=csv.writer(out)
    writer.writerow(['记录类型','批次','来源行','业务ID','原因码','内容'])
    rid=state.get('run_id','preview')
    writer.writerow(['运行清单',rid,'','','',json.dumps(state.get('manifest',{}),ensure_ascii=False,sort_keys=True)])
    writer.writerow(['批次身份',rid,'','','',json.dumps({
        'batch_id':rid,'reception_count':state.get('reception_count',0),
        'pending_reception_count':state.get('pending_reception_count',0),
        'review_version':state.get('review_version'),'decision':state.get('decision')},
        ensure_ascii=False,sort_keys=True)])
    for reception in state.get('receptions',[]):
        writer.writerow(['接收',rid,reception.get('id',''),reception.get('source',''),
            reception.get('disposition',''),json.dumps({
                'filename':reception.get('filename'),'received_at':reception.get('received_at'),
                'sha256':reception.get('sha256'),'encoding':reception.get('encoding'),
                'canonical':reception.get('canonical'),'byte_size':reception.get('byte_size'),
                'basis':reception.get('basis')},ensure_ascii=False,sort_keys=True)])
    writer.writerow(['对账',rid,'','','',json.dumps(state.get('reconcile',{}),ensure_ascii=False,sort_keys=True)])
    for row in state.get('calculations',[]):writer.writerow(['计算',rid,row.get('line',''),row.get('id',''),row.get('code',''),json.dumps(row,ensure_ascii=False,sort_keys=True)])
    for row in state.get('quarantine',[]):writer.writerow(['隔离',rid,row['line'],row.get('id',''),row['code'],json.dumps(row.get('raw',{}),ensure_ascii=False,sort_keys=True)])
    return '﻿'+out.getvalue()

def checksum(state):
    return hashlib.sha256(bundle(state).encode('utf-8')).hexdigest()
