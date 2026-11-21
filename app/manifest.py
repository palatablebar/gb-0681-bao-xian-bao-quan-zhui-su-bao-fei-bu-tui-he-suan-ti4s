import hashlib,json,time
from datetime import datetime,timezone
from app.config import RULE_VERSION

def fingerprint(canonical, parameters):
    raw = canonical.encode() + json.dumps(parameters, sort_keys=True, ensure_ascii=False).encode() + RULE_VERSION.encode()
    return hashlib.sha256(raw).hexdigest()[:20]

def manifest(content, parameters, elapsed, canonical=''):
    return {'sha256':hashlib.sha256(content).hexdigest(),'canonical':canonical,'rule_version':RULE_VERSION,
        'parameters':parameters,'elapsed_ms':round(elapsed*1000),
        'generated_at':datetime.now(timezone.utc).isoformat(),'timezone':'Asia/Shanghai'}
