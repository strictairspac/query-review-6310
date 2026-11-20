"""Strict read-only bounded SQL execution against a single seeded business table."""
import json,hashlib,time,os
from collections import Counter
from decimal import Decimal
import sqlglot
from sqlglot import exp
from sqlalchemy import text
from ..config.database import engine
ALLOWED_FUNCTIONS={'SUM','COUNT','AVG','MIN','MAX','ROUND','COALESCE','CAST'}
ALLOWED_COLUMNS={'customer','route','amount','temperature','invoice_id','status'}
# 演示/测试钩子：榜单核对两侧查询之间人为停顿毫秒数，默认 0 不停顿。
LEADERBOARD_DELAY_MS=int(os.getenv('LEADERBOARD_SNAPSHOT_DELAY_MS','0'))
def validate_sql(sql):
    try: trees=sqlglot.parse(sql,read='postgres')
    except Exception as exc: raise ValueError('SQL 无法解析：'+str(exc)[:180])
    if len(trees)!=1 or not isinstance(trees[0],exp.Select): raise ValueError('只允许单条 SELECT')
    tree=trees[0]
    if tree.args.get('into') or tree.args.get('locks') or tree.find(exp.With): raise ValueError('不允许 INTO、锁或 CTE')
    tables=list(tree.find_all(exp.Table))
    if not tables or any(t.name!='invoices' or t.db or t.catalog for t in tables): raise ValueError('只允许访问 invoices 表')
    for col in tree.find_all(exp.Column):
        if col.name not in ALLOWED_COLUMNS and col.name!='*': raise ValueError('未知业务字段：'+col.name)
    for f in tree.find_all(exp.Func):
        if f.sql_name() not in ALLOWED_FUNCTIONS: raise ValueError('不允许函数：'+f.sql_name())
    if len(list(tree.walk()))>250: raise ValueError('SQL 结构过大')
    return tree.sql(dialect='postgres')
def output_columns(sql):
    """Static output column names of a single SELECT; '*' marks an unexpanded star."""
    try: tree=sqlglot.parse_one(sql,read='postgres')
    except Exception: return []
    names=[]
    for e in getattr(tree,'expressions',[]):
        if isinstance(e,exp.Star): names.append('*')
        elif e.alias_or_name: names.append(e.alias_or_name.lower())
    return names
def _strip_limit(sql):
    """核对依据：去掉 LIMIT/OFFSET/FETCH 的完整结果，避免截断末位并列范围。"""
    tree=sqlglot.parse_one(sql,read='postgres')
    stripped=False
    for arg in ('limit','offset','fetch'):
        if tree.args.get(arg): tree.set(arg,None); stripped=True
    return tree.sql(dialect='postgres'),stripped
def _run_side(conn,sql,explain=True):
    plan=conn.execute(text('EXPLAIN (ANALYZE, FORMAT JSON, BUFFERS) '+sql)).scalar() if explain else None
    started=time.perf_counter()
    cursor=conn.execute(text(sql)); columns=list(cursor.keys()); rows=cursor.fetchmany(1001)
    if len(rows)>1000: raise ValueError('结果超过 1000 行，请聚合或 LIMIT')
    rows=[[str(v) if v is not None else None for v in row] for row in rows]
    return {'columns':columns,'rows':rows,'row_count':len(rows),'elapsed_ms':round((time.perf_counter()-started)*1000,3),'plan':plan}
def execute_query(sql):
    sql=validate_sql(sql)
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text('SET TRANSACTION READ ONLY'))
            conn.execute(text("SET LOCAL statement_timeout='2500ms'"))
            conn.execute(text('SET LOCAL ROLE query_reader'))
            return _run_side(conn,sql)
def compare(original,candidate):
    left=execute_query(original);right=execute_query(candidate)
    canon=lambda data:Counter(json.dumps(row,ensure_ascii=False) for row in data['rows'])
    a,b=canon(left),canon(right)
    equivalent=len(left['columns'])==len(right['columns']) and a==b
    digest=lambda x:hashlib.sha256(json.dumps(sorted(canon(x).items()),ensure_ascii=False).encode()).hexdigest()
    return {'equivalent':equivalent,'comparison':'忽略行顺序、保留重复行；按输出列位置比较，数字按数据库文本表示','original':left,'candidate':right,'original_checksum':digest(left),'candidate_checksum':digest(right),'missing_rows':list((a-b).elements())[:10],'extra_rows':list((b-a).elements())[:10],'note':'执行时间受缓存与机器负载影响；不据一次耗时断言优化收益'}
def _is_timeout(exc):
    if getattr(getattr(exc,'orig',exc),'pgcode',None)=='57014': return True
    return 'statement timeout' in str(exc).lower()
def _column_indexes(result):
    cols=[str(c).lower() for c in result['columns']]
    if 'invoice_id' not in cols or 'amount' not in cols: raise ValueError('榜单核对要求 SQL 输出 invoice_id 与 amount 两列')
    return cols.index('invoice_id'),cols.index('amount')
def _entries(result):
    ii,ai=_column_indexes(result)
    entries=[]
    for row in result['rows']:
        if row[ii] is None or row[ai] is None: continue
        entries.append((int(row[ii]),row[ai],Decimal(row[ai])))
    return entries
def _leaderboard(basis,picked_rows,top_n,stripped):
    entries=sorted(_entries(basis),key=lambda e:(-e[2],e[0]))
    cutoff=entries[min(top_n,len(entries))-1][2] if entries else None
    must=sorted({(i,a) for i,a,d in entries if cutoff is not None and d>cutoff})
    tie=sorted({(i,a) for i,a,d in entries if cutoff is not None and d==cutoff})
    return {'picked':[[i,a] for i,a,_ in _entries(picked_rows)[:top_n]],'cutoff_amount':str(cutoff) if cutoff is not None else None,'must_include':[list(x) for x in must],'tie_range':[list(x) for x in tie],'basis_limit_stripped':stripped}
def compare_leaderboard(original,candidate,top_n):
    """Top-N high-freight leaderboard check on one shared REPEATABLE READ snapshot.

    榜单（用户查询实际挑中的前 top_n 笔）与核对依据（同一查询去掉 LIMIT/OFFSET
    后的完整结果）在同一事务快照读取。分界金额以上的必入账单与末位并列的可互
    换范围两侧都相同即通过，末位并列挑中的单号允许不同。任一侧查询超时降级为
    待核（review），已完成的比对仍给出 passed/different 结论。
    """
    original=validate_sql(original);candidate=validate_sql(candidate)
    base={'mode':'leaderboard','top_n':top_n,
          'comparison':'两侧各取前 N 笔高额运费；分界金额以上的必入账单与末位并列的可互换范围都相同即通过，末位并列挑中的单号允许不同；漏掉高额账单判差异',
          'note':'榜单与核对依据（去掉 LIMIT/OFFSET 的完整结果）在同一 REPEATABLE READ 只读事务快照上执行；并发改账照常入库，不改变本批次所读版本'}
    sides={};review=None
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ'))
            conn.execute(text('SET TRANSACTION READ ONLY'))
            conn.execute(text("SET LOCAL statement_timeout='2500ms'"))
            conn.execute(text('SET LOCAL ROLE query_reader'))
            snapshot=conn.execute(text('SELECT pg_current_snapshot()::text')).scalar()
            count,total=conn.execute(text('SELECT COUNT(*)::text, COALESCE(SUM(amount),0)::text FROM invoices')).fetchone()
            base['data_version']={'snapshot':snapshot,'invoice_count':count,'amount_sum':total}
            for key,sql in (('original',original),('candidate',candidate)):
                try:
                    result=_run_side(conn,sql)
                    basis_sql,stripped=_strip_limit(sql)
                    basis=_run_side(conn,basis_sql,explain=False) if stripped else result
                    sides[key]={'result':result,'basis':basis,'stripped':stripped}
                except Exception as exc:
                    if not _is_timeout(exc): raise
                    conn.rollback()
                    review={'conclusion':'review','equivalent':None,'error':('基准' if key=='original' else '候选')+'查询超时，已降级为待核：'+str(exc)[:200]}
                    break
                if key=='original' and LEADERBOARD_DELAY_MS>0: time.sleep(LEADERBOARD_DELAY_MS/1000)
    if review: return {**base,**review,'original':sides.get('original',{}).get('result'),'candidate':sides.get('candidate',{}).get('result')}
    lo=_leaderboard(sides['original']['basis'],sides['original']['result'],top_n,sides['original']['stripped'])
    lc=_leaderboard(sides['candidate']['basis'],sides['candidate']['result'],top_n,sides['candidate']['stripped'])
    equivalent=lo['cutoff_amount']==lc['cutoff_amount'] and lo['must_include']==lc['must_include'] and lo['tie_range']==lc['tie_range']
    diff=lambda a,b:[x for x in a if x not in b]
    return {**base,'conclusion':'passed' if equivalent else 'different','equivalent':equivalent,
            'original':sides['original']['result'],'candidate':sides['candidate']['result'],
            'leaderboard':{'original':lo,'candidate':lc},
            'differences':{'missing_must_include':diff(lo['must_include'],lc['must_include']),'extra_must_include':diff(lc['must_include'],lo['must_include']),'tie_range_missing':diff(lo['tie_range'],lc['tie_range']),'tie_range_extra':diff(lc['tie_range'],lo['tie_range'])}}
