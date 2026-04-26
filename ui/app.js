/* ════════════════════════════════════════════════════════════════════════
   全局状态 & 工具
════════════════════════════════════════════════════════════════════════ */
const API = ''; // 同源，留空即可；若跨域填 http://localhost:8765
let currentQuestion = '';
let currentSQL = '';
let currentSSE = null;
let currentAskEngine = 'semantic';
let currentPage = 'query';
let currentManageTab = 'sql';
let promptVersions = [];
let activePromptVersion = 'default';
let abTestConfig = {enabled:false, version_a:'default', version_b:''};
let sqlSourceData = [];
let docSourceData = [];
let lineageSourceData = [];
const pageCache = new Map();
const tabCache = new Map();

const SUGGESTED = [
  '上个月各地区销售总额排名？',
  '今天的GMV、订单量、下单人数？',
  'PLUS会员和普通会员消费对比？',
  '近7天销量最高的商品Top10？',
  '昨天各支付渠道的占比？',
];

function toast(msg, type='success'){
  const el = document.createElement('div');
  el.className = `toast ${type}`;
  el.textContent = msg;
  document.body.appendChild(el);
  setTimeout(()=>el.remove(), 3000);
}

async function apiFetch(path, opts={}){
  const res = await fetch(API + path, {
    headers:{'Content-Type':'application/json'},
    ...opts
  });
  if(!res.ok) throw new Error(await res.text());
  return res.json();
}

function fmt(n){ return n != null ? Number(n).toLocaleString() : '—'; }
function esc(s){ return String(s??'').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

async function loadHtmlFragment(path, cache){
  if(cache.has(path)) return cache.get(path);
  const res = await fetch(path, {cache: 'no-store'});
  if(!res.ok) throw new Error(`加载页面失败: ${path}`);
  const html = await res.text();
  cache.set(path, html);
  return html;
}

function ensurePageContainer(){
  return document.getElementById('page-container');
}

function renderSuggestedQuestions(){
  const sqEl = document.getElementById('suggested-qs');
  const qEl = document.getElementById('question');
  if(!sqEl || !qEl) return;
  sqEl.innerHTML = '<span style="font-size:12px;color:var(--text3);margin-right:4px">示例：</span>';
  SUGGESTED.forEach(q=>{
    const chip = document.createElement('span');
    chip.className = 'sq-chip';
    chip.textContent = q;
    chip.onclick = ()=>{ qEl.value = q; doAsk(); };
    sqEl.appendChild(chip);
  });
  if(currentQuestion) qEl.value = currentQuestion;
}

function activateNav(name){
  document.querySelectorAll('.nav-btn').forEach(b=>b.classList.remove('active'));
  document.getElementById('nav-'+name)?.classList.add('active');
}

async function afterPageLoad(name){
  Prism.highlightAll();
  if(name === 'query'){
    renderSuggestedQuestions();
  }
  if(name === 'manage'){
    await showTab(currentManageTab);
  }
  if(name === 'log'){
    loadLogs();
  }
  if(name === 'config'){
    loadConfig();
  }
}

/* ── 导航 ──────────────────────────────────────────────────────────────── */
async function showPage(name){
  currentPage = name;
  if(currentSSE && name !== 'query'){
    currentSSE.close();
    currentSSE = null;
  }
  activateNav(name);
  const container = ensurePageContainer();
  const html = await loadHtmlFragment(`/ui/pages/${name}.html`, pageCache);
  container.innerHTML = html;
  container.querySelector('.page')?.classList.add('active');
  await afterPageLoad(name);
}

async function showTab(name){
  currentManageTab = name;
  if(currentPage !== 'manage'){
    await showPage('manage');
    return;
  }
  document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));
  document.getElementById('tab-'+name)?.classList.add('active');
  const container = document.getElementById('manage-tab-container');
  if(!container) return;
  const html = await loadHtmlFragment(`/ui/pages/manage/${name}.html`, tabCache);
  container.innerHTML = html;
  container.querySelector('.tab-pane')?.classList.add('active');
  Prism.highlightAll();
  if(name==='sql') loadSqlSources();
  if(name==='doc') loadDocSources();
  if(name==='meta') loadMetaTables();
  if(name==='lineage') loadLineageSources();
  if(name==='prompt') loadPromptLab();
  if(name==='semantic') loadSemanticList();
}

/* ═══════════════════════════════════════════════════════════════════════════
   🧬 语义配置
═══════════════════════════════════════════════════════════════════════════ */
function toggleCard(bodyId){
  const el = document.getElementById(bodyId);
  if(el) el.style.display = el.style.display === 'none' ? '' : 'none';
}

async function loadSemanticList(){
  const type = document.getElementById('sem-type-filter').value;
  const tbody = document.getElementById('sem-tbody');
  tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;color:var(--text3)">加载中…</td></tr>';
  try {
    const data = await apiFetch('/semantic/catalog');
    const typeMap = { metric:'metrics', dimension:'dimensions', business:'business_domains', entity:'entities' };
    const items = data[typeMap[type]] || [];
    document.getElementById('sem-count').textContent = `共 ${items.length} 条`;
    if(!items.length){
      tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;color:var(--text3)">暂无数据</td></tr>';
      return;
    }
    tbody.innerHTML = items.map(it => {
      const expr = it.expression || it.dim_type || it.entity_type || '';
      const desc = it.description || it.label || '';
      return `<tr>
        <td><code>${esc(it.name)}</code></td>
        <td>${esc(desc)}</td>
        <td style="font-family:var(--mono);font-size:11px;max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(expr)}</td>
        <td><button class="btn btn-sm" style="background:var(--red);color:#fff"
          onclick="deleteSemanticNode('${esc(type)}','${esc(it.name)}')">🗑️ 删除</button></td>
      </tr>`;
    }).join('');
  } catch(e){
    tbody.innerHTML = `<tr><td colspan="4" style="color:var(--red)">${esc(e.message)}</td></tr>`;
  }
}

async function deleteSemanticNode(type, name){
  if(!confirm(`确认删除 ${type}：${name}？`)) return;
  try {
    await apiFetch(`/semantic/node/${type}/${encodeURIComponent(name)}`, {method:'DELETE'});
    toast(`已删除 ${name}`);
    loadSemanticList();
  } catch(e){ toast(e.message, 'error'); }
}

async function upsertMetric(){
  const payload = {
    name: document.getElementById('m-name').value.trim(),
    label: document.getElementById('m-label').value.trim(),
    primary_table: document.getElementById('m-table').value.trim(),
    primary_alias: document.getElementById('m-alias').value.trim(),
    metric_type: document.getElementById('m-type').value,
    expression: document.getElementById('m-expr').value.trim(),
    time_column: document.getElementById('m-time-col').value.trim(),
    synonyms: document.getElementById('m-synonyms').value.split(',').map(s=>s.trim()).filter(Boolean),
  };
  if(!payload.name){ toast('名称不能为空','error'); return; }
  const msg = document.getElementById('m-msg');
  msg.textContent = '保存中…';
  try {
    await apiFetch('/semantic/metric', {method:'PUT', body: JSON.stringify(payload)});
    msg.textContent = '✅ 保存成功';
    toast('指标已保存');
    loadSemanticList();
  } catch(e){ msg.textContent = '❌ '+e.message; toast(e.message,'error'); }
}

async function upsertDimension(){
  const payload = {
    name: document.getElementById('d-name').value.trim(),
    label: document.getElementById('d-label').value.trim(),
    dim_type: document.getElementById('d-type').value,
    time_grain: document.getElementById('d-grain').value || null,
    expression: document.getElementById('d-expr').value.trim(),
    synonyms: document.getElementById('d-synonyms').value.split(',').map(s=>s.trim()).filter(Boolean),
  };
  if(!payload.name){ toast('名称不能为空','error'); return; }
  const msg = document.getElementById('d-msg');
  msg.textContent = '保存中…';
  try {
    await apiFetch('/semantic/dimension', {method:'PUT', body: JSON.stringify(payload)});
    msg.textContent = '✅ 保存成功';
    toast('维度已保存');
    loadSemanticList();
  } catch(e){ msg.textContent = '❌ '+e.message; toast(e.message,'error'); }
}

/* ═══════════════════════════════════════════════════════════════════════════
   🤖 自动扫描
═══════════════════════════════════════════════════════════════════════════ */
let scanProposals = [];

async function doScan(){
  const btn = document.getElementById('btn-scan');
  const status = document.getElementById('scan-status');
  btn.disabled = true;
  btn.textContent = '⏳ 扫描中…';
  status.textContent = '正在读取 information_schema 和 audit_log…';
  document.getElementById('scan-empty').style.display = '';
  document.getElementById('scan-result-wrap').style.display = 'none';

  const tablesRaw = document.getElementById('scan-tables').value.trim();
  const payload = {
    audit_limit: parseInt(document.getElementById('scan-audit-limit').value) || 2000,
    min_confidence: parseFloat(document.getElementById('scan-min-conf').value) || 0.5,
    include_tables: tablesRaw ? tablesRaw.split(',').map(s=>s.trim()).filter(Boolean) : [],
    apply_to_db: false,
  };

  try {
    const result = await apiFetch('/semantic/scan', {method:'POST', body: JSON.stringify(payload)});
    scanProposals = result.proposals || [];
    status.textContent = `扫描完成：共 ${scanProposals.length} 条建议，覆盖 ${result.tables_scanned||0} 张表`;
    renderScanProposals();
    document.getElementById('scan-result-wrap').style.display = '';
    document.getElementById('scan-empty').style.display = 'none';
  } catch(e){
    status.textContent = '❌ '+e.message;
    toast(e.message,'error');
  } finally {
    btn.disabled = false;
    btn.textContent = '🚀 开始扫描';
  }
}

function renderScanProposals(){
  const groups = { entity:[], dimension:[], metric:[] };
  for(const p of scanProposals){
    const g = p.node_type in groups ? p.node_type : 'metric';
    groups[g].push(p);
  }
  document.getElementById('scan-result-count').textContent =
    `实体 ${groups.entity.length} · 维度 ${groups.dimension.length} · 指标 ${groups.metric.length}`;

  for(const [type, list] of Object.entries(groups)){
    const container = document.getElementById(`scan-proposals-${type}-list`);
    const wrap = document.getElementById(`scan-proposals-${type}`);
    wrap.style.display = list.length ? '' : 'none';
    container.innerHTML = list.map((p,i) => {
      const idx = scanProposals.indexOf(p);
      const conf = Math.round((p.confidence||0)*100);
      const confColor = conf>=80 ? 'var(--green)' : conf>=60 ? 'var(--yellow)' : 'var(--text3)';
      return `<div style="display:flex;align-items:center;gap:10px;padding:6px 0;border-bottom:1px solid var(--border)">
        <input type="checkbox" id="sp-${idx}" checked style="flex-shrink:0">
        <label for="sp-${idx}" style="flex:1;cursor:pointer">
          <code style="font-size:12px">${esc(p.name)}</code>
          <span style="font-size:11px;color:var(--text3);margin-left:8px">${esc(p.description||'')}</span>
        </label>
        <span style="font-size:11px;color:${confColor};flex-shrink:0">置信度 ${conf}%</span>
      </div>`;
    }).join('');
  }
}

function selectAllProposals(checked){
  document.querySelectorAll('[id^="sp-"]').forEach(cb => cb.checked = checked);
}

async function applyScanProposals(){
  const selected = scanProposals.filter((_,i) => {
    const cb = document.getElementById(`sp-${i}`);
    return cb && cb.checked;
  });
  if(!selected.length){ toast('请选择至少一条建议','error'); return; }
  try {
    const result = await apiFetch('/semantic/scan/apply', {
      method:'POST',
      body: JSON.stringify({proposals: selected})
    });
    toast(`写入成功：共 ${result.applied||0} 条语义定义已写入 DB`);
    loadSemanticList();
  } catch(e){ toast(e.message,'error'); }
}

/* ═══════════════════════════════════════════════════════════════════════════
   📤 同步管理
═══════════════════════════════════════════════════════════════════════════ */
async function exportYamlToEditor(){
  const msg = document.getElementById('sync-export-msg');
  msg.textContent = '导出中…';
  try {
    const res = await fetch(API + '/semantic/export');
    if(!res.ok) throw new Error(await res.text());
    const text = await res.text();
    document.getElementById('yaml-editor').value = text;
    msg.textContent = `✅ 已加载（${text.split('\n').length} 行）`;
  } catch(e){ msg.textContent = '❌ '+e.message; toast(e.message,'error'); }
}

async function downloadYaml(){
  const a = document.createElement('a');
  a.href = API + '/semantic/export';
  a.download = 'semantic_catalog.yaml';
  a.click();
}

async function importYamlFromEditor(saveFile){
  const yaml = document.getElementById('yaml-editor').value.trim();
  if(!yaml){ toast('编辑器为空','error'); return; }
  const msg = document.getElementById('sync-import-msg');
  msg.textContent = '导入中…';
  try {
    const result = await apiFetch('/semantic/import', {
      method:'POST',
      body: JSON.stringify({yaml_content: yaml, save_file: saveFile})
    });
    const s = result.stats || {};
    msg.textContent = `✅ ${result.message||'导入完成'} | 指标 ${s.metrics||0}，维度 ${s.dimensions||0}，实体 ${s.entities||0}，业务域 ${s.businesses||0}`;
    toast('YAML 导入成功');
    loadSemanticList();
  } catch(e){ msg.textContent = '❌ '+e.message; toast(e.message,'error'); }
}

async function reloadYamlFile(){
  const msg = document.getElementById('sync-reload-msg');
  msg.textContent = '重新加载中…';
  try {
    const result = await apiFetch('/semantic/reload', {method:'POST'});
    msg.textContent = `✅ ${result.message||'重载完成'}`;
    toast('文件重载成功');
    loadSemanticList();
  } catch(e){ msg.textContent = '❌ '+e.message; toast(e.message,'error'); }
}

function setAskEngine(engine){
  currentAskEngine = engine;
  document.getElementById('engine-vanna').classList.toggle('active', engine==='vanna');
  document.getElementById('engine-langchain').classList.toggle('active', engine==='langchain');
  document.getElementById('engine-semantic').classList.toggle('active', engine==='semantic');
}

/* ════════════════════════════════════════════════════════════════════════
   问数验证 — SSE 调用链
════════════════════════════════════════════════════════════════════════ */
function doAsk(){
  const q = document.getElementById('question').value.trim();
  if(!q){ toast('请输入问题','error'); return; }
  currentQuestion = q;
  currentSQL = '';

  // Reset UI
  resetChain();
  document.getElementById('sql-card').style.display='none';
  document.getElementById('exec-result').style.display='none';
  document.getElementById('result-empty').style.display='block';
  document.getElementById('result-empty').innerHTML='<div class="empty-icon">⏳</div><div class="empty-text">正在推理…</div>';
  document.getElementById('btn-exec').disabled=true;
  document.getElementById('btn-exec2').disabled=true;
  document.getElementById('trace-id-display').textContent='';
  document.getElementById('trace-time-display').textContent='';

  // Close previous SSE
  if(currentSSE){ currentSSE.close(); currentSSE=null; }

  let streamPath;
  if(currentAskEngine === 'langchain') streamPath = '/ask-lc/stream';
  else if(currentAskEngine === 'semantic') streamPath = '/ask/semantic/stream';
  else streamPath = '/ask/stream';
  const promptVersion = currentAskEngine === 'langchain' ? getSelectedPromptVersion() : '';
  const promptQuery = promptVersion ? `&prompt_version=${encodeURIComponent(promptVersion)}` : '';
  const url = API + streamPath + '?q=' + encodeURIComponent(q) + promptQuery;
  currentSSE = new EventSource(url);

  currentSSE.addEventListener('start', e=>{
    const d = JSON.parse(e.data);
    document.getElementById('trace-id-display').textContent = 'ID: '+d.trace_id;
  });

  currentSSE.addEventListener('step_start', e=>{
    const d = JSON.parse(e.data);
    addStepCard(d.name, d.label||d.name, 'running');
  });

  currentSSE.addEventListener('step_done', e=>{
    const d = JSON.parse(e.data);
    updateStepCard(d);
  });

  currentSSE.addEventListener('final', e=>{
    const d = JSON.parse(e.data);
    const trace = d.trace || {};
    document.getElementById('trace-time-display').textContent =
      `⏱ ${trace.total_ms?.toFixed(0)||0}ms`;
    if(d.sql){
      currentSQL = d.sql;
      showSQL(d.sql);
    } else if(d.error){
      showError(d.error);
    }
    currentSSE.close(); currentSSE=null;
  });

  currentSSE.addEventListener('error', e=>{
    console.warn('SSE error', e);
    if(currentSSE) currentSSE.close();
  });
}

function resetChain(){
  document.getElementById('chain-body').innerHTML =
    '<div style="padding:20px;text-align:center;color:var(--text3);font-size:12px">⏳ 推理中…</div>';
}

const STEP_LABELS = {
  generate_embedding: '① 生成 Embedding 向量',
  vector_search_sql:  '② 向量检索相似 SQL',
  vector_search_ddl:  '③ 向量检索相关 DDL',
  build_prompt:       '④ 组装 Prompt',
  llm_generate:       '⑤ LLM 推理生成 SQL',
  extract_sql:        '⑥ 提取 SQL',
  router_intent:      '① 意图解析与标准化',
  multi_recall:       '② 多路召回与融合',
  sql_guard:          '⑤ SQL Guard / EXPLAIN',
};

function addStepCard(name, label, status){
  const container = document.getElementById('chain-body');
  if(container.querySelector('.empty-state')||container.childElementCount===1&&container.firstChild.tagName==='DIV'&&!container.firstChild.className.includes('step-card')){
    container.innerHTML='';
  }
  const displayLabel = STEP_LABELS[name] || label;
  const card = document.createElement('div');
  card.className = `step-card ${status}`;
  card.id = `step-${name}`;
  card.innerHTML = `
    <div class="step-header" onclick="toggleStepDetail('${name}')">
      <div class="step-icon ${status}">${status==='running'?'↻':'…'}</div>
      <span class="step-name">${esc(displayLabel)}</span>
      <span class="step-dur" id="dur-${name}"></span>
    </div>
    <div class="step-detail" id="detail-${name}"></div>
  `;
  container.appendChild(card);
  container.scrollTop = container.scrollHeight;
}

function updateStepCard(step){
  const name = step.name;
  let card = document.getElementById('step-'+name);
  if(!card){ addStepCard(name, STEP_LABELS[name]||name, step.status); card=document.getElementById('step-'+name); }

  card.className = `step-card ${step.status}`;
  const iconEl = card.querySelector('.step-icon');
  iconEl.className = `step-icon ${step.status}`;
  const icons = {ok:'✓', error:'✗', cached:'○', running:'↻'};
  iconEl.textContent = icons[step.status]||'?';

  const durEl = document.getElementById('dur-'+name);
  if(durEl && step.duration_ms!=null) durEl.textContent = step.duration_ms.toFixed(0)+'ms';

  // Badge (note)
  const header = card.querySelector('.step-header');
  const existBadge = header.querySelector('.step-badge');
  if(existBadge) existBadge.remove();
  if(step.note){
    const badge = document.createElement('span');
    const cls = step.note==='CACHED'?'cached':step.note.includes('gemini')?'model':'fresh';
    badge.className = `step-badge ${cls}`;
    badge.textContent = step.note;
    header.insertBefore(badge, durEl);
  }

  // Detail
  const detail = document.getElementById('detail-'+name);
  if(detail && step.outputs){
    let html = '';
    const out = step.outputs;
    const formatters = {
      generate_embedding: ()=>`
        <div class="detail-row"><span class="detail-label">向量维度</span><span class="detail-val highlight">${out.dims}</span></div>`,
      vector_search_sql: ()=>`
        <div class="detail-row"><span class="detail-label">找到</span><span class="detail-val highlight">${out.found} 条</span></div>
        ${(out.top_questions||[]).map((q,i)=>`
          <div class="detail-row"><span class="detail-label">Top${i+1}</span>
          <span class="detail-val">${esc(q)} <span style="color:var(--text3)">(${out.top_scores?.[i]??''})</span></span></div>`).join('')}`,
      vector_search_ddl: ()=>`
        <div class="detail-row"><span class="detail-label">找到</span><span class="detail-val highlight">${out.found} 张表</span></div>
        ${(out.tables||[]).map(t=>`<div class="detail-row"><span class="detail-label"></span><span class="detail-val">${esc(t)}</span></div>`).join('')}`,
      router_intent: ()=>`
        <div class="detail-row"><span class="detail-label">标准化</span><span class="detail-val">${esc(out.normalized_query||'')}</span></div>
        <div class="detail-row"><span class="detail-label">意图</span><span class="detail-val highlight">${esc(out.intent||'')}</span></div>
        <div class="detail-row"><span class="detail-label">实体</span><span class="detail-val">${esc(out.entity||'')}</span></div>`,
      multi_recall: ()=>`
        <div class="detail-row"><span class="detail-label">SQL示例</span><span class="detail-val">${out.sql_example_count||0} 条</span></div>
        <div class="detail-row"><span class="detail-label">DDL</span><span class="detail-val">${out.ddl_count||0} 张</span></div>
        <div class="detail-row"><span class="detail-label">文档</span><span class="detail-val">${out.doc_count||0} 条</span></div>
        <div class="detail-row"><span class="detail-label">audit</span><span class="detail-val">${out.audit_count||0} 条</span></div>
        <div class="detail-row"><span class="detail-label">血缘</span><span class="detail-val">${out.lineage_count||0} 条</span></div>
        <div class="detail-subtitle">Skill 输出</div>
        <div class="detail-block">${esc(JSON.stringify(out.skill_outputs||{}, null, 2))}</div>`,
      build_prompt: ()=>`
        <div class="detail-row"><span class="detail-label">Prompt</span><span class="detail-val highlight">${fmt(out.prompt_len)} 字符</span></div>
        <div class="detail-row"><span class="detail-label">版本</span><span class="detail-val highlight">${esc(out.prompt_name||'Default')} (${esc(out.prompt_version||'default')})</span></div>
        <div class="detail-row"><span class="detail-label">SQL示例</span><span class="detail-val">${out.sim_sql_count} 条</span></div>
        <div class="detail-row"><span class="detail-label">DDL</span><span class="detail-val">${out.ddl_count} 张</span></div>
        <div class="detail-row"><span class="detail-label">文档</span><span class="detail-val">${out.doc_count} 条</span></div>
        <div class="detail-row"><span class="detail-label">血缘</span><span class="detail-val">${out.lineage_count||0} 条</span></div>
        ${(out.question_sql_examples||[]).length?`
          <div class="detail-subtitle">相似 SQL 示例</div>
          ${(out.question_sql_examples||[]).map((item,i)=>`
            <div class="detail-row"><span class="detail-label">Q${i+1}</span><span class="detail-val">${esc(item.question||'')}</span></div>
            <div class="detail-block">${esc(item.sql||'')}</div>
          `).join('')}
        `:''}
        ${(out.ddl_list||[]).length?`
          <div class="detail-subtitle">召回 DDL</div>
          ${(out.ddl_list||[]).map((item,i)=>`
            <div class="detail-row"><span class="detail-label">DDL${i+1}</span></div>
            <div class="detail-block">${esc(item)}</div>
          `).join('')}
        `:''}
        ${(out.doc_list||[]).length?`
          <div class="detail-subtitle">召回文档</div>
          ${(out.doc_list||[]).map((item,i)=>`
            <div class="detail-row"><span class="detail-label">DOC${i+1}</span></div>
            <div class="detail-block">${esc(item)}</div>
          `).join('')}
        `:''}
        ${(out.lineage_list||[]).length?`
          <div class="detail-subtitle">血缘上下文</div>
          ${(out.lineage_list||[]).map((item,i)=>`
            <div class="detail-row"><span class="detail-label">LIN${i+1}</span><span class="detail-val">${esc(item.table_name||'')}</span></div>
            <div class="detail-block">${esc(item.summary||'')}</div>
          `).join('')}
        `:''}
        <div class="detail-subtitle">完整 Prompt</div>
        <div class="detail-block">${esc(out.prompt_full||'')}</div>`,
      llm_generate: ()=>`
        <div class="detail-row"><span class="detail-label">模型</span><span class="detail-val highlight">${esc(out.preview?'':step.note||'')}</span></div>
        <div class="detail-row"><span class="detail-label">输出</span><span class="detail-val">${fmt(out.response_len)} 字符</span></div>`,
      sql_guard: ()=>`
        <div class="detail-row"><span class="detail-label">校验</span><span class="detail-val highlight">${out.ok?'通过':'失败'}</span></div>
        <div class="detail-row"><span class="detail-label">结果</span><span class="detail-val">${esc(out.reason||'')}</span></div>`,
      extract_sql: ()=>`
        <div class="detail-row"><span class="detail-label">提取</span><span class="detail-val highlight">${out.sql?'成功':'失败'}</span></div>`,
    };
    html = (formatters[name]||(() => JSON.stringify(out,null,2).split('\n').map(l=>`<div class="detail-row"><span class="detail-val">${esc(l)}</span></div>`).join('')))();
    if(step.error) html += `<div class="detail-row"><span class="detail-label" style="color:var(--red)">错误</span><span class="detail-val" style="color:var(--red)">${esc(step.error)}</span></div>`;
    detail.innerHTML = html;

    // auto-expand errors
    if(step.status==='error') detail.classList.add('open');
  }
}

function toggleStepDetail(name){
  const d = document.getElementById('detail-'+name);
  if(d) d.classList.toggle('open');
}

function showSQL(sql){
  currentSQL = sql;
  document.getElementById('result-empty').style.display='none';
  const card = document.getElementById('sql-card');
  card.style.display='block';
  const codeEl = document.getElementById('sql-code');
  codeEl.textContent = sql;
  Prism.highlightElement(codeEl);
  document.getElementById('correct-form').style.display='none';
  document.getElementById('corrected-sql').value = sql;
  document.getElementById('btn-exec').disabled=false;
  document.getElementById('btn-exec2').disabled=false;
}

function showError(msg){
  document.getElementById('result-empty').style.display='block';
  document.getElementById('result-empty').innerHTML = `<div class="empty-icon">❌</div><div class="empty-text" style="color:var(--red)">${esc(msg)}</div>`;
}

function copySQL(){
  navigator.clipboard.writeText(currentSQL).then(()=>toast('已复制到剪贴板'));
}

function showCorrectForm(){
  const f = document.getElementById('correct-form');
  f.style.display = f.style.display==='none'?'block':'none';
}

async function doExec(){
  if(!currentSQL){ toast('还没有 SQL','error'); return; }
  document.getElementById('exec-result').style.display='block';
  document.getElementById('result-tbody').innerHTML=
    '<tr><td colspan="99" style="text-align:center;padding:20px;color:var(--text3)"><span class="spinner"></span> 执行中…</td></tr>';
  try{
    const data = await apiFetch('/execute',{method:'POST',body:JSON.stringify({sql:currentSQL})});
    renderTable(data);
    document.getElementById('exec-stats').textContent = `返回 ${data.total} 行（最多显示 200）`;
  }catch(e){ toast('执行失败: '+e.message,'error'); }
}

function renderTable(data){
  const thead = document.getElementById('result-thead');
  const tbody = document.getElementById('result-tbody');
  thead.innerHTML = '<tr>'+data.columns.map(c=>`<th>${esc(c)}</th>`).join('')+'</tr>';
  tbody.innerHTML = data.rows.map(row=>
    '<tr>'+row.map(v=>`<td title="${esc(v)}">${esc(v)}</td>`).join('')+'</tr>'
  ).join('');
}

async function sendFeedback(isCorrect){
  const corrected = isCorrect ? null : document.getElementById('corrected-sql').value;
  try{
    await apiFetch('/feedback',{method:'POST',body:JSON.stringify({
      question:currentQuestion, sql:currentSQL,
      is_correct:isCorrect, corrected_sql:corrected
    })});
    toast(isCorrect?'👍 已加入知识库！':'✏️ 修正版已保存！');
    if(!isCorrect) document.getElementById('correct-form').style.display='none';
  }catch(e){ toast('反馈失败: '+e.message,'error'); }
}

/* ════════════════════════════════════════════════════════════════════════
   训练数据管理
════════════════════════════════════════════════════════════════════════ */
async function loadSqlSources(){
  try{
    const res = await apiFetch('/sources/sql?limit=300');
    sqlSourceData = res.data||[];
    document.getElementById('sql-count').textContent = `共 ${sqlSourceData.length} 条`;
    renderSqlTable(sqlSourceData);
  }catch(e){ toast('加载 SQL 失败: '+e.message,'error'); }
}

function filterSqlTable(){
  const q = document.getElementById('sql-filter-search').value.toLowerCase();
  renderSqlTable(sqlSourceData.filter(r =>
    (r.question||'').toLowerCase().includes(q) ||
    (r.content_preview||'').toLowerCase().includes(q)
  ));
}

function renderSqlTable(data){
  document.getElementById('sql-tbody').innerHTML = data.map(r=>`
    <tr>
      <td style="color:var(--text3);font-size:11px">${r.id}</td>
      <td><span class="tag">${r.source||'—'}</span></td>
      <td style="max-width:200px;color:var(--text)">${esc(r.question||'—')}</td>
      <td style="max-width:260px;color:var(--text2);font-family:var(--mono);font-size:11.5px">${esc(r.content_preview||'')}</td>
      <td style="text-align:center">${r.use_count??0}</td>
      <td>${r.quality_score!=null?Number(r.quality_score).toFixed(1):'—'}</td>
      <td><button class="btn btn-sm btn-danger" onclick="deleteTraining('${r.id}')">🗑</button></td>
    </tr>
  `).join('');
}

async function loadDocSources(){
  try{
    const res = await apiFetch('/sources/doc?limit=300');
    docSourceData = res.data||[];
    document.getElementById('doc-count').textContent = `共 ${docSourceData.length} 条`;
    renderDocTable(docSourceData);
  }catch(e){ toast('加载文档失败: '+e.message,'error'); }
}

function filterDocTable(){
  const q = document.getElementById('doc-filter-search').value.toLowerCase();
  renderDocTable(docSourceData.filter(r =>
    (r.question||'').toLowerCase().includes(q) ||
    (r.content_preview||'').toLowerCase().includes(q)
  ));
}

function renderDocTable(data){
  document.getElementById('doc-tbody').innerHTML = data.map(r=>`
    <tr>
      <td style="color:var(--text3);font-size:11px">${r.id}</td>
      <td><span class="tag">${r.source||'—'}</span></td>
      <td style="max-width:180px;color:var(--text)">${esc(r.question||'—')}</td>
      <td style="max-width:320px;color:var(--text2)">${esc(r.content_preview||'')}</td>
      <td style="text-align:center">${r.use_count??0}</td>
      <td>${r.quality_score!=null?Number(r.quality_score).toFixed(1):'—'}</td>
      <td><button class="btn btn-sm btn-danger" onclick="deleteTraining('${r.id}')">🗑</button></td>
    </tr>
  `).join('');
}

async function loadLineageSources(){
  try{
    const res = await apiFetch('/sources/lineage?limit=300');
    lineageSourceData = res.data || [];
    document.getElementById('lineage-count').textContent = `共 ${lineageSourceData.length} 条`;
    document.getElementById('lineage-tbody').innerHTML = lineageSourceData.map(r=>`
      <tr>
        <td><code>${esc(r.source_table||'')}</code></td>
        <td><code>${esc(r.target_table||'')}</code></td>
        <td>${esc(r.relation_type||'')}</td>
        <td>${esc(r.sql_type||'')}</td>
        <td>${esc(r.source||'')}</td>
        <td>${fmt(r.freq)}</td>
      </tr>
    `).join('');
  }catch(e){ toast('加载血缘失败: '+e.message,'error'); }
}

async function rebuildLineage(){
  const btn = event.target;
  btn.disabled = true;
  btn.textContent = '⏳ 重建中…';
  try{
    const res = await apiFetch('/lineage/rebuild', {method:'POST'});
    const box = document.getElementById('lineage-rebuild-result');
    box.style.display = 'block';
    const diag = res.diagnosis || {};
    box.innerHTML = `
      <div style="font-weight:700;color:var(--text);margin-bottom:6px">血缘重建结果</div>
      <div>audit_log 取回：${fmt(res.audit_fetched || 0)} 条</div>
      <div>audit_log 解析 edge：${fmt(res.audit_edges || 0)} 条</div>
      <div>知识库补充 edge：${fmt(res.knowledge_edges || 0)} 条</div>
      <div>最终写入 vanna_lineage：${fmt(res.inserted || 0)} 条</div>
      <div style="margin-top:8px;color:var(--text3)">诊断：带 FROM 的 SQL ${fmt(diag.has_from || 0)} 条，纯 VALUES/无 FROM ${fmt(diag.values_only || 0)} 条，诊断解析 edge ${fmt(diag.parsed_edges || 0)} 条</div>
    `;
    console.log('lineage rebuild', res);
    toast(`血缘重建完成：${res.inserted||0} 条`);
    loadLineageSources();
  }catch(e){ toast('重建血缘失败: '+e.message,'error'); }
  finally{
    btn.disabled = false;
    btn.textContent = '🔁 重建血缘';
  }
}

async function deleteTraining(id){
  if(!confirm('确认删除 ID='+id+'？')) return;
  try{
    await apiFetch('/training-data/'+id,{method:'DELETE'});
    toast('已删除');
    loadSqlSources();
    loadDocSources();
    loadMetaTables();
    loadLineageSources();
  }catch(e){ toast('删除失败: '+e.message,'error'); }
}

async function addQA(){
  const q=document.getElementById('add-q').value.trim();
  const sql=document.getElementById('add-sql').value.trim();
  if(!q||!sql){toast('问题和SQL不能为空','error');return;}
  try{
    await apiFetch('/training-data/sql',{method:'POST',body:JSON.stringify({question:q,sql,source:'manual'})});
    toast('Q&A 已添加！');
    document.getElementById('add-q').value='';
    document.getElementById('add-sql').value='';
    loadSqlSources();
  }catch(e){toast('添加失败: '+e.message,'error');}
}

async function addDDL(){
  const ddl=document.getElementById('add-ddl').value.trim();
  if(!ddl){toast('DDL不能为空','error');return;}
  try{
    await apiFetch('/training-data/ddl',{method:'POST',body:JSON.stringify({ddl,source:'manual'})});
    toast('DDL 已添加！');
    document.getElementById('add-ddl').value='';
    loadMetaTables();
  }catch(e){toast('添加失败: '+e.message,'error');}
}

async function addDoc(){
  const doc=document.getElementById('add-doc').value.trim();
  if(!doc){toast('文档不能为空','error');return;}
  try{
    await apiFetch('/training-data/doc',{method:'POST',body:JSON.stringify({documentation:doc,source:'manual'})});
    toast('文档已添加！');
    document.getElementById('add-doc').value='';
    loadDocSources();
  }catch(e){toast('添加失败: '+e.message,'error');}
}

function getSelectedPromptVersion(){
  const id = document.getElementById('prompt-version-id')?.value?.trim();
  return id || activePromptVersion || 'default';
}

function renderPromptVersionOptions(){
  const opts = promptVersions.map(v=>`<option value="${esc(v.id)}">${esc(v.name)} (${esc(v.id)})</option>`).join('');
  const aEl = document.getElementById('ab-version-a');
  const bEl = document.getElementById('ab-version-b');
  if(aEl) aEl.innerHTML = opts;
  if(bEl) bEl.innerHTML = `<option value="">不启用 B 版本</option>${opts}`;
  if(aEl) aEl.value = abTestConfig.version_a || activePromptVersion || 'default';
  if(bEl) bEl.value = abTestConfig.version_b || '';
  const badge = document.getElementById('active-prompt-badge');
  if(badge) badge.textContent = `ACTIVE · ${activePromptVersion}`;
}

function fillPromptVersionForm(versionId){
  const current = promptVersions.find(v=>v.id===versionId) || promptVersions[0];
  if(!current) return;
  document.getElementById('prompt-version-id').value = current.id || '';
  document.getElementById('prompt-version-name').value = current.name || '';
  document.getElementById('prompt-version-desc').value = current.description || '';
  document.getElementById('prompt-version-text').value = current.system_prompt || '';

  const list = document.getElementById('prompt-version-list');
  if(list){
    list.querySelectorAll('.version-item').forEach(el=>{
      el.classList.toggle('active', el.dataset.versionId===current.id);
    });
  }
}

function renderPromptVersionList(){
  const list = document.getElementById('prompt-version-list');
  if(!list) return;
  list.innerHTML = promptVersions.map(v=>`
    <div class="version-item ${v.id===activePromptVersion?'active':''}" data-version-id="${esc(v.id)}" onclick="fillPromptVersionForm('${esc(v.id)}')">
      <div class="version-item-top">
        <span class="version-title">${esc(v.name||v.id)}</span>
        <span class="tag">${esc(v.id)}</span>
        ${v.id===activePromptVersion?'<span class="tag" style="color:var(--green);border-color:#23863655">激活中</span>':''}
      </div>
      <div class="version-desc">${esc(v.description||'暂无说明')}</div>
    </div>
  `).join('') || '<div class="form-hint">还没有 Prompt 版本，请先创建。</div>';
}

async function loadPromptLab(){
  const msg = document.getElementById('prompt-lab-msg');
  const abMsg = document.getElementById('ab-test-msg');
  try{
    const res = await apiFetch('/prompt-versions');
    promptVersions = res.prompt_versions || [];
    activePromptVersion = res.active_prompt_version || 'default';
    abTestConfig = res.ab_test || {enabled:false, version_a:activePromptVersion, version_b:''};
    renderPromptVersionList();
    renderPromptVersionOptions();
    fillPromptVersionForm(activePromptVersion);
    document.getElementById('ab-enabled').checked = !!abTestConfig.enabled;
    msg.textContent = '已加载';
    abMsg.textContent = abTestConfig.enabled ? '当前为展示型 A/B 配置' : '';
  }catch(e){
    msg.textContent = '';
    toast('加载 Prompt Lab 失败: '+e.message,'error');
  }
}

async function savePromptVersion(){
  const msg = document.getElementById('prompt-lab-msg');
  const body = {
    id: document.getElementById('prompt-version-id').value.trim(),
    name: document.getElementById('prompt-version-name').value.trim(),
    description: document.getElementById('prompt-version-desc').value.trim(),
    system_prompt: document.getElementById('prompt-version-text').value,
  };
  if(!body.id){ toast('版本 ID 不能为空','error'); return; }
  try{
    const res = await apiFetch('/prompt-versions/save',{
      method:'POST',
      body:JSON.stringify(body)
    });
    msg.innerHTML = `<span style="color:var(--green)">${esc(res.message)}</span>`;
    await loadPromptLab();
    fillPromptVersionForm(body.id);
    toast('Prompt 版本已保存');
  }catch(e){
    msg.textContent = '';
    toast('保存 Prompt 版本失败: '+e.message,'error');
  }
}

async function activatePromptVersion(){
  const msg = document.getElementById('prompt-lab-msg');
  const version_id = getSelectedPromptVersion();
  try{
    const res = await apiFetch('/prompt-versions/activate',{
      method:'POST',
      body:JSON.stringify({version_id})
    });
    msg.innerHTML = `<span style="color:var(--green)">${esc(res.message)}</span>`;
    await loadPromptLab();
    fillPromptVersionForm(version_id);
    toast('已切换激活版本');
  }catch(e){
    msg.textContent = '';
    toast('激活失败: '+e.message,'error');
  }
}

async function saveABTest(){
  const msg = document.getElementById('ab-test-msg');
  const body = {
    enabled: document.getElementById('ab-enabled').checked,
    version_a: document.getElementById('ab-version-a').value,
    version_b: document.getElementById('ab-version-b').value,
  };
  try{
    const res = await apiFetch('/prompt-versions/ab-test',{
      method:'POST',
      body:JSON.stringify(body)
    });
    abTestConfig = res.ab_test || body;
    msg.innerHTML = `<span style="color:var(--green)">${esc(res.message)}</span>`;
    toast('A/B 配置已保存');
  }catch(e){
    msg.textContent = '';
    toast('保存 A/B 配置失败: '+e.message,'error');
  }
}

/* ── 元数据 ──────────────────────────────────────────────────────────────── */
let metaTables=[];

async function loadMetaTables(){
  try{
    const res=await apiFetch('/metadata/tables');
    metaTables=res.tables||[];
    renderMetaTables(metaTables);
  }catch(e){toast('加载元数据失败: '+e.message,'error');}
}

function filterMeta(){
  const q=document.getElementById('meta-search').value.toLowerCase();
  renderMetaTables(metaTables.filter(t=>(t.table_name||'').toLowerCase().includes(q)||(t.comment||'').toLowerCase().includes(q)));
}

function renderMetaTables(tables){
  document.getElementById('meta-container').innerHTML = `
    <div class="result-table-wrap">
      <div class="table-scroll" style="max-height:calc(100vh - 300px)">
        <table>
          <thead><tr><th>表名</th><th>注释</th><th>引擎</th><th>预估行数</th><th>字段数</th><th>操作</th></tr></thead>
          <tbody>${tables.map(t=>`
            <tr>
              <td><code>${esc(t.table_name)}</code></td>
              <td>${esc(t.comment||'')}</td>
              <td><span class="tag">${t.engine||''}</span></td>
              <td>${fmt(t.rows)}</td>
              <td>${t.col_count||'—'}</td>
              <td><button class="btn btn-sm btn-secondary" onclick="showTableDetail('${esc(t.table_name)}')">详情</button></td>
            </tr>`).join('')}
          </tbody>
        </table>
      </div>
    </div>`;
}

async function showTableDetail(name){
  try{
    const m=await apiFetch('/metadata/tables/'+encodeURIComponent(name));
    const cols=m.columns||[];
    const html=`
      <div class="card" style="margin-top:12px">
        <div class="card-header">
          <span class="card-title">${esc(m.table_name)}</span>
          <span style="color:var(--text3);font-size:12px;margin-left:8px">${esc(m.comment)}</span>
        </div>
        <div class="card-body">
          <details style="margin-bottom:12px">
            <summary style="cursor:pointer;color:var(--blue);font-size:13px">📐 DDL 预览</summary>
            <pre style="background:var(--bg);padding:12px;border-radius:6px;font-size:12px;overflow:auto;margin-top:8px"><code class="language-sql">${esc(m.ddl)}</code></pre>
          </details>
          <table style="width:100%">
            <thead><tr><th>字段名</th><th>类型</th><th>可空</th><th>注释</th></tr></thead>
            <tbody>${cols.map(c=>`
              <tr>
                <td><code>${esc(c.name)}</code></td>
                <td><span class="tag">${esc(c.type)}</span></td>
                <td style="color:${c.nullable==='YES'?'var(--text3)':'var(--green)'}">${c.nullable}</td>
                <td style="color:var(--text2)">${esc(c.comment||'')}</td>
              </tr>`).join('')}
            </tbody>
          </table>
        </div>
      </div>`;
    const existing=document.getElementById('meta-detail-card');
    if(existing) existing.remove();
    const div=document.createElement('div');
    div.id='meta-detail-card';
    div.innerHTML=html;
    document.getElementById('meta-container').appendChild(div);
    Prism.highlightAll();
    div.scrollIntoView({behavior:'smooth'});
  }catch(e){toast('获取表详情失败: '+e.message,'error');}
}

async function syncMeta(){
  const btn=event.target;
  btn.disabled=true; btn.textContent='⏳ 同步中…';
  try{
    const res=await apiFetch('/metadata/sync',{method:'POST'});
    toast(`✅ 同步完成，共 ${res.tables_synced} 张表`);
    loadMetaTables();
  }catch(e){toast('同步失败: '+e.message,'error');}
  finally{btn.disabled=false;btn.textContent='🔄 同步元数据到知识库';}
}

/* ── 挖掘 ──────────────────────────────────────────────────────────────────── */
async function doMine(){
  const btn=document.getElementById('btn-mine');
  const ms=parseInt(document.getElementById('mine-ms').value)||30000;
  const limit=parseInt(document.getElementById('mine-limit').value)||500;
  btn.disabled=true; btn.textContent='⏳ 挖掘中…';
  document.getElementById('mine-result').innerHTML='<span class="spinner"></span> 正在执行，请稍候…';
  try{
    const res=await apiFetch(`/audit/mine?limit=${limit}&max_ms=${ms}`,{method:'POST'});
    document.getElementById('mine-result').innerHTML=`
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:8px">
        ${Object.entries(res).map(([k,v])=>`
          <div style="background:var(--surface2);border-radius:6px;padding:8px 10px;border:1px solid var(--border)">
            <div style="font-size:16px;font-weight:700;color:var(--text)">${v}</div>
            <div style="font-size:11px;color:var(--text3)">${k}</div>
          </div>`).join('')}
      </div>`;
  }catch(e){
    document.getElementById('mine-result').innerHTML=`<span style="color:var(--red)">失败: ${esc(e.message)}</span>`;
  }
  btn.disabled=false; btn.textContent='🚀 开始挖掘';
}

/* ════════════════════════════════════════════════════════════════════════
   调用日志
════════════════════════════════════════════════════════════════════════ */
async function loadLogs(){
  try{
    const res=await apiFetch('/traces?n=100');
    const traces=res.traces||[];
    const stats=res.stats||{};
    document.getElementById('log-stats').textContent =
      `共${stats.total||0}条 · 成功率${stats.success_rate||'—'} · 均${stats.avg_ms||0}ms`;
    renderLogList(traces);
  }catch(e){document.getElementById('log-list').innerHTML=`<div style="padding:16px;color:var(--red)">${esc(e.message)}</div>`;}
}

function renderLogList(traces){
  const listEl=document.getElementById('log-list');
  listEl.innerHTML=traces.map(t=>`
    <div class="log-item" onclick="showLogDetail('${t.trace_id}',this)">
      <div class="log-item-top">
        <span class="log-status ${t.status}">${{ok:'✓ 成功',error:'✗ 失败',running:'⏳ 运行中'}[t.status]||t.status}</span>
        <span class="log-meta">${t.created_at} · ${(t.total_ms||0).toFixed(0)}ms</span>
      </div>
      <div class="log-q">${esc(t.question)}</div>
    </div>
  `).join('') || '<div style="padding:24px;text-align:center;color:var(--text3)">暂无记录</div>';
}

function showLogDetail(traceId, el){
  document.querySelectorAll('.log-item').forEach(i=>i.classList.remove('selected'));
  if(el) el.classList.add('selected');
  apiFetch('/traces/'+traceId).then(t=>{
    const steps=t.steps||[];
    document.getElementById('log-detail').innerHTML=`
      <div style="margin-bottom:16px">
        <div style="display:flex;gap:12px;align-items:center;margin-bottom:8px">
          <span class="log-status ${t.status}">${{ok:'✓ 成功',error:'✗ 失败'}[t.status]||t.status}</span>
          <span style="font-size:12px;color:var(--text3)">Trace: <code>${t.trace_id}</code></span>
          <span style="font-size:12px;color:var(--text3)">${t.created_at}</span>
          <span style="font-size:12px;color:var(--blue)">${(t.total_ms||0).toFixed(0)}ms</span>
          <span style="font-size:12px;color:var(--text2)">${t.model_used||''}</span>
        </div>
        <div style="font-size:14px;font-weight:600;margin-bottom:8px">${esc(t.question)}</div>
        ${t.error?`<div style="color:var(--red);font-size:13px">错误: ${esc(t.error)}</div>`:''}
      </div>
      ${steps.map((s,i)=>`
        <div class="step-card ${s.status}" style="margin-bottom:8px">
          <div class="step-header" style="cursor:default">
            <div class="step-icon ${s.status}">${{ok:'✓',error:'✗',running:'↻',cached:'○'}[s.status]||'?'}</div>
            <span class="step-name">${esc(STEP_LABELS[s.name]||s.name)}</span>
            ${s.note?`<span class="step-badge ${s.note==='CACHED'?'cached':'model'}">${esc(s.note)}</span>`:''}
            <span class="step-dur">${(s.duration_ms||0).toFixed(0)}ms</span>
          </div>
          ${Object.keys(s.outputs||{}).length?`
          <div class="step-detail open">
            ${Object.entries(s.outputs).map(([k,v])=>`
              <div class="detail-row">
                <span class="detail-label">${esc(k)}</span>
                <span class="detail-val">${esc(Array.isArray(v)?v.join(' · '):String(v))}</span>
              </div>`).join('')}
          </div>`:''}
        </div>`).join('')}
      ${t.final_sql?`
        <div style="margin-top:16px">
          <div style="font-size:12px;color:var(--text2);margin-bottom:6px">生成的 SQL</div>
          <pre style="background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:12px;font-size:12px;overflow:auto"><code class="language-sql">${esc(t.final_sql)}</code></pre>
        </div>`:''}
    `;
    Prism.highlightAll();
  }).catch(e=>{ document.getElementById('log-detail').innerHTML=`<div style="color:var(--red)">${esc(e.message)}</div>`; });
}

/* ════════════════════════════════════════════════════════════════════════
   配置
════════════════════════════════════════════════════════════════════════ */
async function loadConfig(){
  try{
    const c=await apiFetch('/config');
    document.getElementById('cfg-host').value=c.host||'';
    document.getElementById('cfg-port').value=c.port||19030;
    document.getElementById('cfg-user').value=c.user||'';
    document.getElementById('cfg-db').value=c.database||'';
    document.getElementById('cfg-key').value='';
    document.getElementById('cfg-key').placeholder=c.qwen_api_key_masked||'sk-...';
    document.getElementById('cfg-model').value=c.model||'qwen-plus';
    document.getElementById('cfg-n').value=c.n_results||5;
    document.getElementById('cfg-lc-fallback').checked=!!c.langchain_fallback_enabled;
    document.getElementById('cfg-semantic-fallback').checked=!!c.semantic_to_langchain_fallback_enabled;
    document.getElementById('db-info').textContent=`${c.database} @ ${c.host}`;
  }catch(e){ console.warn('loadConfig failed',e); }
}

async function saveConfig(){
  const body={
    host:document.getElementById('cfg-host').value,
    port:parseInt(document.getElementById('cfg-port').value),
    user:document.getElementById('cfg-user').value,
    password:document.getElementById('cfg-pwd').value,
    database:document.getElementById('cfg-db').value,
    qwen_api_key:document.getElementById('cfg-key').value,
    model:document.getElementById('cfg-model').value,
    n_results:parseInt(document.getElementById('cfg-n').value),
    langchain_fallback_enabled:document.getElementById('cfg-lc-fallback').checked,
    semantic_to_langchain_fallback_enabled:document.getElementById('cfg-semantic-fallback').checked,
  };
  try{
    const res=await apiFetch('/config',{method:'POST',body:JSON.stringify(body)});
    toast('✅ 配置已保存');
    document.getElementById('cfg-msg').innerHTML=`<span style="color:var(--green)">${res.message}</span>`;
    document.getElementById('db-info').textContent=`${body.database} @ ${body.host}`;
  }catch(e){ toast('保存失败: '+e.message,'error'); }
}

async function testConn(){
  const msg=document.getElementById('cfg-msg');
  msg.innerHTML='<span class="spinner"></span> 测试中…';
  try{
    const res=await apiFetch('/health');
    const ok=res.doris==='ok';
    msg.innerHTML=`<span style="color:${ok?'var(--green)':'var(--red)'}">${ok?'✅ Doris 连接正常':'❌ Doris 连接失败'}</span>
      &nbsp; Qwen缓存:${res.qwen_stats?.cache_size||0}条 命中:${res.qwen_stats?.embed_cache_hit_rate||'N/A'}`;
    document.getElementById('status-dot').className='status-dot '+(ok?'ok':'err');
  }catch(e){
    msg.innerHTML=`<span style="color:var(--red)">❌ 连接异常: ${esc(e.message)}</span>`;
    document.getElementById('status-dot').className='status-dot err';
  }
}

/* ════════════════════════════════════════════════════════════════════════
   初始化
════════════════════════════════════════════════════════════════════════ */
(async()=>{
  // 健康检查
  try{
    const h=await apiFetch('/health');
    document.getElementById('status-dot').className='status-dot '+(h.doris==='ok'?'ok':'err');
  }catch(e){ document.getElementById('status-dot').className='status-dot err'; }

  setAskEngine('semantic');
  await showPage('query');
})();
