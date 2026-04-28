/* ════════════════════════════════════════════════════════════════════════
   全局状态 & 工具
════════════════════════════════════════════════════════════════════════ */
const API = ''; // 同源，留空即可；若跨域填 http://localhost:8765
let currentQuestion = '';
let currentSQL = '';
let currentSSE = null;
let currentAskEngine = 'cube';
let currentPage = 'query';
let currentManageTab = 'sql';
let promptVersions = [];
let activePromptVersion = 'default';
let abTestConfig = {enabled:false, version_a:'default', version_b:''};
let sqlSourceData = [];
let docSourceData = [];
let lineageSourceData = [];
let regressionReports = [];
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

// 页面名称 → 顶栏标题
const PAGE_TITLES = {
  query:    '问数工作台',
  manage:   '指标详情',
  semantic: '语义层管理',
  cube:     'Metric Cube',
  regression: '回归测试',
  log:      '查询审计',
  config:   'Agent 调试台',
  arch:     '系统架构图',
  rca:      '归因分析',
  'smart-rca': '智能归因',
};

function activateNav(name){
  // 清除所有 sidebar nav-item 的 active
  document.querySelectorAll('.nav-item').forEach(b=>b.classList.remove('active'));
  // 激活对应项（nav-semantic 对应 manage 页 semantic tab，共用 nav-semantic id）
  const navId = name === 'semantic' ? 'nav-semantic' : 'nav-' + name;
  document.getElementById(navId)?.classList.add('active');
  // 更新顶栏标题
  const titleEl = document.getElementById('topbar-title');
  if(titleEl) titleEl.textContent = PAGE_TITLES[name] || name;
}

async function afterPageLoad(name){
  Prism.highlightAll();
  if(name === 'query'){
    renderSuggestedQuestions();
  }
  if(name === 'manage'){
    await showTab(currentManageTab);
  }
  if(name === 'cube'){
    await loadCubeAdmin();
  }
  if(name === 'log'){
    loadLogs();
  }
  if(name === 'config'){
    loadConfig();
  }
  if(name === 'regression'){
    initRegressionPage();
  }
  if(name === 'rca'){
    initRcaPage();
  }
  if(name === 'smart-rca'){
    initSmartRcaPage();
  }
}

/* ── 导航 ──────────────────────────────────────────────────────────────── */
async function showPage(name){
  // 「语义层管理」是 manage 页下的 semantic tab，特殊路由
  if(name === 'semantic'){
    activateNav('semantic');
    currentManageTab = 'semantic';
    if(currentPage !== 'manage'){
      currentPage = 'manage';
      const container = ensurePageContainer();
      container.innerHTML = `<div class="page active"><div class="empty-state"><div class="empty-icon">⏳</div><div class="empty-text">页面加载中…</div></div></div>`;
      try{
        const html = await loadHtmlFragment('/ui/pages/manage.html', pageCache);
        container.innerHTML = html;
        container.querySelector('.page')?.classList.add('active');
      }catch(e){ console.error(e); }
    }
    await showTab('semantic');
    return;
  }

  currentPage = name;
  if(currentSSE && !['query','smart-rca'].includes(name)){
    currentSSE.close();
    currentSSE = null;
  }
  activateNav(name);
  const container = ensurePageContainer();
  container.innerHTML = `<div class="page active"><div class="empty-state"><div class="empty-icon">⏳</div><div class="empty-text">页面加载中…</div></div></div>`;
  try{
    const html = await loadHtmlFragment(`/ui/pages/${name}.html`, pageCache);
    container.innerHTML = html;
    container.querySelector('.page')?.classList.add('active');
    await afterPageLoad(name);
  }catch(e){
    console.error('showPage failed', name, e);
    container.innerHTML = `<div class="page active"><div class="empty-state"><div class="empty-icon">⚠️</div><div class="empty-text">页面加载失败：${esc(e.message||e)}</div></div></div>`;
  }
}

async function showTab(name){
  currentManageTab = name;
  if(currentPage !== 'manage'){
    await showPage('manage');
    return;
  }
  if(name === 'semantic') activateNav('semantic');
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
  if(name==='regression') initRegressionPage();
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

async function refreshSemanticCache(){
  const msg = document.getElementById('sem-cache-msg');
  if(msg) msg.textContent = '刷新中…';
  try {
    const result = await apiFetch('/semantic/cache/refresh', {method:'POST'});
    const s = result.stats || {};
    if(msg){
      msg.textContent = `已刷新：指标 ${s.metrics||0} · 维度 ${s.dimensions||0} · 实体 ${s.entities||0} · 业务域 ${s.businesses||0}`;
    }
    toast('语义内存已刷新');
    loadSemanticList();
  } catch(e){
    if(msg) msg.textContent = '刷新失败';
    toast('刷新语义内存失败: '+e.message, 'error');
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

/* ═══════════════════════════════════════════════════════════════════════════
   ◎ Metric Cube 管理
═══════════════════════════════════════════════════════════════════════════ */
const CUBE_ENTITY_LABELS = {
  models: '模型',
  measures: '指标',
  dimensions: '维度',
  'dimension-values': '枚举值',
  joins: '关联关系',
  segments: '业务分段',
  templates: 'SQL 模板',
  versions: '模型版本',
  'validation-results': '校验结果',
  'regression-cases': '回归用例',
  'publish-history': '发布历史',
  'metric-influences': '指标影响',
};
const CUBE_MODULES = [
  {id:'overview', title:'模块首页', desc:'查看 Metric Cube 的治理对象和快捷入口。', icon:'⌁', kind:'overview'},
  {id:'models', title:'模型管理', desc:'维护 Cube 到 Doris 物理表或 SQL 表达式的映射。', icon:'▣', kind:'entity', entity:'models'},
  {id:'measures', title:'指标管理', desc:'维护 GMV、客单价、净收入等业务指标口径。', icon:'Σ', kind:'entity', entity:'measures'},
  {id:'dimensions', title:'维度管理', desc:'维护城市、会员、类目、时间等分析维度。', icon:'◇', kind:'entity', entity:'dimensions'},
  {id:'dimension-values', title:'枚举值管理', desc:'维护枚举值、同义词和自动采集结果，用于自然语言参数识别。', icon:'≋', kind:'entity', entity:'dimension-values'},
  {id:'joins', title:'关系管理', desc:'维护 Cube 之间的 Join 关系，保证多表查询可控。', icon:'↔', kind:'entity', entity:'joins'},
  {id:'segments', title:'业务分段', desc:'维护 PLUS 会员、有效订单等可复用过滤片段。', icon:'◐', kind:'entity', entity:'segments'},
  {id:'templates', title:'SQL 模板', desc:'维护通用 SQL 片段、业务模板和参数定义。', icon:'▤', kind:'entity', entity:'templates'},
  {id:'versions', title:'版本管理', desc:'查看模型版本和激活状态。', icon:'⑂', kind:'entity', entity:'versions'},
  {id:'governance', title:'治理体检', desc:'校验模型完整性、表达式、Join、枚举和 Doris EXPLAIN。', icon:'✓', kind:'governance'},
  {id:'regression-cases', title:'回归用例', desc:'维护智能问数回归问题和期望结果。', icon:'🧪', kind:'entity', entity:'regression-cases'},
  {id:'publish-history', title:'发布历史', desc:'记录发布批次、校验运行和模型 checksum。', icon:'↑', kind:'entity', entity:'publish-history'},
  {id:'metric-influences', title:'指标影响', desc:'维护指标之间的影响关系，为归因分析打底。', icon:'⛓', kind:'entity', entity:'metric-influences'},
];
let cubeEntities = [];
let currentCubeModule = 'overview';
let openCubeMenuGroup = '';
let currentCubeEntity = 'models';
let currentCubeMeta = null;
let currentCubeRows = [];
let currentCubeRow = null;

async function loadCubeAdmin(){
  const navEl = document.getElementById('cube-module-nav');
  if(!navEl) return;
  try{
    const res = await apiFetch('/cube/admin/entities');
    cubeEntities = res.entities || [];
    renderCubeModuleNav();
    await showCubeModule(currentCubeModule || 'overview');
  }catch(e){
    navEl.innerHTML = `<div style="padding:16px;color:var(--red)">${esc(e.message)}</div>`;
  }
}

function renderCubeModuleNav(){
  const navEl = document.getElementById('cube-module-nav');
  if(!navEl) return;
  const entitySet = new Set(cubeEntities.map(e => e.name));
  const groups = [
    {title:'总览', ids:['overview']},
    {title:'模型配置', ids:['models','joins','segments']},
    {title:'指标口径', ids:['measures','dimensions','dimension-values']},
    {title:'模板版本', ids:['templates','versions']},
    {title:'治理闭环', ids:['governance','regression-cases','publish-history','metric-influences']},
  ];
  navEl.innerHTML = groups.map(group => {
    const isOpen = openCubeMenuGroup === group.title;
    const buttons = group.ids.map(id => CUBE_MODULES.find(m => m.id === id)).filter(Boolean).filter(m => !m.entity || entitySet.has(m.entity)).map(m => `
        <button class="cube-module-btn ${m.id===currentCubeModule?'active':''}" onclick="showCubeModule('${m.id}')">
          <span class="cube-module-icon">${esc(m.icon)}</span>
          <span>
            <strong>${esc(m.title)}</strong>
            <small>${esc(m.entity ? CUBE_ENTITY_LABELS[m.entity] || m.entity : m.kind)}</small>
          </span>
        </button>
      `).join('');
    if(!buttons) return '';
    return `
      <div class="cube-module-group ${isOpen ? 'open' : ''}">
        <button class="cube-module-title" onclick="toggleCubeMenuGroup('${esc(group.title)}')">
          <span>${esc(group.title)}</span>
          <em>${isOpen ? '收起' : '展开'}</em>
        </button>
        <div class="cube-module-items">${buttons}</div>
      </div>
    `;
    }).join('');
}

function toggleCubeMenuGroup(title){
  openCubeMenuGroup = openCubeMenuGroup === title ? '' : title;
  renderCubeModuleNav();
}

function toggleCubeMenuAll(){
  openCubeMenuGroup = openCubeMenuGroup ? '' : '模型配置';
  renderCubeModuleNav();
}

async function showCubeModule(moduleId){
  const module = CUBE_MODULES.find(item => item.id === moduleId) || CUBE_MODULES[0];
  currentCubeModule = module.id;
  if(module.entity) currentCubeEntity = module.entity;
  currentCubeRow = null;
  renderCubeModuleNav();
  const titleEl = document.getElementById('cube-module-title');
  const descEl = document.getElementById('cube-module-desc');
  const eyebrowEl = document.getElementById('cube-module-eyebrow');
  if(titleEl) titleEl.textContent = module.title;
  if(descEl) descEl.textContent = module.desc;
  if(eyebrowEl) eyebrowEl.textContent = module.kind === 'entity' ? 'Doris-backed Configuration' : 'Metric Cube Control Plane';
  const container = document.getElementById('cube-module-container');
  if(!container) return;
  container.innerHTML = `<div class="empty-state"><div class="empty-icon">⏳</div><div class="empty-text">加载 ${esc(module.title)}…</div></div>`;
  if(module.kind === 'overview'){
    container.innerHTML = await loadHtmlFragment('/ui/pages/cube/overview.html', tabCache);
    renderCubeOverview();
    return;
  }
  if(module.kind === 'governance'){
    container.innerHTML = await loadHtmlFragment('/ui/pages/cube/governance.html', tabCache);
    await loadCubeValidationLatest();
    return;
  }
  container.innerHTML = await loadHtmlFragment('/ui/pages/cube/entity.html', tabCache);
  await loadCubeRows();
}

function renderCubeOverview(){
  const grid = document.getElementById('cube-overview-grid');
  if(!grid) return;
  const entitySet = new Set(cubeEntities.map(e => e.name));
  grid.innerHTML = CUBE_MODULES.filter(m => m.id !== 'overview' && (!m.entity || entitySet.has(m.entity))).map(m => `
    <button class="cube-overview-card" onclick="showCubeModule('${m.id}')">
      <span class="cube-overview-icon">${esc(m.icon)}</span>
      <strong>${esc(m.title)}</strong>
      <small>${esc(m.desc)}</small>
    </button>
  `).join('');
}

async function loadCubeRows(){
  const tbody = document.getElementById('cube-tbody');
  const thead = document.getElementById('cube-thead');
  if(!tbody || !thead) return;
  tbody.innerHTML = '<tr><td style="color:var(--text3)">加载中…</td></tr>';
  try{
    const res = await apiFetch(`/cube/admin/${currentCubeEntity}?limit=500`);
    currentCubeMeta = res;
    currentCubeRows = res.rows || [];
    const fields = res.fields || [];
    document.getElementById('cube-table-title').textContent = CUBE_ENTITY_LABELS[currentCubeEntity] || currentCubeEntity;
    document.getElementById('cube-table-meta').textContent = `${res.table} · ${currentCubeRows.length} 行`;
    thead.innerHTML = `<tr>${fields.slice(0, 6).map(f=>`<th>${esc(f)}</th>`).join('')}<th>操作</th></tr>`;
    tbody.innerHTML = currentCubeRows.map((row, idx) => `
      <tr onclick="editCubeRow(${idx})">
        ${fields.slice(0, 6).map(f=>`<td title="${esc(String(row[f]??''))}">${esc(String(row[f]??''))}</td>`).join('')}
        <td><button class="btn btn-sm btn-secondary" onclick="event.stopPropagation();editCubeRow(${idx})">编辑</button></td>
      </tr>
    `).join('') || `<tr><td colspan="${fields.length+1}" style="color:var(--text3);text-align:center">暂无数据</td></tr>`;
    if(currentCubeRows.length) editCubeRow(0);
    else newCubeRow();
  }catch(e){
    tbody.innerHTML = `<tr><td style="color:var(--red)">${esc(e.message)}</td></tr>`;
  }
}

function editCubeRow(index){
  currentCubeRow = currentCubeRows[index] || null;
  const editor = document.getElementById('cube-row-json');
  if(editor) editor.value = JSON.stringify(currentCubeRow || {}, null, 2);
  const state = document.getElementById('cube-editor-state');
  if(state && currentCubeMeta && currentCubeRow){
    state.textContent = `${currentCubeMeta.pk}=${currentCubeRow[currentCubeMeta.pk]}`;
  }
}

function newCubeRow(){
  const row = {};
  (currentCubeMeta?.fields || []).forEach(f => {
    if(['visible','version','public_flag'].includes(f)) row[f] = 1;
    else if(f.endsWith('_json')) row[f] = ['aliases_json','drill_members_json','hierarchy_json'].includes(f) ? '[]' : '{}';
    else row[f] = '';
  });
  currentCubeRow = row;
  document.getElementById('cube-row-json').value = JSON.stringify(row, null, 2);
  document.getElementById('cube-editor-state').textContent = '新增';
}

function formatCubeJson(){
  const editor = document.getElementById('cube-row-json');
  try{ editor.value = JSON.stringify(JSON.parse(editor.value || '{}'), null, 2); }
  catch(e){ toast('JSON 格式错误: '+e.message, 'error'); }
}

async function saveCubeRow(){
  const editor = document.getElementById('cube-row-json');
  try{
    const row = JSON.parse(editor.value || '{}');
    const res = await apiFetch(`/cube/admin/${currentCubeEntity}`, {
      method:'POST',
      body:JSON.stringify({row})
    });
    toast('Cube 配置已保存');
    document.getElementById('cube-admin-msg').textContent = `已保存 ${res.entity}：${res.pk}=${res.id}`;
    await loadCubeRows();
  }catch(e){ toast('保存失败: '+e.message, 'error'); }
}

async function deleteCubeRow(){
  if(!currentCubeMeta || !currentCubeRow) return;
  const id = currentCubeRow[currentCubeMeta.pk];
  if(!id && id !== 0){ toast('没有可删除的主键', 'error'); return; }
  if(!confirm(`确认删除 ${currentCubeEntity}: ${id}？`)) return;
  try{
    await apiFetch(`/cube/admin/${currentCubeEntity}/${encodeURIComponent(id)}`, {method:'DELETE'});
    toast('已删除');
    await loadCubeRows();
  }catch(e){ toast('删除失败: '+e.message, 'error'); }
}

async function syncCubeCache(){
  try{
    const res = await apiFetch(`/cube/admin/${currentCubeEntity || 'models'}/sync-cache`, {method:'POST'});
    toast('Cube 内存缓存已同步');
    const msg = document.getElementById('cube-admin-msg');
    if(msg) msg.textContent = `已同步模型版本 ${res.version_no || res.rendered_version || ''}`;
  }catch(e){ toast('同步失败: '+e.message, 'error'); }
}

async function collectCubeEnums(){
  try{
    const res = await apiFetch('/cube/admin/dimension-values/collect', {
      method:'POST',
      body:JSON.stringify({max_values:200, max_cardinality:500})
    });
    toast(`枚举采集完成：${res.inserted || 0} 条`);
    await showCubeModule('dimension-values');
  }catch(e){ toast('枚举采集失败: '+e.message, 'error'); }
}

async function runCubeValidation(explainSql=false){
  if(!document.getElementById('cube-validation-tbody') && document.getElementById('cube-module-container')){
    await showCubeModule('governance');
  }
  const summaryEl = document.getElementById('cube-validation-summary');
  const tbody = document.getElementById('cube-validation-tbody');
  if(summaryEl) summaryEl.textContent = explainSql ? 'EXPLAIN 校验中…' : '校验中…';
  if(tbody) tbody.innerHTML = '<tr><td colspan="4" style="color:var(--text3);text-align:center">正在扫描 Cube 配置…</td></tr>';
  try{
    const res = await apiFetch('/cube/validate', {
      method:'POST',
      body:JSON.stringify({explain_sql:!!explainSql, persist:true})
    });
    renderCubeValidation(res);
    toast(res.status === 'ok' ? 'Cube 配置校验通过' : 'Cube 配置存在治理问题', res.status === 'ok' ? 'success' : 'error');
  }catch(e){
    if(summaryEl) summaryEl.textContent = '校验失败';
    if(tbody) tbody.innerHTML = `<tr><td colspan="4" style="color:var(--red)">${esc(e.message)}</td></tr>`;
    toast('Cube 校验失败: '+e.message, 'error');
  }
}

async function loadCubeValidationLatest(){
  try{
    const res = await apiFetch('/cube/validate/latest?limit=300');
    renderCubeValidation(res);
  }catch(e){ toast('加载校验结果失败: '+e.message, 'error'); }
}

function renderCubeValidation(res){
  const summary = res.summary || {};
  const issues = res.issues || [];
  const summaryEl = document.getElementById('cube-validation-summary');
  const metricsEl = document.getElementById('cube-validation-metrics');
  const tbody = document.getElementById('cube-validation-tbody');
  if(summaryEl){
    summaryEl.textContent = res.run_id ? `run ${res.run_id} · ${issues.length} 条` : '暂无结果';
  }
  if(metricsEl){
    metricsEl.innerHTML = `
      <span class="validation-pill error">Error ${summary.error || 0}</span>
      <span class="validation-pill warning">Warning ${summary.warning || 0}</span>
      <span class="validation-pill info">Info ${summary.info || 0}</span>
      ${res.model_version!==undefined ? `<span class="validation-pill muted">模型版本 ${esc(String(res.model_version || 0))}</span>` : ''}
    `;
  }
  if(!tbody) return;
  if(!issues.length){
    tbody.innerHTML = '<tr><td colspan="4" style="color:var(--text3);text-align:center">暂无校验结果</td></tr>';
    return;
  }
  tbody.innerHTML = issues.map(item => {
    const detail = item.detail ? `<div class="validation-detail">${esc(item.detail)}</div>` : '';
    return `
      <tr>
        <td><span class="validation-severity ${esc(item.severity || 'info')}">${esc(item.severity || 'info')}</span></td>
        <td><strong>${esc(item.entity_type || '')}</strong><br><code>${esc(item.entity_name || '')}</code></td>
        <td><code>${esc(item.rule_code || '')}</code></td>
        <td>${esc(item.message || '')}${detail}</td>
      </tr>
    `;
  }).join('');
}

/* ═══════════════════════════════════════════════════════════════════════════
   🔍 归因分析
═══════════════════════════════════════════════════════════════════════════ */
let rcaOptions = null;
let lastRcaSql = '';

async function initRcaPage(){
  const state = document.getElementById('rca-option-state');
  if(!state) return;
  state.textContent = '加载中';
  try{
    rcaOptions = await apiFetch('/rca/options');
    renderRcaOptions(rcaOptions);
    state.textContent = '已加载';
  }catch(e){
    state.textContent = '加载失败';
    toast('归因配置加载失败: '+e.message, 'error');
  }
}

function renderRcaOptions(options){
  const metricEl = document.getElementById('rca-metric');
  const timeEl = document.getElementById('rca-time-dim');
  const dimEl = document.getElementById('rca-dim-list');
  if(!metricEl || !timeEl || !dimEl) return;
  metricEl.innerHTML = (options.measures || []).map(m =>
    `<option value="${esc(m.name)}">${esc(m.title || m.name)} · ${esc(m.name)}</option>`
  ).join('');
  const timeDims = options.time_dimensions?.length ? options.time_dimensions : (options.dimensions || []).filter(d => d.type === 'time');
  timeEl.innerHTML = timeDims.map(d =>
    `<option value="${esc(d.name)}">${esc(d.title || d.name)} · ${esc(d.name)}</option>`
  ).join('');
  const dims = (options.dimensions || []).filter(d => d.type !== 'time').slice(0, 40);
  dimEl.innerHTML = dims.map((d, idx) => `
    <label class="rca-dim-chip">
      <input type="checkbox" value="${esc(d.name)}" ${idx < 4 ? 'checked' : ''}>
      <span>${esc(d.title || d.name)}</span>
      <code>${esc(d.name)}</code>
    </label>
  `).join('');
}

function selectedRcaDimensions(){
  return Array.from(document.querySelectorAll('#rca-dim-list input:checked')).map(el => el.value);
}

async function runRcaAnalysis(){
  const state = document.getElementById('rca-run-state');
  if(state) state.textContent = '分析中…';
  try{
    const payload = {
      metric: document.getElementById('rca-metric').value,
      time_dimension: document.getElementById('rca-time-dim').value,
      current_start: document.getElementById('rca-current-start').value.trim(),
      current_end: document.getElementById('rca-current-end').value.trim(),
      baseline_start: document.getElementById('rca-baseline-start').value.trim(),
      baseline_end: document.getElementById('rca-baseline-end').value.trim(),
      dimensions: selectedRcaDimensions(),
      filters: [],
      limit: Number(document.getElementById('rca-limit').value || 20),
    };
    const res = await apiFetch('/rca/analyze', {method:'POST', body:JSON.stringify(payload)});
    renderRcaResult(res);
    if(state) state.textContent = '完成';
  }catch(e){
    if(state) state.textContent = '失败';
    toast('归因分析失败: '+e.message, 'error');
  }
}

function renderRcaResult(res){
  const current = res.periods?.current?.value ?? 0;
  const baseline = res.periods?.baseline?.value ?? 0;
  const delta = res.delta ?? 0;
  const rate = res.delta_rate;
  document.getElementById('rca-current-value').textContent = fmtNumber(current);
  document.getElementById('rca-baseline-value').textContent = fmtNumber(baseline);
  document.getElementById('rca-delta-value').textContent = fmtNumber(delta);
  document.getElementById('rca-rate-value').textContent = rate == null ? '—' : (rate * 100).toFixed(2) + '%';
  document.getElementById('rca-summary-text').textContent = res.summary || '';

  const list = document.getElementById('rca-contribution-list');
  const dimensions = res.dimensions || [];
  list.innerHTML = dimensions.map(dim => `
    <div class="rca-dim-result">
      <div class="rca-dim-result-title">${esc(dim.dimension)}</div>
      ${(dim.items || []).slice(0, 10).map(item => `
        <div class="rca-factor-row">
          <span class="rca-factor-name">${esc(item.value)}</span>
          <span>${fmtNumber(item.current)}</span>
          <span>${fmtNumber(item.baseline)}</span>
          <strong class="${item.delta >= 0 ? 'pos' : 'neg'}">${fmtNumber(item.delta)}</strong>
          <em>${item.contribution == null ? '—' : (item.contribution * 100).toFixed(1) + '%'}</em>
        </div>
      `).join('') || '<div class="empty-state small"><div class="empty-text">无贡献项</div></div>'}
    </div>
  `).join('');

  lastRcaSql = (res.sql_trace || []).map(item => `-- ${item.dimension} / ${item.period}\n${item.sql}`).join('\n\n');
  document.getElementById('rca-sql-box').textContent = lastRcaSql || '-- 暂无 SQL';
}

function copyRcaSql(){
  if(!lastRcaSql){ toast('暂无 SQL 可复制', 'error'); return; }
  navigator.clipboard?.writeText(lastRcaSql);
  toast('归因 SQL 已复制');
}

function fmtNumber(value){
  const num = Number(value || 0);
  return Number.isFinite(num) ? num.toLocaleString(undefined, {maximumFractionDigits: 2}) : '—';
}

/* ═══════════════════════════════════════════════════════════════════════════
   ⌁ 智能归因（对话式）
═══════════════════════════════════════════════════════════════════════════ */
const SMART_RCA_SUGGESTED = [
  '高价值用户流失风险与消费异动深度归因分析',
  '4月份GMV相比3月份为什么变化？按城市和门店类型归因',
  '上个月PLUS会员消费金额波动的主要原因是什么？',
  '净收入下降是什么维度导致的？',
];
const SMART_RCA_STEPS = [
  {name:'rca_intent', label:'① 归因意图识别'},
  {name:'rca_multi_recall', label:'② 多路 Agent 召回'},
  {name:'rca_plan', label:'③ 归因计划生成'},
  {name:'rca_execute', label:'④ RCA 工具执行'},
  {name:'rca_summary', label:'⑤ 归因结论生成'},
];
let _smartRcaDone = 0;

function initSmartRcaPage(){
  const wrap = document.getElementById('smart-rca-suggested');
  const input = document.getElementById('smart-rca-question');
  if(!wrap || !input) return;
  wrap.innerHTML = '<span style="font-size:12px;color:var(--text3);margin-right:4px">示例：</span>';
  SMART_RCA_SUGGESTED.forEach(q => {
    const chip = document.createElement('span');
    chip.className = 'sq-chip';
    chip.textContent = q;
    chip.onclick = () => { input.value = q; doSmartRca(); };
    wrap.appendChild(chip);
  });
}

function doSmartRca(){
  const q = document.getElementById('smart-rca-question').value.trim();
  if(!q){ toast('请输入归因问题','error'); return; }
  if(currentSSE){ currentSSE.close(); currentSSE=null; }
  _smartRcaDone = 0;
  const result = document.getElementById('smart-rca-result');
  result.innerHTML = '<div class="empty-state"><div class="empty-icon">⏳</div><div class="empty-text">智能归因分析中…</div></div>';
  resetSmartRcaChain();
  const url = API + '/rca/smart/stream?q=' + encodeURIComponent(q);
  const sse = new EventSource(url);
  currentSSE = sse;

  sse.addEventListener('start', e => {
    const d = JSON.parse(e.data);
    document.getElementById('trace-id-display').textContent = 'ID: ' + d.trace_id;
  });
  sse.addEventListener('step_start', e => {
    const d = JSON.parse(e.data);
    updateStepCard({name:d.name, status:'running'});
    const idx = SMART_RCA_STEPS.findIndex(s => s.name === d.name);
    if(idx > 0) updateConnector(idx - 1, 'active');
  });
  sse.addEventListener('step_done', e => {
    const d = JSON.parse(e.data);
    updateStepCard(d);
    _smartRcaDone++;
    setChainProgress(_smartRcaDone, SMART_RCA_STEPS.length);
    const idx = SMART_RCA_STEPS.findIndex(s => s.name === d.name);
    if(idx > 0) updateConnector(idx - 1, 'done');
  });
  sse.addEventListener('final', e => {
    const d = JSON.parse(e.data);
    const trace = d.trace || {};
    document.getElementById('trace-time-display').textContent = `⏱ ${trace.total_ms?.toFixed(0)||0}ms`;
    renderSmartRcaResult(d);
    setChainProgress(SMART_RCA_STEPS.length, SMART_RCA_STEPS.length);
    sse.close();
    currentSSE = null;
  });
  sse.addEventListener('error', () => {
    showSmartRcaError('智能归因连接中断，请重试');
    sse.close();
    currentSSE = null;
  });
}

function resetSmartRcaChain(){
  const body = document.getElementById('chain-body');
  body.innerHTML = '';
  SMART_RCA_STEPS.forEach((s, idx) => {
    if(idx > 0) body.appendChild(makeConnector('pending'));
    body.appendChild(makeStepCardEl(s.name, s.label, 'pending'));
  });
  setChainProgress(0, SMART_RCA_STEPS.length);
}

function renderSmartRcaResult(data){
  const result = data.result || {};
  const periods = result.periods || {};
  const current = periods.current?.value ?? 0;
  const baseline = periods.baseline?.value ?? 0;
  const delta = result.delta ?? 0;
  const rate = result.delta_rate;
  const sqlText = (result.sql_trace || []).map(item => `-- ${item.dimension} / ${item.period}\n${item.sql}`).join('\n\n');
  const dimsHtml = (result.dimensions || []).map(dim => `
    <div class="rca-dim-result">
      <div class="rca-dim-result-title">${esc(dim.dimension)}</div>
      ${(dim.items || []).slice(0, 8).map(item => `
        <div class="rca-factor-row">
          <span class="rca-factor-name">${esc(item.value)}</span>
          <span>${fmtNumber(item.current)}</span>
          <span>${fmtNumber(item.baseline)}</span>
          <strong class="${item.delta >= 0 ? 'pos' : 'neg'}">${fmtNumber(item.delta)}</strong>
          <em>${item.contribution == null ? '—' : (item.contribution * 100).toFixed(1) + '%'}</em>
        </div>
      `).join('')}
    </div>
  `).join('');
  document.getElementById('smart-rca-result').innerHTML = `
    <div class="smart-rca-answer">
      <div class="card glow-card rca-summary-card">
        <div class="card-header"><span class="card-title">智能归因结论</span><span class="card-kicker">${esc(data.plan?.metric||'')}</span></div>
        <div class="card-body">
          <div class="rca-kpis">
            <div><span>当前期</span><strong>${fmtNumber(current)}</strong></div>
            <div><span>对比期</span><strong>${fmtNumber(baseline)}</strong></div>
            <div><span>变化量</span><strong>${fmtNumber(delta)}</strong></div>
            <div><span>变化率</span><strong>${rate == null ? '—' : (rate * 100).toFixed(2) + '%'}</strong></div>
          </div>
          <div class="rca-summary-text">${esc(data.summary || result.summary || '')}</div>
        </div>
      </div>
      <div class="card glow-card">
        <div class="card-header"><span class="card-title">维度贡献</span></div>
        <div class="card-body">${dimsHtml || '<div class="empty-state small"><div class="empty-text">暂无维度贡献</div></div>'}</div>
      </div>
      <div class="card glow-card">
        <div class="card-header"><span class="card-title">执行 SQL</span></div>
        <div class="card-body"><pre class="rca-sql-box">${esc(sqlText || '-- 暂无 SQL')}</pre></div>
      </div>
    </div>
  `;
}

function showSmartRcaError(message){
  const el = document.getElementById('smart-rca-result');
  if(el) el.innerHTML = `<div class="empty-state"><div class="empty-icon">⚠️</div><div class="empty-text">${esc(message)}</div></div>`;
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
  document.getElementById('engine-cube').classList.toggle('active', engine==='cube');
  document.getElementById('engine-vanna').classList.toggle('active', engine==='vanna');
  document.getElementById('engine-langchain').classList.toggle('active', engine==='langchain');
  document.getElementById('engine-semantic')?.classList.toggle('active', engine==='semantic');
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

  if(currentAskEngine === 'cube'){
    runCubeAsk(q);
    return;
  }

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

// Cube pipeline 固定步骤顺序（用于预渲染 pending 卡片）
const CUBE_PIPELINE_STEPS = [
  { name: 'cube_model_check', label: '① Cube 模型检查' },
  { name: 'intent',           label: '② 意图理解' },
  { name: 'semantic_sql_rag', label: '③ SQL RAG 召回' },
  { name: 'cube_prompt',      label: '④ Prompt 构建' },
  { name: 'cube_llm_parse',   label: '⑤ LLM 语义解析' },
  { name: 'cube_heuristics',  label: '⑥ 规则修正' },
  { name: 'cube_compile',     label: '⑦ SQL 编译' },
];

function setChainProgress(n, total){
  const bar = document.getElementById('chain-progress-bar');
  if(bar) bar.style.width = (n / total * 100) + '%';
}

let _cubeStepsDone = 0;

function runCubeAsk(question){
  _cubeStepsDone = 0;

  // 立即渲染全部步骤为 pending，让用户看到完整流水线
  const body = document.getElementById('chain-body');
  body.innerHTML = '';
  CUBE_PIPELINE_STEPS.forEach((s, idx) => {
    if(idx > 0) body.appendChild(makeConnector('pending'));
    body.appendChild(makeStepCardEl(s.name, s.label, 'pending'));
  });
  setChainProgress(0, CUBE_PIPELINE_STEPS.length);

  const url = API + '/ask/cube/stream?q=' + encodeURIComponent(question);
  const sse = new EventSource(url);
  currentSSE = sse;

  sse.addEventListener('start', e => {
    const d = JSON.parse(e.data);
    document.getElementById('trace-id-display').textContent = 'ID: ' + d.trace_id;
  });

  sse.addEventListener('step_start', e => {
    const d = JSON.parse(e.data);
    // 找到对应的 pending 卡片，切换为 running
    updateStepCard({ name: d.name, status: 'running' });
    // 更新前一个 connector 为 active
    const idx = CUBE_PIPELINE_STEPS.findIndex(s => s.name === d.name);
    if(idx > 0) updateConnector(idx - 1, 'active');
  });

  sse.addEventListener('step_done', e => {
    const d = JSON.parse(e.data);
    updateStepCard(d);
    _cubeStepsDone++;
    setChainProgress(_cubeStepsDone, CUBE_PIPELINE_STEPS.length);
    // 更新对应 connector 为 done
    const idx = CUBE_PIPELINE_STEPS.findIndex(s => s.name === d.name);
    if(idx > 0) updateConnector(idx - 1, 'done');
  });

  sse.addEventListener('final', e => {
    const d = JSON.parse(e.data);
    const trace = d.trace || {};
    document.getElementById('trace-time-display').textContent =
      `⏱ ${trace.total_ms?.toFixed(0) || 0}ms`;
    setChainProgress(CUBE_PIPELINE_STEPS.length, CUBE_PIPELINE_STEPS.length);
    if(d.sql){
      currentSQL = d.sql;
      showSQL(d.sql);
    } else if(d.error){
      showError(d.error);
    }
    sse.close();
    currentSSE = null;
  });

  sse.addEventListener('error', e => {
    console.warn('Cube SSE error', e);
    showError('连接中断，请重试');
    sse.close();
    currentSSE = null;
  });
}

function makeConnector(state){
  const div = document.createElement('div');
  div.className = 'step-connector' + (state !== 'pending' ? ' ' + state : '');
  div.setAttribute('data-connector', '');
  div.innerHTML = '<div class="step-connector-dot"></div>';
  return div;
}

// 更新第 idx 个 connector（0-based）
function updateConnector(idx, state){
  const connectors = document.querySelectorAll('#chain-body [data-connector]');
  const c = connectors[idx];
  if(c){
    c.className = 'step-connector ' + state;
  }
}

function makeStepCardEl(name, label, status){
  const card = document.createElement('div');
  card.className = 'step-card ' + status;
  card.id = 'step-' + name;
  const icon = status === 'pending' ? '·' : status === 'running' ? '↻' : '…';
  card.innerHTML = `
    <div class="step-header" onclick="toggleStepDetail('${name}')">
      <div class="step-icon ${status}">${icon}</div>
      <span class="step-name">${esc(label)}</span>
      <span class="step-dur" id="dur-${name}"></span>
    </div>
    <div class="step-detail" id="detail-${name}"></div>
  `;
  return card;
}

function renderSyncTraceResult(result){
  const trace = result.trace || {};
  document.getElementById('trace-id-display').textContent = trace.trace_id ? ('ID: ' + trace.trace_id) : '';
  document.getElementById('trace-time-display').textContent = `⏱ ${trace.total_ms?.toFixed(0)||0}ms`;
  const steps = trace.steps || [];
  if(!steps.length){
    resetChain();
    return;
  }
  const body = document.getElementById('chain-body');
  body.innerHTML = '';
  steps.forEach(step => updateStepCard(step));
}

function resetChain(){
  document.getElementById('chain-body').innerHTML =
    '<div style="padding:20px;text-align:center;color:var(--text3);font-size:12px">⏳ 推理中…</div>';
  setChainProgress(0, 1);
}

const STEP_LABELS = {
  generate_embedding:  '① 生成 Embedding 向量',
  vector_search_sql:   '② 向量检索相似 SQL',
  vector_search_ddl:   '③ 向量检索相关 DDL',
  build_prompt:        '④ 组装 Prompt',
  llm_generate:        '⑤ LLM 推理生成 SQL',
  extract_sql:         '⑥ 提取 SQL',
  router_intent:       '① 意图解析与标准化',
  // Cube pipeline (7 steps)
  cube_model_check:    '① Cube 模型检查',
  intent:              '② 意图理解',
  rca_intent:          '① 归因意图识别',
  rca_multi_recall:    '② 多路 Agent 召回',
  rca_plan:            '③ 归因计划生成',
  rca_execute:         '④ RCA 工具执行',
  rca_summary:         '⑤ 归因结论生成',
  semantic_sql_rag:    '③ SQL RAG 召回',
  cube_prompt:         '④ Prompt 构建',
  cube_llm_parse:      '⑤ LLM 语义解析',
  cube_heuristics:     '⑥ 规则修正',
  cube_compile:        '⑦ SQL 编译',
  // Other pipelines
  cube_parse:          '③ Cube 语义解析',
  semantic_parse:      '③ 语义解析',
  multi_recall:        '② 多路召回与融合',
  sql_guard:           '⑤ SQL Guard / EXPLAIN',
};

function addStepCard(name, label, status){
  const container = document.getElementById('chain-body');
  // 清空 loading 占位
  if(container.querySelector('.empty-state') ||
     (container.childElementCount===1 && container.firstChild.tagName==='DIV' && !container.firstChild.id.startsWith('step-'))){
    container.innerHTML='';
  }
  // 如果已经有这个卡片（pending 预渲染），不重复添加
  if(document.getElementById('step-'+name)) return;
  const displayLabel = STEP_LABELS[name] || label;
  // 加连接线（非第一张卡）
  if(container.querySelectorAll('.step-card').length > 0){
    container.appendChild(makeConnector('pending'));
  }
  container.appendChild(makeStepCardEl(name, displayLabel, status));
  container.scrollTop = container.scrollHeight;
}

function updateStepCard(step){
  const name = step.name;
  let card = document.getElementById('step-'+name);
  if(!card){ addStepCard(name, STEP_LABELS[name]||name, step.status); card=document.getElementById('step-'+name); }

  card.className = `step-card ${step.status}`;
  const iconEl = card.querySelector('.step-icon');
  iconEl.className = `step-icon ${step.status}`;
  const icons = {ok:'✓', error:'✗', cached:'○', running:'↻', pending:'·'};
  iconEl.textContent = icons[step.status] || '?';

  const durEl = document.getElementById('dur-'+name);
  if(durEl && step.duration_ms != null) durEl.textContent = step.duration_ms.toFixed(0) + 'ms';

  // Badge (note)
  const header = card.querySelector('.step-header');
  const existBadge = header.querySelector('.step-badge');
  if(existBadge) existBadge.remove();
  if(step.note && step.note !== 'DISABLED'){
    const badge = document.createElement('span');
    const cls = step.note==='CACHED'?'cached':step.note.includes('gemini')?'model':'fresh';
    badge.className = `step-badge ${cls}`;
    badge.textContent = step.note;
    header.insertBefore(badge, durEl);
  } else if(step.note === 'DISABLED'){
    const badge = document.createElement('span');
    badge.className = 'step-badge cached';
    badge.textContent = '已跳过';
    header.insertBefore(badge, durEl);
  }

  // Detail
  const detail = document.getElementById('detail-'+name);
  if(detail && step.outputs){
    let html = '';
    const out = step.outputs;
    const scoreBadge = (score, dist, quality) =>
      '<span class="rag-score">score '+esc(String(score ?? '—'))
      +' · dist '+esc(String(dist ?? '—'))
      +' · quality '+esc(String(quality ?? '—'))+'</span>';
    const formatters = {
      generate_embedding: ()=>`
        <div class="detail-row"><span class="detail-label">向量维度</span><span class="detail-val highlight">${out.dims}</span></div>`,
      vector_search_sql: ()=>`
        <div class="detail-row"><span class="detail-label">找到</span><span class="detail-val highlight">${out.found} 条</span></div>
        ${(out.top_questions||[]).map((q,i)=>`
          <div class="detail-row"><span class="detail-label">Top${i+1}</span>
          <span class="detail-val">${esc(q)}
            ${scoreBadge(out.top_scores?.[i], out.top_distances?.[i], out.quality_scores?.[i])}
          </span></div>`).join('')}`,
      vector_search_ddl: ()=>`
        <div class="detail-row"><span class="detail-label">找到</span><span class="detail-val highlight">${out.found} 张表</span></div>
        ${(out.tables||[]).map(t=>`<div class="detail-row"><span class="detail-label"></span><span class="detail-val">${esc(t)}</span></div>`).join('')}`,
      router_intent: ()=>`
        <div class="detail-row"><span class="detail-label">标准化</span><span class="detail-val">${esc(out.normalized_query||'')}</span></div>
        <div class="detail-row"><span class="detail-label">意图</span><span class="detail-val highlight">${esc(out.intent||'')}</span></div>
        <div class="detail-row"><span class="detail-label">实体</span><span class="detail-val">${esc(out.entity||'')}</span></div>`,
      intent: ()=>`
        <div class="detail-row"><span class="detail-label">意图</span><span class="detail-val highlight">${esc(out.intent_type||'')}</span></div>
        <div class="detail-row"><span class="detail-label">业务域</span><span class="detail-val">${esc(out.business_domain||'')}</span></div>
        <div class="detail-row"><span class="detail-label">复杂度</span><span class="detail-val">${esc(out.complexity||'')}</span></div>
        <div class="detail-row"><span class="detail-label">时间提示</span><span class="detail-val">${esc(out.time_hint||'')}</span></div>`,
      rca_intent: ()=>`
        <div class="detail-row"><span class="detail-label">意图</span><span class="detail-val highlight">${esc(out.intent_type||'')}</span></div>
        <div class="detail-row"><span class="detail-label">模式</span><span class="detail-val">${esc(out.analysis_mode||'')}</span></div>
        <div class="detail-row"><span class="detail-label">信号词</span><span class="detail-val">${esc((out.signals||[]).join(', ')||'—')}</span></div>`,
      rca_multi_recall: ()=>`
        <div class="detail-row"><span class="detail-label">指标召回</span><span class="detail-val highlight">${out.measure_count||0} 个</span></div>
        <div class="detail-row"><span class="detail-label">维度召回</span><span class="detail-val">${out.dimension_count||0} 个</span></div>
        <div class="detail-row"><span class="detail-label">时间维度</span><span class="detail-val">${out.time_dimension_count||0} 个</span></div>
        <div class="detail-row"><span class="detail-label">影响关系</span><span class="detail-val">${out.influence_count||0} 条</span></div>
        <div class="detail-row"><span class="detail-label">Top指标</span><span class="detail-val">${esc((out.top_measures||[]).join(', ')||'—')}</span></div>
        <div class="detail-row"><span class="detail-label">Top维度</span><span class="detail-val">${esc((out.top_dimensions||[]).join(', ')||'—')}</span></div>
        ${(out.candidate_measures||[]).length?`
          <div class="detail-subtitle">候选指标 Top ${Math.min((out.candidate_measures||[]).length, 20)}</div>
          <div class="detail-block">${esc(JSON.stringify(out.candidate_measures||[], null, 2))}</div>
        `:''}
        ${(out.candidate_dimensions||[]).length?`
          <div class="detail-subtitle">候选维度 Top ${Math.min((out.candidate_dimensions||[]).length, 30)}</div>
          <div class="detail-block">${esc(JSON.stringify(out.candidate_dimensions||[], null, 2))}</div>
        `:''}
        ${(out.candidate_influences||[]).length?`
          <div class="detail-subtitle">指标影响关系 Top ${Math.min((out.candidate_influences||[]).length, 20)}</div>
          <div class="detail-block">${esc(JSON.stringify(out.candidate_influences||[], null, 2))}</div>
        `:''}`,
      rca_plan: ()=>`
        <div class="detail-row"><span class="detail-label">指标</span><span class="detail-val highlight">${esc(out.metric||'')}</span></div>
        <div class="detail-row"><span class="detail-label">时间维度</span><span class="detail-val">${esc(out.time_dimension||'')}</span></div>
        <div class="detail-row"><span class="detail-label">当前期</span><span class="detail-val">${esc(out.current_start||'')} ~ ${esc(out.current_end||'')}</span></div>
        <div class="detail-row"><span class="detail-label">对比期</span><span class="detail-val">${esc(out.baseline_start||'')} ~ ${esc(out.baseline_end||'')}</span></div>
        <div class="detail-row"><span class="detail-label">归因维度</span><span class="detail-val highlight">${esc((out.dimensions||[]).join(', ')||'—')}</span></div>
        <div class="detail-row"><span class="detail-label">Prompt</span><span class="detail-val">${fmt(out.prompt_chars||0)} 字符</span></div>
        ${(out.normalization_notes||[]).length?`
          <div class="detail-subtitle">规范化修正</div>
          ${(out.normalization_notes||[]).map(n=>`<div class="detail-row"><span class="detail-label">fix</span><span class="detail-val">${esc(n)}</span></div>`).join('')}
        `:''}
        <div class="detail-subtitle">LLM 原始规划</div>
        <div class="detail-block">${esc(JSON.stringify(out.raw_plan||{}, null, 2))}</div>
        <div class="detail-subtitle">最终归因计划</div>
        <div class="detail-block">${esc(JSON.stringify(out.normalized_plan||{}, null, 2))}</div>`,
      rca_execute: ()=>`
        <div class="detail-row"><span class="detail-label">指标</span><span class="detail-val highlight">${esc(out.metric||'')}</span></div>
        <div class="detail-row"><span class="detail-label">维度</span><span class="detail-val">${esc((out.dimensions||[]).join(', ')||'—')}</span></div>
        <div class="detail-row"><span class="detail-label">SQL数量</span><span class="detail-val">${out.sql_count||0} 条</span></div>
        <div class="detail-row"><span class="detail-label">变化量</span><span class="detail-val highlight">${fmt(out.delta||0)}</span></div>
        <div class="detail-row"><span class="detail-label">变化率</span><span class="detail-val">${out.delta_rate == null ? '—' : (out.delta_rate * 100).toFixed(2) + '%'}</span></div>
        ${(out.top_contributors||[]).length?`
          <div class="detail-subtitle">Top 贡献项</div>
          <div class="detail-block">${esc(JSON.stringify(out.top_contributors||[], null, 2))}</div>
        `:''}
        ${(out.sql_preview||[]).length?`
          <div class="detail-subtitle">SQL 预览</div>
          ${(out.sql_preview||[]).map((item,i)=>`
            <div class="detail-row"><span class="detail-label">SQL${i+1}</span><span class="detail-val">${esc(item.dimension||'')} / ${esc(item.period||'')}</span></div>
            <div class="detail-block">${esc(item.sql||'')}</div>
          `).join('')}
        `:''}`,
      rca_summary: ()=>`
        <div class="detail-block">${esc(out.summary||'')}</div>`,
      semantic_parse: ()=>`
        <div class="detail-row"><span class="detail-label">指标</span><span class="detail-val">${esc((out.metrics||[]).join(', ')||'—')}</span></div>
        <div class="detail-row"><span class="detail-label">维度</span><span class="detail-val">${esc((out.dimensions||[]).join(', ')||'—')}</span></div>
        <div class="detail-row"><span class="detail-label">Coverage</span><span class="detail-val highlight">${out.coverage_score ?? 0}</span></div>
        <div class="detail-row"><span class="detail-label">分析类型</span><span class="detail-val">${esc(out.analysis_type||'')}</span></div>
        <div class="detail-row"><span class="detail-label">未解析</span><span class="detail-val">${esc((out.unresolved||[]).join(', ')||'—')}</span></div>`,
      semantic_sql_rag: ()=>{
        const rows = (out.top_questions||[]).map((q,i)=>
          '<div class="detail-row"><span class="detail-label">Q'+(i+1)+'</span><span class="detail-val">'+esc(q)
          +' '+scoreBadge(out.top_scores?.[i], out.top_distances?.[i], out.quality_scores?.[i])+'</span></div>'
          +((out.top_sqls||[])[i]?'<div class="detail-block">'+esc((out.top_sqls||[])[i])+'</div>':'')
        ).join('');
        const statusSpan = out.enabled===false
          ? '<span style="color:var(--text3)">已禁用</span>'
          : '<span class="highlight">已启用</span>';
        return '<div class="detail-row"><span class="detail-label">状态</span><span class="detail-val">'+statusSpan+'</span></div>'
          +'<div class="detail-row"><span class="detail-label">召回数</span><span class="detail-val highlight">'+(out.count||0)+' 条</span></div>'
          +(rows?'<div class="detail-subtitle">Top 召回样本</div>'+rows:'');
      },
      cube_model_check: ()=>`
        <div class="detail-row"><span class="detail-label">模型版本</span><span class="detail-val highlight">${esc(String(out.model_version||0))}</span></div>
        ${out.model_checksum?`<div class="detail-row"><span class="detail-label">Checksum</span><span class="detail-val" style="font-family:var(--mono);font-size:11px">${esc(out.model_checksum)}</span></div>`:''}
        <div class="detail-row"><span class="detail-label">指标数</span><span class="detail-val highlight">${out.measures} 个</span></div>
        <div class="detail-row"><span class="detail-label">维度数</span><span class="detail-val">${out.dimensions} 个</span></div>
        <div class="detail-row"><span class="detail-label">分段数</span><span class="detail-val">${out.segments} 个</span></div>
        <div class="detail-row"><span class="detail-label">关联关系</span><span class="detail-val">${out.joins} 个</span></div>`,
      cube_prompt: ()=>`
        <div class="detail-row"><span class="detail-label">Prompt 长度</span><span class="detail-val highlight">${fmt(out.prompt_chars)} 字符</span></div>
        <div class="detail-row"><span class="detail-label">指标数（传入）</span><span class="detail-val">${out.measures_in_prompt} 个</span></div>
        <div class="detail-row"><span class="detail-label">维度数（传入）</span><span class="detail-val">${out.dimensions_in_prompt} 个</span></div>
        <div class="detail-row"><span class="detail-label">RAG 示例</span><span class="detail-val">${out.rag_examples} 条</span></div>
        <div class="detail-row"><span class="detail-label">时间提示</span><span class="detail-val highlight">${esc(out.time_hint||'—')}</span></div>`,
      cube_llm_parse: ()=>`
        <div class="detail-row"><span class="detail-label">LLM 响应</span><span class="detail-val highlight">${fmt(out.response_chars)} 字符</span></div>
        <div class="detail-row"><span class="detail-label">原始指标</span><span class="detail-val">${esc((out.raw_measures||[]).join(', ')||'—')}</span></div>
        <div class="detail-row"><span class="detail-label">原始维度</span><span class="detail-val">${esc((out.raw_dimensions||[]).join(', ')||'—')}</span></div>
        ${(out.raw_segments||[]).length?`<div class="detail-row"><span class="detail-label">原始分段</span><span class="detail-val">${esc(out.raw_segments.join(', '))}</span></div>`:''}
        ${out.raw_limit!=null?`<div class="detail-row"><span class="detail-label">Limit</span><span class="detail-val">${out.raw_limit}</span></div>`:''}
        ${(out.raw_filters||[]).length?`
          <div class="detail-subtitle">原始过滤（${out.raw_filters.length} 个）</div>
          ${(out.raw_filters||[]).map(f=>`<div class="detail-row"><span class="detail-label">${esc(f.member||'')}</span><span class="detail-val">${esc(f.operator||'')} <strong>${esc(Array.isArray(f.values)?f.values.join(', '):String(f.values??''))}</strong></span></div>`).join('')}
        `:''}`,
      cube_heuristics: ()=>`
        <div class="detail-row"><span class="detail-label">解析后指标</span><span class="detail-val highlight">${esc((out.measures||[]).join(', ')||'—')}</span></div>
        <div class="detail-row"><span class="detail-label">解析后维度</span><span class="detail-val">${esc((out.dimensions||[]).join(', ')||'—')}</span></div>
        ${(out.segments||[]).length?`<div class="detail-row"><span class="detail-label">分段</span><span class="detail-val">${esc(out.segments.join(', '))}</span></div>`:''}
        <div class="detail-row"><span class="detail-label">分析类型</span><span class="detail-val highlight">${esc(out.analysis_type||'—')}</span></div>
        ${(out.time_scope&&out.time_scope.start)?`
          <div class="detail-row"><span class="detail-label">时间范围</span>
            <span class="detail-val highlight">${esc(out.time_scope.start)} ~ ${esc(out.time_scope.end||'')}
              ${out.time_scope.label?'（'+esc(out.time_scope.label)+'）':''}
            </span>
          </div>`:''}
        ${out.limit!=null?`<div class="detail-row"><span class="detail-label">Limit</span><span class="detail-val">${out.limit}</span></div>`:''}
        ${(out.filters||[]).length?`
          <div class="detail-subtitle">过滤条件（${out.filters.length} 个）</div>
          ${(out.filters||[]).map(f=>`<div class="detail-row"><span class="detail-label">${esc(f.member||'')}</span><span class="detail-val">${esc(f.op||'')} <strong>${esc(Array.isArray(f.values)?f.values.join(', '):String(f.values??''))}</strong></span></div>`).join('')}
        `:''}
        ${(out.unresolved||[]).length?`<div class="detail-row"><span class="detail-label" style="color:var(--yellow)">未解析</span><span class="detail-val" style="color:var(--yellow)">${esc(out.unresolved.join(', '))}</span></div>`:''}`,
      cube_parse: ()=>`
        <div class="detail-row"><span class="detail-label">指标</span><span class="detail-val highlight">${esc((out.measures||[]).join(', ')||'—')}</span></div>
        <div class="detail-row"><span class="detail-label">维度</span><span class="detail-val">${esc((out.dimensions||[]).join(', ')||'—')}</span></div>
        <div class="detail-row"><span class="detail-label">分段</span><span class="detail-val">${esc((out.segments||[]).join(', ')||'—')}</span></div>
        <div class="detail-row"><span class="detail-label">分析类型</span><span class="detail-val highlight">${esc(out.analysis_type||'')}</span></div>
        <div class="detail-row"><span class="detail-label">Limit</span><span class="detail-val">${out.limit??'—'}</span></div>
        ${(out.time_scope&&out.time_scope.start)?`
          <div class="detail-row"><span class="detail-label">时间范围</span>
            <span class="detail-val highlight">${esc(out.time_scope.start)} ~ ${esc(out.time_scope.end)}
              ${out.time_scope.label?'（'+esc(out.time_scope.label)+'）':''}
            </span>
          </div>`:''
        }
        ${(out.comparison&&out.comparison.enabled)?`
          <div class="detail-row"><span class="detail-label">对比模式</span>
            <span class="detail-val highlight">${esc(out.comparison.mode||'')}：${esc(out.comparison.compare_start||'')} ~ ${esc(out.comparison.compare_end||'')}</span>
          </div>`:''}
        ${(out.filters||[]).length?`
          <div class="detail-subtitle">过滤条件（${out.filters.length} 个）</div>
          ${(out.filters||[]).map(f=>`
            <div class="detail-row">
              <span class="detail-label">${esc(f.member)}</span>
              <span class="detail-val">${esc(f.operator)} <strong>${esc(Array.isArray(f.values)?f.values.join(', '):String(f.values))}</strong></span>
            </div>`).join('')}
        `:'<div class="detail-row"><span class="detail-label">过滤条件</span><span class="detail-val" style="color:var(--text3)">无</span></div>'}
        ${(out.unresolved||[]).length?`
          <div class="detail-row"><span class="detail-label" style="color:var(--yellow)">未解析</span>
            <span class="detail-val" style="color:var(--yellow)">${esc(out.unresolved.join(', '))}</span>
          </div>`:''}`,
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
            <div class="detail-row"><span class="detail-label">Q${i+1}</span><span class="detail-val">${esc(item.question||'')} ${scoreBadge(item.score, item.distance, item.quality)}</span></div>
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
      cube_compile: ()=>`
        <div class="detail-row"><span class="detail-label">路径</span><span class="detail-val highlight">${esc(out.path||'cube')}</span></div>
        <div class="detail-row"><span class="detail-label">模型版本</span><span class="detail-val">${esc(String(out.model_version||0))}</span></div>
        ${out.model_checksum?`<div class="detail-row"><span class="detail-label">Checksum</span><span class="detail-val" style="font-family:var(--mono);font-size:11px">${esc(out.model_checksum)}</span></div>`:''}
        <div class="detail-subtitle">Cube Query（LLM 解析结果）</div>
        <div class="detail-block">${esc(JSON.stringify(out.cube_query||{}, null, 2))}</div>
        <div class="detail-subtitle">生成 SQL（完整）</div>
        <div class="detail-block" style="white-space:pre">${esc(out.sql_full||out.sql_preview||'')}</div>`,
      extract_sql: ()=>`
        <div class="detail-row"><span class="detail-label">提取</span><span class="detail-val highlight">${out.sql?'成功':'失败'}</span></div>`,
    };
    html = (formatters[name]||(() => JSON.stringify(out,null,2).split('\n').map(l=>`<div class="detail-row"><span class="detail-val">${esc(l)}</span></div>`).join('')))();
    if(step.error) html += `<div class="detail-row"><span class="detail-label" style="color:var(--red)">错误</span><span class="detail-val" style="color:var(--red)">${esc(step.error)}</span></div>`;
    detail.innerHTML = html;

    // 默认折叠，只有错误自动展开；需要看细节时手动点击。
    if(step.status === 'error'){
      detail.classList.add('open');
    }
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
      is_correct:isCorrect, corrected_sql:corrected,
      engine: currentAskEngine,
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
   回归测试
════════════════════════════════════════════════════════════════════════ */
const DEFAULT_REGRESSION_QUESTIONS = [
  '每个城市销售额排名前三的门店分别是哪些？',
  '4月份各城市销售额是多少',
  '昨日各城市销售额各环比同期增长多少？',
  'PLUS会员和普通会员消费对比？',
  '今天的总交易额是多少？',
  '按一级类目查看商品销量。',
  '不同门店类型的销售额分布是怎样的？',
  '北京，PLUS会员的总消费金额是多少？',
  '帮我看看北京的总消费金额是多少？其中 PLUS 会员消费了多少？',
  '上个月的笔单价和成交笔数分别多少？',
  '卖得最好的前5个城市是哪些？',
  '查看各城市、各门店类型下的总收入和客单价。',
  '今年每个月的GMV趋势是怎样的？',
  '普通会员购买一级类目为101的商品，花了多少钱？',
  '昨日各城市销售额环比同期增长多少？',
  '上海会员、非会员的总消费金额分别是多少，对比值？',
  '高价值用户流失风险与消费异动深度归因分析'
];

function initRegressionPage(){
  const input = document.getElementById('regression-questions');
  if(input && !input.value.trim()){
    input.value = DEFAULT_REGRESSION_QUESTIONS.join('\n');
  }
  regressionReports = [];
  const resultsEl = document.getElementById('regression-results');
  const summaryEl = document.getElementById('regression-summary');
  const copyBtn = document.getElementById('btn-copy-regression-all');
  if(resultsEl){
    resultsEl.innerHTML = '<div class="empty-state"><div class="empty-icon">🧪</div><div class="empty-text">粘贴多条问题后开始回归</div></div>';
  }
  if(summaryEl) summaryEl.textContent = '尚未执行';
  if(copyBtn) copyBtn.disabled = true;
}

function fillRegressionSamples(){
  const input = document.getElementById('regression-questions');
  if(input) input.value = DEFAULT_REGRESSION_QUESTIONS.join('\n');
}

function getRegressionEndpoint(){
  if(currentAskEngine === 'cube') return '/ask/cube';
  if(currentAskEngine === 'langchain') return '/ask-lc';
  if(currentAskEngine === 'semantic') return '/ask/semantic';
  return '/ask';
}

async function runRegressionBatch(){
  const input = document.getElementById('regression-questions');
  const statusEl = document.getElementById('regression-status');
  const summaryEl = document.getElementById('regression-summary');
  const resultsEl = document.getElementById('regression-results');
  const copyBtn = document.getElementById('btn-copy-regression-all');
  const btn = document.getElementById('btn-run-regression');
  const questions = (input?.value || '')
    .split('\n')
    .map(s=>s.trim())
    .filter(Boolean);

  if(!questions.length){
    toast('请至少输入一条问题', 'error');
    return;
  }

  regressionReports = [];
  btn.disabled = true;
  btn.textContent = '⏳ 回归中…';
  if(copyBtn) copyBtn.disabled = true;
  if(statusEl) statusEl.textContent = `正在使用 ${currentAskEngine} 引擎执行 0/${questions.length}`;
  if(summaryEl) summaryEl.textContent = `待执行 ${questions.length} 条`;
  if(resultsEl) resultsEl.innerHTML = '';

  let passed = 0;
  let failed = 0;
  for(let i=0;i<questions.length;i++){
    const question = questions[i];
    appendRegressionPending(question, i + 1);
    try{
      const payload = {question};
      const path = getRegressionEndpoint();
      const result = await apiFetch(path, {method:'POST', body: JSON.stringify(payload)});
      const report = buildRegressionReport(question, result, i + 1);
      regressionReports.push(report);
      if(result.error || result.guard_ok === false) failed += 1;
      else passed += 1;
      renderRegressionResult(report);
    }catch(e){
      const report = buildRegressionReport(question, {error: e.message}, i + 1);
      regressionReports.push(report);
      failed += 1;
      renderRegressionResult(report);
    }
    if(statusEl) statusEl.textContent = `正在使用 ${currentAskEngine} 引擎执行 ${i + 1}/${questions.length}`;
    if(summaryEl) summaryEl.textContent = `已完成 ${i + 1}/${questions.length} · 通过 ${passed} · 失败 ${failed}`;
  }

  if(statusEl) statusEl.textContent = `执行完成：${questions.length} 条`;
  if(summaryEl) summaryEl.textContent = `共 ${questions.length} 条 · 通过 ${passed} · 失败 ${failed}`;
  if(copyBtn) copyBtn.disabled = regressionReports.length === 0;
  btn.disabled = false;
  btn.textContent = '▶ 开始回归';
  toast(`回归完成：通过 ${passed}，失败 ${failed}`);
}

function appendRegressionPending(question, index){
  const resultsEl = document.getElementById('regression-results');
  if(!resultsEl) return;
  const empty = resultsEl.querySelector('.empty-state');
  if(empty) resultsEl.innerHTML = '';
  const block = document.createElement('div');
  block.className = 'step-card running';
  block.id = `regression-item-${index}`;
  block.style.marginBottom = '12px';
  block.innerHTML = `
    <div class="step-header" style="cursor:default">
      <div class="step-icon running">↻</div>
      <span class="step-name">[${index}] ${esc(question)}</span>
      <span class="step-dur">执行中</span>
    </div>
    <div class="step-detail open">
      <div class="detail-row"><span class="detail-label">状态</span><span class="detail-val">等待结果…</span></div>
    </div>
  `;
  resultsEl.appendChild(block);
}

function buildRegressionReport(question, result, index){
  const trace = result.trace || {};
  const steps = trace.steps || [];
  const status = result.error || result.guard_ok === false ? 'error' : 'ok';
  const stepSummaries = steps.map((step, idx)=>({
    index: idx + 1,
    name: step.name || '',
    label: STEP_LABELS[step.name] || step.label || step.name || `step_${idx + 1}`,
    status: step.status || '',
    duration_ms: step.duration_ms,
    note: step.note || '',
    outputs: step.outputs || {},
    error: step.error || '',
  }));

  const formattedText = [
    `# [${index}] ${question}`,
    `engine: ${currentAskEngine}`,
    `path: ${result.path || '-'}`,
    `guard_ok: ${result.guard_ok === undefined ? '-' : String(result.guard_ok)}`,
    `attempts: ${result.attempts ?? '-'}`,
    `error: ${result.error || result.guard_reason || '-'}`,
    '',
    '## SQL',
    result.sql || '-',
    '',
    '## Trace Summary',
    ...stepSummaries.map(step => [
      `- step_${step.index}: ${step.label}`,
      `  name: ${step.name}`,
      `  status: ${step.status}`,
      `  duration_ms: ${step.duration_ms ?? '-'}`,
      `  note: ${step.note || '-'}`,
      `  outputs: ${JSON.stringify(step.outputs, null, 2)}`,
      `  error: ${step.error || '-'}`,
    ].join('\n')),
    '',
    '## Raw Result JSON',
    JSON.stringify(result, null, 2),
  ].join('\n');

  return {
    index,
    question,
    status,
    result,
    trace,
    steps: stepSummaries,
    formattedText,
  };
}

function renderRegressionResult(report){
  const el = document.getElementById(`regression-item-${report.index}`);
  if(!el) return;
  const result = report.result || {};
  const sql = result.sql || '';
  const stepHtml = report.steps.map(step => `
    <details style="margin-top:8px">
      <summary style="cursor:pointer;color:var(--text);font-size:12px">
        ${esc(step.label)} · ${esc(step.status || '-')} · ${step.duration_ms != null ? `${step.duration_ms.toFixed(0)}ms` : '-'}
      </summary>
      <pre style="margin-top:8px;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:10px;font-size:11.5px;overflow:auto"><code>${esc(JSON.stringify({
        name: step.name,
        status: step.status,
        note: step.note,
        outputs: step.outputs,
        error: step.error,
      }, null, 2))}</code></pre>
    </details>
  `).join('');

  el.className = `step-card ${report.status}`;
  el.innerHTML = `
    <div class="step-header" style="cursor:default">
      <div class="step-icon ${report.status}">${report.status === 'ok' ? '✓' : '✗'}</div>
      <span class="step-name">[${report.index}] ${esc(report.question)}</span>
      <span class="step-dur">${esc(result.path || '-')}</span>
    </div>
    <div class="step-detail open">
      <div class="detail-row"><span class="detail-label">执行引擎</span><span class="detail-val">${esc(currentAskEngine)}</span></div>
      <div class="detail-row"><span class="detail-label">路径</span><span class="detail-val highlight">${esc(result.path || '-')}</span></div>
      <div class="detail-row"><span class="detail-label">Guard</span><span class="detail-val">${result.guard_ok === undefined ? '-' : (result.guard_ok ? '通过' : '失败')}</span></div>
      <div class="detail-row"><span class="detail-label">尝试次数</span><span class="detail-val">${result.attempts ?? '-'}</span></div>
      <div class="detail-row"><span class="detail-label">错误</span><span class="detail-val" style="color:${report.status === 'ok' ? 'var(--text2)' : 'var(--red)'}">${esc(result.error || result.guard_reason || '-')}</span></div>
      <div class="detail-subtitle">SQL</div>
      <pre style="background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:10px;font-size:11.5px;overflow:auto"><code class="language-sql">${esc(sql || '-')}</code></pre>
      <div class="detail-subtitle">关键步骤</div>
      ${stepHtml || '<div class="form-hint">无 trace 详情</div>'}
      <div class="detail-subtitle" style="display:flex;align-items:center;justify-content:space-between;gap:8px">
        <span>原始 Result JSON</span>
        <button class="btn btn-sm btn-secondary" onclick="copyRegressionItem(${report.index})">📋 复制本条报告</button>
      </div>
      <pre style="background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:10px;font-size:11.5px;overflow:auto"><code>${esc(JSON.stringify(result, null, 2))}</code></pre>
    </div>
  `;
  Prism.highlightAll();
}

function copyRegressionItem(index){
  const report = regressionReports.find(item => item.index === index);
  if(!report){
    toast('未找到对应报告', 'error');
    return;
  }
  navigator.clipboard.writeText(report.formattedText).then(()=>toast(`已复制第 ${index} 条报告`));
}

function copyRegressionReport(){
  if(!regressionReports.length){
    toast('还没有可复制的报告', 'error');
    return;
  }
  const combined = regressionReports.map(r => r.formattedText).join('\n\n' + '='.repeat(100) + '\n\n');
  navigator.clipboard.writeText(combined).then(()=>toast('已复制全部回归报告'));
}

/* ════════════════════════════════════════════════════════════════════════
   调用日志
════════════════════════════════════════════════════════════════════════ */
async function loadLogs(){
  const listEl = document.getElementById('log-list');
  const detailEl = document.getElementById('log-detail');
  const statsEl = document.getElementById('log-stats');
  if(listEl) listEl.innerHTML = '<div style="padding:24px;text-align:center;color:var(--text3)">加载中…</div>';
  if(detailEl) detailEl.innerHTML = '<div class="empty-state"><div class="empty-icon">⏳</div><div class="empty-text">正在加载调用日志…</div></div>';
  try{
    const res=await apiFetch('/traces?n=30');
    const traces=res.traces||[];
    const stats=res.stats||{};
    if(statsEl) statsEl.textContent =
      `共${stats.total||0}条 · 成功率${stats.success_rate||'—'} · 均${stats.avg_ms||0}ms`;
    renderLogList(traces);
    if(traces.length){
      const first = traces[0];
      const firstEl = document.querySelector('.log-item');
      await showLogDetail(first.trace_id, firstEl);
    }else if(detailEl){
      detailEl.innerHTML = '<div class="empty-state"><div class="empty-icon">📋</div><div class="empty-text">暂无记录</div></div>';
    }
  }catch(e){
    if(listEl) listEl.innerHTML=`<div style="padding:16px;color:var(--red)">${esc(e.message)}</div>`;
    if(detailEl) detailEl.innerHTML=`<div style="padding:16px;color:var(--red)">${esc(e.message)}</div>`;
  }
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

async function showLogDetail(traceId, el){
  document.querySelectorAll('.log-item').forEach(i=>i.classList.remove('selected'));
  if(el) el.classList.add('selected');
  const detailEl = document.getElementById('log-detail');
  if(detailEl){
    detailEl.innerHTML = '<div class="empty-state"><div class="empty-icon">⏳</div><div class="empty-text">正在加载详情…</div></div>';
  }
  try{
    const t = await apiFetch('/traces/'+traceId);
    const steps=t.steps||[];
    detailEl.innerHTML=`
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
  }catch(e){
    if(detailEl) detailEl.innerHTML=`<div style="padding:16px;color:var(--red)">${esc(e.message)}</div>`;
  }
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
    document.getElementById('cfg-semantic-sql-rag').checked=!!c.semantic_sql_rag_enabled;
    document.getElementById('cfg-cube-store-db').value=c.cube_store_database||'cube_store';
    document.getElementById('cfg-cube-reload-db').checked=!!c.cube_model_reload_each_request;
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
    semantic_sql_rag_enabled:document.getElementById('cfg-semantic-sql-rag').checked,
    cube_store_database:document.getElementById('cfg-cube-store-db').value||'cube_store',
    cube_model_reload_each_request:document.getElementById('cfg-cube-reload-db').checked,
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
  // 健康检查 & 顶栏状态更新
  try{
    const h=await apiFetch('/health');
    document.getElementById('status-dot').className='status-dot '+(h.doris==='fail'?'err':'ok');
  }catch(e){ document.getElementById('status-dot').className='status-dot err'; }

  // 数据截止时间（取昨天日期）
  const yesterday = new Date(Date.now() - 86400000);
  const yStr = yesterday.toLocaleDateString('zh-CN',{year:'numeric',month:'2-digit',day:'2-digit'}).replace(/\//g,'-');
  const cutoffEl = document.getElementById('data-cutoff');
  if(cutoffEl) cutoffEl.textContent = yStr + ' 09:00';

  setAskEngine('cube');
  await showPage('query');
})();
