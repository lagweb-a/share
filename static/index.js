/* ===== データロード ===== */
let PLACES = [];
let GEO_TREE = {};
let GEO_READY = false;

// Flask の /api/spots からJSONを取得
async function fetchSpots(params = {}) {
  const usp = new URLSearchParams(params);
  const res = await fetch(`/api/spots?${usp.toString()}`);
  if (!res.ok) throw new Error('スポットデータの取得に失敗しました');
  return res.json();
}

async function fetchGeoTree() {
  const res = await fetch('/api/geo');
  if (!res.ok) throw new Error('地域データの取得に失敗しました');
  return res.json();
}

// API返却: id,name,url,address,lat,lon,tags,description,image_url,price
function normalizeSpot(raw, idx) {
  const id  = raw.id ?? String(idx);
  const lat = (raw.lat !== '' && raw.lat != null && !isNaN(Number(raw.lat))) ? Number(raw.lat) : null;
  const lon = (raw.lon !== '' && raw.lon != null && !isNaN(Number(raw.lon))) ? Number(raw.lon) : null;
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lon);

  // "横浜|赤レンガ|イベント" → ["横浜","赤レンガ","イベント"]
  const tags = typeof raw.tags === 'string'
    ? raw.tags.split('|').map(s=>s.trim()).filter(Boolean)
    : (Array.isArray(raw.tags) ? raw.tags : []);

  // 価格は必要に応じて拡張可能（現状は学割UIなしなので固定で unavailable）
  return {
    id,
    name: raw.name || '',     // ← 
    lat,
    lon,
    hasCoords,
    desc: raw.description || '',
    tags,
    address: raw.address || '',
    url: raw.url || '',
    thumb: raw.image_url,
    region: raw.region || '',
    pref: raw.prefecture || raw.pref || '',
    city: raw.city || '',
    
    // 学割フラグ：データ列が無いのでUI上はオフにしておく
    student: { available:false }
  };
}

function populateRegions(){
  if (!GEO_READY) return;
  const regionSelect = document.getElementById('regionSelect');
  if (!regionSelect) return;
  regionSelect.innerHTML = '<option value="">地方（選択）</option>';
  Object.keys(GEO_TREE).sort((a,b)=>a.localeCompare(b,'ja')).forEach(r=>{
    const opt = document.createElement('option');
    opt.value = r; opt.textContent = r;
    regionSelect.appendChild(opt);
  });
  // 初期化
  const prefSelectEl = document.getElementById('prefSelect');
  const citySelectEl = document.getElementById('citySelect');
  if (prefSelectEl){
    prefSelectEl.innerHTML = '<option value="">都道府県（まず地方を選択）</option>';
    prefSelectEl.disabled = true;
  }
  if (citySelectEl){
    citySelectEl.innerHTML = '<option value="">市区町村（まず都道府県を選択）</option>';
    citySelectEl.disabled = true;
  }
}

const regionSelect = document.getElementById('regionSelect');
const prefSelect   = document.getElementById('prefSelect');
const citySelect   = document.getElementById('citySelect');

function populatePrefs(regionKey){
  if (!GEO_READY) return;
  prefSelect.innerHTML = '<option value="">都道府県（まず地方を選択）</option>';
  citySelect.innerHTML = '<option value="">市区町村（まず都道府県を選択）</option>';
  citySelect.disabled = true;
  const region = GEO_TREE[regionKey];
  if (!region){ prefSelect.disabled = true; return; }
  Object.keys(region.prefs || {}).sort((a,b)=>a.localeCompare(b,'ja')).forEach(pk=>{
    const opt=document.createElement('option'); opt.value=pk; opt.textContent=pk; prefSelect.appendChild(opt);
  });
  prefSelect.disabled = false;
}
function populateCities(regionKey, prefKey){
  if (!GEO_READY) return;
  citySelect.innerHTML = '<option value="">市区町村（任意）</option>';
  const pref = GEO_TREE[regionKey]?.prefs?.[prefKey];
  if (!pref){ citySelect.disabled = true; return; }
  const cityKeys = Object.keys(pref.cities||{}).sort((a,b)=>a.localeCompare(b,'ja'));
  if (cityKeys.length){
    const optAll = document.createElement('option');
    optAll.value = '全域';
    optAll.textContent = '全域';
    citySelect.appendChild(optAll);
  }
  cityKeys.forEach(ck=>{
    const opt=document.createElement('option'); opt.value=ck; opt.textContent=ck; citySelect.appendChild(opt);
  });
  citySelect.disabled = false;
}

let geoScope = { level:null, key:null, prefKey:null, cityKey:null, center:null, radius:null, zoom:null, bbox:null };
let geoCircle = null;

function setGeoScopeByUI(){
  const regionKey = regionSelect?.value || null;
  const prefKey   = prefSelect?.value   || null;
  const rawCity   = citySelect?.value   || null;
  const cityKey   = rawCity && rawCity !== '全域' ? rawCity : null;

  if (cityKey && regionKey && prefKey){
    const c = ((GEO_TREE[regionKey]||{}).prefs?.[prefKey]?.cities||{})[cityKey] || {};
    geoScope = { level:'city', key:cityKey, prefKey, cityKey, center:c.center||null, radius:c.radius||null, zoom:c.zoom||13, bbox:c.bbox||null };
  } else if (prefKey && regionKey){
    const p = ((GEO_TREE[regionKey]||{}).prefs||{})[prefKey] || {};
    geoScope = { level:'pref', key:prefKey, prefKey, cityKey:null, center:p.center||null, radius:p.radius||null, zoom:p.zoom||10, bbox:p.bbox||null };
  } else if (regionKey){
    const r = GEO_TREE[regionKey] || {};
    geoScope = { level:'region', key:regionKey, prefKey:null, cityKey:null, center:r.center||null, radius:r.radius||null, zoom:r.zoom||6, bbox:r.bbox||null };
  } else {
    geoScope = { level:null, key:null, prefKey:null, cityKey:null, center:null, radius:null, zoom:null, bbox:null };
  }
  drawGeoCircle(); applyFilters();
}

function drawGeoCircle(){
  if (!map) return;
  if (geoCircle){ geoCircle.remove(); geoCircle=null; }
  if (geoScope.bbox && Array.isArray(geoScope.bbox) && geoScope.bbox.length===4){
    const [minLon,minLat,maxLon,maxLat] = geoScope.bbox;
    const bounds = L.latLngBounds([ [minLat,minLon], [maxLat,maxLon] ]);
    map.fitBounds(bounds, { padding: [24,24] });
  } else if (geoScope.center && geoScope.zoom){
    map.setView(geoScope.center, geoScope.zoom);
  }
  if (geoScope.center && geoScope.radius){
    geoCircle = L.circle(geoScope.center, { radius:geoScope.radius, color:'#3b82f6', weight:2, dashArray:'6 6', fillColor:'#3b82f6', fillOpacity:.06 }).addTo(map);
  }
}

document.getElementById('clearGeo')?.addEventListener('click', ()=>{
  if (regionSelect) regionSelect.value='';
  if (prefSelect){ prefSelect.innerHTML='<option value="">都道府県（まず地方を選択）</option>'; prefSelect.disabled=true; }
  if (citySelect){ citySelect.innerHTML='<option value="">市区町村（まず都道府県を選択）</option>'; citySelect.disabled=true; }
  geoScope = { level:null, key:null, prefKey:null, cityKey:null, center:null, radius:null, zoom:null, bbox:null };
  if (geoCircle){ geoCircle.remove(); geoCircle=null; }
  applyFilters();
});
regionSelect?.addEventListener('change', ()=>{ populatePrefs(regionSelect.value); setGeoScopeByUI(); });
prefSelect?.addEventListener('change',  ()=>{ populateCities(regionSelect.value, prefSelect.value); setGeoScopeByUI(); });
citySelect?.addEventListener('change',  ()=> setGeoScopeByUI());

/* ===== 便利関数 ===== */
const fmt = n => new Intl.NumberFormat('ja-JP').format(n);
const fmt1= n => new Intl.NumberFormat('ja-JP',{maximumFractionDigits:1}).format(n);
const yen = v => v==null ? '—' : `¥${fmt(v)}`;
const norm= s => (s||'').toString().trim().toLowerCase();
const tokens=q => norm(q).split(/\s+/).filter(Boolean);
const gmapLink = (lat, lon)=> lat!=null && lon!=null ? `https://www.google.com/maps/search/?api=1&query=${lat},${lon}` : '#';
const gmapEmbed = (lat, lon, z=16)=> lat!=null && lon!=null ? `https://www.google.com/maps?q=${lat},${lon}&z=${z}&hl=ja&output=embed` : '';
const R=6371; const haversineKm=(a,b)=>{const r=x=>x*Math.PI/180;const dLat=r(b[0]-a[0]),dLon=r(b[1]-a[1]),la1=r(a[0]),la2=r(b[0]);const h=Math.sin(dLat/2)**2+Math.cos(la1)*Math.cos(la2)*Math.sin(dLon/2)**2;return 2*R*Math.asin(Math.sqrt(h));};
const showToast=m=>{const t=document.getElementById('toast'); if(!t) return; t.textContent=m; t.classList.add('show'); setTimeout(()=>t.classList.remove('show'),1500);};
const starHTML=(n,max=5)=>{ n=Math.max(0,Math.min(max,Math.round(n))); return '<span class="stars">'+Array.from({length:max},(_,i)=>`<span class="star ${i<n?'filled':''}">★</span>`).join('')+'</span>'; };

/* ===== お気に入り（localStorage） ===== */
const FAV_KEY='yokohama.demo.favs.v1';
function loadFavs(){ try{ return new Set(JSON.parse(localStorage.getItem(FAV_KEY)||'[]')); } catch{ return new Set(); } }
function saveFavs(set){ localStorage.setItem(FAV_KEY, JSON.stringify(Array.from(set))); }
let favs = loadFavs();
const isFav = id => favs.has(id);
function toggleFav(id){ if(favs.has(id)) favs.delete(id); else favs.add(id); saveFavs(favs); updateFavCounter(); }
function updateFavCounter(){ const el=document.getElementById('favCounter'); if(el) el.textContent = `★お気に入り ${favs.size}`; }

/* ===== コメント＆評価（localStorage） ===== */
const COMMENTS_KEY='yokohama.demo.placeComments.v1';
function loadAllComments(){ try{ return JSON.parse(localStorage.getItem(COMMENTS_KEY)||'{}'); } catch{ return {}; } }
function saveAllComments(obj){ localStorage.setItem(COMMENTS_KEY, JSON.stringify(obj)); }
function getComments(placeId){ const all=loadAllComments(); return Array.isArray(all[placeId])? all[placeId] : []; }
function setComments(placeId, arr){ const all=loadAllComments(); all[placeId]=arr; saveAllComments(all); }
function addComment(placeId, c){ const arr=getComments(placeId); arr.push(c); setComments(placeId, arr); }
function avgRating(placeId){ const arr=getComments(placeId); if(!arr.length) return {avg:0,count:0}; const s=arr.reduce((a,b)=>a+(+b.rating||0),0); return {avg:s/arr.length, count:arr.length}; }

/* ===== 地図・マーカー ===== */
let map, cluster, selectionLayer=null, centerDot=null, currentLoc=null, selectedId=null;
const defaultCenter=[36.2048,138.2529], defaultZoom=5;
const markerPool = new Map();


function initMap(){
  map = L.map('map', { boxZoom:true }).setView(defaultCenter, defaultZoom);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom:19, attribution:'&copy; OpenStreetMap contributors' }).addTo(map);

  cluster = L.markerClusterGroup();
  map.addLayer(cluster);

  PLACES.filter(p=>p.hasCoords).forEach(p=>{
    const m = L.marker([p.lat,p.lon]);
    m.bindPopup(`<strong>${p.name}</strong><br>${p.desc||''}`);
    m.on('click', ()=>{ selectedId=p.id; highlightSelected(); openPanelFor(p); });
    markerPool.set(p.id, m);
  });
  syncMarkers(new Set(PLACES.filter(p=>p.hasCoords).map(p=>p.id)));

  // タップ＝円
  map.on('click', (e)=> placeCircleAt(e.latlng));
  // Shift+ドラッグ＝四角
  map.on('boxzoomend', (ev)=>{
    if (!ev.boxZoomBounds) return;
    if (selectionLayer){ selectionLayer.remove(); selectionLayer=null; }
    if (centerDot){ centerDot.remove(); centerDot=null; }
    selectionLayer = L.rectangle(ev.boxZoomBounds, {color:'#2563eb',weight:2,fillColor:'#2563eb',fillOpacity:.12,interactive:false}).addTo(map);
    applyFilters();
  });
  map.on('moveend', ()=>{ const bOnlyEl=document.getElementById('boundsOnly'); if (bOnlyEl && bOnlyEl.checked) applyFilters(); });
}

function placeCircleAt(latlng){
  const rEl=document.getElementById('radius'); const r = rEl ? +rEl.value || 600 : 600;
  if (selectionLayer) selectionLayer.remove();
  if (centerDot) centerDot.remove();
  selectionLayer = L.circle(latlng, { radius:r, color:'#3b82f6', fillColor:'#3b82f6', fillOpacity:.12, weight:2 }).addTo(map);
  centerDot = L.circleMarker(latlng, { radius:5, color:'#1d4ed8', fillColor:'#60a5fa', fillOpacity:1, weight:2 }).addTo(map);
  applyFilters();
}

function syncMarkers(keepIds){
  if (!cluster) return;
  const current = new Set();
  cluster.eachLayer(l=>{
    for (const [id, mk] of markerPool) if (mk===l){ current.add(id); break; }
  });
  for (const id of keepIds){ if (!current.has(id)) { const mk=markerPool.get(id); if (mk) cluster.addLayer(mk); } }
  for (const id of current){ if (!keepIds.has(id)) { const mk=markerPool.get(id); if (mk) cluster.removeLayer(mk); } }
}

/* ===== タグバー ===== */
function uniqueTagsFromPlaces(){
  const s = new Set();
  PLACES.forEach(p => (p.tags||[]).forEach(t => s.add(t)));
  return Array.from(s).sort((a,b)=> a.localeCompare(b,'ja'));
}
function buildTagBar(){
  const box = document.getElementById('tagBar');
  if (!box) return;
  box.innerHTML = '';
  uniqueTagsFromPlaces().forEach(tag=>{
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'tagchip';
    btn.textContent = `# ${tag}`;
    btn.dataset.tag = tag;
    btn.addEventListener('click', ()=>{
      if (selectedTags.has(tag)) selectedTags.delete(tag); else selectedTags.add(tag);
      btn.classList.toggle('active', selectedTags.has(tag));
      applyFilters();
    });
    box.appendChild(btn);
  });
}
document.getElementById('clearTags')?.addEventListener('click', ()=>{
  selectedTags.clear();
  document.querySelectorAll('#tagBar .tagchip').forEach(b=> b.classList.remove('active'));
  applyFilters();
});
const selectedTags = new Set();

/* ===== 絞り込み ===== */
function inSelection(lat,lon){
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return false;
  if (!selectionLayer) return true;
  const ll = L.latLng(lat,lon);
  if (selectionLayer instanceof L.Circle)   return ll.distanceTo(selectionLayer.getLatLng()) <= selectionLayer.getRadius();
  if (selectionLayer instanceof L.Rectangle) return selectionLayer.getBounds().contains(ll);
  return true;
}
function inGeoScope(lat,lon){
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return false;
  if (!geoScope.center || !geoScope.radius) return true;
  return L.latLng(lat,lon).distanceTo(L.latLng(geoScope.center[0], geoScope.center[1])) <= geoScope.radius;
}
function filterPlaces(query, opts){
  const ts=tokens(query); let arr=PLACES.slice();
  // 地域→円/四角→表示範囲→タグ→テキスト→学割→お気に入り→距離順
  arr = arr.filter(p=> inGeoScope(p.lat,p.lon));
  if (geoScope.level === 'region' && geoScope.key){
    arr = arr.filter(p => p.region === geoScope.key);
  }
  if (geoScope.prefKey){
    arr = arr.filter(p => {
      if (p.pref === geoScope.prefKey) return true;
      return (p.address || '').includes(geoScope.prefKey);
    });
  }
  if (geoScope.cityKey){
    arr = arr.filter(p => {
      if (p.city === geoScope.cityKey) return true;
      return (p.address || '').includes(geoScope.cityKey);
    });
  }
  arr = arr.filter(p=> inSelection(p.lat,p.lon));
  if (opts.boundsOnly){
    const b=map.getBounds();
    arr=arr.filter(p=> p.hasCoords && b.contains([p.lat,p.lon]));
  }
  if (selectedTags.size){ arr = arr.filter(p => (p.tags||[]).some(t => selectedTags.has(t))); }
  if (ts.length){
    arr=arr.filter(p=>{
      const hay=(p.name+'\u0000'+(p.tags||[]).join('\u0000')).toLowerCase();
      return ts.every(t=>hay.includes(t));
    });
  }
  if (opts.onlyDiscount) arr=arr.filter(p=>p.student?.available);
  if (opts.onlyFavs)     arr=arr.filter(p=> isFav(p.id));
  if (opts.sortByDistance && currentLoc){
    arr.forEach(p=>p._d=p.hasCoords ? haversineKm([currentLoc.lat,currentLoc.lon],[p.lat,p.lon]) : Infinity);
    arr.sort((a,b)=>(a._d||1e9)-(b._d||1e9));
  } else arr.forEach(p=>delete p._d);
  return arr;
}

/* ===== リスト＆ピン同期（カード版） ===== */
function cardImage(p){
  const src = p.thumb || 'https://source.unsplash.com/800x600/?travel,city';
  return `<img src="${src}" alt="${p.name}" onerror="this.style.display='none'">`;
}
function render(list){
  const listEl=document.getElementById('list'); listEl.innerHTML='';
  const rc=document.getElementById('resultCount'); if(rc) rc.textContent=`— ${fmt(list.length)}件`;
  if(!list.length){ listEl.innerHTML='<p class="muted" style="margin:8px">該当スポットが見つかりませんでした。</p>'; }

  list.forEach(p=>{
    const st=p.student; const ri=avgRating(p.id);
    const price=st?.available?`<span class="tag">学生 ${yen(st.price?.student)}${st.price?.adult!=null?`／一般 ${yen(st.price.adult)}`:''}</span>`:'';

    const card=document.createElement('div');
    card.className='card'; card.dataset.id=p.id;
    card.innerHTML=`
      <a class="card-hero" href="javascript:void(0)" data-action="detail">
        ${cardImage(p)}
        ${st?.available?'<span class="badge">学割</span>':''}
        <span class="rating">${starHTML(ri.avg)} <span>${ri.count?ri.avg.toFixed(1):'—'}</span></span>
      </a>
      <div class="card-body">
        <h3>${p.name}</h3>
        <p class="muted" style="margin:.2em 0 .4em">${p.desc||''}</p>
        <div class="row">${(p.tags||[]).map(t=>`<span class="tag">${t}</span>`).join(' ')} ${price||''}</div>
        <div class="row" style="margin-top:6px">
          <button type="button" class="btn primary" data-action="detail">詳細（地図/口コミ/★）</button>
          ${p.hasCoords ? '<button type="button" class="btn" data-action="map">地図で見る</button>' : '<span class="muted">位置情報なし</span>'}
          ${p.hasCoords ? `<a class="btn" href="${gmapLink(p.lat,p.lon)}" target="_blank" rel="noopener">Googleで開く</a>` : ''}
          <button type="button" class="btn fav-btn ${isFav(p.id)?'active':''}" data-action="fav">${isFav(p.id)?'♥':'♡'} お気に入り</button>
           ${p._d!=null && p._d !== Infinity?`<small class="muted">／ 約 ${fmt1(p._d)} km</small>`:''}
        </div>
      </div>`;
    card.querySelectorAll('[data-action="map"]').forEach(b=> b.addEventListener('click', ()=>{
      if (!p.hasCoords) return;
      map.setView([p.lat,p.lon],16);
      const mk=markerPool.get(p.id); if (mk){ mk.openPopup(); }
      selectedId=p.id; highlightSelected();
    }));
    card.querySelectorAll('[data-action="detail"]').forEach(b=> b.addEventListener('click', ()=> openPanelFor(p)));
    card.querySelector('[data-action="fav"]').addEventListener('click', ()=>{
      toggleFav(p.id);
      applyFilters();
    });
    listEl.appendChild(card);
  });

  const keep = new Set(list.filter(p=>p.hasCoords).map(p=>p.id));
  syncMarkers(keep);

  const statEl=document.getElementById('stat');
  if (statEl){
    const label = geoScope.cityKey || geoScope.prefKey || geoScope.key;
    const level = geoScope.cityKey ? 'city' : (geoScope.prefKey ? 'pref' : geoScope.level);
    const stat = (selectionLayer instanceof L.Circle)   ? `選択範囲：円（半径 約 ${document.getElementById('radius')?.value||'-'} m）`
               : (selectionLayer instanceof L.Rectangle)? '選択範囲：四角（Shift+ドラッグで変更）'
               : (label ? `地域：${label}（${level||'region'}）` : '選択範囲：なし（地図をタップ / Shift+ドラッグ）');
    statEl.textContent = stat;
  }
}
function highlightSelected(){ document.querySelectorAll('.card').forEach(el=> el.classList.toggle('selected', el.dataset.id===selectedId)); }

/* ===== 適用／現在地 ===== */
function applyFilters(){
  const q=document.getElementById('q')?.value||'';
  const list=filterPlaces(q,{
    onlyFavs: document.getElementById('onlyFavs')?.checked,
    onlyDiscount: document.getElementById('onlyDiscount')?.checked,
    boundsOnly:   document.getElementById('boundsOnly')?.checked,
    sortByDistance: document.getElementById('sortByDistance')?.checked && !!currentLoc
  });
  render(list);
}
function locate(){
  if(!navigator.geolocation){ showToast('現在地が使えません'); return; }
  navigator.geolocation.getCurrentPosition(pos=>{
    currentLoc={lat:pos.coords.latitude, lon:pos.coords.longitude};
    L.circleMarker([currentLoc.lat,currentLoc.lon]).addTo(map).bindPopup('現在地').openPopup();
    map.setView([currentLoc.lat,currentLoc.lon],12);
    const gs=document.getElementById('geoStatus'); if(gs) gs.textContent=`現在地: ${currentLoc.lat.toFixed(4)}, ${currentLoc.lon.toFixed(4)}`;
    applyFilters();
  }, err=> showToast('位置エラー: '+err.message), {enableHighAccuracy:true, timeout:7000, maximumAge:60000});
}

/* ===== スライドパネル（詳細・Google地図・口コミ・お気に入り） ===== */
const slidePanel = document.getElementById('slidePanel');
const panelTitle = document.getElementById('panelTitle');
const panelContent = document.getElementById('panelContent');
const panelLink = document.getElementById('panelLink');
const favInPanel = document.getElementById('favInPanel');
document.getElementById('closePanel')?.addEventListener('click', ()=>{
  slidePanel?.classList.remove('is-active');
  document.body.style.overflow = ''; // 背面スクロール再開
});

function starHTMLInline(n){ return starHTML(n).replaceAll('class="stars"','class="stars" style="transform:translateY(2px)"'); }
function openPanelFor(p){
  selectedId = p.id;
  const ratingInfo = avgRating(p.id);
  if (panelTitle) panelTitle.textContent = p.name;

  let mapSection = '';
  if (p.hasCoords){
    const embed = gmapEmbed(p.lat,p.lon,16);
    mapSection = `<div class="map-embed"><iframe loading="lazy" referrerpolicy="no-referrer-when-downgrade" src="${embed}" title="${p.name}の地図"></iframe><p><a class="link-on-dark" href="${gmapLink(p.lat,p.lon)}" target="_blank" rel="noopener">Googleマップで開く</a></p></div>`;
  } else {
    mapSection = '<p class="muted-on-dark">位置情報が登録されていません。</p>';
  }

  if (panelContent){
    panelContent.innerHTML = `
      ${mapSection}
      <div class="comment-block">
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
          <div>${starHTMLInline(ratingInfo.avg)}</div>
          <span class="muted-on-dark">${ratingInfo.count? ratingInfo.avg.toFixed(1) : '—'} / 5 ・ ${ratingInfo.count}件</span>
          </div>
        <p class="muted-on-dark" style="margin:.4em 0 0">${p.desc||''}</p>
        <p>${(p.tags||[]).map(t=>`<span class="tag" style="background:#0b1220;color:#e5e7eb;border:1px solid #374151">${t}</span>`).join(' ')}</p>
        ${p.student?.available ? `<p class="muted-on-dark"><strong>学割:</strong> 学生 ${yen(p.student.price?.student)}${p.student.price?.adult!=null?`／一般 ${yen(p.student.price.adult)}`:''}<br><small>${p.student.condition||''}</small></p>` : '<p class="muted-on-dark"><small>学割情報なし</small></p>'}
      </div>

      <div>
        <h4 style="margin:6px 0">口コミを投稿</h4>
        <form id="panelCommentForm" class="comment-form">
          <div class="rating-input" id="ratingInput" aria-label="星評価（1〜5）">
            <button type="button" class="star-btn" data-v="1">★</button>
            <button type="button" class="star-btn" data-v="2">★</button>
            <button type="button" class="star-btn" data-v="3">★</button>
            <button type="button" class="star-btn" data-v="4">★</button>
            <button type="button" class="star-btn" data-v="5">★</button>
            <span class="muted-on-dark">（タップで選択）</span>
            <input type="hidden" id="ratingValue" value="0">
          </div>
          <input id="cName" type="text" placeholder="お名前（任意)">
          <textarea id="cText" rows="3" placeholder="コメントを書く…"></textarea>
          <button type="submit">投稿</button>
        </form>
      </div>

      <div>
        <h4 style="margin:6px 0">みんなの口コミ</h4>
        <div id="panelCommentList"></div>
      </div>
    `;
  }
  updateFavInPanel(p.id);
  if (favInPanel) favInPanel.onclick = ()=>{ toggleFav(p.id); updateFavInPanel(p.id); applyFilters(); };

  // 評価入力
  const ratingInput = document.getElementById('ratingInput');
  const ratingValue = document.getElementById('ratingValue');
  ratingInput.querySelectorAll('.star-btn').forEach(btn=>{
    btn.addEventListener('click', ()=>{
      const v = +btn.dataset.v;
      ratingValue.value = v;
      ratingInput.querySelectorAll('.star-btn').forEach(b=> b.classList.toggle('active', +b.dataset.v <= v));
    });
  });
  document.getElementById('panelCommentForm').addEventListener('submit', async (e)=>{
  e.preventDefault();
  const name = document.getElementById('cName').value.trim();
  const text = document.getElementById('cText').value.trim();
  const rating = +document.getElementById('ratingValue').value || 0;
  if (!text){ showToast('コメントを入力してください'); return; }
  if (!(rating>=1 && rating<=5)){ showToast('星の数を選択してください（1〜5）'); return; }

  // localStorageにも保存
  addComment(p.id, { name, text, rating, ts: Date.now() });

  // サーバーにもPOST
  try {
    await fetch('/api/reviews', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        place_id: p.id,
        place_name: p.name,
        author: name || '名無しさん',
        comment: text,
        rating: rating
      })
    });
  } catch (err) {
    showToast('サーバー保存に失敗しました');
  }

  // 入力欄リセット
  document.getElementById('cText').value = '';
  document.getElementById('cName').value = '';
  ratingValue.value = 0;
  ratingInput.querySelectorAll('.star-btn').forEach(b=> b.classList.remove('active'));

  // localStorageのコメント表示
  renderPanelComments(p.id);
  // 必要ならサーバー側の最新コメント取得も追加可能

  applyFilters();
});
  renderPanelComments(p.id);

  // 公式/学割ページへの導線（なければGoogleマップ）
  if (panelLink){
    const preferredUrl = p.student?.url || p.url;
    if (preferredUrl){
      panelLink.href = preferredUrl;
      panelLink.textContent = p.student?.url ? '学割/公式ページへ' : '公式ページへ';
      panelLink.target = '_blank';
      panelLink.rel = 'noopener';
    } else if (p.hasCoords){
      panelLink.href = gmapLink(p.lat,p.lon);
      panelLink.textContent = 'Googleマップを開く';
      panelLink.target = '_blank';
      panelLink.rel = 'noopener';
    } else {
      panelLink.href = '#';
      panelLink.textContent = 'リンク情報なし';
      panelLink.removeAttribute('target');
      panelLink.removeAttribute('rel');
    }
  }

  slidePanel?.classList.add('is-active');
  document.body.style.overflow = 'hidden';
}
function updateFavInPanel(id){
  if (!favInPanel) return;
  favInPanel.classList.toggle('active', isFav(id));
  favInPanel.textContent = (isFav(id) ? '♥' : '♡') + ' お気に入り';
  updateFavCounter();
}
function renderPanelComments(placeId){
  const listEl = document.getElementById('panelCommentList');
  if (!listEl) return;
  const arr = getComments(placeId);
  if (!arr.length){
    listEl.innerHTML = `<p class="muted-on-dark" style="margin:6px 0">口コミはまだありません。最初のレビューを書きませんか？</p>`;
    return;
  }
  listEl.innerHTML = arr.slice().reverse().map(c=>{
    const when = new Date(c.ts).toLocaleString();
    return `
      <div class="comment-line">
        <div>${starHTML(c.rating)}</div>
        <div style="margin:.2em 0 .2em">${c.text.replace(/</g,'&lt;').replace(/>/g,'&gt;')}</div>
        <small>${c.name||'名無しさん'} ・ ${when}</small>
      </div>`;
  }).join('');
}

/* ===== 起動・イベント ===== */
(async function bootstrap(){
  try {
    const [rawSpots, geoTree] = await Promise.all([
      fetchSpots(),
      fetchGeoTree(),
    ]);
    PLACES = rawSpots.map(normalizeSpot);
    GEO_TREE = geoTree;
    GEO_READY = Object.keys(GEO_TREE).length > 0;
  } catch (e) {
    console.error(e);
    showToast('データ取得に失敗しました');
    PLACES = [];
    GEO_TREE = {};
    GEO_READY = false;
  }
  initMap();
  buildTagBar();
  render(PLACES);
  updateFavCounter();
  populateRegions();
})();

document.getElementById('searchForm')?.addEventListener('submit', e=>{ e.preventDefault(); applyFilters(); });
document.getElementById('q')?.addEventListener('input', ()=> applyFilters());
['onlyFavs','onlyDiscount','boundsOnly'].forEach(id=>{
  const el=document.getElementById(id); el&&el.addEventListener('change', applyFilters);
});
document.getElementById('sortByDistance')?.addEventListener('change', ()=>{
  if (document.getElementById('sortByDistance').checked && !currentLoc) showToast('距離順には現在地が必要です');
  applyFilters();
});
document.getElementById('locateBtn')?.addEventListener('click', locate);

document.getElementById('radius')?.addEventListener('input', ()=>{
  const radiusInput=document.getElementById('radius');
  const radiusVal=document.getElementById('radiusVal');
  if (!radiusInput || !radiusVal) return;
  radiusVal.textContent=`${radiusInput.value}m`;
  document.querySelectorAll('.pill').forEach(p=> p.classList.toggle('active', +p.dataset.r===+radiusInput.value));
  if (selectionLayer instanceof L.Circle){ selectionLayer.setRadius(+radiusInput.value); applyFilters(); }
});
document.querySelectorAll('.pill').forEach(p=>{
  p.addEventListener('click', ()=>{
    const radiusInput=document.getElementById('radius');
    if (!radiusInput) return;
    radiusInput.value=p.dataset.r;
    radiusInput.dispatchEvent(new Event('input'));
  });
});
document.getElementById('centerHere')?.addEventListener('click', ()=>{
  if (!currentLoc){ showToast('まず「現在地」を取得してください'); return; }
  placeCircleAt(L.latLng(currentLoc.lat, currentLoc.lon));
});

document.getElementById('radius')?.addEventListener('touchstart', ()=> map?.dragging?.disable());
document.getElementById('radius')?.addEventListener('touchend',   ()=> map?.dragging?.enable());

document.getElementById('resetBtn')?.addEventListener('click', ()=>{
  const qEl=document.getElementById('q'); if(qEl) qEl.value='';
  ['onlyFavs','onlyDiscount','boundsOnly','sortByDistance'].forEach(id=>{ const el=document.getElementById(id); if(el) el.checked=false; });
  if (selectionLayer){ selectionLayer.remove(); selectionLayer=null; }
  if (centerDot){ centerDot.remove(); centerDot=null; }
  if (geoCircle){ geoCircle.remove(); geoCircle=null; }
  geoScope = { level:null, key:null, prefKey:null, cityKey:null, center:null, radius:null, zoom:null, bbox:null };
  if (regionSelect) regionSelect.value='';
  if (prefSelect){ prefSelect.innerHTML='<option value="">都道府県（まず地方を選択）</option>'; prefSelect.disabled=true; }
  if (citySelect){ citySelect.innerHTML='<option value="">市区町村（まず都道府県を選択）</option>'; citySelect.disabled=true; }
  selectedId=null; currentLoc=null; const gs=document.getElementById('geoStatus'); if(gs) gs.textContent='';
  map.setView(defaultCenter, defaultZoom);
  selectedTags.clear(); document.querySelectorAll('#tagBar .tagchip').forEach(b=> b.classList.remove('active'));
  render(PLACES); updateFavCounter();
});
