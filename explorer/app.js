const $ = id => document.getElementById(id);
const fmt = n => new Intl.NumberFormat('en-US').format(n);
let data, current = 0, visible = [], selected;
const safeURL = value => {
  try { const u = new URL(value); return ['https:', 'http:'].includes(u.protocol) ? u.href : ''; }
  catch { return ''; }
};
function node(tag, text, cls) {
  const el = document.createElement(tag); if (text !== undefined) el.textContent = text;
  if (cls) el.className = cls; return el;
}
function sourceLink(text, url) {
  const a = node('a', text); a.href = safeURL(url); a.target = '_blank'; a.rel = 'noopener'; return a;
}
function selectWord(id) {
  const row = data.pages[current], word = row.words.find(w => w.id === id);
  if (!word) return;
  selected = id;
  document.querySelectorAll('[data-word-id]').forEach(el => el.classList.toggle('selected', el.dataset.wordId === id));
  $('selection').textContent = `${word.text} · pixels [${word.bbox.join(', ')}] · inherited line score ${word.confidence.toFixed(1)}/100 (uncalibrated)`;
}
function renderList() {
  const q = $('search').value.trim().toLocaleLowerCase();
  visible = data.pages.map((p, i) => ({p, i})).filter(({p}) => !q || [p.page_text,p.source_title_ar,p.source_title_en,p.source_provider,p.document_id].join(' ').toLocaleLowerCase().includes(q)).map(x => x.i);
  $('examples').replaceChildren(); $('no-results').hidden = visible.length > 0;
  for (const i of visible) {
    const p = data.pages[i], b = node('button',undefined,'example'); b.classList.toggle('active',i===current);
    b.type='button'; b.setAttribute('aria-label',`Open ${p.document_id}, source page ${p.source_pdf_page_index+1}`);
    const thumb = node('img'); thumb.src=p.sample_image;thumb.alt='';thumb.loading='lazy';
    const label=node('span'), title=node('span',p.source_title_ar||p.source_title_en||p.document_id,'example-title');
    title.lang='ar'; title.dir='auto'; label.append(title,node('small',`${p.source_pdf_page_index+1} / ${p.source_pdf_page_count} · ${p.source_year_label||'Historical print'}`));
    b.append(thumb,label);b.addEventListener('click',()=>showPage(i));$('examples').append(b);
  }
}
function showPage(index) {
  current=index;selected=null;const p=data.pages[index];renderList();
  $('scan').src=p.sample_image;$('scan').alt=`Historical Arabic scanned page from source record ${p.document_id}, PDF page ${p.source_pdf_page_index+1}`;
  $('page-number').textContent=`Example ${index+1} / ${data.pages.length}`;
  $('previous').disabled=index===0;$('next').disabled=index===data.pages.length-1;
  $('overlay').setAttribute('viewBox',`0 0 ${p.width} ${p.height}`);$('overlay').replaceChildren();
  for(const w of p.words){
    const r=document.createElementNS('http://www.w3.org/2000/svg','rect');
    r.setAttribute('x',w.bbox[0]);r.setAttribute('y',w.bbox[1]);r.setAttribute('width',w.bbox[2]-w.bbox[0]);r.setAttribute('height',w.bbox[3]-w.bbox[1]);
    r.dataset.wordId=w.id;r.setAttribute('aria-label',w.text);r.addEventListener('click',()=>selectWord(w.id));$('overlay').append(r);
  }
  $('transcription').replaceChildren();const map=new Map(p.words.map(w=>[w.id,w]));
  for(const line of p.lines){const paragraph=node('p');for(const id of line.word_ids){const w=map.get(id);if(!w)continue;const b=node('button',w.text);b.type='button';b.dataset.wordId=id;b.addEventListener('click',()=>selectWord(id));paragraph.append(b,document.createTextNode(' '));}$('transcription').append(paragraph);}
  if(!p.words.length)$('transcription').append(node('p','No words were recognized on this page.'));
  $('selection').textContent='Select a word in the page or transcription.';
  const source=$('source-view');source.replaceChildren(node('p','SOURCE CATALOG TITLE','machine-label'),node('h3',p.source_title_en||p.source_title_ar||p.document_id));
  const dl=node('dl');
  const fields=[['Source institution',p.source_provider],['Edition metadata',p.source_publication_date],['PDF page (one-based display)',`${p.source_pdf_page_index+1} / ${p.source_pdf_page_count}`],['Published raster',`${p.width} × ${p.height} pixels`],['OCR words / lines',`${p.words_count} / ${p.lines_count}`],['Mechanical screen',p.curated_eligible?'Passed text/script/encoding checks':'Flagged or sparse; not an accuracy score'],['Page identifier',p.page_id],['PDF SHA-256',p.source_pdf_sha256],['Image SHA-256',p.image_sha256],['OCR engine',`${p.ocr_engine} · revision ${p.ocr_engine_revision}`],['Rights basis',p.source_rights_basis],['Original rights statement',p.source_rights]];
  for(const [label,value]of fields){dl.append(node('dt',label),node('dd',value??'Not recorded'));}source.append(dl);
  source.append(sourceLink('Original book ↗',p.source_url),sourceLink('Source manifest ↗',p.source_manifest_url),sourceLink('Original PDF ↗',p.source_pdf_url));
  $('canvas-scroll').scrollTop=0;$('text-view').scrollTop=0;
}
async function start(){
  const response=await fetch('samples.json');if(!response.ok)throw Error(`Example data returned HTTP ${response.status}`);data=await response.json();
  if(!Array.isArray(data.pages)||!data.pages.length)throw Error('No actual page examples were packaged.');
  const s=data.summary;$('stats').replaceChildren();
  for(const [value,label]of [[s.unique_image_pages,'unique real page images'],[s.source_documents_with_images,'source books with images'],[s.machine_word_records,'machine word records'],[s.community_text_records,'separate community-text records']]){const item=node('div',undefined,'stat');item.append(node('b',fmt(value)),node('span',label));$('stats').append(item);}
  $('sample-note').textContent=`${data.pages.length} selected examples from snapshot ${s.version}. This browser shows a small subset; use the dataset for the complete ${fmt(s.unique_image_pages)}-image release. Source page numbers refer to PDF position, not printed page labels.`;
  $('snippet').textContent=`from datasets import load_dataset\n\npages = load_dataset(\n    "Bayanat-Labs/arabic-document-atlas",\n    "pages", split="train",\n    streaming=True, revision="${s.version}"\n)\npage = next(iter(pages))`;
  $('search').addEventListener('input',renderList);$('previous').addEventListener('click',()=>showPage(Math.max(0,current-1)));$('next').addEventListener('click',()=>showPage(Math.min(data.pages.length-1,current+1)));
  $('boxes').addEventListener('change',()=>{$('overlay').style.display=$('boxes').checked?'':'none';});$('zoom').addEventListener('change',()=>{$('page-canvas').classList.toggle('enlarged',$('zoom').checked);});
  for(const name of ['text','source'])$(name+'-tab').addEventListener('click',()=>{for(const other of ['text','source']){$(other+'-tab').classList.toggle('active',name===other);$(other+'-tab').setAttribute('aria-selected',String(name===other));$(other+'-view').hidden=name!==other;}});
  showPage(0);
}
start().catch(error=>{$('error').textContent=`The example viewer could not load: ${error.message}. The complete dataset and pipeline links above remain available.`;$('error').hidden=false;});
