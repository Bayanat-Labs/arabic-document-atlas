"""Read-only analysis of a quiet Atlas build, with explicit count denominators.

No image downloads, source changes, benchmark scores or corpus-size projections.
The SQLite snapshot is checked for concurrent changes after its read transaction.
"""
from __future__ import annotations
import argparse
from collections import Counter,defaultdict
from datetime import datetime,timezone
import gzip
import hashlib
import json
import math
from pathlib import Path
import sqlite3

SOURCE=Path(__file__).resolve().parents[1]


def sha_file(path):
    digest=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(8*1024**2),b''):digest.update(block)
    return digest.hexdigest()


def record_hash(book):
    return hashlib.sha256(json.dumps({k:v for k,v in book.items() if not k.startswith('_')},sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()


def percentile(values,p):
    """Exact observed counts with linear-interpolated quantile (Hyndman/Fan type7)."""
    if not values:return None
    values=sorted(values);position=(len(values)-1)*p
    low=math.floor(position);high=math.ceil(position)
    return values[low]+(values[high]-values[low])*(position-low)


def distribution(values):
    values=list(values)
    return {'count':len(values),'total':sum(values),'minimum':min(values) if values else None,
            'maximum':max(values) if values else None,'mean':sum(values)/len(values) if values else None,
            'p05':percentile(values,.05),'p25':percentile(values,.25),'p50':percentile(values,.5),
            'p75':percentile(values,.75),'p95':percentile(values,.95),'p99':percentile(values,.99),
            'quantile_method':'linear interpolation on sorted observed values (type7)'}


def failure_category(error):
    """Attempt categorization, not a diagnosis of intrinsic page quality."""
    text=str(error).casefold()
    if 'timeout' in text or 'timed out' in text:return 'native_timeout'
    if 'invalid input range' in text or 'localization was unavailable' in text:return 'native_word_localization_error'
    if 'native ocr' in text:return 'other_native_ocr_error'
    if any(x in text for x in ('bbox','geometry','word_membership','line_word','line_id','invalid_dimension')):return 'geometry_or_annotation_structure_validation'
    if any(x in text for x in ('line_text','page_text','engine_line','text_reconstruction')):return 'text_reconstruction_validation'
    if any(x in text for x in ('provenance','required_string','unsafe_or_missing_url','sha256','id_mismatch')):return 'provenance_or_identity_validation'
    if any(x in text for x in ('unicode','surrogate','forbidden_control')):return 'unicode_validation'
    if any(x in text for x in ('pdfium','render','image could not be decoded','cannot identify image')):return 'pdf_render_or_image_decode_error'
    if any(x in text for x in ('storage floor','no space','disk')):return 'local_storage_error'
    return 'other_or_unclassified'


def catalog_year(book):
    years=sorted({x for x in book.get('publication_year_tokens',[]) if isinstance(x,int) and not isinstance(x,bool)})
    return str(years[0]) if len(years)==1 else 'ambiguous_or_missing'


def coverage(books,counts,*,providers=False):
    """Fractional allocation preserves each denominator if multi-valued provider."""
    rows=defaultdict(lambda:{'source_book_weight':0.0,'retained_image_page_weight':0.0,'successful_source_occurrence_weight':0.0,'source_book_memberships':0})
    for doc,book in books.items():
        count=counts.get(doc,{'pages':0,'occurrences':0})
        if not count['occurrences']:continue
        labels=sorted({str(x).strip() for x in book.get('provider',[]) if str(x).strip()}) if providers else [catalog_year(book)]
        labels=labels or ['not_recorded'];weight=1/len(labels)
        for label in labels:
            item=rows[label];item['source_book_weight']+=weight;item['retained_image_page_weight']+=count['pages']*weight
            item['successful_source_occurrence_weight']+=count['occurrences']*weight;item['source_book_memberships']+=1
    book_total=sum(x['occurrences']>0 for x in counts.values());pages_total=sum(x['pages'] for x in counts.values());occ_total=sum(x['occurrences'] for x in counts.values())
    return {'allocation':'equal fractional allocation across distinct recorded provider labels' if providers else 'one unambiguous source catalog publication-year token; ambiguous/missing retained as separate label',
            'denominators':{'source_books_with_successful_occurrences':book_total,'retained_unique_images':pages_total,'successful_source_occurrences':occ_total},
            'rows':[{'label':label,**item,'source_book_share':item['source_book_weight']/book_total if book_total else None,
                     'retained_image_page_share':item['retained_image_page_weight']/pages_total if pages_total else None,
                     'successful_source_occurrence_share':item['successful_source_occurrence_weight']/occ_total if occ_total else None}
                    for label,item in sorted(rows.items())],
            'caveat':'Image-page attribution follows the first retained exact pixel copy; occurrence attribution preserves duplicate source origins. These are processed eligible books, not the entire ACO catalog. Catalog metadata is not independently verified content/date labeling for every bound work.'}


def throughput(log_path,expected):
    if not log_path:return {'available':False,'reason':'No run-specific progress log supplied; restart timing is not inferred from aggregate state.'}
    records=[];ignored=0
    for line in Path(log_path).read_text().splitlines():
        try:item=json.loads(line)
        except (ValueError,TypeError):ignored+=1;continue
        if item.get('stage')=='progress':records.append(item)
    segments=[];current=[]
    for item in records:
        required=('elapsed_seconds','pages','processed_source_occurrences','words','shards','updated_utc')
        if any(k not in item for k in required):continue
        try:
            elapsed=float(item['elapsed_seconds']);stamp=datetime.fromisoformat(item['updated_utc'].replace('Z','+00:00'))
            if not math.isfinite(elapsed) or elapsed<0:continue
            if any(not isinstance(item[k],int) or item[k]<0 for k in ('pages','processed_source_occurrences','words','shards')):continue
        except (ValueError,TypeError):continue
        item={**item,'_timestamp':stamp}
        if current and (elapsed<current[-1]['elapsed_seconds'] or stamp<current[-1]['_timestamp'] or any(item[k]<current[-1][k] for k in ('pages','processed_source_occurrences','words','shards'))):
            segments.append(current);current=[]
        current.append(item)
    if current:segments.append(current)
    base={'log_sha256':sha_file(log_path),'progress_records':len(records),'detected_run_segments':len(segments),'ignored_non_json_lines':ignored,
          'scope':'Observed interval within the final monotonic run segment, including download/render/OCR/validation/upload/wait time; not isolated OCR speed. Restarts are never pooled.'}
    if not segments or len(segments[-1])<2:return {**base,'available':False,'reason':'Fewer than two valid progress observations in final run segment.'}
    segment=segments[-1];first,last=segment[0],segment[-1]
    if any(last[k]!=expected[k] for k in ('pages','processed_source_occurrences','words','shards')):
        return {**base,'available':False,'reason':'Final log counters do not match the quiet SQLite snapshot.'}
    seconds=last['elapsed_seconds']-first['elapsed_seconds'];delta=last['processed_source_occurrences']-first['processed_source_occurrences']
    if seconds<=0 or delta<=0:return {**base,'available':False,'reason':'No positive observed work interval in final run segment.'}
    return {**base,'available':True,'observed_interval_seconds':seconds,'first_observation_utc':first['updated_utc'],'last_observation_utc':last['updated_utc'],
            'new_successful_source_occurrences':delta,'new_retained_unique_images':last['pages']-first['pages'],
            'successful_source_occurrences_per_second':delta/seconds,'retained_unique_images_per_second':(last['pages']-first['pages'])/seconds,
            'omitted_initial_elapsed_seconds':first['elapsed_seconds'],'earlier_run_segments_not_combined':max(0,len(segments)-1)}


def collect_report(output,books_path,progress_log=None):
    output=Path(output).resolve();books_path=Path(books_path).resolve()
    db=sqlite3.connect(f'file:{output/"private/build.sqlite"}?mode=ro',uri=True);db.row_factory=sqlite3.Row
    try:
        start_version=db.execute('PRAGMA data_version').fetchone()[0];db.execute('BEGIN')
        bindings={r['document_id']:dict(r) for r in db.execute('SELECT * FROM documents')}
        if any(r['status']=='processing' for r in bindings.values()):raise ValueError('Build is not quiet: a document is processing')
        shards=[dict(r) for r in db.execute('SELECT * FROM shards ORDER BY path')]
        if any(not r['uploaded'] for r in shards):raise ValueError('Build is not ready: unverified/unuploaded shards remain')
        books={}
        with gzip.open(books_path,'rt',encoding='utf-8') as stream:
            for line in stream:
                if not line.strip():continue
                book=json.loads(line);doc=book['source_book_id']
                if doc not in bindings:continue
                if doc in books:raise ValueError('Repeated bound source book in inventory')
                bound=bindings[doc]
                if book['manifest_sha256']!=bound['manifest_sha256'] or record_hash(book)!=bound['book_record_sha256']:
                    raise ValueError('Book metadata differs from frozen document binding: '+doc)
                books[doc]=book
        if set(books)!=set(bindings):raise ValueError('Inventory missing a document in build state')
        pages=[dict(r) for r in db.execute('SELECT * FROM pages ORDER BY page_id')]
        occurrences=[dict(r) for r in db.execute('SELECT * FROM occurrences ORDER BY page_id')]
        page_map={r['page_id']:r for r in pages};occ_map={(r['document_id'],r['page_index']):r for r in occurrences}
        duplicates=[r for r in occurrences if r['is_duplicate']]
        if len(occurrences)!=len(pages)+len(duplicates):raise ValueError('Retained/duplicate occurrence totals disagree')
        if len({r['pixel_sha256'] for r in pages})!=len(pages):raise ValueError('Retained exact pixel hashes are not unique')
        if any(r['retained_page_id'] not in page_map or r['pixel_sha256']!=page_map[r['retained_page_id']]['pixel_sha256'] for r in occurrences):raise ValueError('Occurrence points to an absent or different retained image')
        if sum(r['rows'] for r in shards if r['kind']=='pages')!=len(pages):raise ValueError('Page shard rows do not match retained images')
        if sum(r['rows'] for r in shards if r['kind']=='duplicate_occurrences')!=len(duplicates):raise ValueError('Duplicate shard rows do not match occurrences')
        counts={doc:{'pages':0,'occurrences':0} for doc in books}
        for row in pages:counts[row['document_id']]['pages']+=1
        for row in occurrences:counts[row['document_id']]['occurrences']+=1
        failures=[dict(r) for r in db.execute('SELECT * FROM failures ORDER BY document_id,page_index,recorded_at')]
        failure_groups=defaultdict(list)
        for row in failures:failure_groups[(row['document_id'],row['page_index'])].append(row)
        resolved=Counter();categories=defaultdict(lambda:{'attempts':0,'source_pages':set()})
        for key,attempts in failure_groups.items():
            success=occ_map.get(key);state='later_retained_image' if success and not success['is_duplicate'] else 'later_exact_duplicate_success' if success else 'no_successful_occurrence'
            resolved[state]+=1
            for row in attempts:
                cat=categories[failure_category(row['error'])];cat['attempts']+=1;cat['source_pages'].add(key)
        text_groups=defaultdict(list)
        for row in pages:
            if row['text_hash']:text_groups[row['text_hash']].append(row)
        text_duplicates=[(key,rows) for key,rows in text_groups.items() if len(rows)>1]
        histogram={label:sum(low<=r['words']<=high for r in pages) for label,low,high in [('0',0,0),('1-19',1,19),('20-99',20,99),('100-299',100,299),('300-599',300,599),('600+',600,10**12)]}
        config_path=output/'private/processing-config.json';config=json.loads(config_path.read_text())
        # Hash the actual ordered analysis input, not the mutable SQLite/WAL files.
        digest=hashlib.sha256()
        for name,rows in [('documents',sorted(bindings.values(),key=lambda r:r['document_id'])),('pages',pages),('occurrences',occurrences),('shards',shards),('failures',failures)]:
            for row in rows:digest.update((json.dumps([name,row],sort_keys=True,separators=(',',':'),ensure_ascii=False)+'\n').encode())
        states=Counter(r['status'] for r in bindings.values())
        total_words=sum(r['words'] for r in pages);screened=sum(r['curated'] for r in pages)
        notes_path=SOURCE/'quality/source_metadata_notes.json';notes=json.loads(notes_path.read_text()) if notes_path.exists() else []
        notes=[r for r in notes if r.get('source_document_id') in counts and counts[r['source_document_id']]['occurrences']]
        report={
            'report_version':'1','generated_at_utc':datetime.now(timezone.utc).isoformat(),
            'analysis_input_sha256':digest.hexdigest(),'source_inventory_sha256':sha_file(books_path),'processing_config_sha256':config['processing_config_sha256'],
            'scope':'Quiet build snapshot of actually processed source books; not an inventory-wide census or accuracy benchmark.',
            'counts':{'retained_unique_images':len(pages),'successful_source_occurrences':len(occurrences),'exact_pixel_duplicate_occurrences':len(duplicates),
                      'machine_word_records':total_words,'source_books_enrolled_in_state':len(bindings),
                      'source_books_with_retained_images':sum(x['pages']>0 for x in counts.values()),'source_books_with_successful_occurrences':sum(x['occurrences']>0 for x in counts.values()),
                      'source_books_with_no_successful_occurrence':sum(x['occurrences']==0 for x in counts.values()),'document_status_counts':dict(sorted(states.items()))},
            'words_per_retained_image':{**distribution(r['words'] for r in pages),'histogram':histogram,'zero_recognized_word_pages':histogram['0'],'zero_word_caveat':'No recognized words is not proof that a page is visually blank.'},
            'mechanical_screen':{'passed_retained_images':screened,'denominator_retained_images':len(pages),'pass_share':screened/len(pages) if pages else None,
                                 'policy':'At least20 words, Arabic-script letter fraction>=0.5, replacement-character fraction<=0.005, no disallowed private-use/bidi review flags, valid geometry/provenance and verified image bytes.',
                                 'caveat':'Mechanical text/script/encoding selection only. Not measured OCR accuracy, language identification, calibrated confidence, manual review or gold labels.'},
            'provider_coverage':coverage(books,counts,providers=True),'catalog_publication_year_coverage':coverage(books,counts),
            'duplicates':{'exact_pixel_duplicate_occurrences':len(duplicates),'exact_pixel_group_count':len({r['pixel_sha256'] for r in duplicates}),
                          'exact_pixel_duplicate_share_of_successful_occurrences':len(duplicates)/len(occurrences) if occurrences else None,
                          'normalized_long_text_duplicate_groups':len(text_duplicates),'retained_images_in_text_duplicate_groups':sum(len(rows) for _,rows in text_duplicates),
                          'additional_text_duplicate_rows_after_first':sum(len(rows)-1 for _,rows in text_duplicates),'cross_document_text_duplicate_groups':sum(len({r['document_id'] for r in rows})>1 for _,rows in text_duplicates),
                          'text_normalization':'Stored build hash: NFC plus collapsed whitespace, minimum80characters; Arabic letter distinctions/digits/diacritics/punctuation/tatweel preserved.',
                          'text_duplicate_policy':'Audit only; equal OCR is not proof of the same physical page or edition. Perceptual candidates are not auto-removed.',
                          'largest_text_groups':[{'normalized_text_sha256':key,'retained_images':len(rows),'source_documents':len({r['document_id'] for r in rows}),'example_page_ids':[r['page_id'] for r in rows[:3]]} for key,rows in sorted(text_duplicates,key=lambda x:(-len(x[1]),x[0]))[:10]]},
            'failures':{'recorded_page_failure_attempts':len(failures),'unique_source_pages_with_a_failed_attempt':len(failure_groups),
                        'unique_failed_source_pages_by_final_success':{k:resolved[k] for k in ('later_retained_image','later_exact_duplicate_success','no_successful_occurrence')},
                        'attempt_reason_categories':[{'category':k,'attempts':v['attempts'],'unique_source_pages':len(v['source_pages'])} for k,v in sorted(categories.items())],
                        'document_errors_without_successful_occurrences':sum(r['status']=='error' and not counts[doc]['occurrences'] for doc,r in bindings.items()),
                        'caveat':'Attempts are not unique pages; a later success can resolve an earlier failure. A page can appear in multiple reason categories. Document preflight/download errors may have no attempted page index or bound page count. Unattempted source pages are not counted as failed.'},
            'shards':{'by_kind':[{ 'kind':kind,'files':sum(r['kind']==kind for r in shards),'rows':sum(r['rows'] for r in shards if r['kind']==kind),'bytes':sum(r['bytes'] for r in shards if r['kind']==kind)} for kind in sorted({r['kind'] for r in shards})],
                      'total_files':len(shards),'total_parquet_bytes':sum(r['bytes'] for r in shards),'all_registered_shards_remote_verified':all(r['uploaded'] for r in shards)},
            'throughput':throughput(progress_log,{'pages':len(pages),'processed_source_occurrences':len(occurrences),'words':total_words,'shards':len(shards)}),
            'known_source_metadata_notes':notes,
            'unmeasured':['OCR character/word error rate','true semantic reading order','human-verified bounding-box accuracy','all near-duplicate or cross-edition overlap','worldwide legal clearance','representativeness of all Arabic documents'],
        }
        db.commit()
        # This check is deliberately OUTSIDE the read transaction, so concurrent
        # commits are visible instead of rereading the same historical snapshot.
        if db.execute('PRAGMA data_version').fetchone()[0]!=start_version:
            raise ValueError('Build changed during report; stop the builder and regenerate from a quiet snapshot')
        return report
    finally:db.close()


def markdown(report):
    c=report['counts'];w=report['words_per_retained_image'];s=report['mechanical_screen'];f=report['failures'];d=report['duplicates']
    lines=['# Image-tier coverage report','',f"This snapshot contains {c['retained_unique_images']:,} retained unique images from {c['source_books_with_retained_images']:,} source books, with {c['machine_word_records']:,} machine word records. It represents processed eligible books, not the complete ACO catalog or a representative sample of all Arabic documents.",'',
           f"Words per retained image: median {w['p50']}, 5th–95th percentile {w['p05']}–{w['p95']}, range {w['minimum']}–{w['maximum']}. {w['zero_recognized_word_pages']:,} pages have no recognized words; this does not establish that they are visually blank.",'',
           f"The mechanical screen passed {s['passed_retained_images']:,} of {s['denominator_retained_images']:,} retained images ({s['pass_share']:.1%}). This measures transparent text/script/encoding criteria, not OCR accuracy or human verification." if s['pass_share'] is not None else 'No retained image denominator is available.','',
           f"Exact decoded-pixel deduplication retained one image for each pixel hash and recorded {d['exact_pixel_duplicate_occurrences']:,} additional source occurrences separately. {d['normalized_long_text_duplicate_groups']:,} normalized long-text groups span multiple retained images; equal OCR is an audit signal and did not authorize deleting distinct scans.",'',
           f"The build recorded {f['recorded_page_failure_attempts']:,} page failure attempts on {f['unique_source_pages_with_a_failed_attempt']:,} distinct source page indices. Final outcomes: {f['unique_failed_source_pages_by_final_success']['later_retained_image']:,} later retained, {f['unique_failed_source_pages_by_final_success']['later_exact_duplicate_success']:,} later succeeded as exact duplicates, and {f['unique_failed_source_pages_by_final_success']['no_successful_occurrence']:,} have no successful occurrence in this snapshot. Document failures and unattempted pages are separate.",'',
           '## Source coverage','', 'Provider and catalog-year tables in `coverage.json` carry three separate denominators: one unit per source book with a successful occurrence, one per retained image, and one per successful source occurrence including duplicates. Multiple provider labels receive equal fractional weights. First-copy image dedup can change provider page weights; occurrence weights preserve the additional source origin. Catalog years and titles are preserved source metadata, not independent labels for every work bound in a volume.','',
           '## Reproducibility and limits','', 'The machine-readable report binds its ordered analysis input, source inventory and processing configuration by SHA-256. It refuses active documents, unverified shards, source-metadata changes or concurrent SQLite commits during reporting. Timing is reported only for a valid observed interval in one monotonic run; restarts are not pooled. No OCR accuracy, semantic reading-order, human box-accuracy, largest-dataset or full-catalog clearance claim is made.','']
    return '\n'.join(lines)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True);p.add_argument('--books',required=True);p.add_argument('--progress-log')
    a=p.parse_args();report=collect_report(a.output,a.books,a.progress_log)
    destination=Path(a.output)/'reports';destination.mkdir(exist_ok=True)
    for name,text in [('coverage.json',json.dumps(report,ensure_ascii=False,indent=2)+'\n'),('COVERAGE.md',markdown(report))]:
        path=destination/name;temp=path.with_suffix(path.suffix+'.tmp');temp.write_text(text);temp.replace(path)
    print(json.dumps({'report':'reports/coverage.json',**report['counts']}))

if __name__=='__main__':main()
