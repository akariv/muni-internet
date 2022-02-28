import json
from os import rename
from pathlib import Path
import requests
import shutil
from shapely.geometry import shape, Point
from pyqtree import Index
from fuzzywuzzy.process import extractOne
from decimal import Decimal


def download(url, outfile):
    print(f'Downloading {url}')
    if outfile.exists():
        return outfile
    stream = requests.get(url, stream=True).raw
    with open(outfile, 'wb') as f:
        shutil.copyfileobj(stream, f)
    return outfile


def point_in_db(point, db):
    eps = 1e-6
    x, y = point
    point = Point(x, y)
    bbox = [x - eps, y - eps, x + eps, y + eps]
    if hasattr(db, 'filter'):
        items = db.filter(bbox=bbox)
    elif hasattr(db, 'intersect'):
        items = db.intersect(bbox)
    for item in items:
        geo = shape(item['geometry'])
        if geo.contains(point):
            return item['properties']
    return None


def fiona_to_index(db, bounds):
    bounds = tuple(bounds)
    print(db, bounds)
    index = Index(bbox=bounds)
    count = 0
    for item in db.filter(bbox=bounds):
        geo = shape(item['geometry'])
        # if geo.bounds[0] > bounds[2] or geo.bounds[2] < bounds[0] or geo.bounds[1] > bounds[3] or geo.bounds[3] < bounds[1]:
        #     continue
        index.insert(item, geo.bounds)
        count += 1
    print(f'Indexed {count} items')
    return index


def translate_muni_name(name, seindex, cache):
    if name in cache:
        return

    query_name = {
        'Deir el Asad': 'Deir al Asad',
        'Biane': 'Biina Nujeidat',
        'Emek Lod Regional Council' :'Sdot Dan Regional Council',
        'Jisr ez Zarqa': 'Jisr az-Zarqa',
        'Arara BaNegev' :'Ar\'arat an-Naqab',
        'Maghar': 'מר\'אר',
        'Sachnin': 'Sakhnin',
        'Migdal Tefen': '----',
    }.get(name, name)

    nominatim_url = f'https://nominatim.openstreetmap.org/search?q={query_name}&format=json&namedetails=1&type=administrative&countrycodes=il'
    response = requests.get(nominatim_url).json()
    admin_regions = [r for r in response if r['type'] == 'administrative' and r['class'] == 'boundary']
    if len(admin_regions) > 0:
        names = admin_regions[0]['namedetails']
        heb = names.get('name:he') or names.get('name')
        heb = heb.replace('מועצה אזורית ', '').replace('מר\'אר', 'מגאר')
        official, score = extractOne(heb, list(seindex.keys()))
        if score > 70:
            # if score < 100:
            #     print('TRANSLATED', name, '->', heb, '->', official, score if score < 100 else '')
            official = seindex[official]
            cache[name] = dict((k, float(v) if isinstance(v, Decimal) else v) for k, v in official.items())
            return
        print('NOT FOUND IN SEINDEX', name, '->', heb, '->', official, score)
    print('NOT TRANSLATED', name)
    cache[name] = None    
    

def get_municipal_dataset():
    muni_name_cache = dict()
    MUNI_NAME_CACHE = Path('muni_names.cache.json')
    if MUNI_NAME_CACHE.exists():
        with open(MUNI_NAME_CACHE, 'r') as f:
            return json.load(f)

    import dataflows as DF
    download('https://www.cbs.gov.il/he/publications/doclib/2019/hamakomiot1999_2017/2019.xlsx', Path('data/rashuiot.xlsx'))
    renames = {
            'שם  הרשות': 'name',
            'ערך מדד (1)': 'seindex',
            'מרחק מגבול מחוז תל אביב (ק"מ)': 'distance',
            "צפיפות אוכלוסייה לקמ''ר ביישובים שמנו 5,000 תושבים ויותר": 'density',
            'סה"כ  אוכלוסייה בסוף השנה': 'population',
            'יהודים (אחוזים)': 'jewish',
            'שכר ממוצע לחודש של שכירים (ש"ח)': 'salary',
            'אחוז זכאים לתעודת בגרות מבין תלמידי כיתות יב': 'bagrut',
            'ערך מדד (3)': 'periphery',
        }
    socioeconomic_index = DF.Flow(
        DF.load('data/rashuiot.xlsx', sheet='נתונים פיזיים ונתוני אוכלוסייה ', headers=4, deduplicate_headers=True, skip_rows=[5]),
        DF.select_fields(list(renames.keys()), regex=False),
        DF.update_schema(-1, missingValues=['..', '-']),
        DF.rename_fields(renames, regex=False),
        DF.filter_rows(lambda row: bool(row['name'])),
        DF.set_type('name', transform=lambda v: v.replace('*', '').strip()),
        DF.validate(),
        DF.set_type('jewish', transform=lambda v: v or 0),
        DF.printer()
    ).results()[0][0]
    socioeconomic_index = dict((x['name'], x) for x in socioeconomic_index)
            
    f = json.load(open('data/geoBoundaries-ISR-ADM2.geojson'))
    for feature in f['features']:
        name = feature['properties']['shapeName']
        translate_muni_name(name, socioeconomic_index, muni_name_cache)
        
    with open(MUNI_NAME_CACHE, 'w') as f:
        json.dump(muni_name_cache, f, ensure_ascii=False, indent=2, sort_keys=True)

    return muni_name_cache


if __name__ == '__main__':
    get_municipal_dataset()