# Import Python3 Dependencies
import os
from flask import request
from flask import Flask
from google.auth import compute_engine
from google.cloud import storage
from google.cloud import language_v1
from google.cloud import translate_v2 as translate
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import gcsfs
import time

# Import Functions From backend_functions
from backend_functions.hello_world import hello_world
from backend_functions.full_processing import FullProcessor
from backend_functions.compute_similarity import ComputeSimilarity
from backend_functions.web_scraper import WebScraper

# export GOOGLE_APPLICATION_CREDENTIALS="secure_keys/coopsightsoftware-fcb3ac1f4518.json"
# export GOOGLE_APPLICATION_CREDENTIALS="secure_keys/coopsightspecial-d2aae2e5a3e9.json"

# Initialize Flask app
app = Flask(__name__)

init_start = time.time()

'''Authentication'''
auth_dict = {
        'main': 'secure_keys/coopsightsoftware-fcb3ac1f4518.json',
        'staging': 'secure_keys/coopsightstaging-db3781b61d96.json',
        'special': None,
        'general': 'secure_keys/coopsight-general-f118d36302df.json',
        'vc': 'secure_keys/coopsight-vc-69ccddb9936d.json',
        'open': 'secure_keys/coopsight-open-network-a2897ba8e2f5.json'
    }

# Firebase and Storage Auth
firebase_app = {}
firebase_store = {}
storage_app = {}
fs = {}
for key in auth_dict:
    if auth_dict[key] is not None:
        cred = credentials.Certificate(auth_dict[key])
        firebase_app[key] = firebase_admin.initialize_app(cred, name=key)
        firebase_store[key] = firebase_admin.firestore.client(app=firebase_app[key])
        storage_app[key] = storage.Client.from_service_account_json(auth_dict[key])
        fs[key] = gcsfs.GCSFileSystem(token=auth_dict[key])
# OS var
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'secure_keys/coopsightsoftware-fcb3ac1f4518.json'

# NLP Auth
main_cred = compute_engine.Credentials()
# nlp_client = language.LanguageServiceClient(credentials = main_cred)
nlp_client = language_v1.LanguageServiceClient()
translate_client = translate.Client()

# Initialize Classes
FP = FullProcessor()
CS = ComputeSimilarity()
WS = WebScraper()
print("all modules loaded in " + str(time.time() - init_start))

# Endpoint Routing
@app.route('/')
def function_1():
    result = hello_world()
    return result

@app.route('/full_processing', methods=['GET', 'POST', 'OPTIONS'])
def function_2():
    """
    Function that handles keywords extraction

    Parameters:
    database: database
    docid: docid
    path: path to where the description is kept
    label: label for the node (open network only)
    parent: docid of parent (open network only)
    """

    # Handle CORS Options Request
    if request.method == "OPTIONS":
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Max-Age": "3600",
        }
        return ("", 204, headers)
    
    # Start main function
    headers = {"Access-Control-Allow-Origin": "*"}
    start = time.time()
    # Verify requset
    data = request.get_json()
    db = data['database']
    docid = data['docid']
    path = data['path']
    label = None
    parent = None
    pdfs = None
    industry = None
    website = None
    if 'label' in data:
        label = data['label']
    if 'parent' in data:
        parent = data['parent']
    if 'pdfs' in data:
        pdfs = data['pdfs']
    if 'industry' in data:
        industry = data['industry']
    if 'website' in data:
        website = data['website']
    result, status_code = FP.execute(db, firebase_store[db], nlp_client, translate_client, fs[db], docid, path, label, parent, pdfs, industry, website, WS)
    print("full processing took " + str(time.time()-start))
    result = "full processing took " + str(time.time()-start) + "; "

    # Continue to call Compute Similarity
    if db == 'general':
        start = time.time()
        result2, status_code2 = CS.update(db, firebase_store[db], path, docid)
        print("background matching took " + str(time.time()-start))
        result = result + "background matching took " + str(time.time()-start)

    return (result, status_code, headers)

@app.route('/compute_similarity', methods=['GET', 'POST', 'OPTIONS'])
def function_3():
    # Handle CORS Options Request
    if request.method == "OPTIONS":
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Max-Age": "3600",
        }
        return ("", 204, headers)
    
    # Start main function
    headers = {"Access-Control-Allow-Origin": "*"}
    start = time.time()
    
    # Verify request
    data = request.get_json()
    db = data['database']
    docid = None
    path = None
    connection = None
    userid = None
    from_type = None
    from_value = None
    from_group = []
    to_type = [u'startup', u'corporation', u'investor']
    ignore = []
    if 'docid' in data:
        docid = data['docid']
    if 'path' in data:
        path = data['path']
    if 'connection' in data:
        connection = data['connection']
    if 'userid' in data:
        userid = data['userid']
    if 'from_type' in data:
        from_type = data['from_type']
    if 'from_value' in data:
        from_value = data['from_value']
    if 'from_group' in data:
        from_group = data['from_group']
    if 'to_type' in data:
        to_type = data['to_type']
    if 'ignore' in data:
        ignore = data['ignore']

    #! Special handler for staging
    if db == 'staging' or db == 'open':
        result, status_code = CS.get_next_g(db, firebase_store[db], userid, from_type, from_value, from_group, to_type, ignore)
    else:
        return ('null', 404, headers)

    print("compute similarity took " + str(time.time()-start))
    return (result, status_code, headers)

# Local Test 
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8081)))
