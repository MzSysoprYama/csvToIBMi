import IBMiUploader

from flask import Flask, request
from flask_cors import CORS
import json

app = Flask(__name__)
CORS(app)

# /IBMiUploaderでPUT この場合 http:localhost:3001/IBMiUploader
# にリクエストを投げます。
@app.route("/IBMiUploader",methods=['PUT'])
def asuploader():
    
    # リクエストされるパラメータです。
    file = request.files['file']
    table_name = request.form['tableName']
    start_row = int(request.form['startRow'])
    column_types = request.form['columnTypes']

    if not IBMiUploader.Upload(file, table_name, start_row, column_types):
        dic = { "result": "success" }
        result = json.dumps(dic)
    else:
        dic = { "result": "fail" }
        result = json.dumps(dic)
    
    return result

# ローカルホストport3001でリッスン 0,0,0,0 
# を指定すると他のリモートPCからも接続できます。
if __name__ == '__main__':
    app.debug = True
    app.run(debug=False, host='0.0.0.0', port=3001)