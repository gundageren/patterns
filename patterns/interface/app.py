from flask import Flask, render_template, request, jsonify
import json
import os

app = Flask(__name__)

with open(os.path.join(app.root_path, '../../recommendations.json')) as f:
    recommendations = json.load(f)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/search_api')
def search_api():
    q = request.args.get('q', '').lower()
    if not q:
        return jsonify([])

    filtered = [rec for rec in recommendations if q in rec['table'].lower()]
    tables_seen = set()
    tables = []
    for rec in filtered:
        table = rec['table']
        if table not in tables_seen:
            tables_seen.add(table)
            tables.append({'table': table})
    return jsonify(tables)

@app.route('/recommendations_api')
def recommendations_api():
    table = request.args.get('table', '')
    if not table:
        return jsonify([])

    recs = [rec for rec in recommendations if rec['table'].lower() == table.lower()]
    return jsonify(recs)

if __name__ == '__main__':
    app.run(debug=True)
