from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
from screener import screen_stocks, get_stock_data
import traceback

app = Flask(__name__)
CORS(app)

@app.route('/api/screen', methods=['POST'])
def screen():
    try:
        print("Received request to /api/screen")
        data = request.get_json()
        print(f"Request data: {data}")
        
        if not data:
            print("No JSON data received")
            return jsonify({
                'success': False,
                'error': 'No JSON data received'
            }), 400

        query = data.get('query')
        print(f"Query received: {query}")
        
        if not query:
            print("No query provided")
            return jsonify({
                'success': False,
                'error': 'No query provided'
            }), 400
            
        # Call the stock screening function
        print("Calling screen_stocks function...")
        result_df = screen_stocks(query)
        print(f"Screening complete. Found {len(result_df)} results")
        
        if result_df.empty:
            print("No results found")
            return jsonify({
                'success': True,
                'data': [],
                'message': 'No stocks found matching your criteria'
            })
        
        # Handle NaN values before converting to JSON
        result_df = result_df.fillna('N/A')
        
        # Convert DataFrame to JSON
        result = result_df.to_dict(orient='records')
        print(f"Successfully converted {len(result)} results to JSON")
        
        return jsonify({
            'success': True,
            'data': result
        })
        
    except Exception as e:
        print(f"Error in /api/screen: {str(e)}")
        print("Full traceback:")
        print(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500

@app.route('/api/columns', methods=['GET'])
def get_columns():
    try:
        print("Received request to /api/columns")
        # Get the available columns from the stock data
        data = get_stock_data()
        columns = data.columns.tolist()
        print(f"Available columns: {columns}")
        
        return jsonify({
            'success': True,
            'columns': columns
        })
        
    except Exception as e:
        print(f"Error in /api/columns: {str(e)}")
        print("Full traceback:")
        print(traceback.format_exc())
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500

if __name__ == '__main__':
    print("Starting Flask server...")
    app.run(debug=True, port=5003) 