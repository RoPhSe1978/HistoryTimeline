import requests
import json
import os

def fetch_full_timeline():
    print("1. Starting Full Range Fetch (-2000 BC to 1800 AD)...")
    
    query = """
    SELECT DISTINCT ?person ?personLabel ?description ?birth ?death ?sitelinks ?article ?image WHERE {
      ?person wikibase:sitelinks ?sitelinks .
      FILTER(?sitelinks > 80)
      
      ?person wdt:P31 wd:Q5 .                
      ?person wdt:P569 ?birth .              
      ?person wdt:P570 ?death .              
      
      FILTER(?birth >= "-2000-01-01T00:00:00Z"^^xsd:dateTime && 
             ?birth <= "1800-01-01T00:00:00Z"^^xsd:dateTime)
      
      OPTIONAL {
        ?article schema:about ?person .
        ?article schema:isPartOf <https://en.wikipedia.org/> .
      }
      
      OPTIONAL {
        ?person wdt:P18 ?image .
      }
      
      SERVICE wikibase:label { 
        bd:serviceParam wikibase:language "en". 
        ?person rdfs:label ?personLabel .
        ?person schema:description ?description .
      }
    }
    ORDER BY DESC(?sitelinks)
    LIMIT 350
    """

    url = 'https://query.wikidata.org/sparql'
    headers = {
        'Accept': 'application/sparql-results+json',
        'User-Agent': 'HistoricalTimelineApp/1.1'
    }
    
    try:
        print("2. Connecting to Wikidata...")
        response = requests.get(url, params={'query': query}, headers=headers, timeout=90)
        response.raise_for_status()
        
        raw_data = response.json()
        results = raw_data.get('results', {}).get('bindings', [])
        
        if not results:
            print("WARNING: Query returned 0 results. Check your filters.")
            return

        processed_data = []
        for result in results:
            # Safely extract values
            name = result['personLabel']['value']
            desc = result.get('description', {}).get('value', 'Historical Figure')
            birth = result['birth']['value']
            death = result['death']['value']
            sitelinks = int(result['sitelinks']['value'])
            article = result.get('article', {}).get('value')
            image = result.get('image', {}).get('value')
            
            processed_data.append({
                "name": name,
                "description": desc,
                "birth": birth,
                "death": death,
                "sitelinks": sitelinks,
                "wikipedia": article,
                "image": image
            })

        print(f"3. Data Processed. Samples found: {', '.join([p['name'] for p in processed_data[:5]])}...")

        # Create folder if it doesn't exist
        data_dir = os.path.join(os.path.dirname(__file__), 'data')
        if not os.path.exists(data_dir):
            os.makedirs(data_dir, exist_ok=True)
            
        # Write file with explicit encoding and indentation
        file_path = os.path.join(data_dir, 'historical_figures.json')
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, ensure_ascii=False, indent=4)
            f.flush() # Force write to disk
            
        # Double check file size
        if os.path.getsize(file_path) > 0:
            print(f"4. SUCCESS! Saved {len(processed_data)} figures to {file_path}")
        else:
            print("ERROR: File was created but it is still empty.")

    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    fetch_full_timeline()