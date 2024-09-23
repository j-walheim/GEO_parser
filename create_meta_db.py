import os
import xml.etree.ElementTree as ET
import duckdb

# Connect to DuckDB
if os.path.exists('gse_metadata.db'):
    os.remove('gse_metadata.db')
con = duckdb.connect('gse_metadata.db')

# Create a table to store the GSE metadata
con.execute('''
    CREATE TABLE IF NOT EXISTS gse_metadata (
        series_id VARCHAR,
        title VARCHAR,
        summary VARCHAR,
        overall_design VARCHAR,
        organism VARCHAR,
        treatment VARCHAR,
        treatment_protocol VARCHAR,
        source VARCHAR,
        characteristics VARCHAR,
        molecule VARCHAR,
        extract_protocol VARCHAR,
        data_processing VARCHAR,
        library_strategy VARCHAR,
        library_source VARCHAR,
        supplementary_data VARCHAR
    )
''')

def extract_metadata(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    
    namespace = {'ns': 'http://www.ncbi.nlm.nih.gov/geo/info/MINiML'}
    
    def find_text(path, element=root):
        return '; '.join(set(e.text.strip() for e in element.findall(path, namespaces=namespace) if e.text))

    series = root.find('.//ns:Series', namespaces=namespace)
    samples = root.findall('.//ns:Sample', namespaces=namespace)
    
    sample_data = {
        'organism': set(),
        'treatment': set(),
        'treatment_protocol': set(),
        'source': set(),
        'characteristics': set(),
        'molecule': set(),
        'extract_protocol': set(),
        'data_processing': set(),
        'library_strategy': set(),
        'library_source': set(),
        'supplementary_data': set()
    }

    for sample in samples:
        sample_data['organism'].add(find_text('.//ns:Channel/ns:Organism', sample))
        sample_data['treatment'].add(find_text('.//ns:Channel/ns:Characteristics[@tag="treatment"]', sample))
        sample_data['treatment_protocol'].add(find_text('.//ns:Channel/ns:Treatment-Protocol', sample))
        sample_data['source'].add(find_text('.//ns:Channel/ns:Source', sample))
        sample_data['characteristics'].update(char.text.strip() for char in sample.findall('.//ns:Channel/ns:Characteristics', namespaces=namespace) if char.text)
        sample_data['molecule'].add(find_text('.//ns:Channel/ns:Molecule', sample))
        sample_data['extract_protocol'].add(find_text('.//ns:Channel/ns:Extract-Protocol', sample))
        sample_data['data_processing'].add(find_text('.//ns:Data-Processing', sample))
        sample_data['library_strategy'].add(find_text('.//ns:Library-Strategy', sample))
        sample_data['library_source'].add(find_text('.//ns:Library-Source', sample))
        sample_data['supplementary_data'].update(supp.text.strip() for supp in sample.findall('.//ns:Supplementary-Data', namespaces=namespace) if supp.text)

    return {
        'series_id': series.attrib['iid'] if series is not None else '',
        'title': find_text('.//ns:Series/ns:Title'),
        'summary': find_text('.//ns:Series/ns:Summary'),
        'overall_design': find_text('.//ns:Series/ns:Overall-Design'),
        'organism': '; '.join(filter(None, sample_data['organism'])),
        'treatment': '; '.join(filter(None, sample_data['treatment'])),
        'treatment_protocol': '; '.join(filter(None, sample_data['treatment_protocol'])),
        'source': '; '.join(filter(None, sample_data['source'])),
        'characteristics': '; '.join(filter(None, sample_data['characteristics'])),
        'molecule': '; '.join(filter(None, sample_data['molecule'])),
        'extract_protocol': '; '.join(filter(None, sample_data['extract_protocol'])),
        'data_processing': '; '.join(filter(None, sample_data['data_processing'])),
        'library_strategy': '; '.join(filter(None, sample_data['library_strategy'])),
        'library_source': '; '.join(filter(None, sample_data['library_source'])),
        'supplementary_data': '; '.join(filter(None, sample_data['supplementary_data']))
    }

# Process XML files in all subfolders
xml_dir = 'data/GSE_meta'
for root, dirs, files in os.walk(xml_dir):
    for filename in files:
        if filename.endswith('.xml'):
            file_path = os.path.join(root, filename)
            try:
                metadata = extract_metadata(file_path)
                con.execute('''
                    INSERT INTO gse_metadata 
                    (series_id, title, summary, overall_design, organism, treatment, treatment_protocol, 
                    source, characteristics, molecule, extract_protocol, data_processing, 
                    library_strategy, library_source, supplementary_data)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', tuple(metadata.values()))
                print(f"Processed: {file_path}")
            except Exception as e:
                print(f"Error processing {file_path}: {str(e)}")

# Verify the data
result = con.execute("SELECT COUNT(*) FROM gse_metadata").fetchone()
print(f"Total records inserted: {result[0]}")

# Close the connection
con.close()
