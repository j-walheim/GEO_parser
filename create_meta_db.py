import os
import xml.etree.ElementTree as ET
import duckdb

# Connect to DuckDB
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
    
    def find_text(path):
        element = root.find(path, namespaces=namespace)
        return element.text.strip() if element is not None else ''

    series = root.find('.//ns:Series', namespaces=namespace)
    sample = root.find('.//ns:Sample', namespaces=namespace)
    
    return {
        'series_id': series.attrib['iid'] if series is not None else '',
        'title': find_text('.//ns:Series/ns:Title'),
        'summary': find_text('.//ns:Series/ns:Summary'),
        'overall_design': find_text('.//ns:Series/ns:Overall-Design'),
        'organism': find_text('.//ns:Sample/ns:Channel/ns:Organism'),
        'treatment': find_text('.//ns:Sample/ns:Channel/ns:Characteristics[@tag="treatment"]'),
        'treatment_protocol': find_text('.//ns:Sample/ns:Channel/ns:Treatment-Protocol'),
        'source': find_text('.//ns:Sample/ns:Channel/ns:Source'),
        'characteristics': '; '.join([char.text.strip() for char in root.findall('.//ns:Sample/ns:Channel/ns:Characteristics', namespaces=namespace)]),
        'molecule': find_text('.//ns:Sample/ns:Channel/ns:Molecule'),
        'extract_protocol': find_text('.//ns:Sample/ns:Channel/ns:Extract-Protocol'),
        'data_processing': find_text('.//ns:Sample/ns:Data-Processing'),
        'library_strategy': find_text('.//ns:Sample/ns:Library-Strategy'),
        'library_source': find_text('.//ns:Sample/ns:Library-Source'),
        'supplementary_data': '; '.join([supp.text.strip() for supp in root.findall('.//ns:Sample/ns:Supplementary-Data', namespaces=namespace)])
    }

# Process XML files
xml_dir = 'data/GSE_meta'
for filename in os.listdir(xml_dir):
    if filename.endswith('_family.xml'):
        file_path = os.path.join(xml_dir, filename)
        metadata = extract_metadata(file_path)
        con.execute('''
            INSERT INTO gse_metadata 
            (series_id, title, summary, overall_design, organism, treatment, treatment_protocol, 
            source, characteristics, molecule, extract_protocol, data_processing, 
            library_strategy, library_source, supplementary_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', tuple(metadata.values()))

# Verify the data
result = con.execute("SELECT * FROM gse_metadata").fetchall()
for row in result:
    print(row)

# Close the connection
con.close()
