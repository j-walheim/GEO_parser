import os
import json
from langfuse import Langfuse
from langfuse.decorators import observe
import re
import time
from llm_extractor.llm_client import get_llm
import duckdb
from tqdm import tqdm

class Extractor:
    langfuse = Langfuse()
    
    def __init__(self, prompt_name, fields, model):
        self.prompt_name = prompt_name
        self.fields = fields
        self.model = model
        self.temp_folder = self.create_temp_folder()
        self.db_connection = duckdb.connect('gse_metadata.db')
        self.setup_parse_results_table()

    def create_temp_folder(self):
        temp_folder = f'data/temporary_{self.prompt_name}'
        os.makedirs(temp_folder, exist_ok=True)
        return temp_folder

    def setup_parse_results_table(self):
        fields_sql = ', '.join([f'{field} VARCHAR' for field in self.fields])
        self.db_connection.execute(f'''
            CREATE TABLE IF NOT EXISTS parse_results (
                series_id VARCHAR,
                prompt_name VARCHAR,
                extracted_text TEXT,
                {fields_sql}
            )
        ''')


    @observe(as_type="generation")
    def get_llm_response(self, msg):
        llm = get_llm(self.model)
        return llm.chat(msg)

    def extract_info(self, response):
        extracted = {}
        for field in self.fields:
            match = re.search(rf'\[{field}\](.*?)\[/{field}\]', response, re.DOTALL)
            extracted[field] = match.group(1).strip() if match else "n/a"
        return extracted

    def create_text_description(self, row):
        columns = [
            'title', 'summary', 'overall_design', 'organism', 'treatment',
            'treatment_protocol', 'source', 'characteristics', 'molecule',
            'extract_protocol', 'data_processing', 'library_strategy', 'library_source'
        ]
        
        description = f"Series ID: {row[0]}\n\n"
        for i, column in enumerate(columns, start=1):
            if row[i]:
                description += f"{column.replace('_', ' ').title()}: {row[i]}\n\n"
        
        return description.strip()

    def process_study(self, row):
        try:
            series_id = row[0]
            json_filename = os.path.join(self.temp_folder, f"{series_id}.json")
            
            if os.path.exists(json_filename):
                return None  # Skip processing if results already exist

            text_description = self.create_text_description(row)
            
            prompt = self.langfuse.get_prompt(self.prompt_name)
            msg = prompt.compile(text=text_description)
            response = self.get_llm_response(msg)
            parsed_result = self.extract_info(response)
            
            fields_placeholders = ', '.join(['?' for _ in self.fields])
            fields_names = ', '.join(self.fields)
            
            self.db_connection.execute(f'''
                INSERT INTO parse_results (
                    series_id, prompt_name, extracted_text, 
                    {fields_names}
                )
                VALUES (?, ?, ?, {fields_placeholders})
            ''', (
                series_id, self.prompt_name, text_description,
                *[parsed_result[field] for field in self.fields]
            ))
            
            json_data = {
                "series_id": series_id,
                "prompt_name": self.prompt_name,
                "extracted_text": text_description,
                **parsed_result
            }
            with open(json_filename, 'w') as json_file:
                json.dump(json_data, json_file, indent=2)
            
            return parsed_result
        except TypeError:
            print(f"Error processing study {row[0]}. Skipping...")
            return None


    def run_extraction(self):
        query = """
        SELECT 
            series_id, title, summary, overall_design, organism, treatment,
            treatment_protocol, source, characteristics, molecule,
            extract_protocol, data_processing, library_strategy, library_source,
            authors_countries
        FROM gse_metadata
        WHERE organism LIKE '%Homo sapiens%'
        LIMIT 1000
        """
        
        results = self.db_connection.execute(query).fetchall()
        
        processed_count = 0
        error_count = 0
        for row in tqdm(results, desc=f"Processing {self.prompt_name} studies", unit="study"):
            result = self.process_study(row)
            if result is not None:
                processed_count += 1
            elif not os.path.exists(os.path.join(self.temp_folder, f"{row[0]}.json")):
                error_count += 1
        
        print(f"Processed {processed_count} new Homo sapiens studies for {self.prompt_name}")
        print(f"Total studies: {len(results)}, Skipped: {len(results) - processed_count - error_count}, Errors: {error_count}")

    def __del__(self):
        self.db_connection.close()

class GSEmetaExtractor(Extractor):
    def __init__(self, model='groq'):
        super().__init__('GSEmeta', ['high_level_indication', 'indication_detailed', 'drug_exposure', 'modalities','tissue_source', 'number_patients', 'sample_description', 'reasoning'], model)