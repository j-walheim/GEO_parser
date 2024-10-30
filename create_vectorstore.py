import os
from tqdm import tqdm
import logging
from pinecone import Pinecone, ServerlessSpec
import time
import pandas as pd
import duckdb
from dotenv import load_dotenv
import json
from openai import AzureOpenAI  # Update import

load_dotenv()

class VectorStore:
    def __init__(self):
        self.pc = Pinecone(api_key=os.getenv('PINECONE_API_KEY'))
        self.METADATA_SIZE_LIMIT = 40960
        self.FIELD_PRIORITY = [
            'data_processing',
            'extract_protocol', 
            'overall_design'
        ]
        self.TRUNCATE_FIELD_SIZE = 1000

        # Initialize Azure OpenAI client
        self.azure_client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version="2024-05-01-preview"
        )

    def prepare_data(self, db_path, limit=1000):
        # Connect to the DuckDB database
        conn = duckdb.connect(db_path)

        # Get column names
        columns_info = conn.execute("DESCRIBE gse_metadata").fetchall()
        columns = [info[0] for info in columns_info]

        # Fetch data from the table with optional limit
        if limit is not None:
            rows = conn.execute("SELECT * FROM gse_metadata LIMIT ?", [limit]).fetchall()
        else:
            rows = conn.execute("SELECT * FROM gse_metadata").fetchall()

        # Transform each row into a JSON object with limited metadata size
        data = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            json_content = self.limit_metadata_size(row_dict, self.FIELD_PRIORITY, self.METADATA_SIZE_LIMIT)
            data.append({
                'id': str(row_dict.get('series_id', 'NA')),  # Assuming there is a 'series_id' column
                'content': json_content
            })

        conn.close()

        # Convert to DataFrame
        df = pd.DataFrame(data)
        print(f"Total number of entries: {len(df)}")
        return df

    def limit_metadata_size(self, row_dict, priority_fields, size_limit):
        """
        Constructs a metadata dictionary by adding fields based on priority.
        Truncates field values if necessary to stay within the size limit.
        """
        metadata = {}
        current_size = 0

        # Function to estimate size when serialized to JSON
        def estimate_size(d):
            return len(json.dumps(d).encode('utf-8'))

        # Add priority fields first
        for field in priority_fields:
            if field in row_dict and row_dict[field]:
                value = str(row_dict[field])
                # Truncate if necessary
                if len(value) > self.TRUNCATE_FIELD_SIZE:
                    value = value[:self.TRUNCATE_FIELD_SIZE] + '...'
                temp_metadata = metadata.copy()
                temp_metadata[field] = value
                estimated_size = estimate_size(temp_metadata)
                if estimated_size <= size_limit:
                    metadata[field] = value
                    current_size = estimated_size
                else:
                    # Try to add as much as possible
                    remaining_size = size_limit - current_size
                    # Estimate the overhead of adding the field name and quotes
                    overhead = len(json.dumps(field).encode('utf-8')) + 3  # for quotes and colon
                    max_value_size = remaining_size - overhead
                    if max_value_size > 0:
                        truncated_value = value[:max_value_size]
                        metadata[field] = truncated_value
                        current_size += estimate_size({field: truncated_value})
                    break  # Cannot add more

        # Add other fields
        for field, value in row_dict.items():
            if field in priority_fields or not value:
                continue
            value = str(value)
            if len(value) > self.TRUNCATE_FIELD_SIZE:
                value = value[:self.TRUNCATE_FIELD_SIZE] + '...'
            temp_metadata = metadata.copy()
            temp_metadata[field] = value
            estimated_size = estimate_size(temp_metadata)
            if estimated_size <= size_limit:
                metadata[field] = value
                current_size = estimated_size
            else:
                remaining_size = size_limit - current_size
                overhead = len(json.dumps(field).encode('utf-8')) + 3  # for quotes and colon
                max_value_size = remaining_size - overhead
                if max_value_size > 0:
                    truncated_value = value[:max_value_size]
                    metadata[field] = truncated_value
                    current_size += estimate_size({field: truncated_value})
                break  # Cannot add more

        return metadata

    def create_or_load_index(self, db_path, index_name='gse-index'):
        # Check if the index exists
        index_names = [index['name'] for index in self.pc.list_indexes()]
        if index_name in index_names:
            print(f"Index {index_name} already exists.")
        else:
            # Create a new index
            self.pc.create_index(
                name=index_name,
                dimension=1536,  # Updated dimension for Azure OpenAI embeddings
                metric="cosine",
                spec=ServerlessSpec(
                    cloud="aws",
                    region="us-east-1"
                )
            )
            # Wait for the index to be ready
            while not self.pc.describe_index(index_name).status['ready']:
                time.sleep(1)

        df_data = self.prepare_data(db_path)

        index = self.pc.Index(index_name)

        # Get list of IDs from df_data
        all_ids = df_data['id'].tolist()

        # Initialize a set to store existing IDs
        existing_ids = set()

        # Batch size for fetch (Pinecone allows up to 1000 IDs per fetch)
        batch_size = 1000

        print("Fetching existing IDs from the index...")
        # Fetch existing IDs in batches
        for i in tqdm(range(0, len(all_ids), batch_size), desc="Checking existing IDs"):
            batch_ids = all_ids[i:i+batch_size]
            fetch_response = index.fetch(ids=batch_ids, namespace='ns1')
            existing_ids.update(fetch_response['vectors'].keys())

        # Determine IDs not yet in the index
        new_ids = set(all_ids) - existing_ids

        print(f"Total IDs in dataset: {len(all_ids)}")
        print(f"Existing IDs in index: {len(existing_ids)}")
        print(f"New IDs to process: {len(new_ids)}")

        # Filter df_data to include only new IDs
        df_data = df_data[df_data['id'].isin(new_ids)].reset_index(drop=True)

        print("Processing new entries and upserting to the index...")
        for _, row in tqdm(df_data.iterrows(), total=df_data.shape[0], desc="Processing entries"):
            # Prepare input text by concatenating key-value pairs
            input_text = ' '.join([f"{key}: {value}" for key, value in row['content'].items()])

            # Generate embedding using Azure OpenAI
            try:
                response = self.azure_client.embeddings.create(
                    model=os.getenv("AZURE_OPENAI_ENDPOINT"), # Use your actual deployment name
                    input=input_text
                )
                embedding = response.data[0].embedding
            except Exception as e:
                print(f"Embedding failed for ID {row['id']}: {e}. Skipping.")
                continue

            # Prepare vector for upsert
            vector = {
                "id": row['id'],
                "values": embedding,
                "metadata": row['content']
            }

            try:
                # Upsert vector into the index
                index.upsert(
                    vectors=[vector],
                    namespace="ns1"
                )
            except Exception as e:
                print(f"Failed to upsert vector ID {row['id']}: {e}")
                continue

    def retrieve(self, index_name, query):
        max_retries = 5
        base_wait_time = 20

        for attempt in range(max_retries):
            try:
                index = self.pc.Index(index_name)

                # Generate embedding for the query using Azure OpenAI
                response = self.azure_client.embeddings.create(
                    model=os.getenv("AZURE_OPENAI_ENDPOINT"), # Use your actual deployment name
                    input=query
                )
                query_embedding = response.data[0].embedding

                results = index.query(
                    namespace="ns1",
                    vector=query_embedding,
                    top_k=10,
                    include_values=False,
                    include_metadata=True
                )

                metadata = [d['metadata'] for d in results['matches']]
                return metadata

            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    wait_time = base_wait_time * (2 ** attempt)
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print("Max retries reached. Returning empty list.")
                    return []

        return []  # Should never be reached

if __name__ == "__main__":
    vector_store = VectorStore()
    db_path = "/teamspace/studios/this_studio/GEO_parser/gse_metadata.db"
    index_name = "gse-index"

    # Create or load the index and populate it with data from the DuckDB database
    vector_store.create_or_load_index(db_path, index_name)

    # Example retrieval
    query = "Homo Sapiens, cancer"
    results = vector_store.retrieve(index_name, query)
    print("Search Results:")
    for result in results:
        print(result)
