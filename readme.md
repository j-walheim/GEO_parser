# GEO Metadata Extractor

This project is designed to extract and analyze metadata from Gene Expression Omnibus (GEO) studies, focusing on human (Homo sapiens) samples. It uses a combination of database operations, natural language processing, and machine learning techniques to process and interpret the metadata.

## Key Components

1. **Database Management**
   - Uses DuckDB to store and query GEO metadata
   - Creates and manages tables for both raw metadata and parsed results

2. **Metadata Extraction**
   - Extracts relevant information from XML files
   - Focuses on human studies by filtering for "Homo sapiens" organisms

3. **Text Processing**
   - Generates text descriptions for each study based on various metadata fields
   - Includes information such as title, summary, overall design, treatment protocols, and more

4. **LLM-based Information Extraction**
   - Utilizes Language Models (LLMs) for advanced information extraction
   - Supports multiple LLM providers including Groq and Azure OpenAI
   - Extracts specific fields like high-level indication, drug exposure, modalities, etc.

5. **Langfuse Integration**
   - Incorporates Langfuse for prompt management and observability

6. **Extensible Architecture**
   - The `Extractor` class serves as a base for creating specialized extractors
   - `GSEmetaExtractor` is an example of a specialized extractor for GEO metadata

## Key Features

- Processes GEO studies in batches
- Saves intermediate results to avoid redundant processing
- Provides progress tracking and error handling
- Allows for easy extension to extract different types of information

## Usage

1. Ensure the necessary environment variables are set (e.g., API keys for LLM providers)
2. Run the metadata extraction script to populate the DuckDB database
3. Use the `GSEmetaExtractor` or create custom extractors to process the metadata
4. Analyze the extracted information stored in the database and JSON files

## Requirements

- Python 3.x
- DuckDB
- Langfuse
- OpenAI or Groq API access

## Environment Setup

This project uses environment variables for configuration. Create a `.env` file in the root directory of the project and add the following variables:

- `GROQ_API_KEY`: API key for Groq
- `LANGFUSE_SECRET_KEY`: Secret key for Langfuse
- `LANGFUSE_PUBLIC_KEY`: Public key for Langfuse
- `LANGFUSE_HOST`: Host URL for Langfuse
- `AZURE_OPENAI_ENDPOINT`: Endpoint URL for Azure OpenAI
- `AZURE_OPENAI_API_KEY`: API key for Azure OpenAI
- `OPENAI_API_VERSION`: Version of the OpenAI API to use

Ensure all necessary API keys and endpoints are properly set in the `.env` file before running the project. The actual values for these variables should be kept confidential and never committed to version control.
