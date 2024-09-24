from llm_extractor.extractor import GSEmetaExtractor

def main():
    # Initialize the GSEmetaExtractor
    extractor = GSEmetaExtractor(model='gpt-4o')

    # Run the extraction process
    extractor.run_extraction()

    print("Extraction process completed.")

if __name__ == "__main__":
    main()
