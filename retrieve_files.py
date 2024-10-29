from create_vectorstore import VectorStore

if __name__ == "__main__":
    vector_store = VectorStore()
    index_name = "gse-index"

    while True:
        query = input("Enter your question (or type 'exit' to quit): ")
        if query.lower() == 'exit':
            break

        results = vector_store.retrieve(index_name, query)
        if results:
            print("Top 10 Matches:")
            for idx, result in enumerate(results, 1):
                print(f"\nResult {idx}:")
                for key, value in result.items():
                    print(f"{key}: {value}")
        else:
            print("No matches found.")
