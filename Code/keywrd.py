from eldar import Query


def keyword_mapping(keyword_query, text):
    if not keyword_query:
        return 1
    if not text:
        return 0
    query = Query(keyword_query)
    if query(text):
        return 1
    else:
        return 0
