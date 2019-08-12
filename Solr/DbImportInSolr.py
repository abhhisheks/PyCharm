import pysolr

# Setup a Solr instance. The timeout is optional.
solr = pysolr.Solr('http://localhost:8983/solr/test_core', timeout=10)

# How you'd index data.
solr.add([
    {
        "id": "doc_1",
        "stype": "A test document",
    },
    {
        "id": "doc_2",
        "stype": "The Banana: Tasty or Dangerous?",
        "conn_user": [1,6,
        ]
    },
])

solr.delete(q='*:*')

print("Hello")