# nemweb2
An attempt at making nemweb service I provide more pythnoic and more reliable

## Example
```python
import nem

for document in nem.current.DispatchIS:
for data in document.filter("PRICE"):
    if data["REGIONID"] == "VIC1":
        print(data["RRP"])
```

### ElasticSearch
This fork is design to import data into elasticsearch


## TODO
- make es_importer just take all files, and place in correct index based on the second and third columns
- multithread
- pypy
- tests
- pip package