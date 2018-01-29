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

## Documentation
Documentation is still being made however the current version can be found on [readthedocs](http://nem.readthedocs.io/en/latest/).

### ElasticSearch
This fork is design to import data into elasticsearch


## TODO
- elasticsearch is probably import the dates and time in UTC but really we want brisbane time
- multiprocessor - for large datasets we hit the GIL pretty quickly. There are probably a few ways to solve this but sharding out to multiple processors is probably the easiest
- pypy
- tests
- we also hit queue capacity - can solve in elasticsearch settings or slow down processing or something