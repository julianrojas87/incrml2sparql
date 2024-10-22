# incrml2sparql

Script to interpret IncRML generated data and create the corresponding SPARQL UPDATE queries.

## Install it

Install the dependencies with `npm install`.

## Use it

This tool receives as input an RDF file containing (Inc)RML generated data, the source name and an optional target named graph to write towards in a triple store. The script will automatically detect if the input data is the result of an IncRML (`CHANGE`) or RML (`ALL`) generation process.

It can be used as follows:

```bash
node bin/cli.js --source [bluebike|delijn|gtfs|nmbs|jcdecaux|kmi|osm] --target-graph http://my.graph.com /path/to/rdf.ttl > query.sparql
```

## Splitting large INSERT queries

A threshold can be defined to produce multiple INSERT queries that avoid hitting triple store limits. The threshold can be defined with the `--limit` option.
