# ETL for trade prices 

Create an ETL pipeline for trade prices.

<br/>

# Learned 

## Linting 
The automated checking of your source code for programmatic and stylistic errors.


```bash 

# Focus on major issues than code convention
pylint --disable=R,C trading_pipeline/etl/extract.py
```


## Code clean-up 

You can use ```black``` to "clean up" your scripts.

```bash 

black trading_pipeline/etl/extract.py
```


<br/>


## Configuring pipelines using metadata in YAML files

Check out the ```config.yaml``` files.


<br/>


## Repo structuring 
I've also learned how to structure a data pipeline project.
