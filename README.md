# Intergation between `pandas` and MS Dynamics CE / CRM / CDS

A Helper pip package for `pandas` interaction with MS Dynamics instances.

The code will try to clean up data available in MS Dynamics instance by casting to correct Python types and pushing it pandas' `DataFrame`.

## Connecting to MS Dynamics instance:

```
import dynamics

connection = dynamics.connect(
    'https://instance.crm4.dynamics.com', 
    'user@instance.onmicrosoft.com', 
    'password')
```

## Reading entity data to dataframe:

```
entity = connection.entity("prefix_entity")
entity.read()

print(entity.data)
```

## Deleting data from MS Dynamics instance:

```
entity = connection.entity("prefix_entity")
entity.read()
entity.delete()
```