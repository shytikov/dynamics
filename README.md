# Intergation between `pandas` and MS Dynamics CE / CRM / CDS

A Helper pip package for `pandas` interaction with MS Dynamics instances.

The code will try to clean up data available in MS Dynamics instance by casting to correct Python types and pushing it pandas' `DataFrame`.

Sample code:

```
import dynamics

connection = dynamics.Connection(
    'https://instance.crm4.dynamics.com', 
    'user@instance.onmicrosoft.com', 
    'password')

entity = connection.entity("prefix_entity")
entity.read()

print(entity.data)
```