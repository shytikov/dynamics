# Intergation between `pandas` and MS Dynamics CE / CRM / CDS

A Helper pip package for `pandas` interaction with MS Dynamics instances.

The code will try to clean up data available in MS Dynamics instance by casting to correct Python types and pushing it pandas' `DataFrame`.

## Reading entity data to dataframe:

```
import dynamics


# Create a dataframe which will hold needed information
data = pandas.DataFrame()

data.dynamics.connect(
    'https://instance.crm4.dynamics.com', 
    'user@instance.onmicrosoft.com', 
    'password').entity("prefix_entity").read()


print(data)
```