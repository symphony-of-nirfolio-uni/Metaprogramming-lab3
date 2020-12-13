## Laboratory work 3

#### Download from pip use command:
```
pip install symphony-of-nirfolio-py2sql
```

#### You can read documentation at this [link](https://htmlpreview.github.io/?https://github.com/symphony-of-nirfolio-uni/Metaprogramming-lab3/blob/dev/documentation/Py2SQL.html)

#### Example of use
```python
from Py2SQL import DatabaseInfo
from Py2SQL import Py2SQL

if __name__ == '__main__':
    db = DatabaseInfo("localhost", "root", "", "db")

    Py2SQL.db_connect(db)
```