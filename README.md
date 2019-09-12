# Mini-SQL-Engine
A mini SQL engine to replicate MySQL functionalities

# Excecuting Code
- Code has been developed and tested in **Python3**
- Additional Required Modules are listed in requirements.txt. Install them by the following command:-
```console
user@linux:~/Mini-SQL-Engine$ pip3 install -r requirements.txt
```
- In the files directory, there is metadata.txt, which holds the schema for tables. Also, data of tables is listed in <Table Name>.csv, and it's structure is written in metadata.txt
- To include any new table, please refer to metadata.txt for the structure syntax.
- For the sake of simplicity, attributes are integers.
- Syntax for executing code:-
```console
user@linux:~/Mini-SQL-Engine$ python3 engine.py <Query within quotes>
For Example:-
user@linux:~/Mini-SQL-Engine$ python3 engine.py "Select * from table1"
```

## Supported Operations
- Select all records from one or more tables:
	- For Example:- `Select * from table_name`
- Aggregate functions: Simple aggregate functions on a single column
	- sum, average, max and min
	- For Example:-. `Select max(col1) from table1`
- Project Columns(could be any number of columns) from one or more tables:
	- For Example:- `Select col1, col2 from table_name`
- Select/project with distinct from one or more tables:
	- For Example:- `Select distinct col1, col2 from table_name;`
- Select with where from one or more tables:
	- For Example:- `Select col1,col2 from table1,table2 where col1 = 10 AND col2 = 20`
	- Conditional Operations supported are:- >,<.<=,>=,=
	- Also, you can only use at max two conditions in where for comparison
- Projection of one or more(including all the columns) from two tables with one join condition:
	- For Example:-`Select * from table1, table2 where table1.col1=table2.col2`
	- For Example:-`Select col1, col2 from table1,table2 where table1.col1=table2.col2`
