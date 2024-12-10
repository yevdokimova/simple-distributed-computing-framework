# Simple Distributed Computing Framework

Distributed Computing Framework is a simple Python-based system designed to distribute and execute data processing tasks across multiple nodes implemented within CS392 Network Architecture class at American University of Armenia. The master node handles client requests, assigns tasks to available workers using a Round Robin scheduling algorithm, manages a task queue with retry mechanisms, and sends results back to clients, after receiving them from respective worker node. 

The framework supports the following operations:

**1. JOIN:**  
Combines two dataframes based on common key columns.  
_Usage:_  
Enter two dataframes.  
Specify the key columns for joining.

**2. DISTINCT:**  
Removes duplicate rows from a dataframe based on specified columns.  
_Usage:_  
Enter a dataframe.  
Specify the columns to apply distinct on.

**3. GROUP BY COUNT:**  
Aggregates data by counting occurrences based on specified group-by columns.  
_Usage:_  
Enter a dataframe.  
Specify the group-by columns.

**4. GROUP BY SUM:**  
Aggregates data by summing values based on specified group-by columns and a target column.  
_Usage:_  
Enter a dataframe.  
Specify the group-by columns.  
Specify the column to sum.

**5. ORDER BY:**  
Sorts data based on specified columns in ascending or descending order.  
_Usage:_  
Enter a dataframe.  
Specify the columns to order by.  
Choose the sort direction (asc or desc).

### Usage Examples
#### GROUP BY SUM Operation
```
Select Operation:
1. JOIN
2. DISTINCT
3. GROUP BY COUNT
4. GROUP BY SUM
5. ORDER BY
0. Exit
```
`Enter DataFrame rows (format: column1=value1,column2=value2,...), end with empty line:`  
`name=Anna,amount=8000`   
`name=Stepan,amount=9000`   
`name=Anna,amount=750`

`Enter group by columns separated by commas: name`  
`Enter the column to sum: amount`

```
Result for Task 77f6c239-3bfc-47da-9430-85227912bdd2
amount	name
8750.0	Anna
9000.0	Stepan
```

## Prerequisites

**Docker:** Ensure Docker is installed on your system.  
**Docker Compose:** Included with Docker Desktop on Windows and macOS. For Linux, follow [Docker Compose Installation](https://docs.docker.com/compose/install/linux/)

## Setup

1. Clone the Repository   
``git clone git@github.com:yevdokimova/simple-distributed-computing-framework.git``


2. Build services:  
``docker compose build``


3. Start services:  
``docker compose up``


4. Accessing Client Containers:  
_To interact with a Client, attach to its container:_  
``docker-compose exec client bash``  
Once inside the container, run the Client application:_  
``python client.py``

