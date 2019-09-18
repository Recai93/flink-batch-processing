# Flink Batch Processing

Flink app which processes files in csv format.

### Input format:

csv input files have the following format

```
date|productId|eventName|userId
```
date: integer timstamp
product id: integer
eventName: view|add|remove|click
userId: integer timestamp

### Output format:

The application generates the following in text file format:

- Unique Product View counts by ProductId
- Unique Event counts
- Top 5 Users who fulfilled all the events (view,add,remove,click)
- All events of #UserId : 47
- Product Views of #UserId : 47

### Example input and output:

Input example:

```
date|productId|eventName|userId
1535816823|496|view|13
1536392928|496|add|69
1536392928|494|add|69
1536272308|496|view|16
1536272308|642|view|47
1536272308|642|view|47
1536757406|164|remove|49
1536757406|164|add|49
1536757406|496|view|49
1536757406|164|click|49
```

Output example:

all-events.txt		

```
add|3
click|1
remove|1
view|5
```

product-views.txt	

```
496|3
642|1
```

top-users.txt	

```
49
```

user-events.txt

```
view|2
```

user-product-views.txt

```
642
```
