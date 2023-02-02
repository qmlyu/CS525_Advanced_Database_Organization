# Compile program

```{bash}
cd assign3
make
```

# Solution Design

The first table page is used for persisting schema info. The second table page is preserved for table info. All data starts from the third page. 


# Test cases

### Command Line

`?` shows some help info.

![test1](img/cmd.png)


### Test 1
```shell
test 1
```
![test1](img/test1.png)

### Test 2
```shell
test 2
```
![test1](img/test2.png)

## Command Line
### View help documentation
```shell
help
```
![test1](img/help.png)

### create table 
```sql
create table test1 a int b string c int
```
![test1](img/create_table.png)

### insert recode
```sql
insert into test1 value 1 'a' 1

insert into test1 value 2 'b' 2
```
![test1](img/insert_recode.png)

### view recode
```sql
select * from test1
```
![test1](img/select.png)

### update recode
```sql
update test1 set b 'i' where 1
```
![test1](img/update.png)

### delete recode
```sql
delete from test1 where id 1
```
![test1](img/delete.png)

### drop table
```sql
drop table test1
```
![test1](img/drop.png)

## Fix all memory leaks

### test_assign3_1
![test1](img/test_assign3_1.png)

### test_expr
![test1](img/test_expr.png)