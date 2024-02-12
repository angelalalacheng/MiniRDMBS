## Project 2 Report


### 1. Basic information
- Team #: 8
- Github Repo Link: https://github.com/angelalalacheng/cs222-winter24-YunJung.git
- Student 1 UCI NetID: yceng
- Student 1 Name: YunJung, Cheng
- Student 2 UCI NetID (if applicable):
- Student 2 Name (if applicable):

### 2. Meta-data
- Show your meta-data design (Tables and Columns table) and information about each column.
  
Tables (table-id:int, table-name:varchar(50), file-name:varchar(50))

Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
### 3. Internal Record Format (in case you have changed from P1, please re-enter here)
- Show your record format design.

| RID | Null Indicator Bits | offset 1 | offset 2 | value1 | value2 |

- Describe how you store a null field.

In the offsets part in the record, if the value is null, then I store the same offset as last one.
And, I don't put any value in the actual data.

- Describe how you store a VarChar field.

If the value is VarChar, I save the (VarChar length + VarChar value) in the record.


- Describe how your record design satisfies O(1) field access.

| RID | Null Indicator Bits | offset 1 | offset 2 | value1 | value2 |

For example, in this format, if I want to get value2, then I can access offset2 and get to the tail of value2.
How about its length?
I can use offset2 - offset1 to get the answer.

### 4. Page Format (in case you have changed from P1, please re-enter here)
- Show your page format design.

The record will insert from the beginning of page and the slot directory will add the slot from the end of the page and move backward.


- Explain your slot directory design if applicable.

Each slot will save its offset in the page and length also whether it is tombstone or not.
| slot n | ... | slot 2 | slot 1 | numOfSlots | FreeSpace |
I save the information above in the end of the page.

### 5. Page Management (in case you have changed from P1, please re-enter here)
- How many hidden pages are utilized in your design?

1 hidden page

- Show your hidden page(s) format design if applicable

| readPageCounter | writePageCounter | appendPageCounter |

I save the information above in the beginning of the hidden page.

### 6. Describe the following operation logic.
- Delete a record
1. Find the record position
2. Shift the records after it left
3. Mark the len and offset of deleted record become -1
4. Update all the slot information

- Update a record
1. Find the record position
2. See whether the new length of records can fit in the original page
3. If yes, shift the records after it left or right
4. If no, find the new page and make the original one become tombstone
5. Update all the slot information


- Scan on normal records
1. Use readAttribute to get the value
2. Do the comparison
3. If it is qualified, then add its RID into vector in the iterator
4. Use goNext to dump the data


- Scan on deleted records
1. See whether it is tombstone. If yes, recursively find the latest one.
2. Use readAttribute to get the value
3. Do the comparison
4. If it is qualified, then add its RID into vector in the iterator 
5. Use goNext to dump the data


- Scan on updated records
1. See whether it is tombstone. If yes, recursively find the latest one.
2. Use readAttribute to get the value
3. Do the comparison
4. If it is qualified, then add its RID into vector in the iterator
5. Use goNext to dump the data


### 7. Implementation Detail
- Other implementation details goes here.

1. Use share pointer to manage the file PFM open and close
2. Use a map to save the relation between file and fileHandle

### 8. Member contribution (for team of two)
- Explain how you distribute the workload in team.

N/A

### 9. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)
I feel confuse about when to use closeFile in RM.