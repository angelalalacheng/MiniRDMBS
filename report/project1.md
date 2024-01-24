## Project 1 Report


### 1. Basic information
 - Team #: 8
 - Github Repo Link: https://github.com/angelalalacheng/cs222-winter24-YunJung.git
 - Student 1 UCI NetID: yceng
 - Student 1 Name: YunJung, Cheng
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):


### 2. Internal Record Format
- Show your record format design. 

| Null Indicator Bits | offset 1 | offset 2 | value1 | value2 |


- Describe how you store a null field.

In the offsets part in the record, if the value is null, then I store the same offset as last one.
And, I don't put any value in the actual data.


- Describe how you store a VarChar field. 

If the value is VarChar, I save the (VarChar length + VarChar value) in the record.


- Describe how your record design satisfies O(1) field access. 

| Null Indicator Bits | offset 1 | offset 2 | value1 | value2 |

For example, in this format, if I want to get value2, then I can access offset2 and get to the tail of value2.
How about its length?
I can use offset2 - offset1 to get the answer.


### 3. Page Format
- Show your page format design. 

The record will insert from the beginning of page and the slot directory will add the slot from the end of the page and move backward.


- Explain your slot directory design if applicable. 

Each slot will save its offset in the page and length.

| slot n | ... | slot 2 | slot 1 | numOfSlots | FreeSpace |

I save the information above in the end of the page.


### 4. Page Management
- Show your algorithm of finding next available-space page when inserting a record. 

If the last (current) page has enough space, insert a new record into this page. 
If not, find the first page with free space large enough to store the record, e.g., looking from the beginning of the file, and store the record at that location.


- How many hidden pages are utilized in your design? 

I only use one hidden layer.


- Show your hidden page(s) format design if applicable 

| readPageCounter | writePageCounter | appendPageCounter |

I save the information above in the beginning of the hidden page.


### 5. Implementation Detail
- Other implementation details goes here.

I use lots of fstream function like seekg and seekp to find the position I want.
Also, I put effort to check the format of record is all aligned.
But, I think I should refactor my code after the deadline of project1 because there is so many repeated code.


### 6. Member contribution (for team of two)
- Explain how you distribute the workload in team.
N/A


### 7. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)


- Feedback on the project to help improve the project. (optional)