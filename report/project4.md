## Project 4 Report


### 1. Basic information
- Team #: 8
- Github Repo Link: https://github.com/angelalalacheng/cs222-winter24-YunJung.git
- Student 1 UCI NetID: yceng
- Student 1 Name: YunJung, Cheng
- Student 2 UCI NetID (if applicable):
- Student 2 Name (if applicable):


### 2. Catalog information about Index
- Show your catalog information about an index (tables, columns).

I have one more catalog file about an index called `Indices` which saves the table-id, table-name, and index-attribute.
I also save `Indices` information in the `Tables` file.

### 3. Filter
- Describe how your filter works (especially, how you check the condition.)
1. I first check the condition type and then check the condition value.
2. I use the `readAttributeValue` function to check the target value in the record.
3. Compare the target value with the condition value and return the result.


### 4. Project
- Describe how your project works.
1. Go through outputAttrNames and then use the `readAttributeValue` function to get the target value in the record.
2. Concatenate the target value and then return the result.
3. Modify `getAttributes` function to return the new schema.


### 5. Block Nested Loop Join
- Describe how your block nested loop join works (especially, how you manage the given buffers.)
1. Use `getNextTuple` function of outerTable to fulfill the hash table which does not exceed numPages * PAGE_SIZE.
2. Call `getNextTuple` function of innerTable and then compare the key value with the hash table.
3. If the key value is in the hash table, then concatenate the record and return the result.
4. If `getNextTuple` function of innerTable returns -1, then call `getNextTuple` function of outerTable to construct new hash table and repeat the process.
5. If `getNextTuple` function of outerTable returns -1, then finish whole process.


### 6. Index Nested Loop Join
- Describe how your index nested loop join works.
1. Get one record from the outer table.
2. Use the `setIterator` to find the matched records in the inner table.
3. Concatenate the record and return the result.



### 7. Grace Hash Join (If you have implemented this feature)
- Describe how your grace hash join works (especially, in-memory structure).



### 8. Aggregation
- Describe how your basic aggregation works.
1. Use the `getNextTuple` and `readAttributeValue` function to get the target value in the record and save in vector.
2. Use the vector to calculate the result (MAX/MIN/AVG/COUNT/SUM)and return the result.


- Describe how your group-based aggregation works. (If you have implemented this feature)



### 9. Implementation Detail
- Have you added your own module or source file (.cc or .h)?
  Clearly list the changes on files and CMakeLists.txt, if any.

- Yes, I added the qe_util.h to help the implementation of the index manager.
- I put some helper functions in the qe_util.h.

- Other implementation details:

- Use 'ixFileHandleCache' to cache the file handle of the index file.
- When do thw insert / delete / update operation on heap file, I also do the same operation on the index file.



### 10. Member contribution (for team of two)
- Explain how you distribute the workload in team.

N/A

### 11. Other (optional)
- Freely use this section to tell us about things that are related to the project 4, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)