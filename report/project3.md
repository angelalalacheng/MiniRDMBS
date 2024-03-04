## Project 3 Report


### 1. Basic information
- Team #: 8
- Github Repo Link: https://github.com/angelalalacheng/cs222-winter24-YunJung.git
- Student 1 UCI NetID: yceng
- Student 1 Name: YunJung, Cheng
- Student 2 UCI NetID (if applicable):
- Student 2 Name (if applicable):


### 2. Meta-data page in an index file
- Show your meta-data page of an index design if you have any. 

I have one meta-data page called `dummy node` in the index file. 
The dummy node is a non leaf node and it is always the page 0 and point to the root page.

### 3. Index Entry Format
- Show your index entry design (structure). 

  - entries on internal nodes:  
    - key: the key value
    - pointer: the page id of the child page
    - rid: distinguish the duplicate key value
  
  - entries on leaf nodes:
    - key: the key value
    - rid: the record id of the key

  
### 4. Page Format
- Show your internal-page (non-leaf node) design.
  - NodeHeader: isLeaf, isDummy, rightSiblingPageId, parentPageId
  - NonLeaf: # of keys, freeSpace, vector of keys, pointers, rids


- Show your leaf-page (leaf node) design. 
  - NodeHeader: isLeaf, isDummy, rightSiblingPageId, parentPageId
  - Leaf: # of keys, freeSpace, vector of keys, rids



### 5. Describe the following operation logic.
- Split
  1. When the freeSpace is not enough, we need to split the page.
  2. Choose the middle key and move it to the parent page.
  3. Create tempNode to store the keys and pointers and insert the new key and pointer to the tempNode.
  4. Create a new page and move half of the keys to the new page.


- Rotation (if applicable)



- Merge/non-lazy deletion (if applicable)



- Duplicate key span in a page
  1. When the key is inserted, we need to check if the key is already in the page.
  2. If the key is already in the page, I compare the key with the pageNum and slotNum of existing keys and find the position to insert the key.



- Duplicate key span multiple pages (if applicable)



### 6. Implementation Detail
- Have you added your own module or source file (.cc or .h)? 
  Clearly list the changes on files and CMakeLists.txt, if any.

  - Yes, I added the ix_util.h to help the implementation of the index manager.
  - I put some helper functions in the ix_util.h.



- Other implementation details:
  - Use the `std::vector` to store the keys, pointers, and rids. It is easy to use and manage the memory.
  - Use recursive function to insert the key into index file.
  - Use recursive function to generate the json format of the B+ tree.
  - Do some abstraction to handle the different types.


### 7. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 8. Other (optional)
- Freely use this section to tell us about things that are related to the project 3, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)
