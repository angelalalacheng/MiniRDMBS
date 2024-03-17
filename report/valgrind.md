## Debugger and Valgrind Report

### 1. Basic information
 - Team #: 8
 - Github Repo Link: https://github.com/angelalalacheng/cs222-winter24-YunJung.git
 - Student 1 UCI NetID: yceng
 - Student 1 Name: YunJung, Cheng
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):


### 2. Using a Debugger
- Describe how you use a debugger (gdb, or lldb, or CLion debugger) to debug your code and show screenshots. 
For example, using breakpoints, step in/step out/step over, evaluate expressions, etc.

I use breakpoints and set the conditions to check where it is wrong with step over.
![image](https://github.com/angelalalacheng/cs222-winter24-YunJung/blob/main/report/reportImage/p3_debugger.png)

### 3. Using Valgrind
- Describe how you use Valgrind to detect memory leaks and other problems in your code and show screenshot of the Valgrind report.
![](/Users/angela_cheng/Desktop/截圖 2024-01-23 中午12.41.45.png)

After reading the message from Valgrind, I find the in BNL Join function I use "new" to hold the record.
However, I forget to "delete". It caused the memory leak.
So, inside the BNL Join destructor, I add "delete" to release the memory.
