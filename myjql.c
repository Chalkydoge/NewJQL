/* You may refer to: https://cstack.github.io/db_tutorial/ */
/* Compile: gcc -o myjql myjql.c -O3 */
/* Test: /usr/bin/time -v ./myjql myjql.db < in.txt > out.txt */
/* Compare: diff out.txt ans.txt */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

#include <stdbool.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

/* shell IO */

#define INPUT_BUFFER_SIZE 31
#define TABLE_MAX_PAGES 65536
#define ROW_SIZE 16

struct {
  char buffer[INPUT_BUFFER_SIZE + 1];
  size_t length;
} input_buffer;

typedef enum {
  INPUT_SUCCESS,
  INPUT_TOO_LONG
} InputResult;

/* pager and table */
const uint32_t PAGE_SIZE = 4096; // 4KB page

// typedef struct ListNode {
//   struct ListNode* prev;
//   struct ListNode* next;
//   uint32_t free_page_id;
// } ListNode_t;

// typedef struct FreeList {
//   ListNode_t* head;
//   ListNode_t* tail;
//   int32_t size;
// } FreeList_t;

typedef struct {
  int file_descriptor;
  uint32_t file_length;
  uint32_t num_pages;
  void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
  Pager* pager;
  uint32_t root_page_num;
} Table;

Table* table; // global variable, entry of the whole table

void* get_page(Pager* pager, uint32_t page_num) {
  if (page_num > TABLE_MAX_PAGES) {
    printf("Tried to fetch page number %d out of bound.\n", page_num);
    exit(EXIT_FAILURE);
  }

  if (!pager->pages[page_num]) {
    // Cache miss. Allocate memory and load from disk.
    void *page = malloc(PAGE_SIZE);
    uint32_t num_pages = pager->file_length / PAGE_SIZE; // pager has 'num_pages' page

    if (pager->file_length % PAGE_SIZE) {
      num_pages += 1; 
    }

    if (page_num <= num_pages) {
      lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
      ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
      if (bytes_read == -1) {
        printf("Error reading file: %d\n", errno);
        exit(EXIT_FAILURE);
      }
    }

    pager->pages[page_num] = page; // malloced page, returning to the pager

    // if allocated new pages, we update pager's count for it.
    if (page_num >= pager->num_pages) {
      pager->num_pages = page_num + 1;
    }
  }

  return pager->pages[page_num];
}

Pager* pager_open(const char* filename) {
  int fd = open(filename, O_RDWR | O_CREAT, // Read/Write mode, Create file if doen't exist
  S_IWUSR | S_IRUSR); // user write permission & read permission

  if (fd == -1) {
    printf("Unable to open file\n");
    exit(EXIT_FAILURE);
  }

  off_t file_length = lseek(fd, 0, SEEK_END);

  Pager* pager = malloc(sizeof(Pager));
  pager->file_descriptor = fd;
  pager->file_length = file_length;
  pager->num_pages = (file_length / PAGE_SIZE);

  if (file_length % PAGE_SIZE != 0) {
    printf("Db file is not a whole number of pages. Corrupt file.\n");
    exit(EXIT_FAILURE);   
  }

  for (int32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
    pager->pages[i] = NULL;
  }

  return pager;
}

uint32_t get_unused_page_num (Pager* pager) {
  return pager->num_pages;
}

// open database and do preparations
void initialize_leaf_node(void*); // needed functions
void set_node_root(void*, bool);
Table* db_open(const char* filename) {
  Pager* pager = pager_open(filename);

  Table* table = malloc(sizeof(Table));
  table->pager = pager;
  table->root_page_num = 0;

  if (pager->num_pages == 0) {
    // New database file, Initialize page 0 as leaf node
    void* root_node = get_page(pager, 0);
    initialize_leaf_node(root_node);
    set_node_root(root_node, true);
  }
  return table;
}

// write back the page, only with complete pages
void pager_flush(Pager* pager, uint32_t page_num) {
  if (pager->pages[page_num] == NULL) {
     printf("Tried to flush null page\n");
     exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE,
     		 SEEK_SET);

  if (offset == -1) {
     printf("Error seeking: %d\n", errno);
     exit(EXIT_FAILURE);
  }

  ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
  // printf("Had Written: %ld\n", bytes_written);
  if (bytes_written == -1) {
     printf("Error writing: %d\n", errno);
     exit(EXIT_FAILURE);
  }
}

// close the file
void db_close(Table* table) {
  Pager* pager = table->pager;

  for (uint32_t i = 0; i < pager->num_pages; ++i) {
    if (!pager->pages[i]) {
      continue;
    }

    // write back these pages & free memory
    pager_flush(pager, i);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }
  
  int result = close(pager->file_descriptor);
  if (result == -1) {
    printf("Error closing db file.\n");
    exit(EXIT_FAILURE);
  }

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
    void* page = pager->pages[i];
    if (page) {
      free(page);
      pager->pages[i] = NULL;
    }
  }

  free(pager);
  free(table);
}

/*-------Cursors------------*/

/* Cursor for B-Tree index */
typedef struct {
  Table* table; // Table that cursor points to.
  uint32_t page_num; // cursor points to which page.
  uint32_t cell_num; // cursor points to which <key, value> cell.
  bool end_of_table; // reached end ?
} Cursor;

typedef enum {
  NODE_INTERNAL, NODE_LEAF
} NodeType;

/* needed declarations */
uint32_t* leaf_node_num_cells(void*);
NodeType get_node_type(void*);
void* leaf_node_cell (void* node, uint32_t cell_num);
char* leaf_node_key(void*, uint32_t);

/* Cursor finding value in leafnode pages */
Cursor* leaf_node_find (Table* table, uint32_t page_num, char* key) {
  void* node = get_page(table->pager, page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->page_num = page_num;

  // Using Binary search
  uint32_t min_index = 0;
  uint32_t one_past_max_index = num_cells;
  while (one_past_max_index != min_index) {
    uint32_t index = (min_index + one_past_max_index) / 2;
    char* key_at_index = leaf_node_key(node, index); // a pointer to the array position(address stored)

    int cmp = strcmp(key, key_at_index);
    if (cmp == 0) {
      // TODO: find the leftmost pos of same keys, then do insert
      int32_t test_index = index;
      while (test_index >= 0) {
        // printf("Test Index is: %d\n", test_index);
        char* same_key = leaf_node_key(node, test_index);
        if (strcmp(key, same_key)) {
          break;
        } else {
          --test_index;
        }
      }
      min_index = test_index + 1;
      break;
    }
    else if (cmp < 0) { // current key is smaller than spotted key
      one_past_max_index = index;
    } else {
      min_index = index + 1;
    }
  }
  // printf("Index where %s is going to insert is: %d\n", key, min_index);
  cursor->cell_num = min_index;

  return cursor;  
}

/* Cursor finding value in internal nodes */
uint32_t* internal_node_num_keys(void*);
char* internal_node_key(void*, uint32_t);
uint32_t* internal_node_child (void*, uint32_t);
uint32_t* leaf_node_next_leaf(void*);
uint32_t internal_node_find_child (void* node, char* key);
uint32_t* node_parent(void* node);
void print_internal_node_info (void*, uint32_t);

Cursor* internal_node_find (Table* table, uint32_t page_num, char* key) {
  void* node = get_page(table->pager, page_num); 
  uint32_t child_index = internal_node_find_child(node, key);
  uint32_t child_page_id = *internal_node_child(node, child_index);
  
  void* child_page = get_page(table->pager, child_page_id);
  // printf("Now we're looking in internal nodes! Page is: %d, Key_num is: %d\n", child_page_id, *internal_node_num_keys(node));

  switch (get_node_type(child_page)) {
    case NODE_LEAF: return leaf_node_find(table, child_page_id, key);
    case NODE_INTERNAL: return internal_node_find(table, child_page_id, key);
    default: break;
  }
}

/* Given key, find where the key to be inserted into. */
Cursor* table_find (Table* table, char* key) {
  uint32_t root_page_num = table->root_page_num;
  void* root_node = get_page(table->pager, root_page_num);
  if (get_node_type(root_node) == NODE_LEAF) {
    return leaf_node_find(table, root_page_num, key);
  } else {
    return internal_node_find(table, root_page_num, key);
  }
}

/* Cursor points to the start of table */
Cursor* table_start (Table* table) {
  char* min_key = "0";
  Cursor* cursor = table_find(table, min_key);
  // printf("Starting Page num is: %d\n", cursor->page_num);

  void* node = get_page(table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);
  cursor->end_of_table = (num_cells == 0);

  return cursor;
}

/* cursor value 包含了叶子节点的一条记录(char[12]+int) */
void* cursor_value (Cursor* cursor) {
  uint32_t page_num = cursor->page_num;
  void* page = get_page(cursor->table->pager, page_num);
  return leaf_node_cell(page, cursor->cell_num);
}

void cursor_advance(Cursor* cursor) {
  uint32_t page_num = cursor->page_num;
  void* node = get_page(cursor->table->pager, page_num);
  uint32_t parent_id = *node_parent(node);
  cursor->cell_num += 1;
  // printf("Now we're visiting Page: %d, its Parent is Page [%d]\n", page_num, parent_id);

  if (cursor->cell_num >= *leaf_node_num_cells(node)) {
    uint32_t next_page_num = *leaf_node_next_leaf(node);
    if (next_page_num == 0) {
      /* reached rightmost leaf */
      cursor->end_of_table = true;
    } else {
      cursor->page_num = next_page_num;
      cursor->cell_num = 0;
    }
  }
}

/*-------------------------*/


/* shell io */
void print_prompt() { printf("myjql> "); }

InputResult read_input() {
  /* we read the entire line as the input */
  input_buffer.length = 0;
  while (input_buffer.length <= INPUT_BUFFER_SIZE
    && (input_buffer.buffer[input_buffer.length++] = getchar()) != '\n'
    && input_buffer.buffer[input_buffer.length - 1] != EOF);
  if (input_buffer.buffer[input_buffer.length - 1] == EOF)
    exit(EXIT_SUCCESS);
  input_buffer.length--;
  /* if the last character is not new-line, the input is considered too long,
     the remaining characters are discarded */
  if (input_buffer.length == INPUT_BUFFER_SIZE
    && input_buffer.buffer[input_buffer.length] != '\n') {
    while (getchar() != '\n');
    return INPUT_TOO_LONG;
  }
  input_buffer.buffer[input_buffer.length] = 0;
  return INPUT_SUCCESS;
}

void open_file(const char* filename) {
  /* open file */
  table = db_open(filename);
}

void exit_nicely(int code) {
  /* do clean work */
  db_close(table);
  exit(code);
}

void exit_success() {
  printf("bye~\n");
  exit_nicely(EXIT_SUCCESS);
}

/* specialization of data structure */

#define COLUMN_B_SIZE 11

typedef struct {
  uint32_t a;
  char b[COLUMN_B_SIZE + 1];
} Row;

void print_row(Row* row) {
  printf("(%d, %s)\n", row->a, row->b);
}

/* statement */

typedef enum {
  STATEMENT_INSERT,
  STATEMENT_SELECT,
  STATEMENT_DELETE
} StatementType;

struct {
  StatementType type;
  Row row;
  uint8_t flag; /* whether row.a, row.b have valid values */
} statement;

/* helper functions */

/* serialize row info into bytes */
void serialize_row(Row* source, void* destination) {
  // |-----b------|------a------|
  //    char(12)       int    
  memcpy(destination + 0, &(source->b), 12);
  memcpy(destination + 12, &(source->a), 4);
}

void deserialize_row(void* source, Row* destination) {
  memcpy(&(destination->b), source + 0, 12);
  memcpy(&(destination->a), source + 12, 4);
}

/* B+ Tree Structures */

/* Common Node Header Formats */
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/* Leaf Node Header Formats */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NEXT_LEAF_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE;

/* Leaf Node Body Formats */
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(char[12]); // index on B
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE - 1;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_LEFT_SPLIT_COUNT;
const uint32_t LEAF_NODE_MIN_CELLS = LEAF_NODE_MAX_CELLS / 2;

/* Internal Node Header Layout */
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t); // pointer to rightmost child's page_num
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

/* Internal Node Body Layout */
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(char[12]);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t); // pointer to child page_num(page_id)
const uint32_t INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
const uint32_t INTERNAL_NODE_SPACE_FOR_CELLS = PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_MAX_CELLS = INTERNAL_NODE_SPACE_FOR_CELLS / INTERNAL_NODE_CELL_SIZE - 1;
const uint32_t INTERNAL_NODE_LEFT_SPLIT_COUNT = (INTERNAL_NODE_MAX_CELLS + 1) / 2;
const uint32_t INTERNAL_NODE_RIGHT_SPLIT_COUNT = (INTERNAL_NODE_MAX_CELLS + 1) - INTERNAL_NODE_LEFT_SPLIT_COUNT;
const uint32_t INTERNAL_NODE_MIN_CELLS = 1; // TODO: MODIFY IT AFTER TESTING


/* Leaf Node Fields Functions */

NodeType get_node_type (void* node) {
  uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
  return (NodeType)value;
}

void set_node_type (void* node, NodeType type) {
  uint8_t value = type;
  *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}

bool is_node_root (void* node) {
  uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
  return (bool)value;  
}

void set_node_root (void* node, bool is_root) {
  uint8_t value = is_root;
  *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
}

uint32_t* node_parent (void* node) {
  return node + PARENT_POINTER_OFFSET;
}

/*---------------Leaf Node Functions ----------*/

// given node, return ptr to its cell_num 
uint32_t* leaf_node_num_cells (void* node) {
  return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

// given cell_num, return ptr to the cell in the leaf node.
void* leaf_node_cell (void* node, uint32_t cell_num) {
  return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

// return ptr to cell's key according to cell_num
char* leaf_node_key (void* node, uint32_t cell_num) {
  return (char*)leaf_node_cell(node, cell_num);
}

void set_leaf_node_key (void* node, uint32_t cell_num, char* key) {
  char* dest = leaf_node_key(node, cell_num);
  memcpy(dest, key, LEAF_NODE_KEY_SIZE);
}

// given cell_num, return ptr to cell's value, value = [key, value] here
uint32_t* leaf_node_value (void* node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

// return leafnode's next leaf's page_id
uint32_t* leaf_node_next_leaf(void* node) {
  return node + LEAF_NODE_NEXT_LEAF_SIZE_OFFSET;
}

/** @params: void* node
 *  return: index of given key in the node, if contains several keys,
 *          return the minimum index.
 */ 
uint32_t leaf_node_find_key_index (void* node, char* key) {
  uint32_t min_index = 0;
  uint32_t max_index = *leaf_node_num_cells(node);

  while (min_index != max_index) {
    uint32_t mid_index = (min_index + max_index) / 2;
    char* key_at_mid_index = leaf_node_key(node, mid_index);
    int cmp = strcmp(key, key_at_mid_index);

    if (cmp == 0) {
      int32_t test_index = mid_index;
      while (test_index >= 0){
        /* code */
        int test_cmp = strcmp(key, leaf_node_key(node, test_index));
        if (test_cmp) {
          break;
        }
        else {
          --test_index;
        }
      }
      min_index = test_index + 1;
      break;
    }
    else if (cmp > 0) {
      // key is larger than mid_key
      max_index = mid_index;
    }
    else {
      min_index = mid_index + 1;
    }
  }
  return min_index;
}

/*---------------------------------------------*/



/*----------Internal Node Functions ----------*/

uint32_t* internal_node_num_keys (void* node) {
  return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

uint32_t* internal_node_right_child (void* node) {
  return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

void* internal_node_cell (void* node, uint32_t cell_num) {
  return node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
}

uint32_t* internal_node_child (void* node, uint32_t child_num) {
  uint32_t num_keys = *internal_node_num_keys(node);
  if (child_num > num_keys) {
    printf("Tried to access child_num %d > num_keys %d\n", child_num, num_keys);
    exit(EXIT_FAILURE);
  } else if (child_num == num_keys) {
    return internal_node_right_child(node);
  } else {
    return internal_node_cell(node, child_num);
  }
}

char* internal_node_key (void* node, uint32_t key_num) {
  return (void*)internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

char* get_node_max_key(void* node) {
  switch (get_node_type(node)) {
    case NODE_INTERNAL:
      return internal_node_key(node, *internal_node_num_keys(node) - 1);
    case NODE_LEAF:
      return leaf_node_key(node, *leaf_node_num_cells(node) - 1);
  }
}

void update_internal_node_key (void* node, char* old_key, char* new_key) {
  uint32_t old_child_index = internal_node_find_child(node, old_key);
  memcpy(internal_node_key(node, old_child_index), new_key, 12);
}

// find child's page_id with given key(varchar) in an internal node
uint32_t internal_node_find_child (void* node, char* key) {
  if (!node) {
    printf("Error! Accessing NULL Pages\n");
    exit(EXIT_FAILURE);
  }

  // return the index of the child which contains given key
  uint32_t num_keys = *internal_node_num_keys(node);
  if (num_keys == 0) {
    printf("Error! You've entered an Empty Page!!!\n");
    exit(EXIT_FAILURE);
  }
  uint32_t min_index = 0;
  uint32_t max_index = num_keys;

  while (min_index != max_index) {
    // find the index where [index <= key]
    uint32_t index = (min_index + max_index) / 2;
    char key_to_compare[12];
    memcpy(key_to_compare, internal_node_key(node, index), 12);
    int cmp = strcmp(key, key_to_compare);

    if (cmp == 0) {
      int32_t test_index = index;
      while (test_index >= 0) {
        char* same_key = internal_node_key(node, test_index);
        if (strcmp(key, same_key) != 0) {
          break;
        } else {
          --test_index;
        }
      }
      min_index = test_index + 1;
      break;
    }
    else if (cmp < 0) { // current key is smaller than spotted key
      max_index = index;
    } else {
      min_index = index + 1;
    }      
  }

  return min_index;
}

/*---------------------------------------------*/



/*---------- Node Initialization --------------*/

// initialize the leaf node's cell_num
void initialize_leaf_node (void* node) {
  set_node_type(node, NODE_LEAF);
  set_node_root(node, false);
  *leaf_node_num_cells(node) = 0;
  *leaf_node_next_leaf(node) = 0;
}

// initializer an internal node
void initialize_internal_node (void* node) {
  set_node_type(node, NODE_INTERNAL);
  set_node_root(node, false);
  *internal_node_num_keys(node) = 0;

}

// 测试用: 打印B+树内部的节点信息
void print_internal_node_info (void* node, uint32_t id) {
  uint32_t num_keys = *internal_node_num_keys(node);
  printf("Internal Node, Page: [%d]\n", id);

  for (int32_t i = 0; i < num_keys; ++i) {
    printf("--> Child Node, Page: [%d]\n", *internal_node_child(node, i));
    printf("--> Key %d: %s\n", i, internal_node_key(node, i));
  }
  printf("--> Rightmost Child Node, Page: [%d]\n", *internal_node_right_child(node));
}

/*---------------------------------------------*/



/* B-Tree operations */

/* the key to select is stored in `statement.row.b` */
void b_tree_search() {
  /* print selected rows */
  Row row;
  char* key_to_find = statement.row.b;
  Cursor* cursor = table_find(table, key_to_find);
  int32_t counter = 0;

  while (!(cursor->end_of_table)) {
    deserialize_row(cursor_value(cursor), &row);
    if (strcmp(row.b, statement.row.b) != 0) {
      break;
    } else {
      print_row(&row);
      cursor_advance(cursor);
      ++counter;
    }
  }

  if (counter == 0) {
    printf("(Empty)\n");
  }
  return;
}

/* 初次分裂才会调用这个函数, 生成一个新的根节点!!!
*/
void create_new_root(Table* table, uint32_t right_child_page_num) {
  // printf("Creating New Root! Page NO is %d\n", right_child_page_num);

  void* root = get_page(table->pager, table->root_page_num);
  void* right_child = get_page(table->pager, right_child_page_num);
  uint32_t left_child_page_num = get_unused_page_num(table->pager);
  void* left_child = get_page(table->pager, left_child_page_num);
  memset(left_child, 0, PAGE_SIZE);

  // printf("Left child's page_id is: %d\n", left_child_page_num);

  /* Left child has data copied from old root */
  memcpy(left_child, root, PAGE_SIZE);
  set_node_root(left_child, false);

  /* Root node is a new internal node with one key and two children */
  initialize_internal_node(root);
  set_node_root(root, true);
  *internal_node_num_keys(root) = 1; // actually is 1 + (rightmost child) 1 = 2
  *internal_node_child(root, 0) = left_child_page_num;

  // get maxchild string in the leftchild page
  char* left_child_max_key = get_node_max_key(left_child);
  char* key_to_be_set = internal_node_key(root, 0);

  memcpy(key_to_be_set, left_child_max_key, 12);

  *internal_node_right_child(root) = right_child_page_num;
  *node_parent(left_child) = table->root_page_num;
  *node_parent(right_child) = table->root_page_num;
  // printf("Left child page_id: %d, Right child page_id: %d\n", left_child_page_num, right_child_page_num);
}

// 上层内部节点的分裂 & 插入父节点的一些操作函数
void insert_into_parent(Table* table, uint32_t old_left_page_id, uint32_t old_right_page_id, char* key_to_liftup) {
  // 1. 分裂过的内部节点是原先的根节点
  if (old_left_page_id == table->root_page_num) {
    // printf("Called Here\n");
    uint32_t new_left_child_id = get_unused_page_num(table->pager);
    void* new_left_child_page = get_page(table->pager, new_left_child_id);
    void* root = get_page(table->pager, table->root_page_num);
    memcpy(new_left_child_page, root, PAGE_SIZE);
    set_node_type(new_left_child_page, NODE_INTERNAL);
    set_node_root(new_left_child_page, false);

    initialize_internal_node(root);
    set_node_root(root, true);
    *internal_node_num_keys(root) = 1; // actually is 1 + (rightmost child) 1 = 2
    *internal_node_child(root, 0) = new_left_child_id;
    *internal_node_right_child(root) = old_right_page_id;

    char* key_to_be_set = internal_node_key(root, 0);
    memcpy(key_to_be_set, key_to_liftup, INTERNAL_NODE_KEY_SIZE);
    // printf("And String: %s will be set as the first key in the root node!\n", key_to_be_set);

    // 完成 子节点 与叶子节点之间的连接 
    *node_parent(new_left_child_page) = table->root_page_num;
    for (int32_t i = 0; i < *internal_node_num_keys(new_left_child_page); ++i) {
      void* child_page = get_page(table->pager, *internal_node_child(new_left_child_page, i));
      // printf("Child id %d\n", *internal_node_child(new_left_child_page, i));
      *node_parent(child_page) = new_left_child_id;
    }
    void* rightmost_child_page = get_page(table->pager, *internal_node_right_child(new_left_child_page));

    // printf("Rightmost Child id %d\n", *internal_node_right_child(new_left_child_page));
    *node_parent(rightmost_child_page) = new_left_child_id;
    
    // 拿到右孩子页面
    void* new_right_child_page = get_page(table->pager, old_right_page_id);
    *node_parent(new_right_child_page) = table->root_page_num;

    // printf("Left child page_id: %d, Right child page_id: %d\n", new_left_child_id, old_right_page_id);
    // printf("OK, have inserted into parent: %s\n", key_to_liftup);
    return;
  }
  else {
    // 分裂过的节点并不是根节点
    void* old_left_page = get_page(table->pager, old_left_page_id);
    uint32_t parent_of_old_id = *node_parent(old_left_page);
    void* parent_of_old_page = get_page(table->pager, parent_of_old_id);
    uint32_t num_keys_in_parent = *internal_node_num_keys(parent_of_old_page);

    if (num_keys_in_parent <= INTERNAL_NODE_MAX_CELLS - 1) {
      // printf("Parent Not Full! Normal Insert\n");

      uint32_t index = internal_node_find_child(parent_of_old_page, key_to_liftup);
      uint32_t rightmost_id_in_parent = *internal_node_right_child(parent_of_old_page);
      void* rightmost_child = get_page(table->pager, rightmost_id_in_parent);
      char* parent_max_key = get_node_max_key(parent_of_old_page);
      
      // print_internal_node_info(parent_of_old_page, parent_of_old_id);

      if (strcmp(key_to_liftup, parent_max_key) >= 0) {
        // 最大! 更新最右指针, 将原来的移到内部
        // printf("Old Max Child is %d\n", *internal_node_right_child(parent_of_old_page));
        *internal_node_num_keys(parent_of_old_page) = num_keys_in_parent + 1;

        *internal_node_child(parent_of_old_page, num_keys_in_parent) = *internal_node_right_child(parent_of_old_page);
        // printf("Mid Key Ptr: %d, Num keys = %d\n", *internal_node_child(parent_of_old_page, num_keys_in_parent), num_keys_in_parent);
        memcpy(internal_node_key(parent_of_old_page, num_keys_in_parent), key_to_liftup, INTERNAL_NODE_KEY_SIZE);
        // 然后将提升的键存入
        *internal_node_right_child(parent_of_old_page) = old_right_page_id;
        
        // only node appended, so update only one child pointer
        void* updated_rightmost_node = get_page(table->pager, old_right_page_id);
        *node_parent(updated_rightmost_node) = parent_of_old_id;
      } 
      else {
        // 不是最大的!
        for (int32_t i = num_keys_in_parent; i > index; --i) {
          void* dest = internal_node_cell(parent_of_old_page, i);
          void* src = internal_node_cell(parent_of_old_page, i - 1);
          memcpy(dest, src, INTERNAL_NODE_CELL_SIZE);
        }
        *internal_node_child(parent_of_old_page, index) = old_left_page_id;
        *node_parent(old_left_page) = parent_of_old_id; // 更新子节点的指针指向

        memcpy(internal_node_key(parent_of_old_page, index), key_to_liftup, 12);   
        *internal_node_child(parent_of_old_page, index + 1) = old_right_page_id;    
        
        void* old_right_page = get_page(table->pager, old_right_page_id);
        *node_parent(old_right_page) = parent_of_old_id; // 更新子节点的指针指向

        *internal_node_num_keys(parent_of_old_page) = num_keys_in_parent + 1;
      }

      // print_internal_node_info(parent_of_old_page, parent_of_old_id);
      return;
    }
    else {
      // 否则做分裂, 再递归调用
      uint32_t key_to_liftup_index = internal_node_find_child(parent_of_old_page, key_to_liftup); // 之前页面溢出的key 在它父结点中对应的位置是什么

      // 特殊! 当前 已满的父结点是根节点的情况! 直接新建一个根, 并分裂, CHECKED
      if (parent_of_old_id == 0) {
        // printf("Splitting Root!!\n");
        uint32_t new_left_part_id = get_unused_page_num(table->pager);
        void* new_left_part_root = get_page(table->pager, new_left_part_id);
        uint32_t new_right_part_id = get_unused_page_num(table->pager);
        void* new_right_part_root = get_page(table->pager, new_right_part_id);

        initialize_internal_node(new_left_part_root);
        initialize_internal_node(new_right_part_root);
        *internal_node_num_keys(parent_of_old_page) = num_keys_in_parent + 1; 

        if (key_to_liftup_index >= num_keys_in_parent) {
          // 加入的key是最大的, 需要修改rightmost信息
          uint32_t original_rightmost_child_id = *internal_node_right_child(parent_of_old_page);
          *internal_node_child(parent_of_old_page, num_keys_in_parent) = original_rightmost_child_id;
          memcpy(internal_node_key(parent_of_old_page, num_keys_in_parent), key_to_liftup, INTERNAL_NODE_KEY_SIZE); // | P(N-1) | K(N) | ... | rightmost
          *internal_node_right_child(parent_of_old_page) = old_right_page_id;
        }
        else {
          for (int32_t i = num_keys_in_parent; i > key_to_liftup_index; --i) {
            void* dest = internal_node_cell(parent_of_old_page, i);
            void* src = internal_node_cell(parent_of_old_page, i - 1);
            memcpy(dest, src, INTERNAL_NODE_CELL_SIZE);
          }
          *internal_node_child(parent_of_old_page, key_to_liftup_index) = old_left_page_id;
          memcpy(internal_node_key(parent_of_old_page, key_to_liftup_index), key_to_liftup, 12);    
          *internal_node_child(parent_of_old_page, key_to_liftup_index + 1) = old_right_page_id;    
        }
        
        int temp = *internal_node_num_keys(parent_of_old_page);
        *internal_node_num_keys(new_left_part_root) = (temp - 1) / 2;
        *internal_node_num_keys(new_right_part_root) = (temp - 1) - *internal_node_num_keys(new_left_part_root);

        uint32_t left_part_root_num_cells = *internal_node_num_keys(new_left_part_root);
        uint32_t right_part_root_num_cells = *internal_node_num_keys(new_right_part_root);
        int32_t mid = left_part_root_num_cells; 
        char mid_key[12];
        memcpy(mid_key, internal_node_key(parent_of_old_page, mid), INTERNAL_NODE_KEY_SIZE);

        for (int i = 0; i < left_part_root_num_cells; ++i) {
          memcpy(internal_node_cell(new_left_part_root, i), internal_node_cell(parent_of_old_page, i), INTERNAL_NODE_CELL_SIZE);
        }
        *internal_node_right_child(new_left_part_root) = *internal_node_child(parent_of_old_page, left_part_root_num_cells);

        for (int i = 0; i < right_part_root_num_cells; ++i) {
          memcpy(internal_node_cell(new_right_part_root, i), internal_node_cell(parent_of_old_page, i + left_part_root_num_cells + 1), INTERNAL_NODE_CELL_SIZE);
        }
        *internal_node_right_child(new_right_part_root) = *internal_node_right_child(parent_of_old_page);

        for (int i = 0; i < left_part_root_num_cells; ++i) {
          void* c = get_page(table->pager, *internal_node_child(new_left_part_root, i));
          *node_parent(c) = new_left_part_id;
        }
        void* rm = get_page(table->pager, *internal_node_right_child(new_left_part_root));
        *node_parent(rm) = new_left_part_id;

        for (int i = 0; i < right_part_root_num_cells; ++i) {
          void* c = get_page(table->pager, *internal_node_child(new_right_part_root, i));
          *node_parent(c) = new_right_part_id;
        }
        rm = get_page(table->pager, *internal_node_right_child(new_right_part_root));
        *node_parent(rm) = new_right_part_id;

        initialize_internal_node(parent_of_old_page);
        set_node_root(parent_of_old_page, true);
        *internal_node_num_keys(parent_of_old_page) = 1; 
        *internal_node_child(parent_of_old_page, 0) = new_left_part_id;
        *internal_node_right_child(parent_of_old_page) = new_right_part_id;

        char* key_to_be_set = internal_node_key(parent_of_old_page, 0);
        memcpy(key_to_be_set, mid_key, 12);

        *node_parent(new_left_part_root) = parent_of_old_id;
        *node_parent(new_right_part_root) = parent_of_old_id;
        return;
      }

      // 否则, 分裂内部的父结点, 并将分裂后的两个页面ID 和 新产生键传递给它的父结点
      uint32_t right_part_id = get_unused_page_num(table->pager); // 分裂之后的右半页面ID
      void* right_part_page = get_page(table->pager, right_part_id); // 分裂之后的右半页面

      // print_internal_node_info(parent_of_old_page, parent_of_old_id);

      *internal_node_num_keys(parent_of_old_page) = num_keys_in_parent + 1; 

      // 先假装不会发生溢出, 选择合适的位置插入相关的key, 更新与之对应的子页面指针
      if (key_to_liftup_index >= num_keys_in_parent) {
        // 加入的key是最大的, 需要修改rightmost信息
        uint32_t original_rightmost_child_id = *internal_node_right_child(parent_of_old_page);
        *internal_node_child(parent_of_old_page, num_keys_in_parent) = original_rightmost_child_id;
        memcpy(internal_node_key(parent_of_old_page, num_keys_in_parent), key_to_liftup, INTERNAL_NODE_KEY_SIZE); // | P(N-1) | K(N) | ... | rightmost
        *internal_node_right_child(parent_of_old_page) = old_right_page_id;
      }
      else {
        for (int32_t i = num_keys_in_parent; i > key_to_liftup_index; --i) {
          void* dest = internal_node_cell(parent_of_old_page, i);
          void* src = internal_node_cell(parent_of_old_page, i - 1);
          memcpy(dest, src, INTERNAL_NODE_CELL_SIZE);
        }
        *internal_node_child(parent_of_old_page, key_to_liftup_index) = old_left_page_id;
        memcpy(internal_node_key(parent_of_old_page, key_to_liftup_index), key_to_liftup, 12);    
        *internal_node_child(parent_of_old_page, key_to_liftup_index + 1) = old_right_page_id;    
      }

      uint32_t mid_liftup_index = (num_keys_in_parent + 1) / 2; // 插入之间溢出键后, 父结点发生溢出, 相应产生的键是什么, 索引是什么
      char* key_to_liftup_by_old_parent = internal_node_key(parent_of_old_page, mid_liftup_index);
      uint32_t reserved_child_for_leftpart = *internal_node_child(parent_of_old_page, mid_liftup_index);

      // 更新新分裂内部节点的键数量
      *internal_node_num_keys(right_part_page) = (num_keys_in_parent) / 2;
      int32_t right_part_size = *internal_node_num_keys(right_part_page);
      int32_t left_part_size = num_keys_in_parent - right_part_size;
      // printf("Parent Size %d, Left Size %d, Right Size %d\n", num_keys_in_parent + 1, left_part_size, right_part_size);
      // printf("Key to be lifted will be %s\n", key_to_liftup_by_old_parent);

      for (int32_t i = 0; i < right_part_size; ++i) {
        memcpy(internal_node_cell(right_part_page, i), internal_node_cell(parent_of_old_page, left_part_size + i + 1), INTERNAL_NODE_CELL_SIZE);
      }

      // 更新子节点的指针信息
      // 最右指针更新
      *internal_node_right_child(right_part_page) = *internal_node_right_child(parent_of_old_page);
      void* right_page_rightmost_child = get_page(table->pager, *internal_node_right_child(right_part_page));
      *node_parent(right_page_rightmost_child) = right_part_id;

      // 其余的子页面指针M 让其指向父页面
      for (int32_t i = 0; i < right_part_size; ++i) {
        // printf("Right child has key %s\n", internal_node_key(right_part_page, i));
        void* right_page_child = get_page(table->pager, *internal_node_child(right_part_page, i));
        *node_parent(right_page_child) = right_part_id;
      }
      
      // 更新左侧部分的信息
      *internal_node_num_keys(parent_of_old_page) = left_part_size;
      for (int32_t i = 0; i < left_part_size; ++i) {
        // printf("Left child has key %s\n", internal_node_key(parent_of_old_page, i));
        void* left_page_child = get_page(table->pager, *internal_node_child(parent_of_old_page, i));
        *node_parent(left_page_child) = parent_of_old_id;
      }
      *internal_node_right_child(parent_of_old_page) = reserved_child_for_leftpart; // !!!左侧孩子的最右指针就是被提上去的key对应的指针!

      // print_internal_node_info(parent_of_old_page, parent_of_old_id);
      // print_internal_node_info(right_part_page, right_part_id);

      return insert_into_parent(table, parent_of_old_id, right_part_id, key_to_liftup_by_old_parent);
    }
  }
}

// 叶子 -> 最底层内部节点的插入 : parent: child(叶子)的父结点, child: 是右侧的孩子页面ID
void internal_node_insert (Table* table, uint32_t parent_page_id, uint32_t child_page_id, char* key_to_insert) {
  void* parent = get_page(table->pager, parent_page_id);
  void* child = get_page(table->pager, child_page_id);
  // char* child_max_key = get_node_max_key(child);
  char* parent_max_key = get_node_max_key(parent);
  uint32_t index = internal_node_find_child(parent, key_to_insert);

  uint32_t rightmost_child_page_id = *internal_node_right_child(parent);
  void* rightmost_child = get_page(table->pager, rightmost_child_page_id); 

  uint32_t original_num_keys = *internal_node_num_keys(parent);
  *internal_node_num_keys(parent) = original_num_keys + 1; // 增加内部节点的指针数量
  // print_internal_node_info(parent, parent_page_id);

  // 先假装没有溢出发生! 正常的插入信息, 并且更新相应的指针(如果必要)
  if (strcmp(key_to_insert, parent_max_key) >= 0) {
    // 叶子分裂发生在最右侧 -> 原先内部节点的最右孩子指针将指向新的生成页面(child_page_id)
    // 并将原先的最右指针移到内部的最后一个child内
    *internal_node_child(parent, original_num_keys) = rightmost_child_page_id;
    memcpy(internal_node_key(parent, original_num_keys), key_to_insert, INTERNAL_NODE_KEY_SIZE);
    *internal_node_right_child(parent) = child_page_id;
  }
  else {
    // | P1 | K1 | ... | P_N | K_N<INDEX> | ... | ... | P_LAST | RIGHTMOST
    // 
    for (int32_t i = original_num_keys; i > index; --i) {
      void* dest = internal_node_cell(parent, i);
      void* src = internal_node_cell(parent, i - 1);
      memcpy(dest, src, INTERNAL_NODE_CELL_SIZE);
    }
    *internal_node_child(parent, index + 1) = child_page_id;
    strncpy(internal_node_key(parent, index), key_to_insert, INTERNAL_NODE_KEY_SIZE);
  }
  // print_internal_node_info(parent, parent_page_id);

  // 插入一个之后超过了上限, 那么就需要分裂
  if (original_num_keys + 1 > INTERNAL_NODE_MAX_CELLS) { // a certain number
    // Fetching a new page from disk 
    uint32_t new_internal_page_id = get_unused_page_num(table->pager);
    void* new_internal_node = get_page(table->pager, new_internal_page_id);

    // split origin data into two pages
    // printf("Left child page id is %d, Right page id is %d\n", parent_page_id, new_internal_page_id);
    int32_t mid_index = (1 + original_num_keys) / 2;

    // update 右孩子节点的指针信息 与 指针数量
    int32_t left_child_num_keys = mid_index;
    // printf("Mid Index is %d\n", mid_index);

    int32_t right_child_num_keys = original_num_keys - left_child_num_keys;
    *internal_node_num_keys(new_internal_node) = right_child_num_keys;

    for (int32_t i = 0; i < right_child_num_keys; ++i) {
      memcpy(internal_node_cell(new_internal_node, i), internal_node_cell(parent, i + left_child_num_keys + 1), INTERNAL_NODE_CELL_SIZE);
    }
    
    *internal_node_right_child(new_internal_node) = *internal_node_right_child(parent);
    void* right_page_rightmost_child = get_page(table->pager, *internal_node_right_child(new_internal_node));
    *node_parent(right_page_rightmost_child) = new_internal_page_id;

    for (int32_t i = 0; i < right_child_num_keys; ++i) {
      // printf("Right child has key %s\n", internal_node_key(new_internal_node, i));
      void* right_page_child = get_page(table->pager, *internal_node_child(new_internal_node, i));
      *node_parent(right_page_child) = new_internal_page_id;
    }

    // update old node's rightmost child_id
    // print_internal_node_info(parent, parent_page_id);
    uint32_t left_s_rightmost = *internal_node_child(parent, mid_index);

    *internal_node_right_child(parent) = *internal_node_child(parent, mid_index);

    void* left_page_rightmost_child = get_page(table->pager, *internal_node_right_child(parent));
    *node_parent(left_page_rightmost_child) = parent_page_id;

    char* key_to_liftup = internal_node_key(parent, mid_index);
    
    *internal_node_num_keys(parent) = left_child_num_keys;
    for (int32_t i = 0; i < left_child_num_keys; ++i) {
      // printf("Left child has key %s\n", internal_node_key(parent, i));
      void* left_page_child = get_page(table->pager, *internal_node_child(parent, i));
      *node_parent(left_page_child) = parent_page_id;
    }

    return insert_into_parent(table, parent_page_id, new_internal_page_id, key_to_liftup);
  }
  else {
    for (int32_t i = 0; i < *internal_node_num_keys(parent); ++i) {
      void* child_page = get_page(table->pager, *internal_node_child(parent, i));
      *node_parent(child_page) = parent_page_id;
    }
    void* rightmost_child_page = get_page(table->pager, *internal_node_right_child(parent));
    *node_parent(rightmost_child_page) = parent_page_id;
  }
}

/* split full leafnodes into equal halves */
void leaf_node_split_and_insert (Cursor* cursor, char* key, Row* value) {
  /**
   * Creating a new node
   * Calling table->pager' to fetch a unused page.
   */
  uint32_t old_page_num = cursor->page_num;
  void* old_node = get_page(cursor->table->pager, old_page_num);
  char* old_max = get_node_max_key(old_node);
  uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
  // printf("Calling LeafNode Split! New Page Id will be %d\n", new_page_num);
  void* new_node = get_page(cursor->table->pager, new_page_num);
  initialize_leaf_node(new_node);
  *node_parent(new_node) = *node_parent(old_node);
  *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node); // update ptrs to next leaf
  *leaf_node_next_leaf(old_node) = new_page_num;

  /**
   * Splitting the old leaf, move data to its desired place.
   */
  for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; --i) {
    void* destination_node;
    uint32_t index_within_node = i;

    if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
      destination_node = new_node;
      index_within_node = i - LEAF_NODE_LEFT_SPLIT_COUNT;
    } else {
      destination_node = old_node;
    }
    
    void* destination = leaf_node_cell(destination_node, index_within_node); // cell = <key, value>

    if (i == cursor->cell_num) {
      serialize_row(value, leaf_node_cell(destination_node, index_within_node));
      memcpy(leaf_node_key(destination_node, index_within_node), key, 12);
    } else if (i > cursor->cell_num) {
      memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE); // copy value
    } else {
      memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
    }

    /* Update cell count on both leaf nodes */
    *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

  }

  if (is_node_root(old_node)) {
    return create_new_root(cursor->table, new_page_num);
  } else {
    uint32_t parent_page_num = *node_parent(old_node); // 拿到父结点的页面号
    char* new_max = get_node_max_key(old_node); // old_node 是左边节点的最大键
    void* parent = get_page(cursor->table->pager, parent_page_num); 

    // Magic Here?
    // update_internal_node_key(parent, old_max, new_max); // 分裂之后把左边的键加入到父结点中,更新
    internal_node_insert(cursor->table, parent_page_num, new_page_num, new_max);
    return;
  }
}

/* insert value into B+ Tree's leafnode */
void leaf_node_insert (Cursor* cursor, char* key, Row* value) {
  void* node = get_page(cursor->table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  // split leaf nodes and do insertion
  if (num_cells >= LEAF_NODE_MAX_CELLS) {
    return leaf_node_split_and_insert(cursor, key, value);
  }

  if (cursor->cell_num < num_cells) {
    // Make room for new cell
    for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
      memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
    }
  }

  *(leaf_node_num_cells(node)) += 1;

  /* memcpy key to target postion (instead of using pointers) */
  char* pos = (leaf_node_key(node, cursor->cell_num));
  memcpy(pos, value->b, 12);

  serialize_row(value, leaf_node_cell(node, cursor->cell_num));
}

/* the row to insert is stored in `statement.row` */
void b_tree_insert() {
  /* insert a row */

  // get the row first
  Row* row_to_insert = &(statement.row);

  // find the right pos to insert
  char* key_to_insert = row_to_insert->b;

  Cursor* cursor = table_find(table, key_to_insert);
  
  leaf_node_insert(cursor, row_to_insert->b, row_to_insert);

  free(cursor);
}

/* ------------------------------------------- */

bool merge_or_redistribute (void* node, uint32_t node_id, char* key);

/* 调整根节点的函数 */
bool adjust_root (void* node, uint32_t node_id) {
  NodeType root_type = get_node_type(node);
  if (root_type == NODE_INTERNAL) {
    uint32_t num_cells_in_root = *internal_node_num_keys(node);
    if (num_cells_in_root >= 1) {
      return false;
    }
    else {
      // 根节点空了!
      printf("Invalid Operation!\n");
      exit(EXIT_FAILURE);
    }
  }
  else {
    // 根是叶子节点, 说明整棵树只有一个节点了!
    uint32_t num_cells_in_root = *leaf_node_num_cells(node);
    if (num_cells_in_root >= 1) {
      // printf("Delete key from RootNode!\n");
      return false; // ok
    }
    else {
      // the whole tree is empty
      // printf("The Whole Tree is now Empty!\n");
      return true;
    }
  }
  
}

/* 内部节点重新分配算法 */
void internalnode_redistribute (void* cur_node, void* sib_node, void* parent_node, uint32_t value_index, bool rightmost) {
  // printf("Called Internal Node Redistribute!\n");

  // 父节点借出的节点键值
  char key_from_parent[12];
  strncpy(key_from_parent, internal_node_key(parent_node, value_index), INTERNAL_NODE_KEY_SIZE);

  // 兄弟节点借出的节点键值
  char key_from_sib[12];
  uint32_t child_id_from_sib;
  uint32_t sib_node_size;
  uint32_t cur_node_size;

  if (rightmost) {
    sib_node_size = *internal_node_num_keys(sib_node);
    strncpy(key_from_sib, internal_node_key(sib_node, sib_node_size - 1), INTERNAL_NODE_KEY_SIZE);
    // 位于左侧, 则借出的是最右指针
    child_id_from_sib = *internal_node_right_child(sib_node);

    // 第一步, 修改兄弟内部节点的指针, 删掉最后一个
    uint32_t new_rightmost_id = *internal_node_child(sib_node, sib_node_size - 1);
    *internal_node_right_child(sib_node) = new_rightmost_id;
    *internal_node_num_keys(sib_node) = sib_node_size - 1;

    // 第二步, 将父结点的键借给右孩子
    cur_node_size = *internal_node_num_keys(cur_node);
    *internal_node_num_keys(cur_node) = cur_node_size + 1;
    memcpy(internal_node_key(cur_node, 0), key_from_parent, INTERNAL_NODE_KEY_SIZE);

    // 第三步, 将父结点对应的键值修改
    strncpy(internal_node_key(parent_node, value_index), key_from_sib, INTERNAL_NODE_KEY_SIZE);

    // 第四步, 修改移入右侧内部节点的最左指针, 为原先兄弟的最右指针
    *internal_node_child(cur_node, 0) = child_id_from_sib;

  }
  else {
    // 不是最右侧内部节点! child_id_from_sib = 右侧第一个孩子节点
    strncpy(key_from_sib, internal_node_key(sib_node, 0), INTERNAL_NODE_KEY_SIZE);

    // 位于右侧的兄弟节点 借出节点 对应的孩子指针
    child_id_from_sib = *internal_node_child(sib_node, 0);

    // 第一步, 修改兄弟内部节点的指针, 删掉第一个
    sib_node_size = *internal_node_num_keys(sib_node);
    for (int32_t i = 0; i < sib_node_size - 1; ++i) {
      void* dst = internal_node_cell(sib_node, i);
      void* src = internal_node_cell(sib_node, i + 1);
      memcpy(dst, src, INTERNAL_NODE_CELL_SIZE);
    }
    *internal_node_num_keys(sib_node) = sib_node_size - 1;

    // 第二步, 将父结点的键借给左孩子
    cur_node_size = *internal_node_num_keys(cur_node) + 1;
    // printf("Current Internal Node has %d keys\n", cur_node_size);

    *internal_node_num_keys(cur_node) = cur_node_size;
    strncpy(internal_node_key(cur_node, cur_node_size - 1), key_from_parent, INTERNAL_NODE_KEY_SIZE);

    // 第三步, 将父结点对应的键值修改
    strncpy(internal_node_key(parent_node, value_index), key_from_sib, INTERNAL_NODE_KEY_SIZE);

    // 第四步, 修改移入左侧内部节点的最右指针, 为原先兄弟的最左指针
    *internal_node_right_child(cur_node) = child_id_from_sib;
  }
}

/* 叶子节点的重新分配算法函数 */
void leaf_redistribute (void* node, void* sib_node, void* parent_node, uint32_t value_index) {
  // value_index : the pointer index in parent_node
  // redistribute: from sib_node to node!
  // node: 删除操作发生的叶子节点
  // sib_node: node的兄弟节点(一般为右结点, 除非删除发生在最右), 将其中一个键分配给node

  // printf("Called Leaf Node Redistribute!\n"); // 测试信息

  uint32_t next_id = *leaf_node_next_leaf(node);
  bool has_right_sib = false;
  if (next_id != 0) {
    has_right_sib = true;
  }

  if (has_right_sib) {
    uint32_t cur_node_num_cells = *leaf_node_num_cells(node);
    char key_to_replace[12]; // temporary buffer
    memcpy(key_to_replace, leaf_node_key(sib_node, 0), LEAF_NODE_KEY_SIZE);
    memcpy(leaf_node_cell(node, cur_node_num_cells), leaf_node_cell(sib_node, 0), LEAF_NODE_CELL_SIZE);
    *leaf_node_num_cells(node) = cur_node_num_cells + 1;

    // change cell arrangements in sib node
    int32_t sib_node_size = *leaf_node_num_cells(sib_node);
    for (int32_t i = 0; i < sib_node_size - 1; ++i) {
      memcpy(leaf_node_cell(sib_node, i), leaf_node_cell(sib_node, i + 1), LEAF_NODE_CELL_SIZE);
    }
    *leaf_node_num_cells(sib_node) = sib_node_size - 1;

    // change key in parent node ?
    // printf("Key index to be replaced in the internal node is [%d], key [%s]\n", value_index, key_to_replace);

    memcpy(internal_node_key(parent_node, value_index), key_to_replace, INTERNAL_NODE_KEY_SIZE);
    return;
  }
  else {
    char left_sib_last[12];
    uint32_t left_sib_size = *leaf_node_num_cells(sib_node);
    memcpy(left_sib_last, leaf_node_key(sib_node, left_sib_size - 1), LEAF_NODE_KEY_SIZE);

    // make room for first key
    uint32_t cur_node_size = *leaf_node_num_cells(node);
    for (int32_t i = cur_node_size; i >= 1; --i) {
      void* dst = leaf_node_cell(node, i);
      void* src = leaf_node_cell(node, i - 1);
      memcpy(dst, src, LEAF_NODE_CELL_SIZE);
    }

    // copying into cur_node(on the right-side)
    memcpy(leaf_node_cell(node, 0), leaf_node_cell(sib_node, left_sib_size - 1), LEAF_NODE_CELL_SIZE);
    
    // modifying key in parent node
    memcpy(internal_node_key(parent_node, value_index), left_sib_last, INTERNAL_NODE_KEY_SIZE);
    return;
  }
}

/* 内部节点合并算法 */
void internalnode_merge (void* sib_page, void* cur_page, void* parent_page, uint32_t parent_id, char* key, bool rightmost) {
  // first borrowing from parent node
  uint32_t child_index_in_parent = internal_node_find_child(parent_page, key);
  uint32_t cur_size = *internal_node_num_keys(cur_page); // 当前节点的键数
  uint32_t sib_size = *internal_node_num_keys(sib_page); // 兄弟节点的键数
  char key_to_borrow_buf[12];

  memcpy(key_to_borrow_buf, internal_node_key(parent_page, child_index_in_parent), INTERNAL_NODE_KEY_SIZE);
  // printf("Borrowed Key [%s] from parent! Index is [%d]\n", key_to_borrow_buf, child_index_in_parent);
  // printf("Before borrowing from parent, cur_node size is [%d]\n", cur_size);
  memcpy(internal_node_key(cur_page, cur_size), key_to_borrow_buf, INTERNAL_NODE_KEY_SIZE);

  // 调整父结点内部的键,指针情况
  int32_t num_keys_in_parent = *internal_node_num_keys(parent_page);
  *internal_node_num_keys(parent_page) = num_keys_in_parent - 1;
  for (int32_t i = child_index_in_parent; i < num_keys_in_parent - 1; ++i) {
    void* dst = internal_node_cell(parent_page, i);
    void* src = internal_node_cell(parent_page, i + 1);
    memcpy(dst, src, INTERNAL_NODE_CELL_SIZE);
  }

  cur_size += 1;
  // printf("After borrowing from parent, cur_node size is [%d]\n", cur_size);  
  *internal_node_num_keys(cur_page) = sib_size + cur_size;
  // printf("%d\n", *internal_node_num_keys(cur_page));

  // 当前内部节点与兄弟节点进行合并
  if (rightmost) {
    // 右边 合并 左边
    // 右边的内部节点给左边的腾出空间
    // cur_size 向后移动 sib_size

    uint32_t leftpart_rightmost_id = *internal_node_right_child(sib_page); // 注意左侧节点的最右侧孩子指针将并为中间指针
    for (int32_t i = 0; i < cur_size; ++i) {
      void* dst = internal_node_cell(cur_page, sib_size + i);
      void* src = internal_node_cell(cur_page, i);
      memcpy(dst, src, INTERNAL_NODE_CELL_SIZE);
    }

    for (int32_t i = 0; i < sib_size; ++i) {
      void* dst = internal_node_cell(cur_page, i);
      void* src = internal_node_cell(sib_page, i);
      memcpy(dst, src, INTERNAL_NODE_CELL_SIZE);
    }

    *internal_node_child(cur_page, sib_size) = leftpart_rightmost_id;
    memset(sib_page, 0, PAGE_SIZE);
  }
  else {
    // 左边 合并 右边
    // printf("Not Merging By RightMost!!! Cur Size is %d\n", cur_size);

    for (int32_t i = 0; i < sib_size; ++i) {
      memcpy(internal_node_cell(cur_page, cur_size + i), internal_node_cell(sib_page, i), INTERNAL_NODE_CELL_SIZE);
    }
    *internal_node_right_child(cur_page) = *internal_node_right_child(sib_page);
    // memset(sib_page, 0, PAGE_SIZE);
  }
  
  // last, check parent_node, if is root, meaning that internal nodes are merged
  // leaving only one root node
  if (parent_id == 0) {
    // printf("Borrowed One From Root Node!\n");

    int32_t parent_size = *internal_node_num_keys(parent_page);
    // printf("After Borrowing, Root Has %d Keys.\n", parent_size);

    if (parent_size <= 0) {
      memcpy(parent_page, cur_page, PAGE_SIZE);
      memset(cur_page, 0, PAGE_SIZE);
      set_node_root(parent_page, true);

      for (uint32_t i = 0; i < parent_size; ++i) {
        void* temp_child = get_page(table->pager, *internal_node_child(parent_page, i));
        *node_parent(temp_child) = parent_id;
      }

      void* rightmost_child = get_page(table->pager, *internal_node_right_child(parent_page));
      *node_parent(rightmost_child) = parent_id;
      // printf("Changing RootNode!\n"); // 整棵树的高度将下降1
      return;
    }
    else {
      return;
    }
  }
  else {
    // else, recursively check his parents
    if (*internal_node_num_keys(parent_page) >= INTERNAL_NODE_MIN_CELLS) {
      return;
    }
    else {
      merge_or_redistribute(parent_page, parent_id, key_to_borrow_buf);
      return;
    }
  }
  // printf("Exited From Internal Node Merge!\n");
}

/* 合并操作: 将src对应的节点数据 全部移植到dst */
void leafnode_move_all_to(void* src, void* dst) {
  uint32_t num_cells_in_src = *leaf_node_num_cells(src);
  uint32_t start_in_dst = *leaf_node_num_cells(dst);

  for (int32_t i = 0; i < num_cells_in_src; ++i) {
    memcpy(leaf_node_cell(dst, start_in_dst + i), leaf_node_cell(src, i), LEAF_NODE_CELL_SIZE);
  }
  uint32_t num_cells_in_dst = *leaf_node_num_cells(dst);
  *leaf_node_num_cells(dst) = num_cells_in_src + num_cells_in_dst;
  *leaf_node_num_cells(src) = 0;

  *leaf_node_next_leaf(dst) = *leaf_node_next_leaf(src);
  *leaf_node_next_leaf(src) = 0;

}

/* 合并两个叶子节点的算法 */
void leafnode_merge (void* sib_page, void* cur_page, void* parent_page, uint32_t parent_id, char* key, bool rightmost) {
  // printf("LeafNode Merging Type: [%d]\n", rightmost);

  // 首先, 将节点合并
  if (rightmost)
    leafnode_move_all_to(cur_page, sib_page); // always returns leftside page, src -> dst
  else
    leafnode_move_all_to(sib_page, cur_page);

  uint32_t key_index = internal_node_find_child(parent_page, key);
  uint32_t key_num = *internal_node_num_keys(parent_page);
  // printf("Key is: [%s], Key's index is [%d] and key_num is [%d]\n", key, key_index, key_num);

  // 删除父结点中指向 被合并的页面(sib)的指针
  if (key_index < key_num - 1) { // 一个小改动: key_index == key_num - 1, 就已经代表是最右key了
    // 左侧&中间节点的情况
    // 这里有问题? pointer[index]需要保留, 而被合并的右侧页面指针(pointer[index + 1]) 将会失效!!!
    uint32_t child_reserve = *internal_node_child(parent_page, key_index);
    // printf("Child is [%d]\n", child_reserve);
    for (uint32_t i = key_index; i < key_num - 1; ++i) {
      void* dst = internal_node_cell(parent_page, i);
      void* src = internal_node_cell(parent_page, i + 1);
      memcpy(dst, src, INTERNAL_NODE_CELL_SIZE);
    }
    *internal_node_child(parent_page, key_index) = child_reserve;
  }
  else {
    // 最右侧页面被合并到左侧, 需要移除最右指针
    if (key_num > 1) {
      int32_t new_right_most_child = *internal_node_child(parent_page, key_index - 1);
      // printf("After Merging, The new Child's Page Id is [%d]\n", new_right_most_child);
      *internal_node_right_child(parent_page) = new_right_most_child;
    }
    else {
      // printf("Removing only key in the parent node!\n");
    }
  }

  // 只需要修改一下父结点中的键即可
  *internal_node_num_keys(parent_page) = key_num - 1;

  if (parent_id == 0) {
    // 父结点就是根节点, 而当前合并了两个叶子
    if (*internal_node_num_keys(parent_page) == 0) {
      // 根节点空了
      if (rightmost) {
        memcpy(parent_page, sib_page, PAGE_SIZE);
        *leaf_node_num_cells(sib_page) = 0;
      }
      else {
        memcpy(parent_page, cur_page, PAGE_SIZE);
        *leaf_node_num_cells(cur_page) = 0;
      }
      set_node_type(parent_page, NODE_LEAF);
      set_node_root(parent_page, true);
      *leaf_node_next_leaf(parent_page) = 0;
    }
  }
  else {
    merge_or_redistribute(parent_page, parent_id, key);
    return;
  }
}

/* 判断节点下溢的情况选择合并 还是 重新分配的函数 */
bool merge_or_redistribute (void* node, uint32_t node_id, char* key) {
  // node: 当前的节点, 这里需要注意节点的类型, 是叶子还是内部!!!
  // node_id: 当前节点ID
  // key: 将要删除的键

  if (node_id == table->root_page_num) {
    return adjust_root(node, node_id);
  } 
  
  // 根据节点类型进行相应的判断
  // 如果删除后节点数不发生下溢, 则直接返回
  NodeType node_type = get_node_type(node);
  switch (node_type) {
    case NODE_LEAF: {
      uint32_t num_cells_in_leaf = *leaf_node_num_cells(node);
      if (num_cells_in_leaf >= LEAF_NODE_MAX_CELLS / 2) {
        return false;
      }
      break;
    }  
    case NODE_INTERNAL: {
      uint32_t num_cells_in_internal = *internal_node_num_keys(node);
      // printf("Cur Internal Node has %d key(s)!\n", num_cells_in_internal);
      if (num_cells_in_internal >= INTERNAL_NODE_MIN_CELLS) {
        return false;
      }
      break;
    }
  }

  // 否则, 需要进行合并 / 重新分配 
  uint32_t parent_id = *node_parent(node);
  void* parent_node = get_page(table->pager, parent_id);
  if (!parent_node) {
    printf("Error! Tried to access a NULL page!\n");
    exit(EXIT_FAILURE);
  }

  uint32_t sib_node_id;
  void* sib_node;
  bool is_redistribute = false;
  bool to_be_merge;
  bool rightmost;
  uint32_t child_index = internal_node_find_child(parent_node, key);
  // printf("Child index is %d\n", child_index);

  if (node_type == NODE_LEAF) {
    // 当前待合并/重新分配的节点是叶子节点   
    // 找它的兄弟节点(在同一父结点下才是兄弟节点)
    uint32_t num_child_in_parent = *internal_node_num_keys(parent_node);
    if (child_index == num_child_in_parent) {
      // rightmost leaf node
      sib_node_id = *internal_node_child(parent_node, num_child_in_parent - 1);
      rightmost = true;
    }
    else {
      // not the rightmost, so direct next is th sibling
      sib_node_id = *leaf_node_next_leaf(node);
      rightmost = false;
    }
    
    sib_node = get_page(table->pager, sib_node_id);
    uint32_t sib_num_cells = *leaf_node_num_cells(sib_node);
    uint32_t cur_num_cells = *leaf_node_num_cells(node);
    if (sib_num_cells >= 1 + LEAF_NODE_MIN_CELLS) {
      is_redistribute = true;
      to_be_merge = false;
    }
  }
  else {
    // 当前待分配的节点是内部节点
    uint32_t cur_node_index_in_parent = internal_node_find_child(parent_node, key);
    uint32_t num_child_in_parent = *internal_node_num_keys(parent_node);
    // printf("Parent has %d keys, while Cur Node index in parent is %d\n", num_child_in_parent, cur_node_index_in_parent);
    if (cur_node_index_in_parent >= num_child_in_parent) {
      sib_node_id = *internal_node_child(parent_node, cur_node_index_in_parent - 1);
      rightmost = true;
    }
    else {
      sib_node_id = *internal_node_child(parent_node, cur_node_index_in_parent + 1);
      rightmost = false;
    }
    sib_node = get_page(table->pager, sib_node_id);

    uint32_t sib_num_cells = *internal_node_num_keys(sib_node);
    uint32_t cur_num_cells = *internal_node_num_keys(node);

    if (sib_num_cells >= 1 + INTERNAL_NODE_MIN_CELLS) {
      is_redistribute = true;
      to_be_merge = false;
    }
  }


  if (is_redistribute) {
    if (node_type == NODE_LEAF) {
      leaf_redistribute(node, sib_node, parent_node, child_index);
    }
    else {
      internalnode_redistribute(node, sib_node, parent_node, child_index, rightmost);
    }
    return false;
  }

  if (node_type == NODE_LEAF) {
    // printf("Leaf Node Merging! Sib is [%d], and Cur is [%d]\n", sib_node_id, node_id);
    leafnode_merge(sib_node, node, parent_node, parent_id, key, rightmost);
    return true;
  }
  else {
    // printf("Internal Node Merging! Sib is [%d], and Cur is [%d]\n", sib_node_id, node_id);
    internalnode_merge(sib_node, node, parent_node, parent_id, key, rightmost);
    return true;
  }
  
  return to_be_merge;
}

/* 实现 叶子节点内部的关键值删除 */
bool leaf_node_delete (uint32_t page_id, uint32_t cell_num, char* keys_to_delete) {
  // printf("Find key in Page [%d], cell num is [%d]\n", page_id, cell_num);

  // 当前执行删除的叶子节点
  void* node = get_page(table->pager, page_id);
  if (!node) {
    return false;
  }
  // printf("Successfully fetched page with %d keys\n", *leaf_node_num_cells(node));
  // 当前叶子节点拥有的键数
  uint32_t leaf_num_cells = *leaf_node_num_cells(node);
  uint32_t index = cell_num;

  // 判断!找到的位置与待删除的键进行比较, 如果不一样, 说明已经不存在未删除的键, 直接返回
  char* key_at_index = leaf_node_key(node, index); 
  // printf("Keys to delete is %s, while key at index is %s, Index is %d\n", keys_to_delete, key_at_index, index);
  if (strcmp(key_at_index, keys_to_delete) != 0 || leaf_num_cells == 0 || cell_num == leaf_num_cells) {
    return false;
  }
  
  memset(leaf_node_cell(node, index), 0, LEAF_NODE_CELL_SIZE);
  for (int32_t i = index; i < leaf_num_cells - 1; ++i) {
    void* dest = leaf_node_cell(node, i);
    void* src = leaf_node_cell(node, i + 1);
    memcpy(dest, src, LEAF_NODE_CELL_SIZE);
  }
  leaf_num_cells -= 1;
  *leaf_node_num_cells(node) = leaf_num_cells;
  
  merge_or_redistribute(node, page_id, keys_to_delete);
  return true;   
}

/* the key to delete is stored in `statement.row.b` */
void b_tree_delete() {
  /* delete row(s) */  
  char* keys_to_delete = statement.row.b;

  /* Memory Leak! */
  while (true) {
    Cursor* cursor = table_find(table, keys_to_delete);
    if (cursor->end_of_table) {
      free(cursor);
      break;
    }
    else {
      bool test_ans = leaf_node_delete(cursor->page_num, cursor->cell_num, keys_to_delete);
      if (!test_ans) {
        free(cursor);
        break;
      }
      else {
        free(cursor);
      }
    }
  }

}

void b_tree_traverse() {
  /* print all rows */
  Row row;
  Cursor* cursor = table_start(table);
  if (cursor->end_of_table) {
    printf("(Empty)\n");
  }
  else {
    while (!(cursor->end_of_table)) {
      deserialize_row(cursor_value(cursor), &row);
      print_row(&row);
      cursor_advance(cursor);
    }
  }
  free(cursor);
  return;
}

/* logic starts */

void print_constants() {
  printf("ROW_SIZE: %d\n", ROW_SIZE);
  printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
  printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
  printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
  printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
  printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

typedef enum {
  EXECUTE_SUCCESS,
  EXECUTE_TABLE_FULL,
  EXECUTE_DUPLICATE_KEY
} ExecuteResult;

typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
  PREPARE_SUCCESS,
  PREPARE_NEGATIVE_VALUE,
  PREPARE_STRING_TOO_LONG,
  PREPARE_SYNTAX_ERROR,
  PREPARE_UNRECOGNIZED_STATEMENT,
  PREPARE_EMPTY_STATEMENT
} PrepareResult;

MetaCommandResult do_meta_command() {
  if (strcmp(input_buffer.buffer, ".exit") == 0) {
    exit(EXIT_SUCCESS);
  } else if (strcmp(input_buffer.buffer, ".constants") == 0) {
    printf("Constants:\n");
    print_constants();
    return META_COMMAND_SUCCESS;    
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

PrepareResult prepare_insert() {
  statement.type = STATEMENT_INSERT;

  char* keyword = strtok(input_buffer.buffer, " ");
  char* a = strtok(NULL, " ");
  char* b = strtok(NULL, " ");
  int x;

  if (a == NULL || b == NULL)
    return PREPARE_SYNTAX_ERROR;

  x = atoi(a);
  if (x < 0)
    return PREPARE_NEGATIVE_VALUE;
  if (strlen(b) > COLUMN_B_SIZE)
    return PREPARE_STRING_TOO_LONG;

  statement.row.a = x;
  strcpy(statement.row.b, b);

  return PREPARE_SUCCESS;
}

PrepareResult prepare_condition() {
  statement.flag = 0;

  char* keyword = strtok(input_buffer.buffer, " ");
  char* b = strtok(NULL, " ");
  char* c = strtok(NULL, " ");

  if (b == NULL) return PREPARE_SUCCESS;
  if (c != NULL) return PREPARE_SYNTAX_ERROR;

  if (strlen(b) > COLUMN_B_SIZE)
    return PREPARE_STRING_TOO_LONG;

  strcpy(statement.row.b, b);
  statement.flag |= 2;

  return PREPARE_SUCCESS;
}

PrepareResult prepare_select() {
  statement.type = STATEMENT_SELECT;
  return prepare_condition();
}

PrepareResult prepare_delete() {
  statement.type = STATEMENT_DELETE;
  PrepareResult result = prepare_condition();
  if (result == PREPARE_SUCCESS && statement.flag == 0)
    return PREPARE_SYNTAX_ERROR;
  return result;
}

PrepareResult prepare_statement() {
  if (strlen(input_buffer.buffer) == 0) {
    return PREPARE_EMPTY_STATEMENT;
  } else if (strncmp(input_buffer.buffer, "insert", 6) == 0) {
    return prepare_insert();
  } else if (strncmp(input_buffer.buffer, "select", 6) == 0) {
    return prepare_select();
  } else if (strncmp(input_buffer.buffer, "delete", 6) == 0) {
    return prepare_delete();
  }
  return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_select() {
  printf("\n");
  if (statement.flag == 0) {
    b_tree_traverse();
  } else {
    b_tree_search();
  }
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement() {
  switch (statement.type) {
    case STATEMENT_INSERT:
      b_tree_insert();
      return EXECUTE_SUCCESS;
    case STATEMENT_SELECT:
      return execute_select();
    case STATEMENT_DELETE:
      b_tree_delete();
      return EXECUTE_SUCCESS;
  }
}

void sigint_handler(int signum) {
  printf("\n");
  exit(EXIT_SUCCESS);
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("Must supply a database filename.\n");
    exit(EXIT_FAILURE);
  }

  atexit(&exit_success);
  signal(SIGINT, &sigint_handler);

  open_file(argv[1]);

  while (1) {
    print_prompt();
    switch (read_input()) {
      case INPUT_SUCCESS:
        break;
      case INPUT_TOO_LONG:
        printf("Input is too long.\n");
        continue;
    }

    if (input_buffer.buffer[0] == '.') {
      switch (do_meta_command()) {
        case META_COMMAND_SUCCESS:
          continue;
        case META_COMMAND_UNRECOGNIZED_COMMAND:
          printf("Unrecognized command '%s'.\n", input_buffer.buffer);
          continue;
      }
    }

    switch (prepare_statement()) {
      case PREPARE_SUCCESS:
        break;
      case PREPARE_EMPTY_STATEMENT:
        continue;
      case PREPARE_NEGATIVE_VALUE:
        printf("Column `a` must be positive.\n");
        continue;
      case PREPARE_STRING_TOO_LONG:
        printf("String for column `b` is too long.\n");
        continue;
      case PREPARE_SYNTAX_ERROR:
        printf("Syntax error. Could not parse statement.\n");
        continue;
      case PREPARE_UNRECOGNIZED_STATEMENT:
        printf("Unrecognized keyword at start of '%s'.\n",
               input_buffer.buffer);
        continue;
    }

    switch (execute_statement()) {
      case EXECUTE_SUCCESS:
        printf("\nExecuted.\n\n");
        break;
    }
  }

  return 0;
}
