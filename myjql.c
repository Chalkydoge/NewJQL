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
#define TABLE_MAX_PAGES 16
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

const uint32_t PAGE_SIZE = 128; // 4KB page

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
    printf("Tried to fetch page number out of bound.\n");
    exit(EXIT_FAILURE);
  }

  if (!pager->pages[page_num]) {
    // Cache miss. Allocate memory and load from disk.
    void *page = malloc(PAGE_SIZE);
    uint32_t num_pages = pager->file_length / PAGE_SIZE; // pager has 'num_pages' page

    if (pager->file_length % PAGE_SIZE) {
      // trailing spaces at the end of file.
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

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
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
  printf("Had Written: %ld\n", bytes_written);
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
void* leaf_node_value(void*, uint32_t);
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

    printf("\nIndex: %d, Address: %p, String is: %s\n", index, key_at_index, key_at_index);

    int cmp = strcmp(key, key_at_index);
    if (cmp == 0) {
      // TODO: find the leftmost pos of same keys, then do insert
      uint32_t test_index = index;
      while (test_index >= 0) {
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
  printf("Index where %s is going to insert is: %d\n", key, min_index);
  cursor->cell_num = min_index;
  return cursor;  
}

/* Cursor finding value in internal nodes */
uint32_t* internal_node_num_keys(void*);
char* internal_node_key(void*, uint32_t);
uint32_t* internal_node_child (void*, uint32_t);
uint32_t* leaf_node_next_leaf(void*);

Cursor* internal_node_find (Table* table, uint32_t page_num, char* key) {
  void* node = get_page(table->pager, page_num);
  uint32_t num_keys = *internal_node_num_keys(node);

  uint32_t min_index = 0;
  uint32_t max_index = num_keys;

  while (min_index != max_index) {
    uint32_t index = (min_index + max_index) / 2;
    char* key_to_right = internal_node_key(node, index);
    int cmp = strcmp(key_to_right, key); 
    if (cmp >= 0) {
      max_index = index;
    } else {
      min_index = index + 1;
    }
  }

  uint32_t child_num = *internal_node_child(node, min_index);
  void* child_page = get_page(table->pager, child_num);

  switch (get_node_type(child_page)) {
    case NODE_LEAF: return leaf_node_find(table, child_num, key);
    case NODE_INTERNAL: return internal_node_find(table, child_num, key);
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
  printf("Starting Page num is: %d\n", cursor->page_num);

  void* node = get_page(table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);
  cursor->end_of_table = (num_cells == 0);

  return cursor;
}

void* cursor_value (Cursor* cursor) {
  uint32_t page_num = cursor->page_num;
  void* page = get_page(cursor->table->pager, page_num);
  return leaf_node_value(page, cursor->cell_num);
}

void cursor_advance(Cursor* cursor) {
  uint32_t page_num = cursor->page_num;
  void* node = get_page(cursor->table->pager, page_num);
  cursor->cell_num += 1;
  printf("Now we're visiting Page: %d\n", page_num);

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
  // |---a---|------b------|
  //  int(4)    char(12)
  memcpy(destination + 0, &(source->a), 4);
  memcpy(destination + 4, &(source->b), 12);
}

void deserialize_row(void* source, Row* destination) {
  memcpy(&(destination->a), source + 0, 4);
  memcpy(&(destination->b), source + 4, 12);
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
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_LEFT_SPLIT_COUNT;

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
  dest = key;
}

// given cell_num, return ptr to cell's value, value = [key, value] here
void* leaf_node_value (void* node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

// return leafnode's next leaf's page_id
uint32_t* leaf_node_next_leaf(void* node) {
  return node + LEAF_NODE_NEXT_LEAF_SIZE_OFFSET;
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
  return internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

char* get_node_max_key(void* node) {
  switch (get_node_type(node)) {
    case NODE_INTERNAL:
      return internal_node_key(node, *internal_node_num_keys(node) - 1);
    case NODE_LEAF:
      return leaf_node_key(node, *leaf_node_num_cells(node) - 1);
  }
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

/*---------------------------------------------*/



/* B-Tree operations */

/* the key to select is stored in `statement.row.b` */
void b_tree_search() {
  /* print selected rows */
  printf("[INFO] select: %s\n", statement.row.b);
}

/* Create a new root
*  Called only in insertion when rootnode have to be updated !
*/
void create_new_root(Table* table, uint32_t right_child_page_num) {
  /*
  Handle splitting the root.
  Old root copied to new page, becomes left child.
  Address of right child passed in.
  Re-initialize root page to contain the new root node.
  New root node points to two children.
  */

  void* root = get_page(table->pager, table->root_page_num);
  void* right_child = get_page(table->pager, right_child_page_num);
  uint32_t left_child_page_num = get_unused_page_num(table->pager);
  void* left_child = get_page(table->pager, left_child_page_num);
  printf("Left child's page_id is: %d\n", left_child_page_num);

  /* Left child has data copied from old root */
  memcpy(left_child, root, PAGE_SIZE);
  set_node_root(left_child, false);

  /* Root node is a new internal node with one key and two children */
  initialize_internal_node(root);
  set_node_root(root, true);
  *internal_node_num_keys(root) = 1;
  *internal_node_child(root, 0) = left_child_page_num;

  // get maxchild string in the leftchild page
  char* left_child_max_key = get_node_max_key(left_child);
  // printf("The maximum key in leftchild is: %s\n", left_child_max_key);
  
  char* key_to_be_set = internal_node_key(root, 0);
  // printf("And it will be set as the first key in the root node!\n");

  memcpy(key_to_be_set, left_child_max_key, 12);

  *internal_node_right_child(root) = right_child_page_num;
  printf("Left child page_id: %d, Right child page_id: %d\n", left_child_page_num, right_child_page_num);
}

void create_new_internal_node(Table* table, uint32_t new_internal_page_num) {

}

/* split full leafnodes into equal halves */
void leaf_node_split_and_insert (Cursor* cursor, char* key, Row* value) {
  /**
   * Creating a new node
   * Calling table->pager' to fetch a unused page.
   */
  void* old_node = get_page(cursor->table->pager, cursor->page_num);
  uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
  void* new_node = get_page(cursor->table->pager, new_page_num);
  initialize_leaf_node(new_node);
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
      printf("String %s\'s Index will be: %d\n", key, index_within_node);
      serialize_row(value, leaf_node_value(destination_node, index_within_node));
      set_leaf_node_key(destination_node, index_within_node, key); // copy key into leaf
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
    return create_new_internal_node(cursor->table, new_page_num);
  }
}

/* insert value into B+ Tree's leafnode */
void leaf_node_insert (Cursor* cursor, char* key, Row* value) {
  void* node = get_page(cursor->table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  // split leaf nodes and do insertion
  if (num_cells >= LEAF_NODE_MAX_CELLS) {
    printf("Need to be split!\n");
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

  serialize_row(value, leaf_node_value(node, cursor->cell_num));
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
  // printf("[INFO] insert: ");
  print_row(&statement.row);

  free(cursor);
}

/* the key to delete is stored in `statement.row.b` */
void b_tree_delete() {
  /* delete row(s) */
  printf("[INFO] delete: %s\n", statement.row.b);
}

void b_tree_traverse() {
  /* print all rows */
  Row row;
  Cursor* cursor = table_start(table);

  while (!(cursor->end_of_table)) {
    deserialize_row(cursor_value(cursor), &row);
    print_row(&row);
    cursor_advance(cursor);
  }
  printf("[INFO] traverse\n");

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