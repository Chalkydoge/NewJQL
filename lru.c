#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <stdbool.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#define TABLE_MAX_PAGES 4
#define PAGE_TABLE_SIZE 65536

const uint32_t PAGE_SIZE = 64; // small page
const uint32_t HASH_TABLE_MAX_SIZE = 65536;

/* struct listnode for LRU cache */
typedef struct ListNode {
    int32_t frame_id;
    struct ListNode* next;
    struct ListNode* prev;
} ListNode_t;

typedef struct LRU {
    int32_t capacity;
    int32_t size;
    ListNode_t head;
    ListNode_t tail;
    ListNode_t** hash_table;   
} LRU_cache;

typedef struct FreeList {
    struct ListNode head;
    struct ListNode tail;
    uint32_t size;
} FreeList_t;

typedef struct Page {
    void* content;
    bool is_dirty;
    int16_t pin_count;
    uint16_t page_id;
} Page_t;

/* A simple buffer pool manager */
typedef struct Pager {
    int file_descriptor;
    uint32_t file_length;
    uint32_t num_pages;

    void* pages[TABLE_MAX_PAGES]; // raw data of pages
    int32_t page_table_[PAGE_TABLE_SIZE]; // page tables containing frame_id
    int32_t page_id_table_[TABLE_MAX_PAGES]; // frame_id => page_id
    bool is_dirty_[TABLE_MAX_PAGES]; // whether page is dirty

    LRU_cache* replacer_;
    FreeList_t* freelist_;
} Pager;

/* ------------------------------------------------------------------------ */

LRU_cache* LRUCacheInit (int capacity) {
    LRU_cache* cache = malloc(sizeof(LRU_cache));
    cache->capacity = capacity;
    cache->size = 0;
    cache->hash_table = malloc(sizeof(ListNode_t*) * capacity);
    cache->head.prev = NULL;
    cache->tail.next = NULL;
    cache->head.next = &cache->tail;
    cache->tail.prev = &cache->head;
    for (int32_t i = 0; i < capacity; ++i) {
        cache->hash_table[i] = NULL;
    }
    return cache;
}

void put_node_to_last (LRU_cache* obj, ListNode_t* node) {
    ListNode_t* prev = obj->tail.prev;
    ListNode_t* next = &obj->tail;
    prev->next = node;
    node->prev = prev;
    node->next = next;
    next->prev = node;
}

void take_node_from_middle (ListNode_t* node) {
    node->prev->next = node->next;
    node->next->prev = node->prev;
}

void delete (LRU_cache* obj, const int32_t frame_id) {
    ListNode_t* node = obj->hash_table[frame_id];
    if (!node) {
        return;
    }
    else {
        take_node_from_middle(node);
        obj->size--;
        obj->hash_table[frame_id] = NULL;
    }
}

void LRUCachePut (LRU_cache* obj, int32_t frame_id) {
    ListNode_t* node = malloc(sizeof(ListNode_t));
    node->frame_id = frame_id;
    obj->hash_table[frame_id] = node;
    put_node_to_last(obj, node);
    obj->size++;
}

void LRUCacheFree (LRU_cache* obj) {
    ListNode_t* node = obj->head.next;
    while (node != &obj->tail) {
        ListNode_t* next = node->next;
        free(node);
        node = next;
    }

    free(obj->hash_table);
    free(obj);
}

/* return frame_id 返回page_table 的下标() */
bool Victim (LRU_cache* obj, int32_t* frame_id) {
    if (!obj->size) {
        return false;
    }
    
    ListNode_t* node = obj->head.next;
    *frame_id = node->frame_id;
    printf("Frame %d\n", *frame_id);
    delete(obj, node->frame_id);
    return true;
}

void Pin (LRU_cache* obj, int32_t frame_id) {
    // Pin physical page, id == page_id
    ListNode_t* temp = obj->hash_table[frame_id];
    if (temp != NULL) {
        delete(obj, frame_id);
    }
}

/* Add frame_id into Replacer, showing that it could be replaced */
void UnPin (LRU_cache* obj, int32_t frame_id) {
    ListNode_t* node = obj->hash_table[frame_id];
    if (!node) {
        while (obj->size >= obj->capacity) {
            ListNode_t* temp = obj->tail.prev;
            delete(obj, temp->frame_id);
        }
        LRUCachePut(obj, frame_id);
    }
}

/* ------------------------------------------------------------------------ */

FreeList_t* FreeListInit () {
    FreeList_t* freelist_ = malloc(sizeof(FreeList_t));
    freelist_->head.next = &freelist_->tail;
    freelist_->head.prev = NULL;

    freelist_->tail.prev = &freelist_->head;
    freelist_->tail.next = NULL;
    freelist_->size = 0;
}

bool is_empty (FreeList_t* freelist_) {
    return (freelist_->size == 0);
}

void push_back (FreeList_t* freelist_, ListNode_t* node) {
    ListNode_t* pre = freelist_->tail.prev;
    ListNode_t* suc = &freelist_->tail;
    pre->next = node;
    node->prev = pre;
    node->next = suc;
    suc->prev = node;
    ++freelist_->size;
}

void remove_node (ListNode_t* node) {
    ListNode_t* pre = node->prev;
    ListNode_t* suc = node->next;
    pre->next = suc;
    suc->prev = pre;
}

int32_t pop_front (FreeList_t* freelist_) {
    // always assume that freelist is not empty 
    ListNode_t* node = freelist_->head.next;
    remove_node(node);
    --freelist_->size;
    int32_t ans = node->frame_id;
    free(node);
    return ans;
}

/* ------------------------------------------------------------------------ */

// write back the page, only with complete pages
void pager_flush(Pager* pager, uint32_t page_num) {
  int32_t frame_id = pager->page_table_[page_num];
  if (pager->pages[frame_id] == NULL) {
     printf("Tried to flush null page\n");
     exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE,
     		 SEEK_SET);

  if (offset == -1) {
     printf("Error seeking: %d\n", errno);
     exit(EXIT_FAILURE);
  }

  ssize_t bytes_written = write(pager->file_descriptor, pager->pages[frame_id], PAGE_SIZE);
  if (bytes_written == -1) {
     printf("Error writing: %d\n", errno);
     exit(EXIT_FAILURE);
  }
}

bool is_all_pinned (Pager* pager) {
    return is_empty(pager->freelist_) && (pager->replacer_->size == 0);
}

// 返回可用页面的frame_id
int32_t find_replace (Pager* pager) {
    int32_t replace_frame_id = -1;
    if (!is_empty(pager->freelist_)) {
        return pop_front(pager->freelist_);
    }
    else if (pager->replacer_->size > 0) {
        Victim(pager->replacer_, &replace_frame_id);
        pager->page_table_[replace_frame_id] = -1;
        if (pager->is_dirty_[replace_frame_id]) {
            pager_flush(pager, pager->page_id_table_[replace_frame_id]);
        }
    }
    return replace_frame_id;
}

void* get_page (Pager* pager, uint32_t page_num) {
    int32_t frame_id = pager->page_table_[page_num];
    printf("Frame id of Page [%d] is %d\n", page_num, frame_id);
    if (frame_id != -1) {
        if (pager->pages[frame_id]) {
            return pager->pages[frame_id];
        }
        else {
            void* page = malloc(PAGE_SIZE);
            return pager->pages[frame_id] = page;            
        }
    }
    else {
        if (!is_all_pinned(pager)) {
            int32_t replace_frame_id = find_replace(pager);
            if (replace_frame_id == -1) {
                return NULL;
            }
            else {
                pager->page_table_[page_num] = replace_frame_id;
                pager->page_id_table_[replace_frame_id] = page_num;
                pager->is_dirty_[replace_frame_id] = false;

                return pager->pages[replace_frame_id];
            }
        }
        else {
            return NULL;
        }
    }
}

uint32_t get_unused_page_num (Pager* pager) {
    if (!is_empty(pager->freelist_)) {
        // get page_id from freelist
        int32_t frame_id =  pop_front(pager->freelist_);
        return pager->page_id_table_[frame_id];
    }
    else if (pager->replacer_->size > 0){
        int32_t replace_frame_id = find_replace(pager);
        return pager->page_id_table_[replace_frame_id];
    }
    else {
        uint32_t page_id = pager->num_pages;
        int32_t frame_id;
        for (int32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
            if (pager->page_id_table_[i] == -1) {
                frame_id = i;
                break;
            }
        }
        pager->is_dirty_[frame_id] = false;
        pager->page_id_table_[frame_id] = page_id;
        pager->page_table_[page_id] = frame_id;
        return page_id;
    }
}

void make_test () {
    LRU_cache* lru = LRUCacheInit(2);
    LRUCachePut(lru, 2);

    int32_t ans;
    Victim(lru, &ans);
    printf("Victim Slot is: [%d]\n", ans); // 2 here

    LRUCachePut(lru, 3);
    LRUCachePut(lru, 5);

    Victim(lru, &ans);
    printf("Victim Slot is: [%d]\n", ans); // 3 here

    UnPin(lru, 6);
    UnPin(lru, 7); 

    Victim(lru, &ans);
    printf("Victim Slot is: [%d]\n", ans); // 5 here

    LRUCacheFree(lru);
}

int main () {
    int fd = open("lrucache.db", O_RDWR | O_CREAT, // Read/Write mode, Create file if doen't exist
    S_IWUSR | S_IRUSR); // user write permission & read permission
    
    if (fd == -1) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);    

    int32_t num_pages = file_length / PAGE_SIZE;
    printf("File Length is %ld, Containg %d pages!\n", file_length, num_pages);
    
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
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
        pager->page_id_table_[i] = -1;
    }
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
        pager->is_dirty_[i] = false;
    }

    memset(pager->page_table_, -1, sizeof(pager->page_table_));
    pager->replacer_ = LRUCacheInit(TABLE_MAX_PAGES);
    pager->freelist_ = FreeListInit();

    // make_test();

    char info[64] = "dasdsdsds";
    uint32_t new_page_id = get_unused_page_num(pager);
    printf("New page id will be %d\n", new_page_id);
    void* new_page = get_page(pager, new_page_id);
    memcpy(new_page, info, PAGE_SIZE);


    for (uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
        if (pager->pages[i]) {
            uint32_t page_id = pager->page_id_table_[i];
            pager_flush(pager, page_id);
            free(pager->pages[i]);
        }
    }

    free(pager);

    return 0;
}