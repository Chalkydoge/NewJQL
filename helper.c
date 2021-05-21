#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

#include <stdbool.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

const uint32_t PAGE_SIZE = 4096;

int main () {
    int fd = open("myjql.db", O_RDWR | O_CREAT, // Read/Write mode, Create file if doen't exist
    S_IWUSR | S_IRUSR); // user write permission & read permission

    if (fd == -1) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);    

    int num_pages = file_length / PAGE_SIZE;
    printf("File Length is %ld, Containg %d pages!\n", file_length, num_pages);


    for (int i = 0; i < num_pages; ++i) {
        void* page = malloc(PAGE_SIZE);
        lseek(fd, i * PAGE_SIZE, SEEK_SET);
        ssize_t bytes_read = read(fd, page, PAGE_SIZE);

        uint8_t node_type = *((uint8_t*)(page));
        if (node_type == 0) {
            printf("Page [%d] Is an Internal Page", i);
            uint8_t is_root = *((uint8_t*)(page + 1));
            if (is_root == 1) {
                printf(", Is root\n");
            }
            else {
                printf("\n");
            }

            uint32_t parent_id = *((uint32_t*)(page + 2));
            printf("- Parent ID is [%d]\n", parent_id);

            uint32_t num_keys = *((uint32_t*)(page + 6));
            printf("- Having %d Keys\n", num_keys);

            uint32_t rightmost_child_id = *((uint32_t*)(page + 10));
            printf("-- Rightmost Child is: %d\n", rightmost_child_id);

            void* cell_start = (void*)page + 14;
            for (int i = 0; i < num_keys; ++i) {
                printf("--- Child id [%d]", *((uint32_t*)(cell_start + i * 16)) );
                printf("--- Key [%d]: %s", i, ((char*)(cell_start + i * 16 + 4)) );
                printf("\n");
            }
        }
        else {
            printf("Page %d, Is a leaf page\n", i);

            uint32_t parent_id = *((uint32_t*)(page + 2));
            printf("- Parent ID is [%d]\n", parent_id);

            uint32_t num_cells = *((uint32_t*)(page + 6));
            printf("- Having %d Cells\n", num_cells);
            
            uint32_t next_leaf_id = *((uint32_t*)(page + 10));
            printf("- Next Leaf's Page Id is: [%d]\n", next_leaf_id);
            for (int32_t i = 0; i < num_cells; ++i) {
                void* start = (void*)(page + 14 + i * 16);
                printf("Key [%s]\t", (char*)(start));
                printf("Value [%d]\n", *((int*)(start + 12)));
            }
        }

        printf("<----------------->\n");
        free(page);
    }
    return 0;
}