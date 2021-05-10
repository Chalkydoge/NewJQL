#include <stdio.h>
#include <stdbool.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>

int main () {
    char* s = "Hello";
    printf("Address: %p, Length: %ld, String: %s\n", s, sizeof(s), s);

    char t[12] = "a";
    printf("Address: %p, Length: %ld, String: %s\n", t, sizeof(t), t);

    char w[12] = "Mello";
    printf("Comparison between s and w: %d\n", strcmp(s, w));
    

    void* page = malloc(40);
    memcpy(page, s, 12);

    char ptr[12];
    memcpy(ptr, page, 12);
    
    printf("%s\n", ptr);
    return 0;
}
// ./myjql.o myjql.db