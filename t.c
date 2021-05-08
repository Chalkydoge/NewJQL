#include <stdio.h>
#include <stdbool.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

int main () {
    char* s = "Hello";
    printf("Address: %p, Length: %ld, String: %s\n", s, sizeof(s), s);

    char t[12] = "a";
    printf("Address: %p, Length: %ld, String: %s\n", t, sizeof(t), t);
    return 0;
}
// ./myjql.o myjql.db